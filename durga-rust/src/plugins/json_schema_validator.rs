//! Port of `org.gautelis.durga.plugins.JsonSchemaValidator` (plugin id
//! `json-schema-validator`).
//!
//! Two config forms:
//! * compact `required=a,b,c` (does not start with `{`) — presence check,
//! * a JSON-Schema-subset object — type / required / properties / enum /
//!   minimum / maximum / minLength / maxLength / pattern / items.
//!
//! An optional `onInvalid` directive selects how invalid payloads are handled:
//! `dlq` (default) routes them to the dead-letter channel, `skip` drops them
//! silently, and `fail` fails the process. It is appended alongside the schema,
//! e.g. `schema={"type":"object"};onInvalid=skip` or, in compact form,
//! `required=order_id,amount;onInvalid=fail`.
//!
//! On success the payload is returned unchanged; on failure the plugin returns
//! a [`PluginResult`] carrying the selected error strategy (the generated worker
//! turns that into the corresponding lifecycle event).

use regex::Regex;
use serde_json::Value;

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};
use crate::result::PluginResult;

#[derive(Default)]
pub struct JsonSchemaValidator;

/// How the plugin handles a payload that fails validation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OnInvalid {
    Dlq,
    Skip,
    Fail,
}

impl JsonSchemaValidator {
    pub fn new() -> Self {
        JsonSchemaValidator
    }

    pub fn validate(payload: &str, config: &str) -> Result<String, PluginError> {
        let (schema_config, _) = parse_config(config);
        match validate_payload(payload, &schema_config)? {
            Some(err) => Err(err.into()),
            None => Ok(payload.to_string()),
        }
    }
}

/// Separates an optional `onInvalid` directive from the schema configuration.
/// Segments are split on top-level `;` only, so a `;` inside the JSON schema
/// (within braces or string literals) does not act as a separator.
fn parse_config(config: &str) -> (String, OnInvalid) {
    let mut on_invalid = OnInvalid::Dlq;
    let mut schema_config = String::new();
    for segment in split_top_level(config) {
        let trimmed = segment.trim();
        if trimmed.is_empty() {
            continue;
        }
        let lower = trimmed.to_ascii_lowercase();
        if let Some(rest) = lower.strip_prefix("oninvalid=") {
            on_invalid = match rest.trim() {
                "skip" => OnInvalid::Skip,
                "fail" => OnInvalid::Fail,
                _ => OnInvalid::Dlq,
            };
        } else if lower.starts_with("schema=") {
            append_schema(&mut schema_config, &trimmed["schema=".len()..]);
        } else {
            append_schema(&mut schema_config, trimmed);
        }
    }
    (schema_config, on_invalid)
}

fn append_schema(schema_config: &mut String, segment: &str) {
    let value = segment.trim();
    if value.is_empty() {
        return;
    }
    if !schema_config.is_empty() {
        schema_config.push(';');
    }
    schema_config.push_str(value);
}

fn split_top_level(config: &str) -> Vec<String> {
    let mut parts = Vec::new();
    let mut current = String::new();
    let mut depth = 0i32;
    let mut in_string = false;
    let mut escaped = false;
    for c in config.chars() {
        if in_string {
            current.push(c);
            if escaped {
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                in_string = false;
            }
            continue;
        }
        match c {
            '"' => {
                in_string = true;
                current.push(c);
            }
            '{' | '[' => {
                depth += 1;
                current.push(c);
            }
            '}' | ']' => {
                if depth > 0 {
                    depth -= 1;
                }
                current.push(c);
            }
            ';' if depth == 0 => {
                parts.push(std::mem::take(&mut current));
            }
            _ => current.push(c),
        }
    }
    parts.push(current);
    parts
}

fn validate_payload(payload: &str, schema_config: &str) -> Result<Option<String>, PluginError> {
    let input: Value = serde_json::from_str(payload)
        .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;

    let trimmed = schema_config.trim();
    if !trimmed.is_empty() && !trimmed.starts_with('{') {
        return Ok(validate_compact(&input, schema_config));
    }

    let schema: Value = serde_json::from_str(schema_config)
        .map_err(|e| -> PluginError { format!("Invalid schema config: {e}").into() })?;
    Ok(validate_node(&input, &schema, "$"))
}

fn validate_compact(input: &Value, config: &str) -> Option<String> {
    for part in config.split(';') {
        let Some(eq) = part.find('=') else { continue };
        if eq == 0 {
            continue;
        }
        let key = part[..eq].trim();
        let value = part[eq + 1..].trim();
        if key == "required" {
            for field in value.split(',') {
                let path = field.trim();
                if !path.is_empty() && field_at(input, path).is_none() {
                    return Some(format!("$: missing required field '{path}'"));
                }
            }
        }
    }
    None
}

fn validate_node(node: &Value, schema: &Value, path: &str) -> Option<String> {
    let ty = schema.get("type").and_then(|t| t.as_str());
    if let Some(ty) = ty {
        if let Some(err) = check_type(node, ty, path) {
            return Some(err);
        }
    }

    if let Some(allowed) = schema.get("enum") {
        if let Some(arr) = allowed.as_array() {
            if !arr.iter().any(|v| v == node) {
                return Some(format!("{path}: value not in allowed enum"));
            }
        }
    }

    if ty == Some("object") && node.is_object() {
        if let Some(req) = schema.get("required").and_then(|r| r.as_array()) {
            for field in req {
                if let Some(name) = field.as_str() {
                    if node.get(name).is_none() {
                        return Some(format!("{path}: missing required field '{name}'"));
                    }
                }
            }
        }
        if let Some(props) = schema.get("properties").and_then(|p| p.as_object()) {
            for (field, child_schema) in props {
                if let Some(child) = node.get(field) {
                    let child_path = format!("{path}.{field}");
                    if let Some(err) = validate_node(child, child_schema, &child_path) {
                        return Some(err);
                    }
                }
            }
        }
    }

    if node.is_number() {
        if let Some(min) = schema.get("minimum").and_then(|m| m.as_f64()) {
            if node.as_f64().unwrap_or(f64::NAN) < min {
                return Some(format!("{path}: numeric value below minimum {min}"));
            }
        }
        if let Some(max) = schema.get("maximum").and_then(|m| m.as_f64()) {
            if node.as_f64().unwrap_or(f64::NAN) > max {
                return Some(format!("{path}: numeric value above maximum {max}"));
            }
        }
    }

    if let Some(text) = node.as_str() {
        if let Some(min) = schema.get("minLength").and_then(|m| m.as_u64()) {
            if (text.chars().count() as u64) < min {
                return Some(format!("{path}: string length {} below minLength {min}", text.chars().count()));
            }
        }
        if let Some(max) = schema.get("maxLength").and_then(|m| m.as_u64()) {
            if (text.chars().count() as u64) > max {
                return Some(format!("{path}: string length {} above maxLength {max}", text.chars().count()));
            }
        }
        if let Some(pattern) = schema.get("pattern").and_then(|p| p.as_str()) {
            match Regex::new(pattern) {
                Ok(re) => {
                    if !re.is_match(text) {
                        return Some(format!("{path}: string does not match configured pattern"));
                    }
                }
                Err(_) => return Some(format!("{path}: invalid or unsafe regex pattern")),
            }
        }
    }

    if let (Some(arr), Some(items_schema)) = (node.as_array(), schema.get("items")) {
        for (i, item) in arr.iter().enumerate() {
            let item_path = format!("{path}[{i}]");
            if let Some(err) = validate_node(item, items_schema, &item_path) {
                return Some(err);
            }
        }
    }

    None
}

fn check_type(node: &Value, expected: &str, path: &str) -> Option<String> {
    let ok = match expected {
        "null" => node.is_null(),
        "string" => node.is_string(),
        "number" => node.is_number(),
        "integer" => {
            node.is_i64()
                || node.is_u64()
                || node.as_f64().map(|f| f.fract() == 0.0).unwrap_or(false)
        }
        "boolean" => node.is_boolean(),
        "object" => node.is_object(),
        "array" => node.is_array(),
        _ => true,
    };
    if ok {
        None
    } else {
        Some(format!("{path}: expected {expected} but got a different type"))
    }
}

impl Plugin for JsonSchemaValidator {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        Ok(Some(Self::validate(payload, config)?))
    }

    fn execute_with_result(&self, payload: &[u8], config: &str) -> Result<PluginResult, PluginError> {
        let text = std::str::from_utf8(payload)?;
        let (schema_config, on_invalid) = parse_config(config);
        let idempotency_key = self.idempotency_key(payload, config);
        match validate_payload(text, &schema_config)? {
            None => Ok(PluginResult::success(Some(payload.to_vec()), idempotency_key)),
            Some(err) => Ok(match on_invalid {
                OnInvalid::Skip => PluginResult::skip(idempotency_key, err),
                OnInvalid::Fail => PluginResult::fail(idempotency_key, err),
                OnInvalid::Dlq => PluginResult::dlq(Some(payload.to_vec()), idempotency_key, err),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::ErrorStrategy;

    #[test]
    fn compact_required_passes_when_present() {
        let out = JsonSchemaValidator::validate(
            r#"{"order_id":1,"amount":9.5,"customer_email":"a@x.com"}"#,
            "required=order_id,amount,customer_email",
        );
        assert!(out.is_ok());
    }

    #[test]
    fn compact_required_reports_first_missing() {
        let err = JsonSchemaValidator::validate(
            r#"{"_value":"default"}"#,
            "required=order_id,amount,customer_email",
        )
        .unwrap_err();
        assert_eq!(err.to_string(), "$: missing required field 'order_id'");
    }

    #[test]
    fn full_schema_type_and_required() {
        let schema = r#"{"type":"object","required":["id"],"properties":{"id":{"type":"integer"}}}"#;
        assert!(JsonSchemaValidator::validate(r#"{"id":5}"#, schema).is_ok());
        assert!(JsonSchemaValidator::validate(r#"{"id":"x"}"#, schema).is_err());
        assert!(JsonSchemaValidator::validate(r#"{}"#, schema).is_err());
    }

    #[test]
    fn invalid_routes_to_dlq_by_default() {
        let schema = r#"{"type":"object","required":["name"]}"#;
        let plugin = JsonSchemaValidator::new();
        let result = plugin
            .execute_with_result(br#"{"email":"a@b.com"}"#, schema)
            .unwrap();
        assert_eq!(result.error_strategy(), Some(ErrorStrategy::Dlq));
    }

    #[test]
    fn on_invalid_skip_and_fail() {
        let plugin = JsonSchemaValidator::new();
        let skip = plugin
            .execute_with_result(
                br#"{"email":"a@b.com"}"#,
                r#"schema={"type":"object","required":["name"]};onInvalid=skip"#,
            )
            .unwrap();
        assert_eq!(skip.error_strategy(), Some(ErrorStrategy::Skip));

        let fail = plugin
            .execute_with_result(
                br#"{"email":"a@b.com"}"#,
                r#"schema={"type":"object","required":["name"]};onInvalid=fail"#,
            )
            .unwrap();
        assert_eq!(fail.error_strategy(), Some(ErrorStrategy::Fail));
    }

    #[test]
    fn on_invalid_with_compact_config() {
        let plugin = JsonSchemaValidator::new();
        let invalid = plugin
            .execute_with_result(br#"{"order_id":7}"#, "required=order_id,amount;onInvalid=skip")
            .unwrap();
        assert_eq!(invalid.error_strategy(), Some(ErrorStrategy::Skip));

        let valid = plugin
            .execute_with_result(
                br#"{"order_id":7,"amount":12.5}"#,
                "required=order_id,amount;onInvalid=skip",
            )
            .unwrap();
        assert!(valid.is_success());
    }

    #[test]
    fn valid_payload_succeeds_with_on_invalid_present() {
        let plugin = JsonSchemaValidator::new();
        let result = plugin
            .execute_with_result(
                br#"{"name":"Alice"}"#,
                r#"schema={"type":"object","required":["name"]};onInvalid=fail"#,
            )
            .unwrap();
        assert!(result.is_success());
    }
}
