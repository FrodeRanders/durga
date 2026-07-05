//! Port of `org.gautelis.durga.plugins.JsonSchemaValidator` (plugin id
//! `json-schema-validator`).
//!
//! Two config forms:
//! * compact `required=a,b,c` (does not start with `{`) — presence check,
//! * a JSON-Schema-subset object — type / required / properties / enum /
//!   minimum / maximum / minLength / maxLength / pattern / items.
//!
//! On success the payload is returned unchanged; on failure the plugin errors
//! (the generated worker turns that into an `ACTIVITY_ESCALATED` /
//! `VALIDATION_FAILED` lifecycle event).

use regex::Regex;
use serde_json::Value;

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct JsonSchemaValidator;

impl JsonSchemaValidator {
    pub fn new() -> Self {
        JsonSchemaValidator
    }

    pub fn validate(payload: &str, config: &str) -> Result<String, PluginError> {
        let input: Value = serde_json::from_str(payload)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;

        let trimmed = config.trim();
        if !config.is_empty() && !trimmed.is_empty() && !trimmed.starts_with('{') {
            if let Some(err) = validate_compact(&input, config) {
                return Err(err.into());
            }
            return Ok(payload.to_string());
        }

        let schema: Value = serde_json::from_str(config)
            .map_err(|e| -> PluginError { format!("Invalid schema config: {e}").into() })?;
        if let Some(err) = validate_node(&input, &schema, "$") {
            return Err(err.into());
        }
        Ok(payload.to_string())
    }
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
