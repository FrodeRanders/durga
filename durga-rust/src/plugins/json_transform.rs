//! Port of `org.gautelis.durga.plugins.JsonTransform`.
//!
//! Remaps fields from input to output using a comma-separated expression:
//! * `"field1, nested.field2"` copies those fields at the same paths;
//! * `"src:dest"` copies a source field to a destination path;
//! * `"key:value"` (source absent) sets a literal value (number/bool/null
//!   auto-detected).

use serde_json::Value;

use crate::pipeline::{field_at, set_field_at};
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct JsonTransform;

impl JsonTransform {
    pub fn new() -> Self {
        JsonTransform
    }

    /// Transforms `json` using the mapping `expression`.
    pub fn transform(json: &str, expression: &str) -> Result<String, PluginError> {
        let input: Value = serde_json::from_str(json).map_err(|e| -> PluginError {
            format!("Invalid input JSON: {e}").into()
        })?;

        let trimmed = expression.trim();
        if trimmed.is_empty() || trimmed == "." {
            return Ok(json.to_string());
        }

        let mut output = Value::Object(serde_json::Map::new());
        for raw in expression.split(',') {
            let mapping = raw.trim();
            if mapping.is_empty() {
                continue;
            }
            match mapping.find(':') {
                Some(idx) => {
                    let left = mapping[..idx].trim();
                    let right = mapping[idx + 1..].trim();
                    if let Some(source) = field_at(&input, left) {
                        set_field_at(&mut output, right, source.clone());
                    } else {
                        set_field_at(&mut output, left, parse_literal(right));
                    }
                }
                None => {
                    if let Some(source) = field_at(&input, mapping) {
                        set_field_at(&mut output, mapping, source.clone());
                    }
                }
            }
        }
        Ok(output.to_string())
    }
}

fn parse_literal(value: &str) -> Value {
    match value {
        "true" => Value::Bool(true),
        "false" => Value::Bool(false),
        "null" => Value::Null,
        _ => {
            if let Ok(i) = value.parse::<i64>() {
                Value::from(i)
            } else if let Ok(f) = value.parse::<f64>() {
                Value::from(f)
            } else {
                Value::String(value.to_string())
            }
        }
    }
}

impl Plugin for JsonTransform {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        Ok(Some(Self::transform(payload, config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn copies_and_renames_fields() {
        let out = JsonTransform::transform(
            r#"{"id":7,"customer":{"name":"Alice","email":"a@x.com"},"total":9.5,"status":"pending"}"#,
            "id:order_id, customer.name:customer_name, customer.email:customer_email, total:amount, status",
        )
        .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["order_id"], 7);
        assert_eq!(v["customer_name"], "Alice");
        assert_eq!(v["customer_email"], "a@x.com");
        assert_eq!(v["amount"], 9.5);
        assert_eq!(v["status"], "pending");
    }

    #[test]
    fn sets_literal_when_source_absent() {
        let out = JsonTransform::transform(r#"{"a":1}"#, "missing:42, flag:true").unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["missing"], 42);
        assert_eq!(v["flag"], true);
    }

    #[test]
    fn identity_expression_passes_through() {
        let json = r#"{"a":1}"#;
        assert_eq!(JsonTransform::transform(json, ".").unwrap(), json);
    }
}
