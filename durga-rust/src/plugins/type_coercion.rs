//! Port of `org.gautelis.durga.plugins.TypeCoercion`.
//!
//! Copies all input fields, then coerces named fields to a target type using a
//! comma-separated `field:type` config. Types: `string`, `int`, `long`,
//! `double`, `decimal`, `boolean`.

use serde_json::Value;

use crate::pipeline::{field_at, set_field_at};
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct TypeCoercion;

impl TypeCoercion {
    pub fn new() -> Self {
        TypeCoercion
    }

    pub fn coerce(json: &str, expression: &str) -> Result<String, PluginError> {
        let input: Value = serde_json::from_str(json).map_err(|e| -> PluginError {
            format!("Invalid input JSON: {e}").into()
        })?;

        if expression.trim().is_empty() || !input.is_object() {
            return Ok(json.to_string());
        }

        let mut output = input.clone();
        for raw in expression.split(',') {
            let mapping = raw.trim();
            if mapping.is_empty() {
                continue;
            }
            let Some(idx) = mapping.find(':') else {
                continue;
            };
            let field = mapping[..idx].trim();
            let ty = mapping[idx + 1..].trim().to_ascii_lowercase();
            let Some(node) = field_at(&output, field) else {
                continue;
            };
            if node.is_null() {
                continue;
            }
            if let Some(coerced) = coerce_value(node, &ty) {
                set_field_at(&mut output, field, coerced);
            }
        }
        Ok(output.to_string())
    }
}

fn as_text(node: &Value) -> String {
    match node {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

fn coerce_value(node: &Value, ty: &str) -> Option<Value> {
    match ty {
        "string" => Some(Value::String(as_text(node))),
        "int" | "long" => {
            if let Some(i) = node.as_i64() {
                Some(Value::from(i))
            } else if let Some(f) = node.as_f64() {
                Some(Value::from(f as i64))
            } else {
                as_text(node).trim().parse::<i64>().ok().map(Value::from)
            }
        }
        "double" | "decimal" => {
            if let Some(f) = node.as_f64() {
                Some(Value::from(f))
            } else {
                as_text(node).trim().parse::<f64>().ok().map(Value::from)
            }
        }
        "boolean" => {
            let text = as_text(node).trim().to_ascii_lowercase();
            match text.as_str() {
                "true" | "1" | "yes" => Some(Value::Bool(true)),
                "false" | "0" | "no" => Some(Value::Bool(false)),
                _ => node.as_bool().map(Value::Bool),
            }
        }
        _ => None,
    }
}

impl Plugin for TypeCoercion {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        Ok(Some(Self::coerce(payload, config)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coerces_declared_fields_and_keeps_others() {
        let out = TypeCoercion::coerce(
            r#"{"order_id":"7","amount":"936.65","status":"pending"}"#,
            "amount:double, order_id:int",
        )
        .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["order_id"], 7);
        assert_eq!(v["amount"], 936.65);
        assert_eq!(v["status"], "pending");
    }

    #[test]
    fn leaves_unparseable_values_alone() {
        let out = TypeCoercion::coerce(r#"{"n":"abc"}"#, "n:int").unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["n"], "abc");
    }
}
