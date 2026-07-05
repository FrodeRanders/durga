//! Port of `org.gautelis.durga.plugins.UuidInject` (plugin id `uuid-inject`).
//!
//! Injects generated UUIDs into configured fields (dot-notation). Config:
//! `fields=id,trace_id;strategy=uuid7`. Strategies: `uuid7` (default) / `random`
//! → v7, `uuid4` → v4. `uuid1` is approximated with a v7 value.

use serde_json::{Map, Value};
use uuid::Uuid;

use crate::pipeline::set_field_at;
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct UuidInject;

impl UuidInject {
    pub fn new() -> Self {
        UuidInject
    }

    pub fn inject(json: &str, fields_list: &str, strategy: &str) -> Result<String, PluginError> {
        let fields: Vec<&str> = fields_list
            .split(',')
            .map(|f| f.trim())
            .filter(|f| !f.is_empty())
            .collect();
        if fields.is_empty() {
            return Ok(json.to_string());
        }

        let input: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;

        let mut output = if input.is_object() {
            input
        } else {
            let mut m = Map::new();
            m.insert("_value".to_string(), input);
            Value::Object(m)
        };

        for field in fields {
            set_field_at(&mut output, field, Value::String(generate(strategy)));
        }
        Ok(output.to_string())
    }
}

fn generate(strategy: &str) -> String {
    match strategy.to_ascii_lowercase().as_str() {
        "uuid4" => Uuid::new_v4().to_string(),
        // uuid7 / random / uuid1 (approximated) all use a time-ordered v7.
        _ => Uuid::now_v7().to_string(),
    }
}

impl Plugin for UuidInject {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let mut fields = "id".to_string();
        let mut strategy = "uuid7".to_string();
        for part in config.split(';') {
            let part = part.trim();
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "fields" => fields = val.to_string(),
                "strategy" => strategy = val.to_string(),
                _ => {}
            }
        }
        Ok(Some(Self::inject(payload, &fields, &strategy)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn injects_uuid_into_field() {
        let out = UuidInject::new()
            .execute_str(r#"{"amount":1}"#, "fields=id;strategy=uuid4")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        let id = v["id"].as_str().unwrap();
        assert!(Uuid::parse_str(id).is_ok());
        assert_eq!(v["amount"], 1);
    }

    #[test]
    fn wraps_non_object_payload() {
        let out = UuidInject::new()
            .execute_str(r#"42"#, "fields=trace_id")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["_value"], 42);
        assert!(v["trace_id"].is_string());
    }
}
