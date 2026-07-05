//! Port of `org.gautelis.durga.plugins.JsonFlatten` (plugin id `json-flatten`).
//!
//! `direction=flatten` collapses nested objects to `separator`-joined keys;
//! `direction=unflatten` nests `separator`-joined keys back into objects.
//! Arrays are kept as leaf values.

use serde_json::{Map, Value};

use crate::pipeline::set_field_at;
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct JsonFlatten;

impl JsonFlatten {
    pub fn new() -> Self {
        JsonFlatten
    }

    pub fn flatten(json: &str, separator: &str, max_depth: usize) -> Result<String, PluginError> {
        let input: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        let Value::Object(obj) = &input else {
            return Ok(json.to_string());
        };
        let mut output = Map::new();
        flatten_node(&mut output, "", obj, separator, 0, max_depth);
        Ok(Value::Object(output).to_string())
    }

    pub fn unflatten(json: &str, separator: &str) -> Result<String, PluginError> {
        let input: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        let Value::Object(obj) = &input else {
            return Ok(json.to_string());
        };
        let mut output = Value::Object(Map::new());
        for (key, value) in obj {
            set_field_at_sep(&mut output, key, separator, value.clone());
        }
        Ok(output.to_string())
    }
}

fn flatten_node(
    output: &mut Map<String, Value>,
    prefix: &str,
    node: &Map<String, Value>,
    separator: &str,
    depth: usize,
    max_depth: usize,
) {
    for (k, value) in node {
        let key = if prefix.is_empty() { k.clone() } else { format!("{prefix}{separator}{k}") };
        match value {
            Value::Object(child) if !child.is_empty() && depth < max_depth => {
                flatten_node(output, &key, child, separator, depth + 1, max_depth);
            }
            _ => {
                output.insert(key, value.clone());
            }
        }
    }
}

/// Sets a value at a separator-joined path. When the separator is `.` this is
/// equivalent to [`set_field_at`]; otherwise the path is split on the custom
/// separator.
fn set_field_at_sep(root: &mut Value, path: &str, separator: &str, value: Value) {
    if separator == "." {
        set_field_at(root, path, value);
        return;
    }
    let segments: Vec<&str> = path.split(separator).collect();
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        let map = current.as_object_mut().expect("ensured object");
        let child = map.entry((*segment).to_string()).or_insert(Value::Null);
        if !child.is_object() {
            *child = Value::Object(Map::new());
        }
        current = child;
    }
    if let Value::Object(map) = current {
        map.insert(segments[segments.len() - 1].to_string(), value);
    }
}

impl Plugin for JsonFlatten {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let mut direction = "flatten".to_string();
        let mut separator = ".".to_string();
        let mut max_depth = usize::MAX;
        for part in config.split(';') {
            let part = part.trim();
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "direction" => direction = val.to_string(),
                "separator" => separator = val.to_string(),
                "maxDepth" => {
                    if let Ok(n) = val.parse::<usize>() {
                        max_depth = n;
                    }
                }
                _ => {}
            }
        }
        let out = if direction.eq_ignore_ascii_case("unflatten") {
            Self::unflatten(payload, &separator)?
        } else {
            Self::flatten(payload, &separator, max_depth)?
        };
        Ok(Some(out))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flattens_nested() {
        let out = JsonFlatten::new()
            .execute_str(r#"{"a":{"b":{"c":1}},"d":2}"#, "direction=flatten;separator=.")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["a.b.c"], 1);
        assert_eq!(v["d"], 2);
    }

    #[test]
    fn unflattens_dot_keys() {
        let out = JsonFlatten::new()
            .execute_str(r#"{"a.b.c":1,"d":2}"#, "direction=unflatten;separator=.")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["a"]["b"]["c"], 1);
        assert_eq!(v["d"], 2);
    }
}
