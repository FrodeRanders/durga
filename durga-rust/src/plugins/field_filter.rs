//! Port of `org.gautelis.durga.plugins.FieldFilter` (plugin id `field-filter`).
//!
//! Retains (`keep`) or removes (`drop`) top-level fields, and optionally hoists
//! (`flatten`) a nested object's fields to the top level. `keep` wins over
//! `drop` on conflict.

use std::collections::HashSet;

use serde_json::{Map, Value};

use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct FieldFilter;

impl FieldFilter {
    pub fn new() -> Self {
        FieldFilter
    }

    pub fn filter(json: &str, keep: Option<&str>, drop: Option<&str>, flatten_prefix: Option<&str>)
        -> Result<String, PluginError>
    {
        let input: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        let Value::Object(obj) = &input else {
            return Ok(json.to_string());
        };

        let keep_set = split_set(keep);
        let drop_set = split_set(drop);

        let mut output = Map::new();
        for (field, value) in obj {
            let should_keep = if let Some(keep) = &keep_set {
                !keep.is_empty() && keep.contains(field.as_str())
            } else {
                drop_set.as_ref().map(|d| !d.contains(field.as_str())).unwrap_or(true)
            };
            if should_keep {
                output.insert(field.clone(), value.clone());
            }
        }

        if let Some(prefix) = flatten_prefix.filter(|p| !p.is_empty()) {
            if let Some(Value::Object(nested)) = obj.get(prefix) {
                for (k, v) in nested {
                    output.insert(k.clone(), v.clone());
                }
            }
        }

        Ok(Value::Object(output).to_string())
    }
}

fn split_set(spec: Option<&str>) -> Option<HashSet<String>> {
    spec.filter(|s| !s.trim().is_empty()).map(|s| {
        s.split(',')
            .map(|f| f.trim().to_string())
            .filter(|f| !f.is_empty())
            .collect()
    })
}

impl Plugin for FieldFilter {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let mut keep = None;
        let mut drop = None;
        let mut flatten = None;
        for part in config.split_whitespace() {
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "keep" => keep = Some(val.to_string()),
                "drop" => drop = Some(val.to_string()),
                "flatten" => flatten = Some(val.to_string()),
                _ => {}
            }
        }
        Ok(Some(Self::filter(payload, keep.as_deref(), drop.as_deref(), flatten.as_deref())?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keep_retains_only_listed() {
        let out = FieldFilter::new()
            .execute_str(r#"{"a":1,"b":2,"c":3}"#, "keep=a,c")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("a").is_some() && v.get("c").is_some() && v.get("b").is_none());
    }

    #[test]
    fn drop_removes_listed() {
        let out = FieldFilter::new()
            .execute_str(r#"{"a":1,"b":2}"#, "drop=b")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("a").is_some() && v.get("b").is_none());
    }
}
