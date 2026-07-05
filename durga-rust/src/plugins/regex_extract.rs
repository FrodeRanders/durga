//! Port of `org.gautelis.durga.plugins.RegexExtract` (plugin id
//! `regex-extract`).
//!
//! Extracts named capture groups from a source field into the payload. Config:
//! `source=log_line;pattern=(?<ip>\d+\.\d+\.\d+\.\d+)\s+(?<method>\w+);target=parsed;all=false`.
//! With no named groups, positional groups are stored as `group1`, `group2`, ...

use regex::Regex;
use serde_json::{Map, Value};

use crate::pipeline::{field_at, set_field_at};
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct RegexExtract;

impl RegexExtract {
    pub fn new() -> Self {
        RegexExtract
    }

    pub fn extract(
        json: &str,
        source_field: Option<&str>,
        regex: Option<&str>,
        target_path: Option<&str>,
        find_all: bool,
    ) -> Result<String, PluginError> {
        let (Some(source_field), Some(regex)) = (source_field, regex) else {
            return Ok(json.to_string());
        };
        let mut input: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        if !input.is_object() {
            return Ok(json.to_string());
        }
        let text = match field_at(&input, source_field) {
            Some(Value::String(s)) => s.clone(),
            _ => return Ok(json.to_string()),
        };
        let re = Regex::new(regex)
            .map_err(|e| -> PluginError { format!("Invalid regex pattern: {e}").into() })?;
        let group_names: Vec<String> =
            re.capture_names().flatten().map(|s| s.to_string()).collect();

        let mut matched = false;
        if find_all {
            let all: Vec<_> = re.captures_iter(&text).collect();
            for caps in &all {
                matched = true;
                apply_groups(&mut input, caps, &group_names, target_path);
            }
        } else if let Some(caps) = re.captures(&text) {
            matched = true;
            apply_groups(&mut input, &caps, &group_names, target_path);
        }
        if !matched {
            return Ok(json.to_string());
        }
        Ok(input.to_string())
    }
}

fn apply_groups(
    root: &mut Value,
    caps: &regex::Captures,
    group_names: &[String],
    target_path: Option<&str>,
) {
    // Collect the entries to write, then place them under the target path.
    let mut entries: Vec<(String, String)> = Vec::new();
    if group_names.is_empty() {
        for i in 1..caps.len() {
            if let Some(m) = caps.get(i) {
                entries.push((format!("group{i}"), m.as_str().to_string()));
            }
        }
    } else {
        for name in group_names {
            if let Some(m) = caps.name(name) {
                entries.push((name.clone(), m.as_str().to_string()));
            }
        }
    }

    match target_path.filter(|p| !p.is_empty()) {
        Some(path) => {
            let mut obj = match field_at(root, path) {
                Some(Value::Object(existing)) => existing.clone(),
                _ => Map::new(),
            };
            for (k, v) in entries {
                obj.insert(k, Value::String(v));
            }
            set_field_at(root, path, Value::Object(obj));
        }
        None => {
            if let Value::Object(map) = root {
                for (k, v) in entries {
                    map.insert(k, Value::String(v));
                }
            }
        }
    }
}

impl Plugin for RegexExtract {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let mut source = None;
        let mut pattern = None;
        let mut target = None;
        let mut all = false;
        for part in config.split(';') {
            let part = part.trim();
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "source" => source = Some(val.to_string()),
                "pattern" => pattern = Some(val.to_string()),
                "target" => target = Some(val.to_string()),
                "all" => all = val.eq_ignore_ascii_case("true"),
                _ => {}
            }
        }
        Ok(Some(Self::extract(
            payload,
            source.as_deref(),
            pattern.as_deref(),
            target.as_deref(),
            all,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_named_groups_to_target() {
        let out = RegexExtract::new()
            .execute_str(
                r#"{"line":"192.168.0.1 GET"}"#,
                r"source=line;pattern=(?<ip>\d+\.\d+\.\d+\.\d+)\s+(?<method>\w+);target=parsed",
            )
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["parsed"]["ip"], "192.168.0.1");
        assert_eq!(v["parsed"]["method"], "GET");
    }

    #[test]
    fn positional_groups_when_unnamed() {
        let out = RegexExtract::new()
            .execute_str(r#"{"s":"ab12"}"#, r"source=s;pattern=([a-z]+)(\d+)")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["group1"], "ab");
        assert_eq!(v["group2"], "12");
    }
}
