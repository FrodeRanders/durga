//! Port of `org.gautelis.durga.plugins.TimestampNormalize` (plugin id
//! `timestamp-normalize`).
//!
//! Converts timestamp fields between formats. Config:
//! `fields=created_at;from=epoch_ms;to=ISO8601;zone=UTC;removeOnError=false`.
//! Supported formats: `epoch_s`, `epoch_ms`, `ISO8601`, `RFC3339`. Unlike the
//! Java plugin, arbitrary custom Java `DateTimeFormatter` patterns are not
//! supported; an unrecognised `from`/`to` falls back to epoch-millis / RFC3339.

use chrono::{DateTime, FixedOffset, SecondsFormat, TimeZone, Utc};
use serde_json::Value;

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct TimestampNormalize;

impl TimestampNormalize {
    pub fn new() -> Self {
        TimestampNormalize
    }

    pub fn normalize(
        json: &str,
        fields_list: Option<&str>,
        from_format: &str,
        to_format: &str,
        zone: &str,
        remove_on_error: bool,
    ) -> Result<String, PluginError> {
        let Some(fields_list) = fields_list.filter(|f| !f.trim().is_empty()) else {
            return Ok(json.to_string());
        };
        let fields: Vec<String> = fields_list
            .split(',')
            .map(|f| f.trim().to_string())
            .filter(|f| !f.is_empty())
            .collect();
        if fields.is_empty() {
            return Ok(json.to_string());
        }

        let mut output: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        if !output.is_object() {
            return Ok(json.to_string());
        }
        let offset = parse_zone(zone);

        for field in &fields {
            let node = match field_at(&output, field) {
                Some(v) if v.is_string() || v.is_number() => v.clone(),
                _ => continue,
            };
            match parse_timestamp(&node, from_format).map(|dt| format_timestamp(dt, to_format, offset)) {
                Ok(normalized) => set_leaf(&mut output, field, Value::String(normalized)),
                Err(_) => {
                    if remove_on_error {
                        remove_leaf(&mut output, field);
                    }
                }
            }
        }
        Ok(output.to_string())
    }
}

fn parse_timestamp(node: &Value, format: &str) -> Result<DateTime<Utc>, PluginError> {
    let text = match node {
        Value::String(s) => s.trim().to_string(),
        other => other.to_string(),
    };
    let dt = match format.to_ascii_lowercase().as_str() {
        "epoch_s" => Utc
            .timestamp_opt(text.parse::<i64>()?, 0)
            .single()
            .ok_or("invalid epoch seconds")?,
        "epoch_ms" => Utc
            .timestamp_millis_opt(text.parse::<i64>()?)
            .single()
            .ok_or("invalid epoch millis")?,
        "iso8601" | "rfc3339" => DateTime::parse_from_rfc3339(&text)?.with_timezone(&Utc),
        _ => Utc
            .timestamp_millis_opt(text.parse::<i64>()?)
            .single()
            .ok_or("unsupported source format")?,
    };
    Ok(dt)
}

fn format_timestamp(dt: DateTime<Utc>, format: &str, offset: FixedOffset) -> String {
    match format.to_ascii_lowercase().as_str() {
        "epoch_s" => dt.timestamp().to_string(),
        "epoch_ms" => dt.timestamp_millis().to_string(),
        "iso8601" => dt.to_rfc3339_opts(SecondsFormat::Secs, true),
        "rfc3339" => dt.with_timezone(&offset).to_rfc3339_opts(SecondsFormat::Secs, false),
        _ => dt.to_rfc3339_opts(SecondsFormat::Secs, true),
    }
}

fn parse_zone(zone: &str) -> FixedOffset {
    if zone.eq_ignore_ascii_case("UTC") || zone.eq_ignore_ascii_case("Z") {
        return FixedOffset::east_opt(0).unwrap();
    }
    // Accept "+02:00" / "-05:00" style offsets; fall back to UTC.
    if let Ok(dt) = DateTime::parse_from_rfc3339(&format!("1970-01-01T00:00:00{zone}")) {
        return *dt.offset();
    }
    FixedOffset::east_opt(0).unwrap()
}

fn set_leaf(root: &mut Value, path: &str, value: Value) {
    crate::pipeline::set_field_at(root, path, value);
}

fn remove_leaf(root: &mut Value, path: &str) {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        match current.get_mut(*segment) {
            Some(child) if child.is_object() => current = child,
            _ => return,
        }
    }
    if let Value::Object(map) = current {
        map.remove(segments[segments.len() - 1]);
    }
}

impl Plugin for TimestampNormalize {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let mut fields = None;
        let mut from = "epoch_ms".to_string();
        let mut to = "ISO8601".to_string();
        let mut zone = "UTC".to_string();
        let mut remove_on_error = false;
        for part in config.split(';') {
            let part = part.trim();
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "fields" => fields = Some(val.to_string()),
                "from" => from = val.to_string(),
                "to" => to = val.to_string(),
                "zone" => zone = val.to_string(),
                "removeOnError" => remove_on_error = val.eq_ignore_ascii_case("true"),
                _ => {}
            }
        }
        Ok(Some(Self::normalize(
            payload,
            fields.as_deref(),
            &from,
            &to,
            &zone,
            remove_on_error,
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn epoch_ms_to_iso8601() {
        let out = TimestampNormalize::new()
            .execute_str(
                r#"{"created_at":1751362800000}"#,
                "fields=created_at;from=epoch_ms;to=ISO8601;zone=UTC",
            )
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["created_at"], "2025-07-01T09:40:00Z");
    }

    #[test]
    fn iso8601_to_epoch_s_roundtrip() {
        let out = TimestampNormalize::new()
            .execute_str(
                r#"{"t":"2025-07-01T09:40:00Z"}"#,
                "fields=t;from=ISO8601;to=epoch_s",
            )
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["t"], "1751362800");
    }

    #[test]
    fn remove_on_error_drops_unparseable() {
        let out = TimestampNormalize::new()
            .execute_str(
                r#"{"t":"not-a-date","keep":1}"#,
                "fields=t;from=ISO8601;to=epoch_s;removeOnError=true",
            )
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert!(v.get("t").is_none());
        assert_eq!(v["keep"], 1);
    }
}
