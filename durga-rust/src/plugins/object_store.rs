//! Local-filesystem object store shared by the object-store plugins, mirroring
//! the Java `ObjectStoreSupport`. Only `file:` handles are supported.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::Utc;
use regex::Regex;
use serde_json::Value;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::pipeline::field_at;
use crate::plugin::PluginError;
use crate::plugins::FormatDetector;

#[derive(Debug, Clone)]
pub struct StoredObject {
    /// Retained for parity with the Java record; not embedded in the handle.
    #[allow(dead_code)]
    pub id: String,
    pub uri: String,
    pub bytes: usize,
    pub sha256: String,
    pub created_at: String,
}

pub fn parse_config(config: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    if config.trim().is_empty() {
        return values;
    }
    for part in config.split(';') {
        let Some(eq) = part.find('=') else { continue };
        if eq == 0 {
            continue;
        }
        let key = part[..eq].trim();
        let value = part[eq + 1..].trim();
        if !key.is_empty() {
            values.insert(key.to_string(), value.to_string());
        }
    }
    values
}

pub fn handle_field(config: &HashMap<String, String>) -> String {
    first_non_blank(&[config.get("handleField"), config.get("field")])
        .unwrap_or_else(|| "dataHandle".to_string())
}

fn root_path(config: &HashMap<String, String>) -> Result<PathBuf, PluginError> {
    let root = first_non_blank(&[config.get("store"), config.get("root"), config.get("rootUri")]);
    let root = match root {
        Some(r) => r,
        None => default_root(),
    };
    if let Some(stripped) = root.strip_prefix("file:") {
        return Ok(PathBuf::from(strip_file_slashes(stripped)));
    }
    if root.contains("://") {
        return Err("Only local file object stores are supported by this plugin".into());
    }
    Ok(PathBuf::from(root))
}

fn default_root() -> String {
    std::env::temp_dir()
        .join("durga-object-store")
        .to_string_lossy()
        .into_owned()
}

pub fn media_type(data: &[u8], file_name: Option<&str>) -> String {
    if let Some(name) = file_name {
        if let Some(mt) = probe_by_name(name) {
            return mt;
        }
    }
    FormatDetector::detect(data).media_type
}

pub fn store(payload: &[u8], config: &HashMap<String, String>) -> Result<StoredObject, PluginError> {
    let root = root_path(config)?;
    let prefix = sanitize_prefix(config.get("prefix").map(|s| s.as_str()).unwrap_or("objects"));
    let extension = extension(config.get("extension").map(|s| s.as_str()), payload, config.get("fileName").map(|s| s.as_str()));
    let id = Uuid::new_v4().to_string();
    let mut dir = root.join(&prefix);
    for segment in layout_segments(config.get("layout").map(|s| s.as_str()), payload) {
        dir = dir.join(segment);
    }
    std::fs::create_dir_all(&dir)?;
    let target = dir.join(format!("{id}{extension}"));
    let canonical_root = root.canonicalize().unwrap_or(root.clone());
    // Best-effort containment check against the configured root.
    if !dir.starts_with(&canonical_root) && !dir.starts_with(&root) {
        return Err("Object target escapes configured root".into());
    }
    std::fs::write(&target, payload)?;
    let uri = format!("file://{}", target.to_string_lossy());
    Ok(StoredObject {
        id,
        uri,
        bytes: payload.len(),
        sha256: sha256_hex(payload),
        created_at: Utc::now().to_rfc3339(),
    })
}

/// Resolves the `layout` config into directory segments between the prefix and
/// the (always UUID) filename. The layout is a `/`-separated list of tokens:
/// `date` / `date:hour` / `date:minute` (expands to `yyyy/MM/dd`[/HH][/mm] UTC),
/// `field:<path>` (sanitized payload field value, `_unknown` when absent), or
/// `const:<text>` / a bare literal. Empty layout means a flat structure. Mirrors
/// the Java `ObjectStoreSupport.layoutSegments`.
fn layout_segments(layout: Option<&str>, payload: &[u8]) -> Vec<String> {
    let mut segments = Vec::new();
    let Some(layout) = layout.filter(|l| !l.trim().is_empty()) else {
        return segments;
    };
    let mut json: Option<Value> = None;
    let mut json_parsed = false;
    let now = Utc::now();
    for raw in layout.split('/') {
        let token = raw.trim();
        if token.is_empty() {
            continue;
        }
        if token == "date" || token.starts_with("date:") {
            let granularity = token.split_once(':').map(|(_, g)| g.trim().to_ascii_lowercase())
                .unwrap_or_else(|| "day".to_string());
            segments.push(now.format("%Y").to_string());
            segments.push(now.format("%m").to_string());
            segments.push(now.format("%d").to_string());
            if granularity == "hour" || granularity == "minute" {
                segments.push(now.format("%H").to_string());
            }
            if granularity == "minute" {
                segments.push(now.format("%M").to_string());
            }
        } else if let Some(path) = token.strip_prefix("field:") {
            if !json_parsed {
                json = serde_json::from_slice(payload).ok();
                json_parsed = true;
            }
            let value = json
                .as_ref()
                .and_then(|j| field_at(j, path.trim()))
                .filter(|v| !v.is_null())
                .map(|v| match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                })
                .filter(|s| !s.trim().is_empty())
                .unwrap_or_else(|| "_unknown".to_string());
            segments.push(sanitize_segment(&value));
        } else if let Some(literal) = token.strip_prefix("const:") {
            segments.push(sanitize_segment(literal.trim()));
        } else {
            segments.push(sanitize_segment(token));
        }
    }
    segments
}

fn sanitize_segment(value: &str) -> String {
    static NON_SAFE: OnceLock<Regex> = OnceLock::new();
    let re = NON_SAFE.get_or_init(|| Regex::new(r"[^A-Za-z0-9_.-]").expect("valid regex"));
    let no_slash = value.replace('\\', "/").replace('/', "_").replace("..", "");
    let sanitized = re.replace_all(&no_slash, "_").into_owned();
    if sanitized.trim().is_empty() {
        "_".to_string()
    } else {
        sanitized
    }
}

pub fn read(uri: &str) -> Result<Vec<u8>, PluginError> {
    if uri.trim().is_empty() {
        return Err("Object handle URI is missing".into());
    }
    let Some(rest) = uri.strip_prefix("file:") else {
        return Err("Only file: object handles are supported by this plugin".into());
    };
    let path = strip_file_slashes(rest);
    Ok(std::fs::read(Path::new(&path))?)
}

fn extension(configured: Option<&str>, payload: &[u8], file_name: Option<&str>) -> String {
    if let Some(c) = configured.filter(|c| !c.trim().is_empty()) {
        return if c.starts_with('.') { c.to_string() } else { format!(".{c}") };
    }
    match media_type(payload, file_name).as_str() {
        "application/json" => ".json",
        "text/plain" => ".txt",
        "text/csv" => ".csv",
        "application/xml" | "text/xml" => ".xml",
        _ => ".bin",
    }
    .to_string()
}

fn sanitize_prefix(prefix: &str) -> String {
    static NON_SAFE: OnceLock<Regex> = OnceLock::new();
    let re = NON_SAFE.get_or_init(|| Regex::new(r"[^A-Za-z0-9_./-]").expect("valid regex"));
    let normalized = prefix.replace('\\', "/");
    let no_lead = normalized.trim_start_matches('/');
    let no_dotdot = no_lead.replace("..", "");
    let sanitized = re.replace_all(&no_dotdot, "_").into_owned();
    if sanitized.trim().is_empty() {
        "objects".to_string()
    } else {
        sanitized
    }
}

fn probe_by_name(file_name: &str) -> Option<String> {
    let lower = file_name.to_ascii_lowercase();
    let ext = lower.rsplit('.').next()?;
    let mt = match ext {
        "json" => "application/json",
        "txt" => "text/plain",
        "csv" => "text/csv",
        "xml" => "application/xml",
        _ => return None,
    };
    Some(mt.to_string())
}

pub fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

fn strip_file_slashes(path: &str) -> String {
    // "file:///abs" -> "/abs"; "file://host/abs" is not expected for local.
    let trimmed = path.trim_start_matches('/');
    format!("/{trimmed}")
}

fn first_non_blank(candidates: &[Option<&String>]) -> Option<String> {
    candidates
        .iter()
        .flatten()
        .find(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flat_layout_has_no_segments() {
        assert!(layout_segments(None, b"{}").is_empty());
        assert!(layout_segments(Some(""), b"{}").is_empty());
    }

    #[test]
    fn date_layout_expands_granularity() {
        assert_eq!(layout_segments(Some("date"), b"{}").len(), 3); // y/m/d
        assert_eq!(layout_segments(Some("date:hour"), b"{}").len(), 4);
        assert_eq!(layout_segments(Some("date:minute"), b"{}").len(), 5);
    }

    #[test]
    fn field_layout_uses_payload_value_sanitized() {
        let segs = layout_segments(Some("field:business_type"), br#"{"business_type":"orders/eu"}"#);
        assert_eq!(segs, vec!["orders_eu".to_string()]);
    }

    #[test]
    fn missing_field_falls_back_to_unknown() {
        let segs = layout_segments(Some("field:nope"), b"{}");
        assert_eq!(segs, vec!["_unknown".to_string()]);
    }

    #[test]
    fn mixed_layout_composes_const_date_field() {
        let segs = layout_segments(Some("const:tenantA/date/field:region"), br#"{"region":"emea"}"#);
        // tenantA + (y/m/d) + emea = 5 segments
        assert_eq!(segs.len(), 5);
        assert_eq!(segs[0], "tenantA");
        assert_eq!(segs[4], "emea");
    }

    #[test]
    fn store_respects_layout_directory() {
        let mut cfg = HashMap::new();
        cfg.insert("prefix".to_string(), "layout-test".to_string());
        cfg.insert("layout".to_string(), "const:tenantA/field:kind".to_string());
        let stored = store(br#"{"kind":"invoice"}"#, &cfg).unwrap();
        assert!(stored.uri.contains("/layout-test/tenantA/invoice/"));
        assert!(stored.uri.ends_with(".json"));
    }
}
