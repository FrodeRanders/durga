//! Port of `org.gautelis.durga.plugins.FormatDetector` (plugin id
//! `format-detector`).
//!
//! Detects coarse payload format / datatype / media type. It is an inspection
//! result, not a replacement payload, so it declares
//! [`OutputDisposition::Passthrough`]. Config: `field=format;includePayload=false`.

use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::plugin::{Plugin, PluginError};
use crate::result::PluginResult;

#[derive(Debug, Clone)]
pub struct Detection {
    pub format: String,
    pub datatype: String,
    pub media_type: String,
    pub encoding: String,
    pub bytes: usize,
    pub sha256: String,
}

impl Detection {
    fn to_json(&self) -> Value {
        let mut node = Map::new();
        node.insert("format".to_string(), Value::from(self.format.clone()));
        node.insert("datatype".to_string(), Value::from(self.datatype.clone()));
        node.insert("mediaType".to_string(), Value::from(self.media_type.clone()));
        node.insert("encoding".to_string(), Value::from(self.encoding.clone()));
        node.insert("bytes".to_string(), Value::from(self.bytes as u64));
        node.insert("sha256".to_string(), Value::from(self.sha256.clone()));
        Value::Object(node)
    }
}

#[derive(Default)]
pub struct FormatDetector;

impl FormatDetector {
    pub fn new() -> Self {
        FormatDetector
    }

    pub fn detect(payload: &[u8]) -> Detection {
        if payload.is_empty() {
            return Detection {
                format: "empty".into(),
                datatype: "empty".into(),
                media_type: "application/octet-stream".into(),
                encoding: "none".into(),
                bytes: 0,
                sha256: String::new(),
            };
        }
        let sha = sha256_hex(payload);
        let text = match try_text(payload) {
            Some(t) => t,
            None => {
                return Detection {
                    format: "binary".into(),
                    datatype: "bytes".into(),
                    media_type: "application/octet-stream".into(),
                    encoding: "binary".into(),
                    bytes: payload.len(),
                    sha256: sha,
                }
            }
        };

        let trimmed = text.trim_start();
        if looks_like_json(trimmed) {
            if let Ok(node) = serde_json::from_str::<Value>(&text) {
                let datatype = match &node {
                    Value::Object(_) => "object",
                    Value::Array(_) => "array",
                    Value::String(_) => "string",
                    Value::Number(n) => {
                        if n.is_i64() || n.is_u64() {
                            "integer"
                        } else {
                            "number"
                        }
                    }
                    Value::Bool(_) => "boolean",
                    Value::Null => "null",
                };
                return Detection {
                    format: "json".into(),
                    datatype: datatype.into(),
                    media_type: "application/json".into(),
                    encoding: "utf-8".into(),
                    bytes: payload.len(),
                    sha256: sha,
                };
            }
        }
        if looks_like_xml(trimmed) {
            return Detection {
                format: "xml".into(),
                datatype: "document".into(),
                media_type: "application/xml".into(),
                encoding: "utf-8".into(),
                bytes: payload.len(),
                sha256: sha,
            };
        }
        if looks_like_csv(&text) {
            return Detection {
                format: "csv".into(),
                datatype: "table".into(),
                media_type: "text/csv".into(),
                encoding: "utf-8".into(),
                bytes: payload.len(),
                sha256: sha,
            };
        }
        Detection {
            format: "text".into(),
            datatype: "string".into(),
            media_type: "text/plain".into(),
            encoding: "utf-8".into(),
            bytes: payload.len(),
            sha256: sha,
        }
    }
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

fn try_text(payload: &[u8]) -> Option<String> {
    let text = std::str::from_utf8(payload).ok()?;
    for &b in payload {
        let c = b as i32;
        if c == 0 || c < 0x09 || (c > 0x0d && c < 0x20) {
            return None;
        }
    }
    Some(text.to_string())
}

fn looks_like_json(text: &str) -> bool {
    text.starts_with('{')
        || text.starts_with('[')
        || text.starts_with('"')
        || text.starts_with("true")
        || text.starts_with("false")
        || text.starts_with("null")
        || {
            let t = text.trim_end();
            !t.is_empty() && t.parse::<f64>().is_ok()
        }
}

fn looks_like_xml(text: &str) -> bool {
    text.starts_with('<') && text.contains('>')
}

fn looks_like_csv(text: &str) -> bool {
    let lines: Vec<&str> = text.lines().take(4).collect();
    if lines.len() < 2 {
        return false;
    }
    let first = lines[0].matches(',').count();
    let second = lines[1].matches(',').count();
    first > 0 && first == second
}

impl Plugin for FormatDetector {
    fn execute_bytes(&self, payload: &[u8], config: &str) -> Result<Option<Vec<u8>>, PluginError> {
        let mut field = "format".to_string();
        let mut include_payload = false;
        for part in config.split(';') {
            let part = part.trim();
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "field" => field = val.to_string(),
                "includePayload" => include_payload = val.eq_ignore_ascii_case("true"),
                _ => {}
            }
        }
        let detection = Self::detect(payload);
        let mut output = Map::new();
        output.insert(field, detection.to_json());
        if include_payload {
            output.insert(
                "payload".to_string(),
                Value::from(String::from_utf8_lossy(payload).into_owned()),
            );
        }
        Ok(Some(Value::Object(output).to_string().into_bytes()))
    }

    fn execute_with_result(&self, payload: &[u8], config: &str) -> Result<PluginResult, PluginError> {
        let output = self.execute_bytes(payload, config)?;
        Ok(PluginResult::passthrough(output, self.idempotency_key(payload, config)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::OutputDisposition;

    #[test]
    fn detects_json_object() {
        let out = FormatDetector::new()
            .execute_bytes(br#"{"a":1}"#, ".")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["format"]["format"], "json");
        assert_eq!(v["format"]["datatype"], "object");
        assert_eq!(v["format"]["mediaType"], "application/json");
    }

    #[test]
    fn uses_configured_field_and_is_passthrough() {
        let fd = FormatDetector::new();
        let res = fd.execute_with_result(br#"{"a":1}"#, "field=fmt").unwrap();
        assert_eq!(res.disposition(), OutputDisposition::Passthrough);
        let v: Value = serde_json::from_slice(res.output().unwrap()).unwrap();
        assert!(v.get("fmt").is_some());
    }
}
