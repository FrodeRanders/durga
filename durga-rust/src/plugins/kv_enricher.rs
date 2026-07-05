//! Port of `org.gautelis.durga.plugins.KvEnricher` (plugin id `kv-enricher`).
//!
//! Looks up a payload key field in an inline map and shallow-merges the
//! matching enrichment JSON. The inline map is parsed brace/quote aware so
//! enrichment objects containing commas are not shredded.

use std::collections::HashMap;

use serde_json::Value;

use crate::pipeline::{field_at, shallow_merge};
use crate::plugin::{Plugin, PluginError};

pub struct KvEnricher {
    key_field: String,
    inline: HashMap<String, String>,
}

impl KvEnricher {
    pub fn new() -> Self {
        KvEnricher { key_field: "_id".to_string(), inline: HashMap::new() }
    }

    fn from_config(config: &str) -> Self {
        let mut key_field = "_id".to_string();
        let mut inline = HashMap::new();
        for part in config.split(';') {
            let Some(eq) = part.find('=') else { continue };
            if eq == 0 {
                continue;
            }
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "keyField" => key_field = val.to_string(),
                "inline" if val.starts_with('{') && val.ends_with('}') => {
                    inline.extend(parse_inline_map(&val[1..val.len() - 1]));
                }
                _ => {}
            }
        }
        KvEnricher { key_field, inline }
    }

    /// Enriches `json` by looking up the key field in the inline map and
    /// shallow-merging the matching enrichment object. Passes through unchanged
    /// when the key is missing or unmatched.
    pub fn enrich(&self, json: &str) -> Result<String, PluginError> {
        let input: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        let key = match field_at(&input, &self.key_field) {
            Some(v) if !v.is_null() => match v {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            },
            _ => return Ok(json.to_string()),
        };
        let Some(enrichment_json) = self.inline.get(&key) else {
            return Ok(json.to_string());
        };
        let enrichment: Value = serde_json::from_str(enrichment_json).map_err(|e| -> PluginError {
            format!("Invalid enrichment JSON for matching key: {e}").into()
        })?;
        Ok(shallow_merge(&input, &enrichment).to_string())
    }
}

impl Default for KvEnricher {
    fn default() -> Self {
        Self::new()
    }
}

/// Parses the inline map body (`key:{json}, key:{json}`) into key -> raw JSON.
/// Splitting is brace/bracket/quote aware so commas inside a value's JSON do
/// not break entry boundaries.
pub fn parse_inline_map(body: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    let bytes = body.as_bytes();
    let mut depth: i32 = 0;
    let mut in_string = false;
    let mut quote = 0u8;
    let mut entry_start = 0usize;
    let mut colon: isize = -1;
    let mut i = 0usize;
    while i < bytes.len() {
        let c = bytes[i];
        if in_string {
            if c == quote && (i == 0 || bytes[i - 1] != b'\\') {
                in_string = false;
            }
            i += 1;
            continue;
        }
        match c {
            b'"' | b'\'' => {
                in_string = true;
                quote = c;
            }
            b'{' | b'[' => depth += 1,
            b'}' | b']' => depth -= 1,
            b':' if depth == 0 && colon < 0 => colon = i as isize,
            b',' if depth == 0 => {
                add_entry(&mut map, body, entry_start, colon, i);
                entry_start = i + 1;
                colon = -1;
            }
            _ => {}
        }
        i += 1;
    }
    add_entry(&mut map, body, entry_start, colon, body.len());
    map
}

fn add_entry(map: &mut HashMap<String, String>, body: &str, start: usize, colon: isize, end: usize) {
    if colon < 0 || (colon as usize) <= start {
        return;
    }
    let colon = colon as usize;
    let key = body[start..colon].trim();
    let value = body[colon + 1..end].trim();
    if !key.is_empty() && !value.is_empty() {
        map.insert(key.to_string(), value.to_string());
    }
}

impl Plugin for KvEnricher {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        Ok(Some(Self::from_config(config).enrich(payload)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enriches_matching_key_with_multifield_object() {
        let p = KvEnricher::new();
        let out = p
            .execute_str(
                r#"{"customer_email":"bob@example.com","amount":900}"#,
                "keyField=customer_email;inline={alice@example.com:{\"tier\":\"gold\",\"rep\":\"Alice\"}, bob@example.com:{\"tier\":\"silver\",\"rep\":\"Bob\"}}",
            )
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["tier"], "silver");
        assert_eq!(v["rep"], "Bob");
        assert_eq!(v["amount"], 900);
    }

    #[test]
    fn passes_through_on_missing_key() {
        let p = KvEnricher::new();
        let input = r#"{"customer_email":"nobody@example.com"}"#;
        let out = p
            .execute_str(input, "keyField=customer_email;inline={a:{\"x\":1}}")
            .unwrap()
            .unwrap();
        assert_eq!(out, input);
    }

    #[test]
    fn parse_inline_respects_brace_depth() {
        let m = parse_inline_map("a:{\"x\":1,\"y\":2}, b:{\"z\":3}");
        assert_eq!(m.len(), 2);
        assert_eq!(m.get("a").unwrap(), "{\"x\":1,\"y\":2}");
        assert_eq!(m.get("b").unwrap(), "{\"z\":3}");
    }
}
