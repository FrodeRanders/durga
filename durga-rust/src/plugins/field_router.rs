//! Port of `org.gautelis.durga.plugins.DeadLetterRouter` (plugin id
//! `field-router`).
//!
//! Maps a field value to a named route key. A router's output is a routing
//! key, not a payload, so it declares [`OutputDisposition::Passthrough`] and
//! the input payload is forwarded unchanged.

use std::collections::HashMap;

use serde_json::Value;

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};
use crate::result::PluginResult;

pub struct DeadLetterRouter {
    field: String,
    routes: HashMap<String, String>,
    default_route: String,
}

impl DeadLetterRouter {
    pub fn new() -> Self {
        DeadLetterRouter {
            field: "status".to_string(),
            routes: HashMap::new(),
            default_route: "default".to_string(),
        }
    }

    fn from_config(config: &str) -> Self {
        let mut field = "status".to_string();
        let mut default_route = "default".to_string();
        let mut routes = HashMap::new();
        for part in config.split_whitespace() {
            let Some(eq) = part.find('=') else { continue };
            let key = part[..eq].trim();
            let val = part[eq + 1..].trim();
            match key {
                "field" => field = val.to_string(),
                "default" => default_route = val.to_string(),
                "routes" if val.starts_with('{') && val.ends_with('}') => {
                    let body = &val[1..val.len() - 1];
                    for pair in body.split(',') {
                        let pair = pair.trim();
                        if let Some(colon) = pair.find(':') {
                            if colon > 0 {
                                routes.insert(
                                    pair[..colon].trim().to_string(),
                                    pair[colon + 1..].trim().to_string(),
                                );
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        DeadLetterRouter { field, routes, default_route }
    }

    /// Determines the route key for a message. Falls back to the default route
    /// when the payload is unparseable or the field is absent/unmatched.
    pub fn route(&self, json: &str) -> String {
        let node: Value = match serde_json::from_str(json) {
            Ok(v) => v,
            Err(_) => return self.default_route.clone(),
        };
        match field_at(&node, &self.field) {
            Some(v) if !v.is_null() => {
                let value = match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                };
                self.routes.get(&value).cloned().unwrap_or_else(|| self.default_route.clone())
            }
            _ => self.default_route.clone(),
        }
    }
}

impl Default for DeadLetterRouter {
    fn default() -> Self {
        Self::new()
    }
}

impl Plugin for DeadLetterRouter {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        Ok(Some(Self::from_config(config).route(payload)))
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
    fn routes_by_field_value() {
        let r = DeadLetterRouter::new();
        let out = r
            .execute_str(
                r#"{"status":"shipped"}"#,
                "field=status routes={pending:default,shipped:high} default=default",
            )
            .unwrap();
        assert_eq!(out.as_deref(), Some("high"));
    }

    #[test]
    fn falls_back_to_default() {
        let r = DeadLetterRouter::new();
        let out = r
            .execute_str(r#"{"status":"unknown"}"#, "field=status routes={a:b} default=fallback")
            .unwrap();
        assert_eq!(out.as_deref(), Some("fallback"));
    }

    #[test]
    fn is_passthrough() {
        let r = DeadLetterRouter::new();
        let res = r
            .execute_with_result(
                br#"{"status":"shipped","order_id":7}"#,
                "field=status routes={shipped:high} default=default",
            )
            .unwrap();
        assert_eq!(res.disposition(), OutputDisposition::Passthrough);
        assert_eq!(res.output(), Some(b"high".as_ref()));
    }
}
