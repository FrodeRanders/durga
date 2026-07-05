//! Port of `org.gautelis.durga.plugins.StringTemplate` (plugin id
//! `string-template`).
//!
//! Renders a template with `${field}` substitution (dot-notation for nested
//! access). Missing fields render as an empty string. Config:
//! `template=Hello ${customer.name}, order ${id}`.

use std::sync::OnceLock;

use regex::Regex;
use serde_json::Value;

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct StringTemplate;

fn token_pattern() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\$\{([^}]+)\}").expect("valid token regex"))
}

impl StringTemplate {
    pub fn new() -> Self {
        StringTemplate
    }

    pub fn render(json: &str, template: &str) -> Result<String, PluginError> {
        if template.trim().is_empty() {
            return Ok(json.to_string());
        }
        let node: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;

        let result = token_pattern().replace_all(template, |caps: &regex::Captures| {
            let path = caps[1].trim();
            match field_at(&node, path) {
                Some(v) if !v.is_null() => match v {
                    Value::String(s) => s.clone(),
                    other => other.to_string(),
                },
                _ => String::new(),
            }
        });
        Ok(result.into_owned())
    }
}

impl Plugin for StringTemplate {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let trimmed = config.trim();
        let template = match trimmed.find('=') {
            Some(eq) if eq > 0 && trimmed[..eq].trim() == "template" => trimmed[eq + 1..].trim(),
            _ => return Ok(Some(payload.to_string())),
        };
        Ok(Some(Self::render(payload, template)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn substitutes_fields_including_nested() {
        let out = StringTemplate::new()
            .execute_str(
                r#"{"id":7,"customer":{"name":"Alice"}}"#,
                "template=Order ${id} for ${customer.name}",
            )
            .unwrap()
            .unwrap();
        assert_eq!(out, "Order 7 for Alice");
    }

    #[test]
    fn missing_field_renders_empty() {
        let out = StringTemplate::new()
            .execute_str(r#"{"a":1}"#, "template=x=${missing}=y")
            .unwrap()
            .unwrap();
        assert_eq!(out, "x==y");
    }
}
