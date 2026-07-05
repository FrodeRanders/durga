//! Port of `org.gautelis.durga.plugins.Mask` (plugin id `mask`).
//!
//! Masks configured textual fields (dot-notation for nested). Config:
//! `fields=ssn,email;mask=*;preserve=3` — `preserve` keeps that many chars at
//! each end.

use serde_json::Value;

use crate::plugin::{Plugin, PluginError};

#[derive(Default)]
pub struct Mask;

impl Mask {
    pub fn new() -> Self {
        Mask
    }

    pub fn mask(json: &str, fields_list: Option<&str>, mask_char: char, preserve: i32)
        -> Result<String, PluginError>
    {
        let Some(fields_list) = fields_list.filter(|f| !f.trim().is_empty()) else {
            return Ok(json.to_string());
        };
        let fields: Vec<&str> = fields_list
            .split(',')
            .map(|f| f.trim())
            .filter(|f| !f.is_empty())
            .collect();
        if fields.is_empty() {
            return Ok(json.to_string());
        }

        let mut value: Value = serde_json::from_str(json)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        if !value.is_object() {
            return Ok(json.to_string());
        }
        for field in fields {
            mask_field(&mut value, field, mask_char, preserve);
        }
        Ok(value.to_string())
    }
}

fn mask_field(root: &mut Value, path: &str, mask_char: char, preserve: i32) {
    let segments: Vec<&str> = path.split('.').collect();
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        match current.get_mut(*segment) {
            Some(child) if child.is_object() => current = child,
            _ => return,
        }
    }
    let leaf = segments[segments.len() - 1];
    if let Some(Value::String(text)) = current.get(leaf) {
        let masked = mask_text(text, mask_char, preserve);
        if let Value::Object(map) = current {
            map.insert(leaf.to_string(), Value::String(masked));
        }
    }
}

fn mask_text(text: &str, mask_char: char, preserve: i32) -> String {
    let chars: Vec<char> = text.chars().collect();
    let len = chars.len() as i32;
    if len == 0 {
        return text.to_string();
    }
    if preserve <= 0 || len <= preserve * 2 {
        return mask_char.to_string().repeat(len as usize);
    }
    let p = preserve as usize;
    let prefix: String = chars[..p].iter().collect();
    let suffix: String = chars[chars.len() - p..].iter().collect();
    let middle = mask_char.to_string().repeat(chars.len() - p * 2);
    format!("{prefix}{middle}{suffix}")
}

impl Plugin for Mask {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let mut fields = None;
        let mut mask_char = '*';
        let mut preserve = 0i32;
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
                "mask" => {
                    if let Some(c) = val.chars().next() {
                        mask_char = c;
                    }
                }
                "preserve" => {
                    if let Ok(n) = val.parse::<i32>() {
                        preserve = n;
                    }
                }
                _ => {}
            }
        }
        Ok(Some(Self::mask(payload, fields.as_deref(), mask_char, preserve)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn masks_fully_without_preserve() {
        let out = Mask::new()
            .execute_str(r#"{"ssn":"12345"}"#, "fields=ssn;mask=*")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["ssn"], "*****");
    }

    #[test]
    fn preserves_ends() {
        let out = Mask::new()
            .execute_str(r#"{"card":"1234567890"}"#, "fields=card;mask=#;preserve=2")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["card"], "12######90");
    }

    #[test]
    fn masks_nested_field() {
        let out = Mask::new()
            .execute_str(r#"{"user":{"email":"secret"}}"#, "fields=user.email;mask=*")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_str(&out).unwrap();
        assert_eq!(v["user"]["email"], "******");
    }
}
