//! Port of `org.gautelis.durga.plugins.ObjectStoreExtractor` (plugin id
//! `object-store-extractor`).
//!
//! Resolves a data handle in the payload to a `file:` URI and replaces the
//! payload with the stored object's bytes.

use serde_json::Value;

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};
use crate::plugins::object_store;

#[derive(Default)]
pub struct ObjectStoreExtractor;

impl ObjectStoreExtractor {
    pub fn new() -> Self {
        ObjectStoreExtractor
    }

    fn resolve_uri(payload: &[u8], handle_field: &str) -> Result<String, PluginError> {
        let text = std::str::from_utf8(payload)?;
        let node: Value = serde_json::from_str(text)
            .map_err(|e| -> PluginError { format!("Invalid input JSON: {e}").into() })?;
        if let Value::String(s) = &node {
            return Ok(s.clone());
        }
        let handle = match field_at(&node, handle_field) {
            Some(h) => Some(h),
            None if node.get("uri").is_some() => Some(&node),
            None => None,
        };
        match handle {
            Some(Value::Object(map)) => match map.get("uri").and_then(|u| u.as_str()) {
                Some(uri) => Ok(uri.to_string()),
                None => Err(missing_handle(handle_field)),
            },
            _ => Err(missing_handle(handle_field)),
        }
    }
}

fn missing_handle(handle_field: &str) -> PluginError {
    format!("Payload does not contain an object-store handle at '{handle_field}'").into()
}

impl Plugin for ObjectStoreExtractor {
    fn execute_bytes(&self, payload: &[u8], config: &str) -> Result<Option<Vec<u8>>, PluginError> {
        let options = object_store::parse_config(config);
        let uri = Self::resolve_uri(payload, &object_store::handle_field(&options))?;
        Ok(Some(object_store::read(&uri)?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plugins::ObjectStoreCollector;

    #[test]
    fn round_trips_through_the_store() {
        let stored = ObjectStoreCollector::new()
            .execute_bytes(br#"{"n":42}"#, "prefix=test-extract")
            .unwrap()
            .unwrap();
        // Feed the collector's handle output straight into the extractor.
        let back = ObjectStoreExtractor::new()
            .execute_bytes(&stored, ".")
            .unwrap()
            .unwrap();
        assert_eq!(back, br#"{"n":42}"#);
    }

    #[test]
    fn errors_when_no_handle() {
        let err = ObjectStoreExtractor::new()
            .execute_bytes(br#"{"no":"handle"}"#, ".")
            .unwrap_err();
        assert!(err.to_string().contains("object-store handle"));
    }
}
