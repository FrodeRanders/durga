//! Port of `org.gautelis.durga.plugins.ObjectStoreCollector` (plugin id
//! `object-store-collector`).
//!
//! Writes the incoming payload to the local object store and replaces the
//! payload with a data handle (name / uri / mediaType / schema / metadata).

use serde_json::{Map, Value};

use crate::plugin::{Plugin, PluginError, PluginExecutionContext};
use crate::plugins::object_store;
use crate::plugins::FormatDetector;
use crate::result::PluginResult;

#[derive(Default)]
pub struct ObjectStoreCollector;

impl ObjectStoreCollector {
    pub fn new() -> Self {
        ObjectStoreCollector
    }

    fn build_output(
        &self,
        payload: &[u8],
        options: &std::collections::HashMap<String, String>,
        stored: &object_store::StoredObject,
    ) -> Vec<u8> {
        let detection = FormatDetector::detect(payload);
        let asset_name = options
            .get("asset")
            .or_else(|| options.get("name"))
            .cloned()
            .unwrap_or_else(|| "payload".to_string());
        let schema = options.get("schema");
        let handle_field = object_store::handle_field(options);

        let mut metadata = Map::new();
        metadata.insert("bytes".to_string(), Value::from(stored.bytes as u64));
        metadata.insert("sha256".to_string(), Value::from(stored.sha256.clone()));
        metadata.insert("createdAt".to_string(), Value::from(stored.created_at.clone()));
        metadata.insert("format".to_string(), Value::from(detection.format.clone()));
        metadata.insert("datatype".to_string(), Value::from(detection.datatype.clone()));
        metadata.insert("encoding".to_string(), Value::from(detection.encoding.clone()));

        let mut handle = Map::new();
        handle.insert("name".to_string(), Value::from(asset_name));
        handle.insert("uri".to_string(), Value::from(stored.uri.clone()));
        handle.insert("mediaType".to_string(), Value::from(detection.media_type.clone()));
        match schema.filter(|s| !s.trim().is_empty()) {
            Some(s) => handle.insert("schema".to_string(), Value::from(s.clone())),
            None => handle.insert("schema".to_string(), Value::Null),
        };
        handle.insert("metadata".to_string(), Value::Object(metadata));

        let mut output = Map::new();
        output.insert(handle_field, Value::Object(handle));
        if flag(options, "includeFormat", true) {
            output.insert("format".to_string(), detection.to_json());
        }
        if flag(options, "includeOriginal", false) {
            output.insert(
                "payload".to_string(),
                Value::from(String::from_utf8_lossy(payload).into_owned()),
            );
        }
        Value::Object(output).to_string().into_bytes()
    }
}

impl Plugin for ObjectStoreCollector {
    fn execute_bytes(&self, payload: &[u8], config: &str) -> Result<Option<Vec<u8>>, PluginError> {
        let options = object_store::parse_config(config);
        let stored = object_store::store(payload, &options)?;
        Ok(Some(self.build_output(payload, &options, &stored)))
    }

    /// In validation mode the external object-store write is suppressed: the plugin computes and
    /// returns the handle it *would* have produced (with a synthetic, non-stored URI) so a
    /// supervisor can compare the candidate response, but nothing is written to storage.
    fn execute_with_result(
        &self,
        payload: &[u8],
        config: &str,
        context: &PluginExecutionContext,
    ) -> Result<PluginResult, PluginError> {
        let options = object_store::parse_config(config);
        if context.validation_mode() {
            let described = object_store::describe(payload);
            let output = self.build_output(payload, &options, &described);
            return Ok(PluginResult::side_effect(
                Some(output),
                self.idempotency_key(payload, config),
                "validation: object-store write suppressed",
            ));
        }
        let stored = object_store::store(payload, &options)?;
        let output = self.build_output(payload, &options, &stored);
        Ok(PluginResult::success(Some(output), self.idempotency_key(payload, config)))
    }
}

fn flag(options: &std::collections::HashMap<String, String>, key: &str, default: bool) -> bool {
    options
        .get(key)
        .map(|v| v.eq_ignore_ascii_case("true"))
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stores_payload_and_emits_handle() {
        let out = ObjectStoreCollector::new()
            .execute_bytes(br#"{"order_id":7}"#, "prefix=test-objects")
            .unwrap()
            .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        let uri = v["dataHandle"]["uri"].as_str().unwrap();
        assert!(uri.starts_with("file://"));
        assert_eq!(v["dataHandle"]["mediaType"], "application/json");
        assert_eq!(v["format"]["format"], "json");
        // The stored file is readable back.
        let bytes = object_store::read(uri).unwrap();
        assert_eq!(bytes, br#"{"order_id":7}"#);
    }

    #[test]
    fn suppresses_store_write_in_validation_mode() {
        let result = ObjectStoreCollector::new()
            .execute_with_result(
                br#"{"order_id":7}"#,
                "prefix=validation-objects;asset=RawOrders",
                &PluginExecutionContext::validation(),
            )
            .unwrap();
        let v: Value = serde_json::from_slice(result.output().unwrap()).unwrap();
        let uri = v["dataHandle"]["uri"].as_str().unwrap();
        assert!(uri.starts_with("validation:not-stored/"), "got {uri}");
        assert_eq!(v["dataHandle"]["name"], "RawOrders");
        assert_eq!(result.disposition(), crate::result::OutputDisposition::SideEffect);
        // Nothing was written, so the synthetic URI is not readable.
        assert!(object_store::read(uri).is_err());
    }
}
