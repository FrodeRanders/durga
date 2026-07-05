//! The [`Plugin`] contract, mirroring the Java
//! `org.gautelis.durga.plugins.Plugin` interface including its binary/text
//! dispatch defaults and content-based idempotency key.

use sha2::{Digest, Sha256};

use crate::result::PluginResult;

/// Boxed error type for plugin execution. Any error implementing
/// [`std::error::Error`] converts into this via `?`, so plugin bodies can use
/// `serde_json`, UTF-8 decoding, etc. ergonomically.
pub type PluginError = Box<dyn std::error::Error + Send + Sync>;

/// Contract for data pipeline plugins.
///
/// Plugins receive a raw payload and a configuration string and return a
/// transformed payload, or an error.
///
/// * **Text / JSON plugins** override [`Plugin::execute_str`]; the default
///   [`Plugin::execute_bytes`] handles UTF-8 conversion.
/// * **Binary plugins** override [`Plugin::execute_bytes`] directly.
/// * **Idempotency-aware / structured plugins** override
///   [`Plugin::execute_with_result`] to return a [`PluginResult`] with an
///   idempotency key, disposition, or error strategy.
///
/// The generated worker always calls [`Plugin::execute_with_result`].
pub trait Plugin {
    /// Binary entry point. Default: UTF-8 decode, delegate to
    /// [`Plugin::execute_str`], then re-encode.
    fn execute_bytes(&self, payload: &[u8], config: &str) -> Result<Option<Vec<u8>>, PluginError> {
        let text = std::str::from_utf8(payload)?;
        match self.execute_str(text, config)? {
            Some(out) => Ok(Some(out.into_bytes())),
            None => Ok(None),
        }
    }

    /// Text / JSON entry point. Only reached via the default
    /// [`Plugin::execute_bytes`]. The default is unsupported: text plugins
    /// override this, binary plugins override `execute_bytes` instead.
    fn execute_str(&self, _payload: &str, _config: &str) -> Result<Option<String>, PluginError> {
        Err("Plugin does not support text payloads — override execute_bytes instead".into())
    }

    /// Structured execution the generated worker calls. Default wraps
    /// [`Plugin::execute_bytes`] with a content-based idempotency key and the
    /// default `PAYLOAD` disposition.
    fn execute_with_result(&self, payload: &[u8], config: &str) -> Result<PluginResult, PluginError> {
        let output = self.execute_bytes(payload, config)?;
        Ok(PluginResult::success(output, self.idempotency_key(payload, config)))
    }

    /// Content-based idempotency key. Override for a domain-specific strategy
    /// (e.g. a business key extracted from the payload).
    fn idempotency_key(&self, payload: &[u8], config: &str) -> String {
        default_idempotency_key(payload, config)
    }
}

/// SHA-256 hex of the payload followed by the config, matching the Java
/// default `Plugin.idempotencyKey`.
pub fn default_idempotency_key(payload: &[u8], config: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(payload);
    hasher.update(config.as_bytes());
    hex::encode(hasher.finalize())
}
