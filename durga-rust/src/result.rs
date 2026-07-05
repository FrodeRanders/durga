//! Structured plugin execution result, mirroring the Java
//! `org.gautelis.durga.plugins.PluginResult`.

use std::collections::BTreeMap;

use serde_json::Value;

/// How a plugin signals a non-exceptional error outcome.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStrategy {
    Fail,
    Skip,
    Dlq,
}

/// How the generated worker should treat [`PluginResult::output`] relative to
/// the task's input payload. Matches the Java `OutputDisposition`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputDisposition {
    /// Output is the new payload and replaces the input payload downstream.
    Payload,
    /// Input payload is forwarded unchanged; output is a control/annotation
    /// value (e.g. a route key or inspection result).
    Passthrough,
    /// Input payload is forwarded unchanged; output describes an external
    /// side effect (e.g. a stored-object handle).
    SideEffect,
}

/// Structured result from a plugin execution, carrying output, idempotency
/// key, disposition, optional error strategy, and metadata.
#[derive(Debug, Clone)]
pub struct PluginResult {
    output: Option<Vec<u8>>,
    idempotency_key: String,
    error_strategy: Option<ErrorStrategy>,
    disposition: OutputDisposition,
    side_effect_description: Option<String>,
    metadata: BTreeMap<String, Value>,
}

impl PluginResult {
    pub fn output(&self) -> Option<&[u8]> {
        self.output.as_deref()
    }

    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    pub fn error_strategy(&self) -> Option<ErrorStrategy> {
        self.error_strategy
    }

    pub fn disposition(&self) -> OutputDisposition {
        self.disposition
    }

    pub fn side_effect_description(&self) -> Option<&str> {
        self.side_effect_description.as_deref()
    }

    pub fn metadata(&self) -> &BTreeMap<String, Value> {
        &self.metadata
    }

    pub fn is_success(&self) -> bool {
        self.error_strategy.is_none()
    }

    // ---- factory methods ----

    pub fn success(output: Option<Vec<u8>>, idempotency_key: impl Into<String>) -> Self {
        Self::new(output, idempotency_key, None, OutputDisposition::Payload, None)
    }

    pub fn success_with_metadata(
        output: Option<Vec<u8>>,
        idempotency_key: impl Into<String>,
        metadata: BTreeMap<String, Value>,
    ) -> Self {
        let mut r = Self::success(output, idempotency_key);
        r.metadata = metadata;
        r
    }

    /// Output is a control/annotation value; the input payload is forwarded
    /// unchanged.
    pub fn passthrough(output: Option<Vec<u8>>, idempotency_key: impl Into<String>) -> Self {
        Self::new(output, idempotency_key, None, OutputDisposition::Passthrough, None)
    }

    /// Output describes an external side effect; the input payload is forwarded
    /// unchanged.
    pub fn side_effect(
        output: Option<Vec<u8>>,
        idempotency_key: impl Into<String>,
        description: impl Into<String>,
    ) -> Self {
        Self::new(
            output,
            idempotency_key,
            None,
            OutputDisposition::SideEffect,
            Some(description.into()),
        )
    }

    pub fn dlq(
        output: Option<Vec<u8>>,
        idempotency_key: impl Into<String>,
        reason: impl Into<String>,
    ) -> Self {
        Self::new(
            output,
            idempotency_key,
            Some(ErrorStrategy::Dlq),
            OutputDisposition::Payload,
            Some(reason.into()),
        )
    }

    pub fn skip(idempotency_key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new(
            None,
            idempotency_key,
            Some(ErrorStrategy::Skip),
            OutputDisposition::Payload,
            Some(reason.into()),
        )
    }

    pub fn fail(idempotency_key: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::new(
            None,
            idempotency_key,
            Some(ErrorStrategy::Fail),
            OutputDisposition::Payload,
            Some(reason.into()),
        )
    }

    fn new(
        output: Option<Vec<u8>>,
        idempotency_key: impl Into<String>,
        error_strategy: Option<ErrorStrategy>,
        disposition: OutputDisposition,
        side_effect_description: Option<String>,
    ) -> Self {
        Self {
            output,
            idempotency_key: idempotency_key.into(),
            error_strategy,
            disposition,
            side_effect_description,
            metadata: BTreeMap::new(),
        }
    }
}
