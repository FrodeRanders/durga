//! Validation-mode contracts for the Rust target, mirroring the Java
//! `org.gautelis.durga.validation` package.
//!
//! [`ValidationCandidateOutput`] is retained for the retired first validation
//! design, where workers emitted explicit candidate-output records instead of
//! normal lifecycle events. Current generated Rust validation workers publish
//! lifecycle events to `process-events-<processId>-validation`.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::event::{ErrorInfo, ProcessEvent};
use crate::result::{ErrorStrategy, OutputDisposition, PluginResult};
use crate::worker::compute_output_payload;

/// Output of a task run in validation mode by a shadow worker. Field names
/// serialize as camelCase to match the Java record component names.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ValidationCandidateOutput {
    pub process_id: Option<String>,
    pub task_id: Option<String>,
    pub process_instance_id: Option<String>,
    pub activity_id: Option<String>,
    pub token_id: Option<String>,
    pub correlation_id: Option<String>,
    pub business_key: Option<String>,
    pub candidate_version: Option<String>,
    #[serde(default)]
    pub input_payload: Option<Value>,
    #[serde(default)]
    pub output_payload: Option<Value>,
    pub disposition: Option<String>,
    pub side_effect_description: Option<String>,
    pub idempotency_key: Option<String>,
    pub error_strategy: Option<String>,
    #[serde(default)]
    pub error: Option<ErrorInfo>,
    pub timestamp: Option<String>,
}

impl ValidationCandidateOutput {
    /// Stable partition/lookup key: `processId:taskId:processInstanceId`.
    pub fn key(&self) -> String {
        format!(
            "{}:{}:{}",
            self.process_id.as_deref().unwrap_or_default(),
            self.task_id.as_deref().unwrap_or_default(),
            self.process_instance_id.as_deref().unwrap_or_default()
        )
    }

    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ValidationCandidateOutput is always serializable")
    }
}

fn disposition_name(disposition: OutputDisposition) -> String {
    match disposition {
        OutputDisposition::Payload => "PAYLOAD",
        OutputDisposition::Passthrough => "PASSTHROUGH",
        OutputDisposition::SideEffect => "SIDE_EFFECT",
    }
    .to_string()
}

fn error_strategy_name(strategy: ErrorStrategy) -> String {
    match strategy {
        ErrorStrategy::Fail => "FAIL",
        ErrorStrategy::Skip => "SKIP",
        ErrorStrategy::Dlq => "DLQ",
    }
    .to_string()
}

/// Computes the candidate output for a validation-mode task run. Mirrors the
/// Java `validationWorkerClass`: no lifecycle events, no forward, no DLQ — the
/// outcome (including error strategies and thrown errors) is captured entirely
/// in the returned record. `result` is `Err(message)` when the plugin threw.
pub fn plan_validation_output(
    input: &ProcessEvent,
    activity_id: &str,
    candidate_version: &str,
    result: Result<PluginResult, String>,
) -> ValidationCandidateOutput {
    let mut out = ValidationCandidateOutput {
        process_id: input.process_id.clone(),
        task_id: Some(activity_id.to_string()),
        process_instance_id: input.process_instance_id.clone(),
        activity_id: Some(activity_id.to_string()),
        token_id: input.token_id.clone(),
        correlation_id: input.correlation_id.clone(),
        business_key: input.business_key.clone(),
        candidate_version: Some(candidate_version.to_string()),
        input_payload: input.payload.clone(),
        output_payload: None,
        disposition: None,
        side_effect_description: None,
        idempotency_key: None,
        error_strategy: None,
        error: None,
        timestamp: Some(now_iso8601()),
    };

    match result {
        Err(message) => {
            out.error_strategy = Some("FAIL".to_string());
            out.side_effect_description = Some(message.clone());
            out.error = Some(ErrorInfo::new(message, "VALIDATION_CANDIDATE_FAILED"));
        }
        Ok(pr) => {
            out.idempotency_key = Some(pr.idempotency_key().to_string());
            out.side_effect_description = pr.side_effect_description().map(|s| s.to_string());
            out.disposition = Some(disposition_name(pr.disposition()));
            if let Some(strategy) = pr.error_strategy() {
                let name = error_strategy_name(strategy);
                let message = out
                    .side_effect_description
                    .clone()
                    .unwrap_or_else(|| format!("Candidate requested {name}"));
                out.error = Some(ErrorInfo::new(message, format!("VALIDATION_CANDIDATE_{name}")));
                out.error_strategy = Some(name);
            } else {
                out.output_payload =
                    Some(compute_output_payload(input, pr.output(), pr.disposition()));
            }
        }
    }
    out
}

fn now_iso8601() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventType, Status};
    use serde_json::json;

    fn sample() -> ProcessEvent {
        ProcessEvent {
            process_instance_id: Some("pi-1".into()),
            process_id: Some("e2e".into()),
            activity_id: Some("prev".into()),
            token_id: Some("t".into()),
            correlation_id: Some("c".into()),
            payload: Some(json!({"order_id": 7, "amount": 900})),
            status: Some(Status::Completed),
            error: None,
            event_type: Some(EventType::ActivityCompleted),
            process_version: Some("v1".into()),
            business_key: None,
            timestamp: Some("2026-07-01T00:00:00Z".into()),
        }
    }

    #[test]
    fn captures_transformed_output_without_lifecycle() {
        let out = plan_validation_output(
            &sample(),
            "coerce",
            "candidate",
            Ok(PluginResult::success(Some(br#"{"order_id":7,"amount":900.0}"#.to_vec()), "k")),
        );
        assert_eq!(out.task_id.as_deref(), Some("coerce"));
        assert_eq!(out.process_instance_id.as_deref(), Some("pi-1"));
        assert_eq!(out.candidate_version.as_deref(), Some("candidate"));
        assert_eq!(out.disposition.as_deref(), Some("PAYLOAD"));
        assert_eq!(out.output_payload.as_ref().unwrap()["amount"], 900.0);
        assert!(out.error.is_none());
        assert!(out.error_strategy.is_none());
    }

    #[test]
    fn preserves_input_payload_for_passthrough() {
        let out = plan_validation_output(
            &sample(),
            "route",
            "candidate",
            Ok(PluginResult::passthrough(Some(b"high".to_vec()), "k")),
        );
        let payload = out.output_payload.unwrap();
        assert_eq!(payload["order_id"], 7);
        assert_eq!(payload["_pluginOutput"], "high");
        assert_eq!(out.disposition.as_deref(), Some("PASSTHROUGH"));
    }

    #[test]
    fn thrown_error_becomes_candidate_error() {
        let out = plan_validation_output(&sample(), "enrich", "candidate", Err("boom".into()));
        assert_eq!(out.error_strategy.as_deref(), Some("FAIL"));
        assert_eq!(out.error.as_ref().unwrap().code.as_deref(), Some("VALIDATION_CANDIDATE_FAILED"));
        assert!(out.output_payload.is_none());
    }

    #[test]
    fn error_strategy_becomes_candidate_error() {
        let out = plan_validation_output(
            &sample(),
            "validate",
            "candidate",
            Ok(PluginResult::dlq(None, "k", "bad record")),
        );
        assert_eq!(out.error_strategy.as_deref(), Some("DLQ"));
        assert_eq!(out.error.as_ref().unwrap().code.as_deref(), Some("VALIDATION_CANDIDATE_DLQ"));
        assert_eq!(out.error.as_ref().unwrap().message.as_deref(), Some("bad record"));
        assert!(out.output_payload.is_none());
    }

    #[test]
    fn round_trips_through_json_with_camel_case() {
        let out = plan_validation_output(
            &sample(),
            "coerce",
            "candidate",
            Ok(PluginResult::success(Some(br#"{"ok":true}"#.to_vec()), "k")),
        );
        let json = out.to_json();
        assert!(json.contains("\"processInstanceId\""));
        assert!(json.contains("\"outputPayload\""));
        assert!(json.contains("\"candidateVersion\""));
        let parsed = ValidationCandidateOutput::from_json(&json).unwrap();
        assert_eq!(parsed, out);
        assert_eq!(parsed.key(), "e2e:coerce:pi-1");
    }
}
