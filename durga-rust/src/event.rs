//! Canonical lifecycle event, wire-compatible with the Java
//! `org.gautelis.durga.ProcessEvent`. Field names serialize as camelCase and
//! enum variants as SCREAMING_SNAKE_CASE so Rust-generated workers emit events
//! the Java monitor reads transparently.

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Status {
    Started,
    Completed,
    Failed,
    Escalated,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum EventType {
    ProcessStarted,
    ActivityEntered,
    ActivityCompleted,
    ActivityEscalated,
    ActivityCancelled,
    GatewayTaken,
    ProcessCompleted,
    ProcessFailed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub message: Option<String>,
    pub code: Option<String>,
}

impl ErrorInfo {
    pub fn new(message: impl Into<String>, code: impl Into<String>) -> Self {
        Self {
            message: Some(message.into()),
            code: Some(code.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ProcessEvent {
    pub process_instance_id: Option<String>,
    pub process_id: Option<String>,
    pub activity_id: Option<String>,
    pub token_id: Option<String>,
    pub correlation_id: Option<String>,
    #[serde(default)]
    pub payload: Option<Value>,
    pub status: Option<Status>,
    #[serde(default)]
    pub error: Option<ErrorInfo>,
    pub event_type: Option<EventType>,
    pub process_version: Option<String>,
    pub business_key: Option<String>,
    pub timestamp: Option<String>,
}

impl ProcessEvent {
    /// Parses a lifecycle event from JSON.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serializes this event as JSON for Kafka transport.
    pub fn to_json(&self) -> String {
        serde_json::to_string(self).expect("ProcessEvent is always serializable")
    }

    /// Base transition carrying instance/token/correlation/version/businessKey
    /// from this event forward, with a fresh timestamp.
    fn transition(
        &self,
        activity_id: &str,
        payload: Option<Value>,
        status: Status,
        error: Option<ErrorInfo>,
        event_type: EventType,
    ) -> ProcessEvent {
        ProcessEvent {
            process_instance_id: self.process_instance_id.clone(),
            process_id: self.process_id.clone(),
            activity_id: Some(activity_id.to_string()),
            token_id: self.token_id.clone(),
            correlation_id: self.correlation_id.clone(),
            payload,
            status: Some(status),
            error,
            event_type: Some(event_type),
            process_version: self.process_version.clone(),
            business_key: self.business_key.clone(),
            timestamp: Some(now_iso8601()),
        }
    }

    /// Successful completion of an activity, carrying the output payload.
    pub fn activity_completed(&self, activity_id: &str, payload: Value) -> ProcessEvent {
        self.transition(activity_id, Some(payload), Status::Completed, None, EventType::ActivityCompleted)
    }

    /// A routing decision at a gateway, carrying the payload forward.
    pub fn gateway_taken(&self, activity_id: &str, payload: Value) -> ProcessEvent {
        self.transition(activity_id, Some(payload), Status::Completed, None, EventType::GatewayTaken)
    }

    /// An aggregate absorbing an instance into an open window: terminal
    /// completion at the aggregate activity.
    pub fn process_completed(&self, activity_id: &str, payload: Value) -> ProcessEvent {
        self.transition(activity_id, Some(payload), Status::Completed, None, EventType::ProcessCompleted)
    }

    /// A validation failure: escalated, payload redacted, `VALIDATION_FAILED`.
    pub fn activity_escalated(&self, activity_id: &str, message: &str) -> ProcessEvent {
        self.transition(
            activity_id,
            Some(redacted_payload()),
            Status::Escalated,
            Some(ErrorInfo::new(message, "VALIDATION_FAILED")),
            EventType::ActivityEscalated,
        )
    }

    /// A plugin failure: failed, payload redacted, `PLUGIN_FAILED`.
    pub fn process_failed(&self, activity_id: &str, message: &str) -> ProcessEvent {
        self.transition(
            activity_id,
            Some(redacted_payload()),
            Status::Failed,
            Some(ErrorInfo::new(message, "PLUGIN_FAILED")),
            EventType::ProcessFailed,
        )
    }
}

fn now_iso8601() -> String {
    chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)
}

fn redacted_payload() -> Value {
    let mut m = serde_json::Map::new();
    m.insert("_payloadRedacted".to_string(), Value::Bool(true));
    Value::Object(m)
}
