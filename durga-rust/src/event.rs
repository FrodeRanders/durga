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
}
