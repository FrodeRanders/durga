//! Pure decision logic for a generated task worker: given an input event, the
//! plugin outcome, the plugin category, and whether the task is terminal,
//! compute which lifecycle events to emit, what (if anything) to forward, and
//! whether to route to the dead-letter queue.
//!
//! The generated Kafka glue is intentionally thin — it consumes a message,
//! runs the plugin, calls [`plan_worker_output`], then emits/forwards/acks
//! accordingly. Keeping the branching here makes it unit-testable without a
//! broker and keeps it faithful to the Java `pluginExecutorClass`.

use serde_json::{Map, Value};

use crate::event::ProcessEvent;
use crate::result::{ErrorStrategy, OutputDisposition, PluginResult};

/// Governed plugin categories, controlling the lifecycle emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Category {
    Route,
    Validate,
    Aggregate,
    Other,
}

impl Category {
    pub fn from_str(s: &str) -> Category {
        match s {
            "route" => Category::Route,
            "validate" => Category::Validate,
            "aggregate" => Category::Aggregate,
            _ => Category::Other,
        }
    }
}

/// The plan the worker glue should carry out.
#[derive(Debug, Default)]
pub struct WorkerPlan {
    /// Lifecycle events to publish to the process-events topic.
    pub emits: Vec<ProcessEvent>,
    /// Event to forward to the next task's input topic (None = terminal or held).
    pub forward: Option<ProcessEvent>,
    /// Dead-letter record to publish, if any.
    pub dlq: Option<String>,
    /// True when the message should simply be acked with no output (skip).
    pub ack_only: bool,
}

/// Computes the worker plan. `result` is `Err(message)` when the plugin threw.
pub fn plan_worker_output(
    input: &ProcessEvent,
    activity_id: &str,
    category: Category,
    terminal: bool,
    result: Result<PluginResult, String>,
) -> WorkerPlan {
    let mut plan = WorkerPlan::default();

    let pr = match result {
        Ok(pr) => pr,
        Err(message) => return fail_plan(input, activity_id, category, &message),
    };

    match pr.error_strategy() {
        Some(ErrorStrategy::Skip) => {
            plan.ack_only = true;
            return plan;
        }
        Some(ErrorStrategy::Dlq) => {
            plan.dlq = Some(pr.side_effect_description().unwrap_or("Plugin routed to DLQ").to_string());
            return plan;
        }
        Some(ErrorStrategy::Fail) => {
            let msg = pr.side_effect_description().unwrap_or("Plugin requested failure").to_string();
            return fail_plan(input, activity_id, category, &msg);
        }
        None => {}
    }

    let output = pr.output();

    // Aggregate that keeps its window open (no output) terminally completes the
    // absorbed instance at the aggregate activity.
    if category == Category::Aggregate && output.is_none() {
        let payload = input.payload.clone().unwrap_or(Value::Null);
        plan.emits.push(input.process_completed(activity_id, payload));
        return plan;
    }

    let out_payload = compute_output_payload(input, output, pr.disposition());

    let completion = match category {
        Category::Route => input.gateway_taken(activity_id, out_payload.clone()),
        _ => input.activity_completed(activity_id, out_payload.clone()),
    };
    plan.emits.push(completion.clone());

    if terminal {
        plan.emits.push(input.process_completed(activity_id, out_payload));
    } else {
        plan.forward = Some(completion);
    }
    plan
}

fn fail_plan(input: &ProcessEvent, activity_id: &str, category: Category, message: &str) -> WorkerPlan {
    let mut plan = WorkerPlan::default();
    let event = if category == Category::Validate {
        input.activity_escalated(activity_id, message)
    } else {
        input.process_failed(activity_id, message)
    };
    plan.emits.push(event);
    plan.dlq = Some(message.to_string());
    plan
}

/// Mirrors the generated worker's payload guardrail: only a `PAYLOAD`
/// disposition with a JSON object replaces the payload; otherwise the input
/// payload is preserved and any output is annotated under `_pluginOutput`.
pub(crate) fn compute_output_payload(input: &ProcessEvent, output: Option<&[u8]>, disposition: OutputDisposition) -> Value {
    let input_payload = input.payload.clone().unwrap_or(Value::Null);
    let Some(bytes) = output.filter(|b| !b.is_empty()) else {
        return input_payload;
    };
    let text = String::from_utf8_lossy(bytes);

    if disposition == OutputDisposition::Payload {
        if let Ok(node) = serde_json::from_str::<Value>(&text) {
            if node.is_object() {
                return node;
            }
        }
    }

    // Preserve the input payload; retain the plugin output for lineage.
    let mut annotated = match input_payload {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    annotated.insert("_pluginOutput".to_string(), Value::String(text.into_owned()));
    Value::Object(annotated)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{EventType, ProcessEvent, Status};
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

    fn ok_object(json: &str) -> Result<PluginResult, String> {
        Ok(PluginResult::success(Some(json.as_bytes().to_vec()), "k"))
    }

    #[test]
    fn transform_forwards_object_payload() {
        let plan = plan_worker_output(&sample(), "coerce", Category::Other, false, ok_object(r#"{"order_id":7,"amount":900.0}"#));
        assert_eq!(plan.emits.len(), 1);
        assert_eq!(plan.emits[0].event_type, Some(EventType::ActivityCompleted));
        let fwd = plan.forward.expect("should forward");
        assert_eq!(fwd.payload.unwrap()["amount"], 900.0);
        assert!(plan.dlq.is_none());
    }

    #[test]
    fn non_object_output_preserves_payload() {
        let plan = plan_worker_output(&sample(), "route", Category::Route, false,
            Ok(PluginResult::passthrough(Some(b"high".to_vec()), "k")));
        let fwd = plan.forward.expect("forward");
        assert_eq!(fwd.event_type, Some(EventType::GatewayTaken));
        let payload = fwd.payload.unwrap();
        assert_eq!(payload["order_id"], 7); // preserved
        assert_eq!(payload["_pluginOutput"], "high");
    }

    #[test]
    fn validate_failure_escalates() {
        let plan = plan_worker_output(&sample(), "validate", Category::Validate, false,
            Err("$: missing required field 'order_id'".into()));
        assert_eq!(plan.emits[0].event_type, Some(EventType::ActivityEscalated));
        assert_eq!(plan.emits[0].error.as_ref().unwrap().code.as_deref(), Some("VALIDATION_FAILED"));
        assert!(plan.dlq.is_some());
        assert!(plan.forward.is_none());
    }

    #[test]
    fn plugin_failure_fails_process() {
        let plan = plan_worker_output(&sample(), "enrich", Category::Other, false, Err("boom".into()));
        assert_eq!(plan.emits[0].event_type, Some(EventType::ProcessFailed));
        assert_eq!(plan.emits[0].error.as_ref().unwrap().code.as_deref(), Some("PLUGIN_FAILED"));
    }

    #[test]
    fn aggregate_absorbed_completes_terminally() {
        let plan = plan_worker_output(&sample(), "aggregate", Category::Aggregate, false,
            Ok(PluginResult::success(None, "k")));
        assert_eq!(plan.emits.len(), 1);
        assert_eq!(plan.emits[0].event_type, Some(EventType::ProcessCompleted));
        assert!(plan.forward.is_none());
    }

    #[test]
    fn terminal_task_completes_process() {
        let plan = plan_worker_output(&sample(), "last", Category::Other, true, ok_object(r#"{"done":true}"#));
        let types: Vec<_> = plan.emits.iter().map(|e| e.event_type).collect();
        assert_eq!(types, vec![Some(EventType::ActivityCompleted), Some(EventType::ProcessCompleted)]);
        assert!(plan.forward.is_none());
    }

    #[test]
    fn skip_acks_only() {
        let plan = plan_worker_output(&sample(), "x", Category::Other, false,
            Ok(PluginResult::skip("k", "not applicable")));
        assert!(plan.ack_only);
        assert!(plan.emits.is_empty());
    }
}
