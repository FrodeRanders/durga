//! Port of `org.gautelis.durga.plugins.WindowCounter` (plugin id
//! `window-counter`).
//!
//! Counts messages within a wall-clock time window and emits a summary record
//! when the window closes (i.e. on the first message of the next window).
//! Optionally groups counts by a field. Like the Java version, the window is
//! expressed in seconds and the counter carries state across calls, so the
//! generated worker holds one instance per task.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{Map, Value};

use crate::pipeline::field_at;
use crate::plugin::{Plugin, PluginError};

const MAX_GROUPS: usize = 10_000;
const MAX_GROUP_VALUE_LEN: usize = 256;

struct WindowState {
    current_window_start: i64,
    total_count: u64,
    group_counts: BTreeMap<String, u64>,
}

pub struct WindowCounter {
    state: Mutex<WindowState>,
}

impl WindowCounter {
    pub fn new() -> Self {
        WindowCounter {
            state: Mutex::new(WindowState {
                current_window_start: -1,
                total_count: 0,
                group_counts: BTreeMap::new(),
            }),
        }
    }

    fn accept(&self, json: &str, window_ms: i64, group_by: Option<&str>) -> Option<String> {
        let now = now_millis();
        let window_bucket = (now / window_ms) * window_ms;

        let mut state = self.state.lock().expect("window state mutex poisoned");

        let mut summary = None;
        if state.current_window_start >= 0 && window_bucket != state.current_window_start {
            summary = Some(build_summary(&state, window_ms, group_by));
            state.total_count = 0;
            state.group_counts.clear();
        }

        state.current_window_start = window_bucket;
        state.total_count += 1;

        if let Some(group_by) = group_by.filter(|g| !g.is_empty()) {
            let group = match serde_json::from_str::<Value>(json) {
                Ok(node) => match field_at(&node, group_by) {
                    Some(v) if !v.is_null() => match v {
                        Value::String(s) => s.clone(),
                        other => other.to_string(),
                    },
                    _ => "_null_".to_string(),
                },
                Err(_) => "_parse_error_".to_string(),
            };
            merge_group(&mut state.group_counts, group);
        }

        summary
    }
}

impl Default for WindowCounter {
    fn default() -> Self {
        Self::new()
    }
}

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn merge_group(group_counts: &mut BTreeMap<String, u64>, mut group: String) {
    if group.len() > MAX_GROUP_VALUE_LEN {
        group.truncate(MAX_GROUP_VALUE_LEN);
    }
    if !group_counts.contains_key(&group) && group_counts.len() >= MAX_GROUPS {
        group = "_overflow_".to_string();
    }
    *group_counts.entry(group).or_insert(0) += 1;
}

fn build_summary(state: &WindowState, window_ms: i64, group_by: Option<&str>) -> String {
    let mut summary = Map::new();
    summary.insert("windowStart".to_string(), Value::from(state.current_window_start));
    summary.insert("windowEnd".to_string(), Value::from(state.current_window_start + window_ms));
    summary.insert("totalCount".to_string(), Value::from(state.total_count));
    if group_by.map(|g| !g.is_empty()).unwrap_or(false) && !state.group_counts.is_empty() {
        let mut groups = Map::new();
        for (k, v) in &state.group_counts {
            groups.insert(k.clone(), Value::from(*v));
        }
        summary.insert("groupCounts".to_string(), Value::Object(groups));
    }
    Value::Object(summary).to_string()
}

fn parse_config(config: &str) -> (i64, Option<String>) {
    let mut window_secs: i64 = 60;
    let mut group_by = None;
    for part in config.split_whitespace() {
        let Some(eq) = part.find('=') else { continue };
        let key = part[..eq].trim();
        let val = part[eq + 1..].trim();
        match key {
            "window" => {
                if let Ok(w) = val.parse::<i64>() {
                    window_secs = w;
                }
            }
            "groupBy" => group_by = Some(val.to_string()),
            _ => {}
        }
    }
    (window_secs.max(1) * 1000, group_by)
}

impl Plugin for WindowCounter {
    fn execute_str(&self, payload: &str, config: &str) -> Result<Option<String>, PluginError> {
        let (window_ms, group_by) = parse_config(config);
        Ok(self.accept(payload, window_ms, group_by.as_deref()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buffers_within_window_then_summarizes_on_boundary() {
        let wc = WindowCounter::new();
        // Same window: first message buffers (no summary).
        assert!(wc.accept(r#"{"tier":"gold"}"#, 10_000, Some("tier")).is_none());
        assert!(wc.accept(r#"{"tier":"gold"}"#, 10_000, Some("tier")).is_none());
        // Force a new window by simulating a later bucket via a tiny window.
        let wc2 = WindowCounter::new();
        assert!(wc2.accept(r#"{"tier":"gold"}"#, 1, Some("tier")).is_none());
        std::thread::sleep(std::time::Duration::from_millis(3));
        let summary = wc2.accept(r#"{"tier":"silver"}"#, 1, Some("tier"));
        assert!(summary.is_some());
        let v: Value = serde_json::from_str(&summary.unwrap()).unwrap();
        assert!(v.get("totalCount").is_some());
        assert!(v.get("groupCounts").is_some());
    }
}
