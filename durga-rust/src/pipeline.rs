//! Utilities shared by plugin implementations: dot-notation field access on
//! JSON values, mirroring the Java `PipelinePlugin.fieldAt` / `setFieldAt`.

use serde_json::{Map, Value};

const MAX_PATH_SEGMENTS: usize = 32;

/// Reads a field by dot-notation path (`"customer.email"`) for nested access.
/// Returns `None` if any intermediate is missing or not an object.
pub fn field_at<'a>(node: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = node;
    for segment in path.split('.') {
        match current {
            Value::Object(map) => current = map.get(segment)?,
            _ => return None,
        }
    }
    Some(current)
}

/// Sets a value at a dot-notation path, creating intermediate objects as
/// needed and replacing any non-object intermediate with a fresh object
/// (matching the Java behaviour).
pub fn set_field_at(root: &mut Value, path: &str, value: Value) {
    let segments: Vec<&str> = path.split('.').collect();
    if segments.is_empty() {
        return;
    }
    if !root.is_object() {
        *root = Value::Object(Map::new());
    }
    let mut current = root;
    for segment in &segments[..segments.len() - 1] {
        let map = current.as_object_mut().expect("ensured object above");
        let child = map.entry((*segment).to_string()).or_insert(Value::Null);
        if !child.is_object() {
            *child = Value::Object(Map::new());
        }
        current = child;
    }
    if let Value::Object(map) = current {
        map.insert(segments[segments.len() - 1].to_string(), value);
    }
}

/// Number of dot-separated segments in a path (bounded like the Java side).
pub fn path_segment_count(path: &str) -> usize {
    path.split('.').take(MAX_PATH_SEGMENTS + 1).count()
}

/// Shallow merge: fields in `over` take precedence over `base`. Both must be
/// JSON objects.
pub fn shallow_merge(base: &Value, over: &Value) -> Value {
    let mut result = Map::new();
    if let Value::Object(map) = base {
        for (k, v) in map {
            result.insert(k.clone(), v.clone());
        }
    }
    if let Value::Object(map) = over {
        for (k, v) in map {
            result.insert(k.clone(), v.clone());
        }
    }
    Value::Object(result)
}
