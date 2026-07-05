//! Local-filesystem object store shared by the object-store plugins, mirroring
//! the Java `ObjectStoreSupport`. Only `file:` handles are supported.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use chrono::Utc;
use regex::Regex;
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::plugin::PluginError;
use crate::plugins::FormatDetector;

#[derive(Debug, Clone)]
pub struct StoredObject {
    /// Retained for parity with the Java record; not embedded in the handle.
    #[allow(dead_code)]
    pub id: String,
    pub uri: String,
    pub bytes: usize,
    pub sha256: String,
    pub created_at: String,
}

pub fn parse_config(config: &str) -> HashMap<String, String> {
    let mut values = HashMap::new();
    if config.trim().is_empty() {
        return values;
    }
    for part in config.split(';') {
        let Some(eq) = part.find('=') else { continue };
        if eq == 0 {
            continue;
        }
        let key = part[..eq].trim();
        let value = part[eq + 1..].trim();
        if !key.is_empty() {
            values.insert(key.to_string(), value.to_string());
        }
    }
    values
}

pub fn handle_field(config: &HashMap<String, String>) -> String {
    first_non_blank(&[config.get("handleField"), config.get("field")])
        .unwrap_or_else(|| "dataHandle".to_string())
}

fn root_path(config: &HashMap<String, String>) -> Result<PathBuf, PluginError> {
    let root = first_non_blank(&[config.get("store"), config.get("root"), config.get("rootUri")]);
    let root = match root {
        Some(r) => r,
        None => default_root(),
    };
    if let Some(stripped) = root.strip_prefix("file:") {
        return Ok(PathBuf::from(strip_file_slashes(stripped)));
    }
    if root.contains("://") {
        return Err("Only local file object stores are supported by this plugin".into());
    }
    Ok(PathBuf::from(root))
}

fn default_root() -> String {
    std::env::temp_dir()
        .join("durga-object-store")
        .to_string_lossy()
        .into_owned()
}

pub fn media_type(data: &[u8], file_name: Option<&str>) -> String {
    if let Some(name) = file_name {
        if let Some(mt) = probe_by_name(name) {
            return mt;
        }
    }
    FormatDetector::detect(data).media_type
}

pub fn store(payload: &[u8], config: &HashMap<String, String>) -> Result<StoredObject, PluginError> {
    let root = root_path(config)?;
    let prefix = sanitize_prefix(config.get("prefix").map(|s| s.as_str()).unwrap_or("objects"));
    let extension = extension(config.get("extension").map(|s| s.as_str()), payload, config.get("fileName").map(|s| s.as_str()));
    let id = Uuid::new_v4().to_string();
    let dir = root.join(&prefix);
    std::fs::create_dir_all(&dir)?;
    let target = dir.join(format!("{id}{extension}"));
    let canonical_root = root.canonicalize().unwrap_or(root.clone());
    // Best-effort containment check against the configured root.
    if !dir.starts_with(&canonical_root) && !dir.starts_with(&root) {
        return Err("Object target escapes configured root".into());
    }
    std::fs::write(&target, payload)?;
    let uri = format!("file://{}", target.to_string_lossy());
    Ok(StoredObject {
        id,
        uri,
        bytes: payload.len(),
        sha256: sha256_hex(payload),
        created_at: Utc::now().to_rfc3339(),
    })
}

pub fn read(uri: &str) -> Result<Vec<u8>, PluginError> {
    if uri.trim().is_empty() {
        return Err("Object handle URI is missing".into());
    }
    let Some(rest) = uri.strip_prefix("file:") else {
        return Err("Only file: object handles are supported by this plugin".into());
    };
    let path = strip_file_slashes(rest);
    Ok(std::fs::read(Path::new(&path))?)
}

fn extension(configured: Option<&str>, payload: &[u8], file_name: Option<&str>) -> String {
    if let Some(c) = configured.filter(|c| !c.trim().is_empty()) {
        return if c.starts_with('.') { c.to_string() } else { format!(".{c}") };
    }
    match media_type(payload, file_name).as_str() {
        "application/json" => ".json",
        "text/plain" => ".txt",
        "text/csv" => ".csv",
        "application/xml" | "text/xml" => ".xml",
        _ => ".bin",
    }
    .to_string()
}

fn sanitize_prefix(prefix: &str) -> String {
    static NON_SAFE: OnceLock<Regex> = OnceLock::new();
    let re = NON_SAFE.get_or_init(|| Regex::new(r"[^A-Za-z0-9_./-]").expect("valid regex"));
    let normalized = prefix.replace('\\', "/");
    let no_lead = normalized.trim_start_matches('/');
    let no_dotdot = no_lead.replace("..", "");
    let sanitized = re.replace_all(&no_dotdot, "_").into_owned();
    if sanitized.trim().is_empty() {
        "objects".to_string()
    } else {
        sanitized
    }
}

fn probe_by_name(file_name: &str) -> Option<String> {
    let lower = file_name.to_ascii_lowercase();
    let ext = lower.rsplit('.').next()?;
    let mt = match ext {
        "json" => "application/json",
        "txt" => "text/plain",
        "csv" => "text/csv",
        "xml" => "application/xml",
        _ => return None,
    };
    Some(mt.to_string())
}

pub fn sha256_hex(value: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value);
    hex::encode(hasher.finalize())
}

fn strip_file_slashes(path: &str) -> String {
    // "file:///abs" -> "/abs"; "file://host/abs" is not expected for local.
    let trimmed = path.trim_start_matches('/');
    format!("/{trimmed}")
}

fn first_non_blank(candidates: &[Option<&String>]) -> Option<String> {
    candidates
        .iter()
        .flatten()
        .find(|s| !s.trim().is_empty())
        .map(|s| s.to_string())
}
