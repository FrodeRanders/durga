//! Build-time BPMN model enrichment for the Durga Rust code-generation target.
//!
//! The Rust counterpart to the Java `ModelEnricher`: it captures the source of
//! locally-developed custom plugins and embeds it into the process's BPMN model
//! as `durga:source` elements (with `customImpl`/`customSource`/`customHash`
//! Camunda properties), so the model published by each worker is self-contained
//! and round-trip regenerable. The embedded format is byte-for-byte compatible
//! with what the Java `ModelEnricher` writes and reads back.
//!
//! Typical use from a generated project's `build.rs`:
//!
//! ```no_run
//! let entries = vec![
//!     durga_rust_build::CustomSource::from_file(
//!         "validate_data", "ValidateData", "src/custom/validate_data.rs",
//!     ).unwrap(),
//! ];
//! durga_rust_build::enrich_to_out_dir("model.bpmn", &entries).unwrap();
//! ```

use std::fs;
use std::path::Path;

use sha2::{Digest, Sha256};
use xmltree::{Element, XMLNode};

/// Durga source-embedding namespace, matching the Java `ModelEnricher`.
pub const DURGA_NS: &str = "http://gautelis.org/durga/schema/1.0";

/// One custom-plugin source captured for a BPMN task.
#[derive(Debug, Clone)]
pub struct CustomSource {
    /// BPMN service-task id whose implementation this is.
    pub task_id: String,
    /// Rust struct name implementing the plugin.
    pub impl_name: String,
    /// Path stored in the model, relative to the project root.
    pub rel_path: String,
    /// Source text.
    pub content: String,
}

impl CustomSource {
    /// Reads a custom source file from disk.
    pub fn from_file(
        task_id: impl Into<String>,
        impl_name: impl Into<String>,
        rel_path: impl Into<String>,
    ) -> std::io::Result<CustomSource> {
        let rel_path = rel_path.into();
        let content = fs::read_to_string(&rel_path)?;
        Ok(CustomSource {
            task_id: task_id.into(),
            impl_name: impl_name.into(),
            rel_path,
            content,
        })
    }

    /// SHA-256 hex of the source content (matches the Java embedded-hash).
    pub fn hash(&self) -> String {
        sha256_hex(self.content.as_bytes())
    }
}

/// Enriches the model at `model_rel_path` with `entries` and writes the result
/// to `$OUT_DIR/<basename>`, returning the written path. Intended for `build.rs`
/// (reads `OUT_DIR` from the environment). When `entries` is empty the model is
/// copied through unchanged, so a generated `lib.rs` can always
/// `include_str!(concat!(env!("OUT_DIR"), "/model.bpmn"))`.
pub fn enrich_to_out_dir(model_rel_path: &str, entries: &[CustomSource]) -> Result<String, String> {
    let out_dir = std::env::var("OUT_DIR").map_err(|_| "OUT_DIR not set".to_string())?;
    let raw = fs::read_to_string(model_rel_path)
        .map_err(|e| format!("failed to read {model_rel_path}: {e}"))?;
    let enriched = if entries.is_empty() {
        raw
    } else {
        enrich_model(&raw, entries)?
    };
    let file_name = Path::new(model_rel_path)
        .file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .unwrap_or_else(|| "model.bpmn".to_string());
    let target = Path::new(&out_dir).join(&file_name);
    fs::write(&target, enriched).map_err(|e| format!("failed to write {target:?}: {e}"))?;
    Ok(target.to_string_lossy().into_owned())
}

/// Enriches a BPMN model string: for each entry, embeds the source as a
/// `durga:source` element under the matching service task's `extensionElements`
/// and sets the `customImpl`/`customSource`/`customHash` Camunda properties.
pub fn enrich_model(bpmn_xml: &str, entries: &[CustomSource]) -> Result<String, String> {
    let mut root = Element::parse(bpmn_xml.as_bytes())
        .map_err(|e| format!("failed to parse BPMN: {e}"))?;

    // Declare the durga namespace on the root definitions element.
    root.attributes.insert("xmlns:durga".to_string(), DURGA_NS.to_string());

    for entry in entries {
        let Some(task) = find_service_task_mut(&mut root, &entry.task_id) else {
            return Err(format!("service task '{}' not found in model", entry.task_id));
        };
        let ext = ensure_child(task, Some("bpmn"), "extensionElements");
        let props = ensure_child(ext, Some("camunda"), "properties");
        set_camunda_property(props, "customImpl", &entry.impl_name);
        set_camunda_property(props, "customSource", &entry.rel_path);
        set_camunda_property(props, "customHash", &entry.hash());

        remove_durga_sources(ext);
        let mut source = Element::new("source");
        source.prefix = Some("durga".to_string());
        source.attributes.insert("path".to_string(), entry.rel_path.clone());
        source.attributes.insert("hash".to_string(), entry.hash());
        source.children.push(XMLNode::CData(entry.content.clone()));
        ext.children.push(XMLNode::Element(source));
    }

    let mut buf = Vec::new();
    root.write(&mut buf).map_err(|e| format!("failed to serialize BPMN: {e}"))?;
    String::from_utf8(buf).map_err(|e| format!("BPMN is not valid UTF-8: {e}"))
}

fn find_service_task_mut<'a>(elem: &'a mut Element, task_id: &str) -> Option<&'a mut Element> {
    if elem.name == "serviceTask" && elem.attributes.get("id").map(|s| s.as_str()) == Some(task_id) {
        return Some(elem);
    }
    for child in &mut elem.children {
        if let XMLNode::Element(e) = child {
            if let Some(found) = find_service_task_mut(e, task_id) {
                return Some(found);
            }
        }
    }
    None
}

fn ensure_child<'a>(parent: &'a mut Element, prefix: Option<&str>, name: &str) -> &'a mut Element {
    let idx = parent.children.iter().position(|c| matches!(c, XMLNode::Element(e) if e.name == name));
    let idx = match idx {
        Some(i) => i,
        None => {
            let mut child = Element::new(name);
            child.prefix = prefix.map(|p| p.to_string());
            parent.children.push(XMLNode::Element(child));
            parent.children.len() - 1
        }
    };
    match &mut parent.children[idx] {
        XMLNode::Element(e) => e,
        _ => unreachable!("index refers to an element"),
    }
}

fn set_camunda_property(props: &mut Element, name: &str, value: &str) {
    for child in &mut props.children {
        if let XMLNode::Element(e) = child {
            if e.name == "property" && e.attributes.get("name").map(|s| s.as_str()) == Some(name) {
                e.attributes.insert("value".to_string(), value.to_string());
                return;
            }
        }
    }
    let mut prop = Element::new("property");
    prop.prefix = Some("camunda".to_string());
    prop.attributes.insert("name".to_string(), name.to_string());
    prop.attributes.insert("value".to_string(), value.to_string());
    props.children.push(XMLNode::Element(prop));
}

fn remove_durga_sources(ext: &mut Element) {
    ext.children.retain(|c| !matches!(c, XMLNode::Element(e) if e.name == "source" && e.prefix.as_deref() == Some("durga")));
}

fn sha256_hex(data: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(data);
    hex::encode(hasher.finalize())
}

#[cfg(test)]
mod tests {
    use super::*;

    const MODEL: &str = r#"<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:camunda="http://camunda.org/schema/1.0/bpmn" id="Definitions_1">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:serviceTask id="validate_data" name="Validate Data">
      <bpmn:extensionElements>
        <camunda:properties>
          <camunda:property name="plugin" value="custom" />
        </camunda:properties>
      </bpmn:extensionElements>
    </bpmn:serviceTask>
    <bpmn:serviceTask id="plain" name="Plain" />
  </bpmn:process>
</bpmn:definitions>"#;

    fn entry() -> CustomSource {
        CustomSource {
            task_id: "validate_data".into(),
            impl_name: "ValidateData".into(),
            rel_path: "src/custom/validate_data.rs".into(),
            content: "pub struct ValidateData;\n// custom, has a , comma and <angle> brackets\n".into(),
        }
    }

    #[test]
    fn embeds_source_and_properties() {
        let out = enrich_model(MODEL, &[entry()]).unwrap();
        assert!(out.contains("xmlns:durga=\"http://gautelis.org/durga/schema/1.0\""));
        assert!(out.contains("durga:source"));
        assert!(out.contains("path=\"src/custom/validate_data.rs\""));
        assert!(out.contains(&format!("hash=\"{}\"", entry().hash())));
        assert!(out.contains("<![CDATA["));
        assert!(out.contains("custom, has a , comma and <angle> brackets"));
        assert!(out.contains("name=\"customImpl\""));
        assert!(out.contains("value=\"ValidateData\""));
        assert!(out.contains("name=\"customHash\""));
    }

    #[test]
    fn creates_extension_elements_when_absent() {
        let e = CustomSource {
            task_id: "plain".into(),
            impl_name: "Plain".into(),
            rel_path: "src/custom/plain.rs".into(),
            content: "pub struct Plain;\n".into(),
        };
        let out = enrich_model(MODEL, &[e]).unwrap();
        // The self-closing 'plain' task gains extensionElements + one durga:source.
        let root = Element::parse(out.as_bytes()).unwrap();
        assert_eq!(count_sources(&root), 1);
        assert!(out.contains("path=\"src/custom/plain.rs\""));
    }

    #[test]
    fn is_idempotent_no_duplicate_sources() {
        let once = enrich_model(MODEL, &[entry()]).unwrap();
        let twice = enrich_model(&once, &[entry()]).unwrap();
        let root = Element::parse(twice.as_bytes()).unwrap();
        assert_eq!(count_sources(&root), 1, "re-enriching must not duplicate durga:source");
    }

    #[test]
    fn errors_on_unknown_task() {
        let e = CustomSource {
            task_id: "nope".into(),
            impl_name: "X".into(),
            rel_path: "x.rs".into(),
            content: "".into(),
        };
        assert!(enrich_model(MODEL, &[e]).is_err());
    }

    fn count_sources(elem: &Element) -> usize {
        let mut n = 0;
        if elem.name == "source" && elem.prefix.as_deref() == Some("durga") {
            n += 1;
        }
        for c in &elem.children {
            if let XMLNode::Element(e) = c {
                n += count_sources(e);
            }
        }
        n
    }
}
