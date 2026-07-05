//! Rust plugin implementations. Each is a direct port of the corresponding
//! Java plugin in `durga-runtime`, honouring the same configuration string
//! format so a BPMN model scaffolds identically for either target.

mod json_transform;
mod type_coercion;

pub use json_transform::JsonTransform;
pub use type_coercion::TypeCoercion;
