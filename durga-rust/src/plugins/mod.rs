//! Rust plugin implementations. Each is a direct port of the corresponding
//! Java plugin in `durga-runtime`, honouring the same configuration string
//! format so a BPMN model scaffolds identically for either target.

mod field_filter;
mod field_router;
mod json_flatten;
mod json_schema_validator;
mod json_transform;
mod kv_enricher;
mod mask;
mod type_coercion;
mod uuid_inject;
mod window_counter;

pub use field_filter::FieldFilter;
pub use field_router::DeadLetterRouter;
pub use json_flatten::JsonFlatten;
pub use json_schema_validator::JsonSchemaValidator;
pub use json_transform::JsonTransform;
pub use kv_enricher::KvEnricher;
pub use mask::Mask;
pub use type_coercion::TypeCoercion;
pub use uuid_inject::UuidInject;
pub use window_counter::WindowCounter;
