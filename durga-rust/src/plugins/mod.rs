//! Rust plugin implementations. Each is a direct port of the corresponding
//! Java plugin in `durga-runtime`, honouring the same configuration string
//! format so a BPMN model scaffolds identically for either target.

mod field_filter;
mod field_router;
mod format_detector;
mod json_flatten;
mod json_schema_validator;
mod json_transform;
mod kv_enricher;
mod mask;
mod regex_extract;
mod string_template;
mod timestamp_normalize;
mod type_coercion;
mod uuid_inject;
mod window_counter;

pub use field_filter::FieldFilter;
pub use field_router::DeadLetterRouter;
pub use format_detector::{Detection, FormatDetector};
pub use json_flatten::JsonFlatten;
pub use json_schema_validator::JsonSchemaValidator;
pub use json_transform::JsonTransform;
pub use kv_enricher::KvEnricher;
pub use mask::Mask;
pub use regex_extract::RegexExtract;
pub use string_template::StringTemplate;
pub use timestamp_normalize::TimestampNormalize;
pub use type_coercion::TypeCoercion;
pub use uuid_inject::UuidInject;
pub use window_counter::WindowCounter;
