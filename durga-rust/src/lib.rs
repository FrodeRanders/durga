//! Durga runtime contracts and plugin implementations for the Rust
//! code-generation target.
//!
//! This crate is the Rust counterpart to the Java `durga-runtime` plugin
//! layer. Generated Rust workers depend on it directly and call the same
//! [`Plugin`] contract; there are no adapters onto the Java implementations.
//! Plugin configuration strings and the lifecycle event wire format are kept
//! byte-compatible with the Java side so a mixed Java/Rust process fleet
//! interoperates over the same Kafka topics.

pub mod event;
pub mod expr;
pub mod pipeline;
pub mod plugin;
pub mod result;
pub mod validation;
pub mod worker;
pub mod plugins;

pub use event::{ErrorInfo, EventType, ProcessEvent, Status};
pub use expr::eval_condition;
pub use plugin::{default_idempotency_key, Plugin, PluginError};
pub use result::{ErrorStrategy, OutputDisposition, PluginResult};
pub use validation::{plan_validation_output, ValidationCandidateOutput};
pub use worker::{plan_worker_output, Category, WorkerPlan};
