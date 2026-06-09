//! Provider-neutral capability contracts for Coral.
//!
//! This crate intentionally has no dependencies on other Coral crates. It owns
//! stable capability identity, invocation schemas, execution binding facts, and
//! diagnostics that downstream projection/runtime crates consume.

#![allow(
    missing_docs,
    reason = "This crate is mostly serializable contract data; field semantics are documented by the capability projection plan and tests."
)]
#![allow(
    clippy::module_name_repetitions,
    reason = "Contract type names intentionally include their domain for generated artifacts."
)]

mod model;
mod schema;

pub use model::*;
pub use schema::{
    code_mode_tool_input_schema, collect_nested_schema_defs, executable_schema_unresolved_refs,
    generated_tool_output_schema, hoist_nested_schema_defs, insert_schema_defs, is_json_media_type,
    json_schema_primary_type, merge_schema_defs,
};
