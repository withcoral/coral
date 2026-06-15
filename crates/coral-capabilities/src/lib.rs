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

mod compact;
mod model;
mod schema;

pub use compact::{
    COMPACT_INPUT_SCHEMA_BUDGET_BYTES, COMPACT_SQL_COLUMN_LIMIT, COMPACT_VALUE_SCHEMA_BUDGET_BYTES,
    CompactEntryFacts, CompactSqlBindingFacts, CompactSqlColumnFacts, CompactSqlInputFacts,
    SchemaRenderMode, compact_candidate_value, compact_entry_path_value, compact_entry_value,
    preferred_ref, preferred_sql_ref,
};
pub use model::*;
pub use schema::{
    SCHEMA_TRUNCATION_KEY, bound_schema_to_budget, code_mode_call_signature,
    code_mode_tool_input_schema, collect_nested_schema_defs, executable_schema_unresolved_refs,
    hoist_nested_schema_defs, insert_schema_defs, is_json_media_type, json_schema_primary_type,
    merge_schema_defs, provider_value_schema, schema_path_segments, schema_subtree_at_path,
    schema_subtree_at_segments, truncated_schema_paths,
};
