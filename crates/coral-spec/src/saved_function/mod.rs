//! Saved function artifact parsing and static validation.
//!
//! Saved functions are source-neutral task capabilities. This module validates
//! `saved_function`
//! artifact shape and artifact-local invariants. Installed-source references,
//! SQL planning, result columns, and publish collisions against live catalog
//! objects are checked by the app/runtime layers.

mod model;
mod parser;
mod validation;

pub use model::{
    SavedFunctionArgumentSpec, SavedFunctionArgumentType, SavedFunctionImplementationSpec,
    SavedFunctionMcpPublishSpec, SavedFunctionPublishSpec, SavedFunctionSpec,
    SavedFunctionTableFunctionPublishSpec, SavedFunctionValidationSpec,
    SavedFunctionValidationValue,
};
pub use parser::parse_saved_function_yaml;
