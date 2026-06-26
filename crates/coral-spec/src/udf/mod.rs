//! Function artifact parsing and static validation.
//!
//! Functions are source-neutral task capabilities. This module validates
//! function artifact shape and artifact-local invariants. Installed-source
//! references, SQL planning, result columns, and publish collisions against live
//! catalog objects are checked by the app/runtime layers.

mod model;
mod parser;
mod validation;

pub use model::{FunctionCoralSqlImplementationSpec, FunctionImplementationSpec, FunctionSpec};
pub use parser::parse_function_sql;
