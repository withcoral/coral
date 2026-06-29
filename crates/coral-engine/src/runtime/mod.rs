//! Source-runtime orchestration: registration into `DataFusion`, system catalog
//! tables, and schema plumbing.

pub(crate) mod catalog;
pub(crate) mod dependent_join;
pub(crate) mod error;
pub(crate) mod json;
pub(crate) mod memory;
pub(crate) mod parameter_inference;
pub(crate) mod pattern_validator;
pub(crate) mod query;
pub(crate) mod query_planner;
pub(crate) mod registry;
pub(crate) mod schema_provider;
pub(crate) mod scoped_table_functions;
pub(crate) mod source_functions;
pub(crate) mod udf_calls;
pub(crate) mod udfs;
