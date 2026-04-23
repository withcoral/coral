//! Source-runtime orchestration: registration into `DataFusion`, system catalog
//! tables, and schema plumbing.

pub(crate) mod catalog;
pub(crate) mod error;
pub(crate) mod json;
pub(crate) mod query;
pub(crate) mod registry;
pub(crate) mod schema_provider;
