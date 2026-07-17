//! Relational database backend registration through `datafusion-table-providers`.

mod catalog;
mod columns;
mod registry;
mod source;

pub use registry::DatabasePoolRegistry;
pub(crate) use source::compile_manifest;
