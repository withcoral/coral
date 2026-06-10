//! Source lifecycle workflow, catalog inspection, and transport adapters.

pub(crate) mod catalog;
pub(crate) mod manager;
pub(crate) mod materialization;
pub(crate) mod model;
pub(crate) mod name;
pub(crate) mod runtime_package;
pub(crate) mod service;

pub(crate) use name::SourceName;
