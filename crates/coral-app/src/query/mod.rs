//! Query orchestration and transport adapters.

pub(crate) mod attribution;
pub(crate) mod context;
pub(crate) mod extensions;
pub(crate) mod manager;
pub(crate) mod service;

pub(crate) use attribution::QueryAttribution;
pub(crate) use context::QueryContext;
