//! Query orchestration and transport adapters.

pub(crate) mod context;
pub(crate) mod extensions;
pub(crate) mod input_resolver;
pub(crate) mod manager;
pub(crate) mod service;

pub(crate) use context::QueryContext;
