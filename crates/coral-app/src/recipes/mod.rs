//! Recipe lifecycle and inventory workflow.

pub(crate) mod manager;
pub(crate) mod model;
pub(crate) mod service;
mod storage;

pub(crate) use model::RecipeName;
