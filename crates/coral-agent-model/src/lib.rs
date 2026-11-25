//! The one model that the Organization's Sub-agents use, as an Organization
//! Owner sets it.
//!
//! # Interface
//!
//! [`AgentModels`] reads, sets, changes and removes the model. Every method
//! denies a caller who is not an enabled Organization Owner.
//! One validation, inside them, checks the [`EnteredSettings`].
//!
//! # Invariants
//!
//! - The Organization has at most one model.
//! - A model's [`id`](AgentModel::id) names where its requests go. It changes
//!   when the base URL does, and when a model is removed and set again. No
//!   other change issues a new one.
//! - An API key is sealed for the model's id and base URL, so it opens for no
//!   other destination. A new base URL drops it, and no method returns it.
//! - A change takes the etag its caller read. An etag read before a removal
//!   admits no model set after it.
//!
//! The Sub-agent's crate depends on the crate that serves the API, so the
//! settings that both use live here.

mod agent_models;
mod error;
mod repository;
mod settings;

#[cfg(test)]
mod tests;

pub use agent_models::{AgentModel, AgentModels, ApiKeyChange};
pub use error::{Error, InvalidSetting};
pub use settings::{
    CONTEXT_WINDOW_MIN, EnteredSettings, ModelProtocol, ModelSettings, REQUEST_ARG_NAMES,
};
