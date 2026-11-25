//! The one model that the Organization's Sub-agents use, as an Organization
//! Owner sets it.
//!
//! # Interface
//!
//! [`AgentModels`] reads, sets, changes and removes the model, and denies a
//! caller who is not an enabled Organization Owner. One validation, inside
//! them, checks the [`EnteredSettings`]. A change joins its caller's
//! transaction, which turns the Sub-agent off in every Workspace when the
//! model's id changes. [`AgentModels::read_for_workspace`] shows the model to
//! a Workspace's Owner, [`AgentModels::active_in_transaction`] is the model as
//! a Turn uses it, and [`AgentModels::test`] asks a model for one reply before
//! anybody depends on it.
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
mod tester;

#[cfg(test)]
mod tests;

pub use agent_models::{
    ActiveModel, AgentModel, AgentModels, ApiKeyChange, ModelChange, current_id_in_transaction,
};
pub use error::{Error, InvalidSetting};
pub use settings::{
    CONTEXT_WINDOW_MIN, EnteredSettings, ModelProtocol, ModelSettings, REQUEST_ARG_NAMES,
};
pub use tester::{ModelTester, TestFailure, TestKey};
