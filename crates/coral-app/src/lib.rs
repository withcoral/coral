//! Internal local server composition for the Coral app.
//!
//! `coral-app` is the local server composition root for Coral. It wires
//! together the generated transport API from `coral-api` with the
//! management-plane stores and the capability/export SQL runtime in
//! `coral-sql`.
//!
//! This crate is primarily an internal workspace boundary. CLI and integration
//! surfaces that need an in-process local server use [`ServerBuilder`] directly;
//! transport clients should normally enter through `coral-client`, not through
//! `coral-app`.
//!
//! # Main Internal Areas
//!
//! - [`ServerBuilder`] starts the local application server with filesystem
//!   config, managed source resources, and plaintext credential material storage.
//! - [`RunningServer`] owns the running local gRPC server task.
//! - [`AppError`] is the transport-neutral application error type used during
//!   bootstrap and management operations.
//! - `sources/` owns managed-source lifecycle and the reviewable installed
//!   source contract.
//! - `state/` owns persisted config-dir layout and config storage.
//! - `credentials/` owns credential-set identity and credential material
//!   persistence.
//! - `query/` owns query-time loading of generated SQL export bindings and
//!   `coral-sql` orchestration.
//!
//! # Crate Relationships
//!
//! - `coral-api` defines the generated gRPC surface.
//! - `coral-spec` owns declarative source-spec parsing, validation, and input
//!   discovery.
//! - `coral-sql` owns SQL projection execution over app-resolved export
//!   bindings.
//!
#![allow(
    dead_code,
    reason = "Internal server composition exposes seams that are exercised through sibling crates and integration tests."
)]

#[cfg(test)]
use wiremock as _;

/// Bootstrap entrypoints and local server assembly.
pub mod bootstrap;
mod capability;
mod code_mode;
mod credentials;
mod discovery;
pub mod features;
mod feedback;
mod graphql_documents;
mod identity;
mod query;
mod runtime;
mod sources;
mod state;
mod storage;
pub mod telemetry;
mod transport;
mod workspaces;

pub use bootstrap::{
    AppError, RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider,
};
pub use runtime::RuntimeExposureMode;
pub use telemetry::{RunContext, RunErrorTelemetry, run_with_context, shutdown_tracing};
pub use workspaces::DEFAULT_WORKSPACE_ID;
