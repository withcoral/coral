//! Internal local server composition for the Coral app.
//!
//! `coral-app` is the local server composition root for Coral. It wires
//! together the generated transport API from `coral-api` with the
//! management-plane stores and the data plane in `coral-engine`.
//!
//! This crate is primarily an internal workspace boundary. Sibling crates such
//! as `coral-client` use its bootstrap seam, but end-user code should normally
//! enter through `coral-client`, not through `coral-app` directly.
//!
//! # Main Internal Areas
//!
//! - [`ServerBuilder`] starts the local application server with filesystem
//!   config, managed source resources, and plaintext source-secret storage.
//! - [`RunningServer`] owns the running local gRPC server task.
//! - [`AppError`] is the transport-neutral application error type used during
//!   bootstrap and management operations.
//! - `sources/` owns managed-source lifecycle and the reviewable installed
//!   source contract.
//! - `state/` owns persisted config-dir layout, config storage, and
//!   source-secret persistence.
//! - `query/` owns query-time source loading and `coral-engine`
//!   orchestration.
//!
//! # Crate Relationships
//!
//! - `coral-api` defines the generated gRPC surface.
//! - `coral-spec` owns declarative source-spec parsing, validation, and input
//!   discovery.
//! - `coral-engine` owns the data plane: backend registration, `DataFusion`
//!   runtime assembly, and `SQL` execution over validated specs.
//!
/// Bootstrap entrypoints and local server assembly.
pub mod bootstrap;
mod query;
mod sources;
mod state;
mod storage;
mod workspaces;

pub use bootstrap::{AppError, RunningServer, ServerBuilder};
pub use workspaces::DEFAULT_WORKSPACE_ID;
