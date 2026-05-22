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
//!   config, managed source resources, and plaintext credential material storage.
//! - [`RunningServer`] owns the running local gRPC server task.
//! - [`AppError`] is the transport-neutral application error type used during
//!   bootstrap and management operations.
//! - `sources/` owns managed-source lifecycle and the reviewable installed
//!   source contract.
//! - `state/` owns persisted config-dir layout and config storage.
//! - `credentials/` owns credential-set identity and credential material
//!   persistence.
//! - `query/` owns query-time source loading and `coral-engine`
//!   orchestration.
//! - `catalog/` owns workspace-scoped discovery semantics over query-visible
//!   table metadata.
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
mod catalog;
mod credentials;
mod feedback;
mod identity;
mod query;
mod sources;
mod state;
mod storage;
pub mod telemetry;
mod transport;
mod workspaces;

pub use bootstrap::{
    AppError, RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider,
};
pub use coral_engine::{EngineExtensions, QuerySource};
pub use query::extensions::{
    AwsEngineExtensionsProvider, EngineExtensionsProvider, NoopEngineExtensionsProvider,
};
pub use telemetry::{RunContext, RunErrorTelemetry, run_with_context, shutdown_tracing};
pub use workspaces::DEFAULT_WORKSPACE_ID;

/// Clears the persistent query cache for the current Coral config directory.
pub fn clear_query_cache(source_name: Option<&str>) -> Result<(), AppError> {
    let env = bootstrap::env::AppEnvironment::discover();
    let layout = state::AppStateLayout::discover(env.coral_config_dir_override())?;
    layout.ensure()?;
    let config_store = state::ConfigStore::new(layout.clone());
    let cache = storage::cache::QueryCache::new(
        layout,
        storage::cache::cache_settings_from_config(config_store.cache_config()?),
    );
    match source_name {
        Some(source_name) => cache.clear_source(source_name)?,
        None => cache.clear_all()?,
    }
    Ok(())
}
