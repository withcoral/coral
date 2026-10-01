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
#![cfg_attr(
    test,
    expect(
        unused_crate_dependencies,
        reason = "wiremock and opentelemetry-proto are only used by integration test targets in this crate's dev-dependencies."
    )
)]

mod auth;
mod authorization_matrix;
/// Bootstrap entrypoints and local server assembly.
pub mod bootstrap;
mod catalog;
mod credentials;
mod encrypted_document;
pub mod features;
mod feedback;
mod functions;
mod gui_onboarding;
mod hash;
mod identity;
mod identity_specs;
mod oauth_resource;
mod outbound_url_policy;
mod query;
mod request_auth;
mod request_context;
mod search;
mod sources;
mod state;
mod storage;
mod task;
pub mod telemetry;
#[cfg(feature = "test-session-tokens")]
pub mod test_session_tokens;
#[cfg(test)]
mod test_support;
mod transport;
mod users;
mod workspace_mcp_urls;
mod workspaces;

pub use auth::{
    AuthServerError, AuthSettings, CoralAuthorizationServer, RunningCoralAuthorizationServer,
};
pub use bootstrap::{
    AppError, McpHttpServeConfig, READINESS_SERVICE_NAME, RunningServer, ServeSettings,
    ServerBuilder, ServerMode, SessionAuthSettings,
};
pub use coral_engine::{EngineExtensions, QuerySource};
pub use identity::{
    BearerAuthenticator, LocalPrincipalProvider, Principal, PrincipalId, PrincipalKind,
    PrincipalProvider, PrincipalProviderError,
};
pub use oauth_resource::{CanonicalOauthUrl, OauthUrlError};
pub use query::extensions::{
    AwsEngineExtensionsProvider, EngineExtensionsProvider, NoopEngineExtensionsProvider,
};
pub use request_auth::SessionPrincipalProvider;
pub use telemetry::{RunContext, RunErrorTelemetry, run_with_context, shutdown_tracing};
pub use workspace_mcp_urls::{
    McpWorkspaceSegment, PROTECTED_RESOURCE_METADATA_ROOT, WORKSPACE_ROUTE_SEGMENT,
    WorkspaceMcpUrls,
};
pub use workspaces::DEFAULT_WORKSPACE_ID;
