//! Application composition for the Coral runtime.
//!
//! [`start_runtime`] is what the `coral` executable calls. `coral-app` owns
//! the fresh-state fence, database migration, current
//! authentication protocol machinery, listener lifecycle, readiness, and
//! telemetry exporter configuration. Product behavior belongs
//! to transport-neutral domain crates.

#[cfg(test)]
use tonic_health as _; // Used by the separate listener integration test target.

mod auth;
/// Bootstrap entrypoints and local server assembly.
pub mod bootstrap;
mod config_toml;
mod oauth_resource;
mod state;
pub mod telemetry;
mod workspace_mcp_urls;

pub use auth::{
    AuthServerError, AuthSettings, CoralAuthorizationServer, RunningCoralAuthorizationServer,
    WorkspaceAuthentication,
};
pub use bootstrap::{
    AppError, LOCAL_OUTBOUND_DEFAULTS, Launch, ListenerReadiness, McpHttpServeConfig,
    PreparedServerBuilder, RunningRuntime, RunningServer, RuntimeError, ServeSettings,
    ServerBuilder, ServerMode, SessionAuthSettings, start_runtime,
};
pub use config_toml::ConfigDecodeError;
pub use oauth_resource::{CanonicalOauthUrl, OauthUrlError};
pub use telemetry::{RunContext, RunErrorTelemetry, run_with_context, shutdown_tracing};
pub use workspace_mcp_urls::{PROTECTED_RESOURCE_METADATA_ROOT, WorkspaceMcpUrls};
