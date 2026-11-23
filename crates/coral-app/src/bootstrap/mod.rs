//! Internal bootstrap seam for assembling the local server runtime.

mod consts;
mod engine;
mod env;
mod error;
mod health;
mod periodic;
mod runtime;
mod search_model;
mod secret_store_config;
mod server;
mod server_config;
mod sub_agent;

pub use error::{AppError, StateRefusal};
pub use health::EngineReadiness as ListenerReadiness;
pub use runtime::{
    Extensions, Launch, McpStartError, MigrationProgress, MigrationReport, RunningRuntime,
    RuntimeError, ShutdownFailures, start_runtime,
};
pub use search_model::{SearchModel, SearchModelState, SearchModelStop};
pub use server::{PreparedServerBuilder, RunningServer, ServerBuilder, ServerMode};
pub use server_config::{
    LOCAL_OUTBOUND_DEFAULTS, McpHttpServeConfig, ServeSettings, SessionAuthSettings,
};
pub use sub_agent::{SubAgentDependencies, SubAgentToolsFactory};

pub(crate) use env::env_var_name;
/// Startup's decode of `config.toml`, for tests outside bootstrap.
#[cfg(test)]
pub(crate) use server_config::tests::{auth_settings, telemetry};

pub(crate) fn env_var(name: &str) -> Result<Option<String>, std::env::VarError> {
    env::AppEnvironment::env_var(name)
}

/// Reports whether `ip` addresses the local machine.
///
/// Shared by the auth-disabled listener guard, auth URL validation, and the
/// process warning for exposed private listeners.
pub use coral_outbound::is_loopback_ip;
