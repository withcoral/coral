//! Internal bootstrap seam for assembling the local server runtime.

mod consts;
mod env;
mod error;
mod health;
mod runtime;
mod server;
mod server_config;

pub use error::AppError;
pub use health::EngineReadiness as ListenerReadiness;
pub use runtime::{Launch, RunningRuntime, RuntimeError, start_runtime};
pub use server::{PreparedServerBuilder, RunningServer, ServerBuilder, ServerMode};
pub use server_config::{
    LOCAL_OUTBOUND_DEFAULTS, McpHttpServeConfig, ServeSettings, SessionAuthSettings,
};

pub(crate) use env::env_var_name;

pub(crate) fn env_var(name: &str) -> Result<Option<String>, std::env::VarError> {
    env::AppEnvironment::env_var(name)
}

/// Reports whether `ip` addresses the local machine.
///
/// Shared by the auth-disabled listener guard, auth URL validation, and the
/// process warning for exposed private listeners.
pub use coral_outbound::is_loopback_ip;
