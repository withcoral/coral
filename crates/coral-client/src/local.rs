//! Explicit local bootstrap helpers for tests and embedding.
//!
//! Normal callers should use [`crate::ClientBuilder`] and let `coral-client`
//! own local server startup. This module is the opt-in escape hatch for callers
//! that need to control local server configuration or lifetime directly.

use crate::{AppClient, ClientError};

/// Re-exported local server builder for explicit local bootstrap scenarios.
pub use coral_app::ServerBuilder;

/// Re-exported local server handle for explicit local bootstrap scenarios.
pub use coral_app::RunningServer;

/// Connects an [`AppClient`] to an already-running local Coral server.
///
/// This is not the default entrypoint. Prefer [`crate::ClientBuilder`] unless a
/// caller needs explicit local server control for tests or embedding.
///
/// # Errors
///
/// Returns [`ClientError`] if the generated gRPC clients cannot connect to the
/// running local server.
pub async fn connect_running_server(
    running_server: RunningServer,
) -> Result<AppClient, ClientError> {
    AppClient::from_running_server(running_server).await
}
