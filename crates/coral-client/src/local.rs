//! Explicit local bootstrap helpers for tests and embedding.
//!
//! This module is the opt-in escape hatch for callers that need to control
//! local server configuration or lifetime directly.

use crate::{AppClient, BearerToken, ClientError};

/// Connects to a plaintext loopback-IP Coral endpoint with bearer authorization.
///
/// This is limited to explicit local composition where the caller controls both
/// ends of the connection. General remote connections must use
/// [`AppClient::connect_with_bearer`], which requires HTTPS.
///
/// # Errors
///
/// Returns [`ClientError::InsecureAuthorizationEndpoint`] unless the endpoint
/// is plaintext HTTP on a numeric loopback IP. Returns [`ClientError`] if the
/// client otherwise cannot connect.
pub async fn connect_with_loopback_bearer(
    endpoint_uri: &str,
    bearer: BearerToken,
) -> Result<AppClient, ClientError> {
    AppClient::connect_with_loopback_bearer(endpoint_uri, bearer).await
}

/// Re-exported local server builder for explicit local bootstrap scenarios.
pub use coral_app::ServerBuilder;

/// Re-exported local server mode for explicit local bootstrap scenarios.
pub use coral_app::ServerMode;

/// Re-exported local server handle for explicit local bootstrap scenarios.
pub use coral_app::RunningServer;

/// Re-exported local server startup error for explicit bootstrap surfaces.
pub use coral_app::AppError as LocalServerError;
