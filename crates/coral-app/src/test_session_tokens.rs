//! Test-only session-token minting for sibling crates' integration tests.
//!
//! Gated behind the `test-session-tokens` feature, which production builds must
//! leave off: this mints an access token for any subject without an OAuth flow.
//! An out-of-crate test uses it so it exercises the real issuer's wire format
//! instead of re-implementing the JWT the verifier expects — a hand-assembled
//! token can drift from the issuer and let the test pass while serving is
//! broken.

use std::path::Path;
use std::time::Duration;

use crate::auth::session::SessionTokenIssuer;
use crate::bootstrap::discover_app_state_layout;
use crate::state::db::{
    CoralDb, DatabaseConfig, ResolvedDatabaseConfig, UpsertLoginOutcome, now_unix_nanos_i64,
};

/// Mints an access token accepted by a server configured with `signing_key`.
///
/// `issuer` must equal the configured `auth.authorization_server.issuer`,
/// `audience` the resource identifier the server accepts, and `signing_key` the
/// PKCS#8 DER P-256 key the server verifies with.
///
/// # Errors
///
/// Returns a message when the key material or the claims are unusable.
pub fn issue_access_token(
    issuer: &str,
    signing_key: &[u8],
    access_token_ttl: Duration,
    subject: &str,
    client_id: &str,
    audience: &str,
) -> Result<String, String> {
    Ok(
        SessionTokenIssuer::new(Some(issuer), signing_key, access_token_ttl)?
            .issue_access_token(subject, client_id, audience)?
            .access_token,
    )
}

/// Persists an identity through the same database transaction used by login.
///
/// Requires initialized `SQLite` state; it runs no migration and creates no workspace or membership.
///
/// # Errors
///
/// Returns a message when the state layout, database, or identity write cannot
/// complete, or when the subject is already bound to another issuer.
pub async fn persist_test_login_identity(
    config_dir: &Path,
    provider_subject: &str,
) -> Result<String, String> {
    let layout = discover_app_state_layout(Some(config_dir.to_path_buf()))
        .map_err(|error| error.to_string())?;
    let database_config = match DatabaseConfig::load(&layout).map_err(|error| error.to_string())? {
        DatabaseConfig::Sqlite { path } => ResolvedDatabaseConfig::Sqlite { path },
        DatabaseConfig::Postgres { .. } => {
            return Err(
                "test login identity persistence requires a configured SQLite database".to_string(),
            );
        }
    };
    let database = CoralDb::open(database_config)
        .await
        .map_err(|error| error.to_string())?;
    let outcome = database
        .persist_login_identity_and_reattribute_legacy_tasks(
            "https://accounts.example",
            provider_subject,
            None,
            "unused-test-attribution",
            now_unix_nanos_i64().map_err(|error| error.to_string())?,
        )
        .await
        .map_err(|error| error.to_string())?;
    let UpsertLoginOutcome::Upserted(user) = outcome else {
        return Err("test login identity is already bound to a different issuer".to_string());
    };
    Ok(user.user_id)
}
