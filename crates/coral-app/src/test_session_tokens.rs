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
use crate::state::ConfigStore;
use crate::state::db::{
    CoralDb, DatabaseConfig, ResolvedDatabaseConfig, UpsertLoginOutcome, now_unix_nanos_i64,
    run_state_migrations,
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

/// Persists a verified provider identity through Coral's production login path.
///
/// The configured database must be `SQLite`. The helper migrates that database,
/// provisions the user and personal default workspace transactionally, and
/// returns the internal user ID that a real authorization flow would place in
/// a session token's subject claim.
///
/// # Errors
///
/// Returns a message when the app-state layout, database, migration, or login
/// provisioning cannot complete.
pub async fn provision_test_login(
    config_dir: &Path,
    provider_issuer: &str,
    provider_subject: &str,
    display_name: Option<&str>,
    pre_v1_task_attribution_id: &str,
) -> Result<String, String> {
    let layout = discover_app_state_layout(Some(config_dir.to_path_buf()))
        .map_err(|error| error.to_string())?;
    layout.ensure().map_err(|error| error.to_string())?;
    let database_config = match DatabaseConfig::load(&layout).map_err(|error| error.to_string())? {
        DatabaseConfig::Sqlite { path } => ResolvedDatabaseConfig::Sqlite { path },
        DatabaseConfig::Postgres { .. } => {
            return Err(
                "test login provisioning requires a configured SQLite database".to_string(),
            );
        }
    };
    let database = CoralDb::open(database_config)
        .await
        .map_err(|error| error.to_string())?;
    database
        .migrate()
        .await
        .map_err(|error| error.to_string())?;
    run_state_migrations(&database, &ConfigStore::new(layout.clone()), &layout)
        .await
        .map_err(|error| error.to_string())?;
    let now_unix_nanos = now_unix_nanos_i64().map_err(|error| error.to_string())?;
    match database
        .provision_login_and_reattribute_pre_v1_tasks(
            provider_issuer,
            provider_subject,
            display_name,
            pre_v1_task_attribution_id,
            now_unix_nanos,
        )
        .await
        .map_err(|error| error.to_string())?
    {
        UpsertLoginOutcome::Upserted(user) => Ok(user.user_id),
        UpsertLoginOutcome::IssuerMismatch { .. } => {
            Err("test login identity is already bound to a different issuer".to_string())
        }
    }
}
