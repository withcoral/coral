//! Authorization codes issued by Coral's OAuth authorization server.
//!
//! Codes are stored so that any process can redeem a code another process
//! issued. A code is stored as its SHA-256 digest; the code itself never
//! reaches the database.
//!
//! Each sign-in issues at most one code. The digest of the sign-in's OIDC
//! `state` is unique among stored codes, so a callback replayed after its
//! sign-in finished issues nothing. Redemption marks a code spent instead of
//! deleting it, so that guard holds until the code expires.

mod repository;
#[cfg(test)]
mod tests;

use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use coral_db::Database;
use sha2::{Digest as _, Sha256};

/// How long an issued code can be redeemed.
pub const AUTHORIZATION_CODE_TTL: Duration = Duration::from_mins(5);

/// What a code authorizes, and the request bindings its redemption must repeat.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AuthorizationGrant {
    /// Coral's own User id, never the upstream subject.
    pub user_id: String,
    /// The OAuth client the code was issued to.
    pub client_id: String,
    /// The exact redirect URI the authorization request used.
    pub redirect_uri: String,
    /// The client's S256 PKCE challenge.
    pub code_challenge: String,
    /// The canonical resource the sign-in approved.
    pub resource: String,
    /// The Workspace the User picked, for the Organization MCP Endpoint only.
    pub workspace_id: Option<String>,
}

/// The request bindings a redemption presents.
#[derive(Clone, Copy, Debug)]
pub struct CodeRedemption<'a> {
    /// The client redeeming the code.
    pub client_id: &'a str,
    /// The redirect URI the token request repeats.
    pub redirect_uri: &'a str,
    /// The S256 challenge of the verifier the client presented.
    pub code_challenge: &'a str,
    /// The canonical resource the token request names.
    pub resource: &'a str,
}

/// The outcome of issuing a code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodeIssue {
    /// The code was stored and can be redeemed.
    Issued,
    /// The sign-in already issued a code. Nothing was stored.
    SignInAlreadyUsed,
}

/// Why a code could not be issued or redeemed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthorizationCodeError {
    /// The code or sign-in state was empty.
    #[error("authorization code secrets must not be empty")]
    EmptySecret,
    /// The expiry falls outside the range the database records.
    #[error("host time is outside the range authorization codes can record")]
    ClockOutOfRange,
    /// The database did not accept or answer the statement.
    #[error("authorization code storage is unavailable")]
    Unavailable,
}

/// Deployment-level store of authorization codes awaiting redemption.
#[derive(Clone)]
pub struct AuthorizationCodes {
    database: Arc<Database>,
}

impl AuthorizationCodes {
    /// Stores codes in `database`, shared by every Coral process.
    #[must_use]
    pub fn new(database: Arc<Database>) -> Self {
        Self { database }
    }

    /// Stores `code` for `grant`, issued by the sign-in whose OIDC state is
    /// `sign_in_state`, at host time `now_unix_nanos`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationCodeError::EmptySecret`] for an empty `code` or
    /// `sign_in_state`, [`AuthorizationCodeError::ClockOutOfRange`] when the
    /// expiry cannot be represented, and
    /// [`AuthorizationCodeError::Unavailable`] when the database does not
    /// accept the write.
    pub async fn issue(
        &self,
        code: &str,
        sign_in_state: &str,
        grant: &AuthorizationGrant,
        now_unix_nanos: i64,
    ) -> Result<CodeIssue, AuthorizationCodeError> {
        let (code_digest, sign_in_digest) = (digest(code)?, digest(sign_in_state)?);
        let expires_at = i64::try_from(AUTHORIZATION_CODE_TTL.as_nanos())
            .ok()
            .and_then(|ttl| now_unix_nanos.checked_add(ttl))
            .ok_or(AuthorizationCodeError::ClockOutOfRange)?;
        let mut tx = self.database.begin().await.map_err(unavailable)?;
        repository::purge_expired(&mut tx, now_unix_nanos).await?;
        let inserted =
            repository::insert(&mut tx, &code_digest, &sign_in_digest, grant, expires_at).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(if inserted {
            CodeIssue::Issued
        } else {
            CodeIssue::SignInAlreadyUsed
        })
    }

    /// Spends `code` at host time `now_unix_nanos` and returns its grant.
    ///
    /// Returns `None` for an unknown, spent or expired code, and for a
    /// redemption whose bindings differ from the grant's. A mismatched
    /// redemption leaves the code redeemable.
    ///
    /// # Errors
    ///
    /// Returns [`AuthorizationCodeError::Unavailable`] when the database does
    /// not answer.
    pub async fn redeem(
        &self,
        code: &str,
        redemption: CodeRedemption<'_>,
        now_unix_nanos: i64,
    ) -> Result<Option<AuthorizationGrant>, AuthorizationCodeError> {
        let Ok(code_digest) = digest(code) else {
            return Ok(None);
        };
        // Anyone can present a code, and on SQLite every writer queues behind
        // `BEGIN`. Only a code that can be spent opens a write transaction, so
        // guessed codes never hold the writer that sign-ins need. The update
        // below still decides single use if two redemptions pass this read.
        if !repository::is_spendable(&self.database, &code_digest, redemption, now_unix_nanos)
            .await?
        {
            return Ok(None);
        }
        let mut tx = self.database.begin().await.map_err(unavailable)?;
        let grant = repository::spend(&mut tx, &code_digest, redemption, now_unix_nanos).await?;
        tx.commit().await.map_err(unavailable)?;
        Ok(grant)
    }
}

fn digest(secret: &str) -> Result<String, AuthorizationCodeError> {
    if secret.is_empty() {
        return Err(AuthorizationCodeError::EmptySecret);
    }
    Ok(URL_SAFE_NO_PAD.encode(Sha256::digest(secret.as_bytes())))
}

fn unavailable(_error: coral_db::Error) -> AuthorizationCodeError {
    AuthorizationCodeError::Unavailable
}
