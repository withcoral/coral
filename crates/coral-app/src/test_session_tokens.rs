//! Test-only session-token minting for sibling crates' integration tests.
//!
//! Gated behind the `test-session-tokens` feature, which production builds must
//! leave off: this mints an access token for any `user_id` without an OAuth
//! flow, skipping the login provisioning that would otherwise mint that id.
//! An out-of-crate test uses it so it exercises the real issuer's wire format
//! instead of re-implementing the JWT the verifier expects — a hand-assembled
//! token can drift from the issuer and let the test pass while serving is
//! broken.

use std::time::Duration;

use crate::auth::session::SessionTokenIssuer;

/// Mints an access token accepted by a server configured with `signing_key`.
///
/// `issuer` must equal the configured `auth.authorization_server.issuer`,
/// `audience` the resource identifier the server accepts, and `signing_key` the
/// PKCS#8 DER P-256 key the server verifies with. `user_id` is Coral's internal
/// user id — the same thing a provisioned login puts in the token's `sub` — so
/// it must be a canonical principal id, not an upstream OIDC subject.
///
/// # Errors
///
/// Returns a message when the key material or the claims are unusable.
pub fn issue_access_token(
    issuer: &str,
    signing_key: &[u8],
    access_token_ttl: Duration,
    user_id: &str,
    client_id: &str,
    audience: &str,
) -> Result<String, String> {
    Ok(
        SessionTokenIssuer::new(Some(issuer), signing_key, access_token_ttl)?
            .issue_access_token(user_id, client_id, audience)?
            .access_token,
    )
}
