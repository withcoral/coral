mod authorization_server;
mod config;
mod error;
mod id_token;
mod provider_client;
#[cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "the verification half is wired later in the OAuth serving stack"
    )
)]
pub(crate) mod session;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired later in the OAuth serving stack")
)]
pub(crate) mod state_store;

pub use authorization_server::{CoralAuthorizationServer, RunningCoralAuthorizationServer};
pub use config::AuthSettings;
pub use error::AuthServerError;

/// Identifier recorded for Coral's single configured OIDC provider.
///
/// Coral authenticates against exactly one upstream provider, so authorization
/// state and session tokens carry this constant instead of an operator-chosen
/// provider name.
pub(crate) const PROVIDER_ID: &str = "oidc";

/// Minimal `[auth]` TOML sections shared by this module's tests.
///
/// `config` and `authorization_server` both build configs from the same
/// required sections, so one copy here keeps a change to the required shape
/// from being applied to one test module and missed in the other.
#[cfg(test)]
pub(super) mod test_config {
    pub(crate) const SESSION: &str = "[auth.session]\nsigning_key_file = 'session.key'\n";
    pub(crate) const AUTHORIZATION_SERVER: &str =
        "[auth.authorization_server]\nissuer = 'http://localhost:9080'\n";
    pub(crate) const PROVIDER: &str = "[auth.provider]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret = 'provider-secret'\nredirect_uri = 'http://localhost:9080/auth/oidc/callback'\n";
}
