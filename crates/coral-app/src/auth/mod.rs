mod id_token;
mod oauth;
mod provider;
mod provider_client;
#[expect(clippy::allow_attributes, reason = "stacked session helpers")]
#[allow(
    dead_code,
    reason = "session login helpers are consumed by later branches in this stack"
)]
pub(crate) mod session;
pub(crate) mod state_store;

pub use oauth::{
    OAuthLoginError, OAuthLoginResult, OidcAuthConfig, RunningOidcAuthServer, run_oauth_login,
};
