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
    OAuthLoginError, OAuthLoginResult, OAuthLoginStoreError, OidcAuthConfig, RunningOidcAuthServer,
    load_oauth_login, run_oauth_login, save_oauth_login,
};
