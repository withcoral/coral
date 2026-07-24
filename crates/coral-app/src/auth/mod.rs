mod authorization_server;
mod config;
#[expect(clippy::allow_attributes, reason = "stacked session core")]
#[allow(dead_code, reason = "stacked session core")]
pub(crate) mod session;
#[cfg_attr(
    not(test),
    expect(dead_code, reason = "wired later in the OAuth serving stack")
)]
pub(crate) mod state_store;

pub use authorization_server::{CoralAuthorizationServer, RunningCoralAuthorizationServer};
pub use config::AuthSettings;
