mod authorization_server;
mod config;
mod error;
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
