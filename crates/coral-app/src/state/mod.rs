//! App-home state layout and persisted config ownership.

mod config;
mod layout;
mod secrets;

pub(crate) use config::ConfigStore;
pub(crate) use layout::AppStateLayout;
pub(crate) use secrets::{CredentialsError, SecretStore};
