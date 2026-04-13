//! App-home state layout and persisted config ownership.

mod config;
mod layout;
mod secrets;

pub(crate) use config::ConfigStore;
pub(crate) use layout::{AppStateLayout, INSTALLED_MANIFEST_FILE_NAME};
pub(crate) use secrets::{CredentialsError, SecretStore};
