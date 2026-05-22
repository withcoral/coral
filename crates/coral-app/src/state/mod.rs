//! App-home state layout and persisted config ownership.

mod config;
mod layout;

pub(crate) use config::{CacheConfig, ConfigStore};
pub(crate) use layout::AppStateLayout;
