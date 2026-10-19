//! App-home state layout and fresh-state ownership.

mod database_config;
mod database_format;
mod format;
mod fs;
mod initialization;
mod layout;
mod lock;
mod migrations;

pub(crate) use database_config::DatabaseConfig;

pub(crate) use initialization::{InitializedState, initialize as initialize_state};
pub(crate) use layout::AppStateLayout;
pub(crate) use lock::{LOCK_FILE_NAME, StateLock};
