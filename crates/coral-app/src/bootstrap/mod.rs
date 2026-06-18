//! Internal bootstrap seam for assembling the local server runtime.

use std::path::PathBuf;

mod consts;
mod env;
mod error;
mod server;

use crate::state::AppStateLayout;

#[cfg(test)]
pub(crate) use error::MAX_STATUS_DETAIL_BYTES;
pub(crate) use error::{app_status, core_status};

pub use error::AppError;
pub use server::{RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider};

pub(crate) fn discover_app_state_layout(
    config_dir_override: Option<PathBuf>,
) -> Result<AppStateLayout, AppError> {
    env::AppEnvironment::discover().app_state_layout(config_dir_override)
}
