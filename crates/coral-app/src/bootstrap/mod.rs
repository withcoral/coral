//! Internal bootstrap seam for assembling the local server runtime.

use std::path::PathBuf;

mod consts;
mod env;
mod error;
mod server;

use crate::state::{AppStateLayout, ConfigStore};
use crate::workspaces::WorkspaceName;

#[cfg(test)]
pub(crate) use error::MAX_STATUS_DETAIL_BYTES;
pub(crate) use error::{app_status, core_status};

pub use crate::query_history::ExampleQuery;
pub use error::AppError;
pub use server::{RunningServer, ServerBuilder, ServerMode, StaticAsset, StaticAssetsProvider};

pub(crate) fn discover_app_state_layout(
    config_dir_override: Option<PathBuf>,
) -> Result<AppStateLayout, AppError> {
    env::AppEnvironment::discover().app_state_layout(config_dir_override)
}

/// Loads installed source names for the default workspace from local config only.
///
/// This is intentionally narrower than starting the local server and calling
/// `ListSources`: startup surfaces such as MCP initialize only need source
/// identity, not enriched source records, manifest versions, or query runtime
/// setup.
///
/// # Errors
///
/// Returns [`AppError`] when the app state layout cannot be discovered or
/// created, or when local config cannot be read or decoded.
pub fn default_workspace_source_names() -> Result<Vec<String>, AppError> {
    let layout = discover_app_state_layout(None)?;
    layout.ensure()?;
    ConfigStore::new(layout).list_workspace_source_names(&WorkspaceName::default())
}

/// Loads selected query-history examples for the default workspace from local
/// state only.
///
/// This intentionally mirrors [`default_workspace_source_names`]: MCP
/// initialize startup needs a cheap local-state read, not query runtime setup.
///
/// # Errors
///
/// Returns [`AppError`] when the app state layout cannot be discovered or
/// created, or when the local query-history file cannot be read.
pub fn default_workspace_example_queries(
    installed_sources: &[String],
    limit: usize,
) -> Result<Vec<ExampleQuery>, AppError> {
    let layout = discover_app_state_layout(None)?;
    layout.ensure()?;
    crate::query_history::select_example_queries(
        &layout,
        &WorkspaceName::default(),
        installed_sources,
        limit,
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))
}
