//! Source lifecycle workflow, catalog inspection, and transport adapters.

use coral_spec::ValidatedSourceManifest;
use coral_spec::v4::SurfaceType;

use crate::bootstrap::AppError;

pub(crate) mod catalog;
pub(crate) mod manager;
pub(crate) mod materialization;
pub(crate) mod model;
pub(crate) mod name;
pub(crate) mod runtime_package;
pub(crate) mod service;

pub(crate) use name::SourceName;

pub(crate) fn ensure_database_source_feature_enabled(
    manifest: &ValidatedSourceManifest,
    database_sources_enabled: bool,
) -> Result<(), AppError> {
    let is_database_source = manifest
        .as_v4()
        .is_some_and(|v4| v4.surface.surface_type == SurfaceType::Database);
    if database_sources_enabled || !is_database_source {
        return Ok(());
    }
    Err(AppError::FailedPrecondition(format!(
        "database source '{}' requires the disabled `database_sources` feature; enable it with `coral features enable database_sources` and retry",
        manifest.schema_name()
    )))
}
