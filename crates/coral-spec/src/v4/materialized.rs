use crate::{ManifestError, Result};

use super::artifacts::{
    OPENAPI_IMPORTER_VERSION, PROJECTION_GENERATOR_VERSION, V4_ARTIFACT_SCHEMA_VERSION,
    V4MaterializedSource,
};
use super::manifest::V4SourceManifest;

pub fn validate_materialized_source(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<()> {
    if materialized.fingerprint.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(ManifestError::validation(
            "DSL v4 materialized artifact schema version mismatch",
        ));
    }
    if materialized.fingerprint.source_name != manifest.common.name {
        return Err(ManifestError::validation(format!(
            "DSL v4 materialized source identity mismatch for '{}'",
            manifest.common.name
        )));
    }
    if materialized.fingerprint.importer_version != OPENAPI_IMPORTER_VERSION
        || materialized.fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err(ManifestError::validation(
            "DSL v4 materialized importer or generator version mismatch",
        ));
    }
    for surface in &manifest.surfaces {
        if !materialized
            .surfaces
            .iter()
            .any(|materialized_surface| materialized_surface.surface_id == surface.id)
        {
            return Err(ManifestError::validation(format!(
                "DSL v4 materialized surface '{}' is missing",
                surface.id
            )));
        }
    }
    Ok(())
}
