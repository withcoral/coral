use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::ir::SemanticIr;
use crate::v4::manifest::{SurfaceType, V4SourceManifest};
use crate::v4::projections::ProjectionCatalog;
use crate::v4::{
    PROJECTION_GENERATOR_VERSION, SURFACE_IMPORTER_VERSION, V4_ARTIFACT_SCHEMA_VERSION,
};
use crate::{ManifestError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4MaterializedSource {
    pub fingerprint: Fingerprint,
    pub surfaces: Vec<MaterializedSurface>,
    pub projections: ProjectionCatalog,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedSurface {
    pub surface_id: String,
    pub semantic_ir: SemanticIr,
    pub source_document_sha256: String,
    pub normalized_source_document_path: PathBuf,
    pub raw_source_document_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fingerprint {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub manifest_sha256: String,
    pub surfaces: Vec<FingerprintSurface>,
    pub importer_version: String,
    pub projection_generator_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintSurface {
    pub surface_id: String,
    pub surface_type: SurfaceType,
    pub descriptor_kind: String,
    pub descriptor_location: String,
    pub descriptor_sha256: String,
    pub input_declarations_sha256: String,
}

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
    if materialized.fingerprint.importer_version != SURFACE_IMPORTER_VERSION
        || materialized.fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err(ManifestError::validation(
            "DSL v4 materialized importer or generator version mismatch",
        ));
    }
    if materialized.surfaces.is_empty() {
        return Err(ManifestError::validation(
            "DSL v4 materialized source has no surfaces",
        ));
    }
    let mut seen = std::collections::BTreeSet::new();
    for materialized_surface in &materialized.surfaces {
        if !seen.insert(materialized_surface.surface_id.as_str()) {
            return Err(ManifestError::validation(format!(
                "DSL v4 materialized surface '{}' is repeated",
                materialized_surface.surface_id
            )));
        }
        if manifest.surface(&materialized_surface.surface_id).is_none() {
            return Err(ManifestError::validation(format!(
                "DSL v4 materialized surface '{}' is not declared",
                materialized_surface.surface_id
            )));
        }
    }
    Ok(())
}
