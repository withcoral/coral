use std::collections::BTreeSet;
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
    /// Optional provenance metadata. Runtime loading must not depend on it.
    pub fingerprint: Option<Fingerprint>,
    pub surfaces: Vec<MaterializedSurface>,
    pub projections: ProjectionCatalog,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedSurface {
    pub surface_id: String,
    pub semantic_ir: SemanticIr,
    pub source_document_sha256: Option<String>,
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
    validate_materialized_source_structure(manifest, materialized)?;
    let fingerprint = materialized.fingerprint.as_ref().ok_or_else(|| {
        ManifestError::validation("new DSL v4 materialization is missing its fingerprint")
    })?;
    if fingerprint.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(ManifestError::validation(
            "DSL v4 materialized artifact schema version mismatch",
        ));
    }
    if fingerprint.source_name != manifest.common.name {
        return Err(ManifestError::validation(format!(
            "DSL v4 materialized source identity mismatch for '{}'",
            manifest.common.name
        )));
    }
    if fingerprint.importer_version != SURFACE_IMPORTER_VERSION
        || fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err(ManifestError::validation(
            "DSL v4 materialized importer or generator version mismatch",
        ));
    }
    let materialized_surface_ids = materialized
        .surfaces
        .iter()
        .map(|surface| surface.surface_id.as_str())
        .collect::<BTreeSet<_>>();
    let mut fingerprint_surface_ids = BTreeSet::new();
    for fingerprint_surface in &fingerprint.surfaces {
        if !fingerprint_surface_ids.insert(fingerprint_surface.surface_id.as_str()) {
            return Err(ManifestError::validation(format!(
                "DSL v4 fingerprint surface '{}' is repeated",
                fingerprint_surface.surface_id
            )));
        }
        if manifest.surface(&fingerprint_surface.surface_id).is_none() {
            return Err(ManifestError::validation(format!(
                "DSL v4 fingerprint surface '{}' is not declared",
                fingerprint_surface.surface_id
            )));
        }
    }
    if let Some(surface_id) = materialized_surface_ids
        .difference(&fingerprint_surface_ids)
        .next()
    {
        return Err(ManifestError::validation(format!(
            "DSL v4 materialized surface '{surface_id}' is missing from the fingerprint"
        )));
    }
    if let Some(surface_id) = fingerprint_surface_ids
        .difference(&materialized_surface_ids)
        .next()
    {
        return Err(ManifestError::validation(format!(
            "DSL v4 fingerprint surface '{surface_id}' is not materialized"
        )));
    }
    Ok(())
}

/// Validates runtime invariants independently of optional artifact provenance.
pub fn validate_materialized_source_structure(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<()> {
    if materialized.surfaces.is_empty() {
        return Err(ManifestError::validation(
            "DSL v4 materialized source has no surfaces",
        ));
    }
    let mut materialized_surface_ids = BTreeSet::new();
    for materialized_surface in &materialized.surfaces {
        if !materialized_surface_ids.insert(materialized_surface.surface_id.as_str()) {
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
    let mut projection_names = BTreeSet::new();
    for projection in &materialized.projections.projections {
        if !projection_names.insert((projection.surface_id.as_str(), projection.name.as_str())) {
            return Err(ManifestError::validation(format!(
                "DSL v4 projection '{}' is repeated for surface '{}'",
                projection.name, projection.surface_id
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use crate::parse_source_manifest_yaml;
    use crate::v4::ir::SemanticIr;
    use crate::v4::projections::ProjectionCatalog;
    use crate::v4::{
        MCP_IMPORTER_VERSION, OPENAPI_IMPORTER_VERSION, PROJECTION_GENERATOR_VERSION,
        SURFACE_IMPORTER_VERSION, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION, V4SourceManifest,
    };

    use super::{
        Fingerprint, FingerprintSurface, MaterializedSurface, V4MaterializedSource,
        validate_materialized_source, validate_materialized_source_structure,
    };

    fn manifest() -> V4SourceManifest {
        parse_source_manifest_yaml(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
",
        )
        .expect("manifest")
        .as_v4()
        .expect("v4")
        .clone()
    }

    fn materialized_source() -> V4MaterializedSource {
        V4MaterializedSource {
            fingerprint: Some(Fingerprint {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                manifest_sha256: "manifest-sha".to_string(),
                surfaces: vec![
                    fingerprint_surface("rest", SurfaceType::OpenApi),
                    fingerprint_surface("mcp", SurfaceType::Mcp),
                ],
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            }),
            surfaces: vec![
                materialized_surface("rest", SurfaceType::OpenApi),
                materialized_surface("mcp", SurfaceType::Mcp),
            ],
            projections: ProjectionCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                generator_version: Some(PROJECTION_GENERATOR_VERSION.to_string()),
                projections: Vec::new(),
                diagnostics: Vec::new(),
            },
            diagnostics: Vec::new(),
        }
    }

    fn fingerprint_surface(surface_id: &str, surface_type: SurfaceType) -> FingerprintSurface {
        FingerprintSurface {
            surface_id: surface_id.to_string(),
            surface_type,
            descriptor_kind: "test".to_string(),
            descriptor_location: "/tmp/test".to_string(),
            descriptor_sha256: format!("{surface_id}-sha"),
            input_declarations_sha256: "inputs-sha".to_string(),
        }
    }

    fn materialized_surface(surface_id: &str, surface_type: SurfaceType) -> MaterializedSurface {
        MaterializedSurface {
            surface_id: surface_id.to_string(),
            semantic_ir: SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                surface_id: surface_id.to_string(),
                surface_type,
                importer_version: importer_version(surface_type).to_string(),
                operations: Vec::new(),
                types: Vec::new(),
                diagnostics: Vec::new(),
            },
            source_document_sha256: Some(format!("{surface_id}-sha")),
            normalized_source_document_path: PathBuf::from(format!(
                "surfaces/{surface_id}/source-document.yaml"
            )),
            raw_source_document_path: PathBuf::from(format!(
                "surfaces/{surface_id}/source-document.raw"
            )),
        }
    }

    fn importer_version(surface_type: SurfaceType) -> &'static str {
        match surface_type {
            SurfaceType::OpenApi => OPENAPI_IMPORTER_VERSION,
            SurfaceType::Mcp => MCP_IMPORTER_VERSION,
        }
    }

    #[test]
    fn rejects_materialized_surface_missing_from_fingerprint() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized
            .fingerprint
            .as_mut()
            .expect("fingerprint")
            .surfaces
            .retain(|surface| surface.surface_id != "mcp");

        let error = validate_materialized_source(&manifest, &materialized)
            .expect_err("fingerprint should include each materialized surface");

        assert!(
            error
                .to_string()
                .contains("materialized surface 'mcp' is missing from the fingerprint"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_fingerprint_surface_missing_from_materialized_surfaces() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized
            .surfaces
            .retain(|surface| surface.surface_id != "mcp");

        let error = validate_materialized_source(&manifest, &materialized)
            .expect_err("materialized artifact should include each fingerprint surface");

        assert!(
            error
                .to_string()
                .contains("fingerprint surface 'mcp' is not materialized"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn structural_validation_rejects_duplicate_projection_names() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized.projections =
            serde_yaml::from_str(include_str!("fixtures/artefact-schema-v3/projections.yaml"))
                .expect("decode projection fixture");
        let duplicate = materialized
            .projections
            .projections
            .first()
            .expect("fixture projection")
            .clone();
        materialized.projections.projections.push(duplicate);

        let error = validate_materialized_source_structure(&manifest, &materialized)
            .expect_err("duplicate projection should fail structural validation");

        assert!(
            error
                .to_string()
                .contains("projection 'items' is repeated for surface 'rest'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn structural_validation_rejects_duplicate_runtime_names_across_artifact_namespaces() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized.projections =
            serde_yaml::from_str(include_str!("fixtures/artefact-schema-v3/projections.yaml"))
                .expect("decode projection fixture");
        let mut duplicate = materialized
            .projections
            .projections
            .first()
            .expect("fixture projection")
            .clone();
        duplicate.namespace = "stale_namespace".to_string();
        materialized.projections.projections.push(duplicate);

        let error = validate_materialized_source_structure(&manifest, &materialized)
            .expect_err("runtime duplicate should fail structural validation");

        assert!(
            error
                .to_string()
                .contains("projection 'items' is repeated for surface 'rest'"),
            "unexpected error: {error}"
        );
    }
}
