use std::collections::BTreeSet;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::v4::diagnostics::Diagnostic;
use crate::v4::manifest::{SurfaceType, V4SourceManifest};
use crate::v4::projections::{ProjectionCatalog, ProjectionKind, projection_schema_name};
use crate::v4::{
    OPERATION_METADATA_GENERATOR_VERSION, PROJECTION_GENERATOR_VERSION, SURFACE_IMPORTER_VERSION,
    V4_ARTIFACT_SCHEMA_VERSION, ValidatedSurfacePlan,
};
use crate::{ManifestError, Result};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct V4MaterializedSource {
    /// Optional provenance metadata. Runtime loading must not depend on it.
    pub fingerprint: Option<Fingerprint>,
    pub surface: MaterializedSurface,
    pub projections: ProjectionCatalog,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MaterializedSurface {
    pub plan: ValidatedSurfacePlan,
    pub source_document_sha256: Option<String>,
    pub normalized_source_document_path: PathBuf,
    pub raw_source_document_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fingerprint {
    pub artifact_schema_version: u32,
    pub source_name: String,
    pub manifest_sha256: String,
    pub surface: FingerprintSurface,
    pub importer_version: String,
    pub operation_metadata_generator_version: String,
    pub projection_generator_version: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FingerprintSurface {
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
        || fingerprint.operation_metadata_generator_version != OPERATION_METADATA_GENERATOR_VERSION
        || fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err(ManifestError::validation(
            "DSL v4 materialized importer or generator version mismatch",
        ));
    }
    if fingerprint.surface.surface_type != manifest.surface.surface_type {
        return Err(ManifestError::validation(
            "DSL v4 fingerprint surface type does not match the manifest",
        ));
    }
    Ok(())
}

/// Validates runtime invariants independently of optional artifact provenance.
pub fn validate_materialized_source_structure(
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
) -> Result<()> {
    if materialized.surface.plan.semantic_ir().surface_type != manifest.surface.surface_type {
        return Err(ManifestError::validation(
            "DSL v4 materialized surface type does not match the manifest",
        ));
    }
    if materialized.projections.source_name != manifest.common.name {
        return Err(ManifestError::validation(
            "DSL v4 projection catalog source name does not match the installed manifest",
        ));
    }
    let mut projection_names = BTreeSet::new();
    for projection in &materialized.projections.projections {
        if projection.catalog_name != manifest.common.name {
            return Err(ManifestError::validation(format!(
                "DSL v4 projection '{}' remaps catalog_name from '{}' to '{}'",
                projection.sql_reference(),
                manifest.common.name,
                projection.catalog_name
            )));
        }
        let (kind, relation_name) = match (
            &projection.kind,
            projection.table_name.as_deref(),
            projection.function_name.as_deref(),
        ) {
            (ProjectionKind::Table, Some(table_name), None) => ("table", table_name),
            (ProjectionKind::TableFunction { .. }, None, Some(function_name)) => {
                ("table_function", function_name)
            }
            (ProjectionKind::Table, _, _) => {
                return Err(ManifestError::validation(format!(
                    "DSL v4 table projection for operation '{}' must define exactly one table_name and no function_name",
                    projection.operation_id
                )));
            }
            (ProjectionKind::TableFunction { .. }, _, _) => {
                return Err(ManifestError::validation(format!(
                    "DSL v4 table-function projection for operation '{}' must define exactly one function_name and no table_name",
                    projection.operation_id
                )));
            }
        };
        if !projection_names.insert((
            projection.catalog_name.as_str(),
            projection.schema_name.as_str(),
            kind,
            relation_name,
        )) {
            return Err(ManifestError::validation(format!(
                "DSL v4 {kind} projection '{}' is repeated",
                projection.sql_reference()
            )));
        }
    }
    let operations = materialized
        .surface
        .plan
        .semantic_ir()
        .operations
        .iter()
        .map(|operation| (operation.id.as_str(), operation))
        .collect::<std::collections::BTreeMap<_, _>>();
    for projection in &materialized.projections.projections {
        let operation = operations
            .get(projection.operation_id.as_str())
            .ok_or_else(|| {
                ManifestError::validation(format!(
                    "DSL v4 projection '{}' references missing operation '{}'",
                    projection.sql_reference(),
                    projection.operation_id
                ))
            })?;
        let expected_schema = projection_schema_name(operation);
        if projection.schema_name != expected_schema {
            return Err(ManifestError::validation(format!(
                "DSL v4 projection '{}' remaps schema_name from '{expected_schema}' to '{}'",
                projection.sql_reference(),
                projection.schema_name
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
    use crate::v4::projections::{
        Projection, ProjectionCatalog, ProjectionKind, ProjectionVisibility,
    };
    use crate::v4::{
        MCP_IMPORTER_VERSION, OPENAPI_IMPORTER_VERSION, OPERATION_METADATA_GENERATOR_VERSION,
        OperationMetadataCatalog, PROJECTION_GENERATOR_VERSION, SURFACE_IMPORTER_VERSION,
        SurfaceType, V4_ARTIFACT_SCHEMA_VERSION, V4SourceManifest, ValidatedSurfacePlan,
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
surface:
  type: openapi
  file: /tmp/openapi.yaml
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
                surface: FingerprintSurface {
                    surface_type: SurfaceType::OpenApi,
                    descriptor_kind: "test".to_string(),
                    descriptor_location: "/tmp/test".to_string(),
                    descriptor_sha256: "surface-sha".to_string(),
                    input_declarations_sha256: "inputs-sha".to_string(),
                },
                importer_version: SURFACE_IMPORTER_VERSION.to_string(),
                operation_metadata_generator_version: OPERATION_METADATA_GENERATOR_VERSION
                    .to_string(),
                projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            }),
            surface: MaterializedSurface {
                plan: empty_plan(SurfaceType::OpenApi),
                source_document_sha256: Some("surface-sha".to_string()),
                normalized_source_document_path: PathBuf::from("source-document.yaml"),
                raw_source_document_path: PathBuf::from("source-document.raw"),
            },
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

    fn empty_plan(surface_type: SurfaceType) -> ValidatedSurfacePlan {
        ValidatedSurfacePlan::new(
            SemanticIr {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                surface_type,
                importer_version: match surface_type {
                    SurfaceType::OpenApi => OPENAPI_IMPORTER_VERSION,
                    SurfaceType::Mcp => MCP_IMPORTER_VERSION,
                    SurfaceType::Database => {
                        panic!("database surfaces do not have materialized plans")
                    }
                }
                .to_string(),
                operations: Vec::new(),
                types: Vec::new(),
                diagnostics: Vec::new(),
            },
            OperationMetadataCatalog {
                artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
                source_name: "demo".to_string(),
                generator_version: Some(OPERATION_METADATA_GENERATOR_VERSION.to_string()),
                operations: std::collections::BTreeMap::default(),
            },
        )
        .expect("empty plan")
    }

    fn projection(name: &str) -> Projection {
        Projection {
            catalog_name: "demo".to_string(),
            schema_name: "public".to_string(),
            table_name: Some(name.to_string()),
            function_name: None,
            kind: ProjectionKind::Table,
            description: String::new(),
            guide: String::new(),
            operation_id: "items/list".to_string(),
            visibility: ProjectionVisibility::Published,
            inputs: Vec::new(),
            columns: Vec::new(),
            search_limits: None,
            detail_hints: Vec::new(),
            diagnostics: Vec::new(),
        }
    }

    #[test]
    fn rejects_fingerprint_surface_type_mismatch() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized
            .fingerprint
            .as_mut()
            .expect("fingerprint")
            .surface
            .surface_type = SurfaceType::Mcp;

        let error = validate_materialized_source(&manifest, &materialized)
            .expect_err("fingerprint surface type should match the manifest");
        assert!(
            error
                .to_string()
                .contains("fingerprint surface type does not match the manifest")
        );
    }

    #[test]
    fn rejects_materialized_surface_type_mismatch() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized.surface.plan = empty_plan(SurfaceType::Mcp);

        let error = validate_materialized_source_structure(&manifest, &materialized)
            .expect_err("materialized surface type should match the manifest");
        assert!(
            error
                .to_string()
                .contains("materialized surface type does not match the manifest")
        );
    }

    #[test]
    fn materialized_artifact_yaml_is_singular_and_uses_root_level_paths() {
        let materialized = materialized_source();
        let yaml = serde_yaml::to_string(&materialized).expect("serialize materialized source");

        assert!(
            yaml.contains("surface:\n"),
            "missing singular surface: {yaml}"
        );
        assert!(
            !yaml.contains("surfaces:"),
            "plural surfaces leaked: {yaml}"
        );
        assert!(!yaml.contains("surface_id:"), "surface ID leaked: {yaml}");
        assert!(
            yaml.contains("normalized_source_document_path: source-document.yaml"),
            "normalized document should live at the materialization root: {yaml}"
        );
        assert!(
            yaml.contains("raw_source_document_path: source-document.raw"),
            "raw document should live at the materialization root: {yaml}"
        );

        serde_yaml::from_str::<V4MaterializedSource>(&yaml)
            .expect("singular materialized source should round-trip");
    }

    #[test]
    fn strict_validation_rejects_previous_artifact_schema_version() {
        let manifest = manifest();
        let mut materialized = materialized_source();
        materialized
            .fingerprint
            .as_mut()
            .expect("fingerprint")
            .artifact_schema_version = V4_ARTIFACT_SCHEMA_VERSION - 1;

        let error = validate_materialized_source(&manifest, &materialized)
            .expect_err("new materializations must use the current artifact schema");
        assert!(
            error
                .to_string()
                .contains("artifact schema version mismatch")
        );
    }

    #[test]
    fn structural_validation_accepts_zero_projections() {
        validate_materialized_source_structure(&manifest(), &materialized_source())
            .expect("a singular source may publish no projections");
    }

    #[test]
    fn structural_validation_rejects_duplicate_projection_identities() {
        let mut materialized = materialized_source();
        materialized.projections.projections = vec![projection("items"), projection("items")];

        let error = validate_materialized_source_structure(&manifest(), &materialized)
            .expect_err("duplicate projection names should fail validation");

        assert_eq!(
            error.to_string(),
            "DSL v4 table projection 'demo.public.items' is repeated"
        );
    }

    #[test]
    fn mcp_importer_version_is_distinct() {
        assert_ne!(OPENAPI_IMPORTER_VERSION, MCP_IMPORTER_VERSION);
    }
}
