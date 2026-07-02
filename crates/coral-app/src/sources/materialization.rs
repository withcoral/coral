//! DSL v4 source materialization and artifact loading.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use coral_spec::v4::{
    Diagnostic, DiagnosticSeverity, Fingerprint, FingerprintSurface, MCP_IMPORTER_VERSION,
    MaterializedSurface, McpToolCatalog, OPENAPI_IMPORTER_VERSION, PROJECTION_GENERATOR_VERSION,
    ProjectionCatalog, SURFACE_IMPORTER_VERSION, SemanticIr, SurfaceType,
    V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource, V4SourceManifest,
    generate_projection_catalog, import_mcp_surface, import_openapi_surface,
    normalize_mcp_tool_catalog, normalize_source_document, openapi_document_metadata,
    validate_materialized_source, validate_openapi_base_url_template,
};
use coral_spec::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
    ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter, ParsedTemplate,
};
use percent_encoding::percent_decode_str;
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

const DESCRIPTOR_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;
const MAX_OPENAPI_EXPANDED_BYTES: usize = 16 * 1024 * 1024;
const MAX_OPENAPI_REF_LOADED_BYTES: usize = 64 * 1024 * 1024;
const MAX_OPENAPI_REF_DEPTH: usize = 64;
const MAX_OPENAPI_REF_DOCUMENTS: usize = 256;
const MAX_OPENAPI_EXPANDED_NODES: usize = 200_000;
const DESCRIPTOR_USER_AGENT: &str = "coral-dsl-v4-materializer";
pub(crate) const PROJECTIONS_FILENAME: &str = "projections.yaml";
pub(crate) const FINGERPRINT_FILENAME: &str = "fingerprint.yaml";
pub(crate) const DIAGNOSTICS_FILENAME: &str = "diagnostics.yaml";

#[derive(Debug)]
pub(crate) struct MaterializationBuild {
    pub(crate) temp_dir: PathBuf,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct MaterializationInputs {
    pub(crate) variables: BTreeMap<String, String>,
    pub(crate) secrets: BTreeMap<String, String>,
}

pub(crate) fn build_v4_materialization_tmp(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    inputs: &MaterializationInputs,
    temp_suffix: &str,
) -> Result<MaterializationBuild, AppError> {
    let temp_dir = layout.v4_materialized_tmp_dir(workspace_name, source_name, temp_suffix);
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    fs::ensure_private_dir(&temp_dir)?;

    match write_materialization(&temp_dir, manifest_yaml, manifest, inputs) {
        Ok(()) => Ok(MaterializationBuild { temp_dir }),
        Err(error) => {
            if temp_dir.exists() {
                drop(std::fs::remove_dir_all(&temp_dir));
            }
            Err(error)
        }
    }
}

pub(crate) fn replace_v4_materialization(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    temp_dir: &Path,
) -> Result<Option<PathBuf>, AppError> {
    let target = layout.v4_materialized_dir(workspace_name, source_name);
    let backup = layout.v4_materialized_tmp_dir(
        workspace_name,
        source_name,
        &format!("rollback.{}", Uuid::new_v4()),
    );
    if let Some(parent) = target.parent() {
        fs::ensure_private_dir(parent)?;
    }
    if backup.exists() {
        std::fs::remove_dir_all(&backup)?;
    }
    let had_existing = target.exists();
    if had_existing {
        std::fs::rename(&target, &backup)?;
    }
    if let Err(error) = std::fs::rename(temp_dir, &target) {
        if had_existing
            && backup.exists()
            && let Err(rollback_error) = std::fs::rename(&backup, &target)
        {
            return Err(AppError::FailedPrecondition(format!(
                "failed to install DSL v4 materialization for source '{source_name}': {error}; failed to restore previous materialization from '{}': {rollback_error}",
                backup.display()
            )));
        }
        return Err(error.into());
    }
    Ok(had_existing.then_some(backup))
}

pub(crate) fn cleanup_materialization_backup(backup: Option<PathBuf>) {
    if let Some(backup) = backup
        && backup.exists()
    {
        drop(std::fs::remove_dir_all(backup));
    }
}

pub(crate) fn cleanup_materialization_tmp(temp_dir: Option<&Path>) {
    if let Some(temp_dir) = temp_dir
        && temp_dir.exists()
    {
        drop(std::fs::remove_dir_all(temp_dir));
    }
}

pub(crate) fn restore_materialization_backup(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    backup: Option<PathBuf>,
) -> Result<(), AppError> {
    let target = layout.v4_materialized_dir(workspace_name, source_name);
    if let Some(backup) = backup {
        if target.exists() {
            std::fs::remove_dir_all(&target)?;
        }
        if backup.exists() {
            std::fs::rename(backup, target)?;
        }
    } else if target.exists() {
        std::fs::remove_dir_all(target)?;
    }
    Ok(())
}

pub(crate) fn load_v4_materialization(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
) -> Result<V4MaterializedSource, AppError> {
    let fingerprint_path = layout.v4_fingerprint_file(workspace_name, source_name);
    let projections_path = layout.v4_projections_file(workspace_name, source_name);
    let diagnostics_path = layout.v4_diagnostics_file(workspace_name, source_name);
    if !fingerprint_path.exists() || !projections_path.exists() || !diagnostics_path.exists() {
        return Err(incompatible_materialization_error(
            source_name,
            "required artifact is missing",
        ));
    }
    let fingerprint: Fingerprint =
        read_artifact_yaml(source_name, "fingerprint", &fingerprint_path)?;
    validate_fingerprint_header(source_name, manifest, &fingerprint)?;
    if fingerprint.manifest_sha256 != sha256_hex(manifest_yaml.as_bytes()) {
        return Err(incompatible_materialization_error(
            source_name,
            "manifest fingerprint does not match installed manifest",
        ));
    }
    let fingerprint_surfaces = validate_fingerprint_surfaces(source_name, manifest, &fingerprint)?;
    let projections: ProjectionCatalog =
        read_artifact_yaml(source_name, "projection catalog", &projections_path)?;
    validate_projection_catalog_header(source_name, manifest, &projections)?;
    let diagnostics: Vec<Diagnostic> =
        read_artifact_yaml(source_name, "diagnostics", &diagnostics_path)?;
    let mut surfaces = Vec::new();
    for fingerprint_surface in &fingerprint.surfaces {
        let surface = manifest
            .surface(&fingerprint_surface.surface_id)
            .ok_or_else(|| {
                incompatible_materialization_error(
                    source_name,
                    format!(
                        "fingerprint references undeclared surface '{}'",
                        fingerprint_surface.surface_id
                    ),
                )
            })?;
        let surface_dir = layout.v4_surface_dir(workspace_name, source_name, &surface.id);
        let raw_source_document_path = surface_dir.join("source-document.raw");
        let normalized_source_document_path = surface_dir.join("source-document.yaml");
        let semantic_ir_path = surface_dir.join("semantic-ir.yaml");
        require_file(source_name, &raw_source_document_path)?;
        require_file(source_name, &normalized_source_document_path)?;
        require_file(source_name, &semantic_ir_path)?;
        let semantic_ir: SemanticIr =
            read_artifact_yaml(source_name, "semantic IR", &semantic_ir_path)?;
        surfaces.push(MaterializedSurface {
            surface_id: surface.id.clone(),
            semantic_ir,
            source_document_sha256: fingerprint_surface.descriptor_sha256.clone(),
            normalized_source_document_path,
            raw_source_document_path,
        });
    }
    let materialized = V4MaterializedSource {
        fingerprint,
        surfaces,
        projections,
        diagnostics,
    };
    validate_loaded_materialization(source_name, manifest, &materialized, &fingerprint_surfaces)?;
    Ok(materialized)
}

fn validate_fingerprint_header(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    fingerprint: &Fingerprint,
) -> Result<(), AppError> {
    if fingerprint.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(incompatible_materialization_error(
            source_name,
            "fingerprint artifact schema version mismatch",
        ));
    }
    if fingerprint.source_name != manifest.common.name {
        return Err(incompatible_materialization_error(
            source_name,
            "fingerprint source name does not match installed manifest",
        ));
    }
    if fingerprint.importer_version != SURFACE_IMPORTER_VERSION
        || fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err(incompatible_materialization_error(
            source_name,
            "fingerprint importer or generator version mismatch",
        ));
    }
    Ok(())
}

fn validate_fingerprint_surfaces(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    fingerprint: &Fingerprint,
) -> Result<BTreeMap<String, FingerprintSurface>, AppError> {
    let declared_ids = manifest
        .surfaces
        .iter()
        .map(|surface| surface.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_ids = BTreeSet::new();
    let mut by_id = BTreeMap::new();
    for surface in &fingerprint.surfaces {
        if !seen_ids.insert(surface.surface_id.as_str()) {
            return Err(incompatible_materialization_error(
                source_name,
                format!("fingerprint repeats surface '{}'", surface.surface_id),
            ));
        }
        if !declared_ids.contains(surface.surface_id.as_str()) {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "fingerprint surface set mismatch; missing [], extra [{}]",
                    surface.surface_id
                ),
            ));
        }
        by_id.insert(surface.surface_id.clone(), surface.clone());
    }
    for fingerprint_surface in &fingerprint.surfaces {
        let surface = manifest
            .surface(&fingerprint_surface.surface_id)
            .ok_or_else(|| {
                incompatible_materialization_error(
                    source_name,
                    format!(
                        "fingerprint references undeclared surface '{}'",
                        fingerprint_surface.surface_id
                    ),
                )
            })?;
        if fingerprint_surface.surface_type != surface.surface_type {
            return Err(incompatible_materialization_error(
                source_name,
                format!("surface '{}' type fingerprint does not match", surface.id),
            ));
        }
        if fingerprint_surface.descriptor_kind != surface.descriptor.kind()
            || fingerprint_surface.descriptor_location != surface.descriptor.location()
        {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "surface '{}' descriptor fingerprint does not match",
                    surface.id
                ),
            ));
        }
        let expected = stable_input_declarations_sha256(&surface.inputs)?;
        if fingerprint_surface.input_declarations_sha256 != expected {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "input declarations fingerprint does not match for surface '{}'",
                    surface.id
                ),
            ));
        }
    }
    Ok(by_id)
}

fn validate_projection_catalog_header(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    projections: &ProjectionCatalog,
) -> Result<(), AppError> {
    if projections.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(incompatible_materialization_error(
            source_name,
            "projection catalog artifact schema version mismatch",
        ));
    }
    if projections.source_name != manifest.common.name {
        return Err(incompatible_materialization_error(
            source_name,
            "projection catalog source name does not match installed manifest",
        ));
    }
    if projections.generator_version != PROJECTION_GENERATOR_VERSION {
        return Err(incompatible_materialization_error(
            source_name,
            "projection catalog generator version mismatch",
        ));
    }
    Ok(())
}

fn require_file(source_name: &SourceName, path: &Path) -> Result<(), AppError> {
    if path.is_file() {
        Ok(())
    } else {
        Err(incompatible_materialization_error(
            source_name,
            format!("required artifact '{}' is missing", path.display()),
        ))
    }
}

fn read_raw_source_document_artifact(
    source_name: &SourceName,
    surface: &coral_spec::v4::V4Surface,
    path: &Path,
) -> Result<Vec<u8>, AppError> {
    std::fs::read(path).map_err(|error| {
        incompatible_materialization_error(
            source_name,
            format!(
                "failed to read raw source document artifact for surface '{}' '{}': {error}",
                surface.id,
                path.display()
            ),
        )
    })
}

fn validate_loaded_materialization(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    fingerprint_surfaces: &BTreeMap<String, FingerprintSurface>,
) -> Result<(), AppError> {
    validate_materialized_source(manifest, materialized).map_err(|error| {
        incompatible_materialization_error(
            source_name,
            format!("artifact validation failed: {error}"),
        )
    })?;
    let mut operations_by_surface: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    for materialized_surface in &materialized.surfaces {
        let surface = manifest
            .surface(&materialized_surface.surface_id)
            .ok_or_else(|| {
                incompatible_materialization_error(
                    source_name,
                    format!(
                        "materialized surface '{}' is not declared",
                        materialized_surface.surface_id
                    ),
                )
            })?;
        let Some(fingerprint_surface) = fingerprint_surfaces.get(&materialized_surface.surface_id)
        else {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "fingerprint is missing surface '{}'",
                    materialized_surface.surface_id
                ),
            ));
        };
        let raw_bytes = read_raw_source_document_artifact(
            source_name,
            surface,
            &materialized_surface.raw_source_document_path,
        )?;
        let observed_raw_hash = sha256_hex(&raw_bytes);
        if observed_raw_hash != fingerprint_surface.descriptor_sha256 {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "raw source document hash does not match for surface '{}'",
                    surface.id
                ),
            ));
        }
        validate_semantic_ir(
            source_name,
            manifest,
            surface,
            &materialized_surface.semantic_ir,
        )?;
        operations_by_surface.insert(
            materialized_surface.surface_id.as_str(),
            materialized_surface
                .semantic_ir
                .operations
                .iter()
                .map(|operation| operation.id.as_str())
                .collect(),
        );
    }
    validate_projection_references(source_name, manifest, materialized, &operations_by_surface)
}

fn validate_projection_references(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    materialized: &V4MaterializedSource,
    operations_by_surface: &BTreeMap<&str, BTreeSet<&str>>,
) -> Result<(), AppError> {
    let relation_namespace_by_surface = manifest
        .surfaces
        .iter()
        .map(|surface| (surface.id.as_str(), surface.relation_namespace.as_str()))
        .collect::<BTreeMap<_, _>>();
    for projection in &materialized.projections.projections {
        let Some(operations) = operations_by_surface.get(projection.surface_id.as_str()) else {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "projection '{}' references missing surface '{}'",
                    projection.name, projection.surface_id
                ),
            ));
        };
        let expected_relation_namespace = relation_namespace_by_surface
            .get(projection.surface_id.as_str())
            .ok_or_else(|| {
                incompatible_materialization_error(
                    source_name,
                    format!(
                        "projection '{}' references missing surface '{}'",
                        projection.name, projection.surface_id
                    ),
                )
            })?;
        if projection.namespace != *expected_relation_namespace {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "projection '{}' namespace '{}' does not match surface '{}' relation namespace '{}'",
                    projection.name,
                    projection.namespace,
                    projection.surface_id,
                    expected_relation_namespace
                ),
            ));
        }
        if !operations.contains(projection.operation_id.as_str()) {
            return Err(incompatible_materialization_error(
                source_name,
                format!(
                    "projection '{}' references missing operation '{}'",
                    projection.name, projection.operation_id
                ),
            ));
        }
    }
    Ok(())
}

fn validate_semantic_ir(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    semantic_ir: &SemanticIr,
) -> Result<(), AppError> {
    if semantic_ir.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(incompatible_materialization_error(
            source_name,
            format!(
                "semantic IR schema version mismatch for surface '{}'",
                surface.id
            ),
        ));
    }
    if semantic_ir.source_name != manifest.common.name
        || semantic_ir.surface_id != surface.id
        || semantic_ir.surface_type != surface.surface_type
    {
        return Err(incompatible_materialization_error(
            source_name,
            format!("semantic IR identity mismatch for surface '{}'", surface.id),
        ));
    }
    if semantic_ir.importer_version != expected_importer_version(surface.surface_type) {
        return Err(incompatible_materialization_error(
            source_name,
            format!(
                "semantic IR importer version mismatch for surface '{}'",
                surface.id
            ),
        ));
    }
    Ok(())
}

fn expected_importer_version(surface_type: SurfaceType) -> &'static str {
    match surface_type {
        SurfaceType::OpenApi => OPENAPI_IMPORTER_VERSION,
        SurfaceType::Mcp => MCP_IMPORTER_VERSION,
    }
}

pub(crate) fn incompatible_materialization_error(
    source_name: &SourceName,
    detail: impl AsRef<str>,
) -> AppError {
    AppError::MissingOrIncompatibleV4Materialization {
        source_name: source_name.to_string(),
        detail: detail.as_ref().to_string(),
    }
}

fn write_materialization(
    temp_dir: &Path,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    inputs: &MaterializationInputs,
) -> Result<(), AppError> {
    let manifest_sha256 = sha256_hex(manifest_yaml.as_bytes());
    let mut materialized_surfaces = Vec::new();
    let mut semantic_irs = Vec::new();
    let mut fingerprint_surfaces = Vec::new();
    let mut diagnostics = Vec::new();
    let mut first_surface_error = None;
    for surface in &manifest.surfaces {
        let materialized_surface = match materialize_surface(manifest, surface, inputs) {
            Ok(materialized_surface) => materialized_surface,
            Err(error) => {
                let message = format!(
                    "failed to materialize source '{}' surface '{}': {error}",
                    manifest.common.name, surface.id
                );
                if first_surface_error.is_none() {
                    first_surface_error = Some(message.clone());
                }
                diagnostics.push(Diagnostic {
                    code: "SURFACE_MATERIALIZATION_FAILED".to_string(),
                    severity: DiagnosticSeverity::Warning,
                    message,
                    surface_id: Some(surface.id.clone()),
                    operation_id: None,
                    projection_name: None,
                });
                continue;
            }
        };
        let surface_dir = temp_dir.join("surfaces").join(&surface.id);
        fs::ensure_private_dir(&surface_dir)?;
        std::fs::write(
            surface_dir.join("source-document.raw"),
            &materialized_surface.raw_document,
        )?;
        std::fs::write(
            surface_dir.join("source-document.yaml"),
            &materialized_surface.normalized_document,
        )?;
        write_yaml(
            &surface_dir.join("semantic-ir.yaml"),
            &materialized_surface.semantic_ir,
        )?;
        materialized_surfaces.push(MaterializedSurface {
            surface_id: surface.id.clone(),
            semantic_ir: materialized_surface.semantic_ir.clone(),
            source_document_sha256: materialized_surface.observed_sha256.clone(),
            normalized_source_document_path: surface_dir.join("source-document.yaml"),
            raw_source_document_path: surface_dir.join("source-document.raw"),
        });
        semantic_irs.push(materialized_surface.semantic_ir);
        fingerprint_surfaces.push(FingerprintSurface {
            surface_id: surface.id.clone(),
            surface_type: surface.surface_type,
            descriptor_kind: surface.descriptor.kind().to_string(),
            descriptor_location: surface.descriptor.location(),
            descriptor_sha256: materialized_surface.observed_sha256,
            input_declarations_sha256: stable_input_declarations_sha256(&surface.inputs)?,
        });
    }
    if semantic_irs.is_empty() {
        return Err(AppError::Unavailable(first_surface_error.unwrap_or_else(
            || {
                format!(
                    "failed to materialize source '{}': no surfaces were materialized",
                    manifest.common.name
                )
            },
        )));
    }
    let projections = generate_projection_catalog(manifest, &semantic_irs)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    diagnostics.extend(projections.diagnostics.clone());
    for ir in &semantic_irs {
        diagnostics.extend(ir.diagnostics.clone());
        diagnostics.extend(
            ir.operations
                .iter()
                .flat_map(|operation| operation.diagnostics.clone()),
        );
    }
    let fingerprint = Fingerprint {
        artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
        source_name: manifest.common.name.clone(),
        manifest_sha256: manifest_sha256.clone(),
        surfaces: fingerprint_surfaces,
        importer_version: SURFACE_IMPORTER_VERSION.to_string(),
        projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
    };
    let materialized = V4MaterializedSource {
        fingerprint: fingerprint.clone(),
        surfaces: materialized_surfaces,
        projections: projections.clone(),
        diagnostics: diagnostics.clone(),
    };
    validate_materialized_source(manifest, &materialized)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    write_yaml(&temp_dir.join(FINGERPRINT_FILENAME), &fingerprint)?;
    write_yaml(&temp_dir.join(PROJECTIONS_FILENAME), &projections)?;
    write_yaml(&temp_dir.join(DIAGNOSTICS_FILENAME), &diagnostics)?;
    Ok(())
}

struct MaterializedSurfaceBuild {
    raw_document: Vec<u8>,
    normalized_document: Vec<u8>,
    observed_sha256: String,
    semantic_ir: SemanticIr,
}

fn materialize_surface(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    inputs: &MaterializationInputs,
) -> Result<MaterializedSurfaceBuild, AppError> {
    match surface.surface_type {
        SurfaceType::OpenApi => materialize_openapi_surface(manifest, surface),
        SurfaceType::Mcp => materialize_mcp_surface(manifest, surface, inputs),
    }
}

fn materialize_openapi_surface(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
) -> Result<MaterializedSurfaceBuild, AppError> {
    let bytes = read_openapi_descriptor(surface)?;
    validate_materialized_surface_base_url(manifest, surface, &bytes)?;
    let observed_sha256 = sha256_hex(&bytes);
    let semantic_ir = import_openapi_surface(manifest, surface, &bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to import source '{}' surface '{}': {error}",
            manifest.common.name, surface.id
        ))
    })?;
    let normalized_document = normalize_source_document(&bytes)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    Ok(MaterializedSurfaceBuild {
        raw_document: bytes,
        normalized_document: normalized_document.into_bytes(),
        observed_sha256,
        semantic_ir,
    })
}

fn materialize_mcp_surface(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    inputs: &MaterializationInputs,
) -> Result<MaterializedSurfaceBuild, AppError> {
    let catalog = discover_mcp_tool_catalog(manifest, surface, inputs)?;
    let normalized_document = normalize_mcp_tool_catalog(&catalog)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    let observed_sha256 = sha256_hex(&normalized_document);
    let semantic_ir = import_mcp_surface(manifest, surface, &catalog).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to import source '{}' surface '{}': {error}",
            manifest.common.name, surface.id
        ))
    })?;
    Ok(MaterializedSurfaceBuild {
        raw_document: normalized_document.clone(),
        normalized_document,
        observed_sha256,
        semantic_ir,
    })
}

fn discover_mcp_tool_catalog(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    inputs: &MaterializationInputs,
) -> Result<McpToolCatalog, AppError> {
    let runtime = surface.mcp_runtime().ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "DSL v4 surface '{}' is not an MCP surface",
            surface.id
        ))
    })?;
    let source_name = manifest.common.name.clone();
    let server = runtime.server.clone();
    let declared_inputs = surface.inputs.clone();
    let variables = inputs.variables.clone();
    let secrets = inputs.secrets.clone();
    std::thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(AppError::from)?;
        runtime
            .block_on(async {
                tokio::time::timeout(
                    DESCRIPTOR_FETCH_TIMEOUT,
                    coral_engine::discover_mcp_tool_catalog(
                        &source_name,
                        server,
                        &declared_inputs,
                        variables,
                        secrets,
                    ),
                )
                .await
            })
            .map_err(|_elapsed| {
                AppError::Unavailable(format!(
                    "timed out discovering MCP tools for source '{source_name}'"
                ))
            })?
            .map_err(app_error_from_core)
    })
    .join()
    .map_err(|_panic| {
        AppError::Unavailable(format!(
            "failed to discover MCP tools for source '{}' surface '{}': discovery thread panicked",
            manifest.common.name, surface.id
        ))
    })?
}

fn app_error_from_core(error: coral_engine::CoreError) -> AppError {
    match error {
        coral_engine::CoreError::InvalidInput(detail) => AppError::InvalidInput(detail),
        coral_engine::CoreError::Unavailable(detail) => AppError::Unavailable(detail),
        other => AppError::FailedPrecondition(other.to_string()),
    }
}

fn validate_materialized_surface_base_url(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    bytes: &[u8],
) -> Result<(), AppError> {
    let Some(openapi_runtime) = surface.openapi_runtime() else {
        return Ok(());
    };
    if !openapi_runtime.base_url.raw().trim().is_empty() {
        return Ok(());
    }
    let metadata = openapi_document_metadata(bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to derive base_url for DSL v4 surface '{}': {error}",
            surface.id
        ))
    })?;
    let Some(server_url) = metadata.server_url.as_deref() else {
        return Ok(());
    };
    let base_url = ParsedTemplate::parse(server_url).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse derived base_url for DSL v4 surface '{}': {error}",
            surface.id
        ))
    })?;
    validate_openapi_base_url_template(
        &manifest.common.name,
        &surface.id,
        &surface.inputs,
        &base_url,
        "derived OpenAPI server",
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum OpenApiDocumentLocation {
    Url(url::Url),
    File(PathBuf),
}

impl OpenApiDocumentLocation {
    fn cache_key(&self) -> String {
        match self {
            Self::Url(url) => url.as_str().to_string(),
            Self::File(file) => file.display().to_string(),
        }
    }

    fn display(&self) -> String {
        self.cache_key()
    }
}

#[derive(Debug)]
struct OpenApiDescriptor {
    location: OpenApiDocumentLocation,
    bytes: Vec<u8>,
}

#[derive(Debug)]
struct OpenApiUrlDescriptor {
    final_url: url::Url,
    bytes: Vec<u8>,
}

fn read_openapi_descriptor(surface: &coral_spec::v4::V4Surface) -> Result<Vec<u8>, AppError> {
    let descriptor = read_descriptor(surface)?;
    let root_value = parse_openapi_document(&descriptor.location, &descriptor.bytes)?;
    let resolver = OpenApiExternalRefResolver::new(descriptor.location, root_value);
    resolver.bundle()
}

fn read_descriptor(surface: &coral_spec::v4::V4Surface) -> Result<OpenApiDescriptor, AppError> {
    match &surface.descriptor {
        coral_spec::v4::SurfaceDescriptor::File { file } => {
            let canonical = canonicalize_file_descriptor(file)?;
            let bytes = read_file_descriptor(&canonical)?;
            Ok(OpenApiDescriptor {
                location: OpenApiDocumentLocation::File(canonical),
                bytes,
            })
        }
        coral_spec::v4::SurfaceDescriptor::Url { url } => {
            let mut parsed = parse_descriptor_url(url)?;
            parsed.set_fragment(None);
            let descriptor = read_url_descriptor(parsed.as_str())?;
            Ok(OpenApiDescriptor {
                location: OpenApiDocumentLocation::Url(descriptor.final_url),
                bytes: descriptor.bytes,
            })
        }
        coral_spec::v4::SurfaceDescriptor::McpServer { .. } => {
            Err(AppError::FailedPrecondition(format!(
                "DSL v4 MCP surface '{}' does not have an OpenAPI descriptor",
                surface.id
            )))
        }
    }
}

fn read_file_descriptor(file: &Path) -> Result<Vec<u8>, AppError> {
    let canonical = canonicalize_file_descriptor(file)?;
    let metadata = std::fs::metadata(&canonical)?;
    if metadata.len() > MAX_DESCRIPTOR_BYTES {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' is too large: {} bytes exceeds {MAX_DESCRIPTOR_BYTES}",
            file.display(),
            metadata.len()
        )));
    }
    std::fs::read(canonical).map_err(AppError::from)
}

fn parse_openapi_document(
    location: &OpenApiDocumentLocation,
    bytes: &[u8],
) -> Result<Value, AppError> {
    serde_yaml::from_slice(bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse OpenAPI descriptor '{}': {error}",
            location.display()
        ))
    })
}

struct OpenApiExternalRefResolver {
    root_location: OpenApiDocumentLocation,
    documents: BTreeMap<String, Value>,
    document_locations: BTreeMap<String, OpenApiDocumentLocation>,
    expanded_bytes: usize,
    expanded_nodes: usize,
    loaded_ref_bytes: usize,
}

impl OpenApiExternalRefResolver {
    fn new(root_location: OpenApiDocumentLocation, root_value: Value) -> Self {
        let mut documents = BTreeMap::new();
        let root_key = root_location.cache_key();
        documents.insert(root_key.clone(), root_value);
        let mut document_locations = BTreeMap::new();
        document_locations.insert(root_key, root_location.clone());
        Self {
            root_location,
            documents,
            document_locations,
            expanded_bytes: 0,
            expanded_nodes: 0,
            loaded_ref_bytes: 0,
        }
    }

    fn bundle(mut self) -> Result<Vec<u8>, AppError> {
        let mut resolving = BTreeSet::new();
        let root_location = self.root_location.clone();
        let root = self.document(&root_location)?.clone();
        let bundled = self.bundle_value(&root_location, root, 0, &mut resolving)?;
        let bytes = serde_yaml::to_string(&bundled)
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "failed to encode resolved OpenAPI descriptor '{}': {error}",
                    root_location.display()
                ))
            })?
            .into_bytes();
        if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_DESCRIPTOR_BYTES {
            return Err(AppError::FailedPrecondition(format!(
                "resolved OpenAPI descriptor '{}' is too large: exceeds {MAX_DESCRIPTOR_BYTES} bytes",
                root_location.display()
            )));
        }
        Ok(bytes)
    }

    fn bundle_value(
        &mut self,
        location: &OpenApiDocumentLocation,
        value: Value,
        depth: usize,
        resolving: &mut BTreeSet<String>,
    ) -> Result<Value, AppError> {
        let mut path = Vec::new();
        self.bundle_value_at(location, value, depth, resolving, &mut path, false)
    }

    fn bundle_value_at(
        &mut self,
        location: &OpenApiDocumentLocation,
        value: Value,
        depth: usize,
        resolving: &mut BTreeSet<String>,
        path: &mut Vec<String>,
        charge_expansion: bool,
    ) -> Result<Value, AppError> {
        if depth > MAX_OPENAPI_REF_DEPTH {
            return Err(AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' exceeds maximum $ref depth {MAX_OPENAPI_REF_DEPTH}",
                location.display()
            )));
        }
        if charge_expansion {
            self.reserve_expanded_node(location)?;
        }
        match value {
            Value::Object(object) => {
                if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
                    if should_inline_openapi_ref(location, &self.root_location, reference) {
                        return self.bundle_ref(location, reference, depth, resolving);
                    }
                    return Ok(Value::Object(object));
                }
                let mut bundled = serde_json::Map::with_capacity(object.len());
                for (key, value) in object {
                    if should_skip_openapi_ref_walk(path, &key) {
                        bundled.insert(key, value);
                        continue;
                    }
                    path.push(key.clone());
                    let value = self.bundle_value_at(
                        location,
                        value,
                        depth,
                        resolving,
                        path,
                        charge_expansion,
                    );
                    path.pop();
                    bundled.insert(key, value?);
                }
                Ok(Value::Object(bundled))
            }
            Value::Array(items) => items
                .into_iter()
                .map(|item| {
                    self.bundle_value_at(location, item, depth, resolving, path, charge_expansion)
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Value::Array),
            other => Ok(other),
        }
    }

    fn reserve_expanded_node(
        &mut self,
        location: &OpenApiDocumentLocation,
    ) -> Result<(), AppError> {
        if self.expanded_nodes >= MAX_OPENAPI_EXPANDED_NODES {
            return Err(AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' expands beyond {MAX_OPENAPI_EXPANDED_NODES} JSON nodes while resolving $refs",
                location.display()
            )));
        }
        self.expanded_nodes += 1;
        Ok(())
    }

    fn reserve_expanded_bytes(
        &mut self,
        location: &OpenApiDocumentLocation,
        bytes: usize,
    ) -> Result<(), AppError> {
        if self.expanded_bytes.saturating_add(bytes) > MAX_OPENAPI_EXPANDED_BYTES {
            return Err(AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' expands beyond {MAX_OPENAPI_EXPANDED_BYTES} bytes while resolving $refs",
                location.display()
            )));
        }
        self.expanded_bytes += bytes;
        Ok(())
    }

    fn reserve_loaded_ref_bytes(
        &mut self,
        location: &OpenApiDocumentLocation,
        bytes: usize,
    ) -> Result<(), AppError> {
        if self.loaded_ref_bytes.saturating_add(bytes) > MAX_OPENAPI_REF_LOADED_BYTES {
            return Err(AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' loads more than {MAX_OPENAPI_REF_LOADED_BYTES} bytes of external $ref documents",
                location.display()
            )));
        }
        self.loaded_ref_bytes += bytes;
        Ok(())
    }

    fn bundle_ref(
        &mut self,
        base: &OpenApiDocumentLocation,
        reference: &str,
        depth: usize,
        resolving: &mut BTreeSet<String>,
    ) -> Result<Value, AppError> {
        let resolved = resolve_openapi_ref_location(base, reference)?;
        self.validate_ref_boundary(reference, &resolved.location)?;
        let document_location = self.ensure_document(&resolved.location)?;
        self.validate_ref_boundary(reference, &document_location)?;
        let guard = format!(
            "{}#{}",
            document_location.cache_key(),
            resolved.pointer.as_deref().unwrap_or_default()
        );
        if !resolving.insert(guard.clone()) {
            return Ok(json!({
                "type": "object",
                "additionalProperties": true,
            }));
        }

        let estimated_bytes = {
            let document = self.document(&document_location)?;
            let target =
                openapi_ref_target(document, resolved.pointer.as_deref()).ok_or_else(|| {
                    AppError::FailedPrecondition(format!(
                        "OpenAPI descriptor '{}' reference '{reference}' was not found",
                        base.display()
                    ))
                })?;
            estimated_json_value_bytes(target)
        };
        self.reserve_expanded_bytes(&document_location, estimated_bytes)?;
        let document = self.document(&document_location)?.clone();
        let target = openapi_ref_target(&document, resolved.pointer.as_deref())
            .cloned()
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "OpenAPI descriptor '{}' reference '{reference}' was not found",
                    base.display()
                ))
            })?;
        let mut path = Vec::new();
        let bundled = self.bundle_value_at(
            &document_location,
            target,
            depth + 1,
            resolving,
            &mut path,
            true,
        );
        resolving.remove(&guard);
        bundled
    }

    fn validate_ref_boundary(
        &self,
        reference: &str,
        location: &OpenApiDocumentLocation,
    ) -> Result<(), AppError> {
        match (&self.root_location, location) {
            (OpenApiDocumentLocation::File(root), OpenApiDocumentLocation::File(file)) => {
                let root_dir = root.parent().unwrap_or_else(|| Path::new("/"));
                if !file.starts_with(root_dir) {
                    return Err(AppError::FailedPrecondition(format!(
                        "OpenAPI descriptor '{}' reference '{reference}' resolves outside descriptor directory '{}'",
                        self.root_location.display(),
                        root_dir.display()
                    )));
                }
            }
            (OpenApiDocumentLocation::Url(root), OpenApiDocumentLocation::Url(url)) => {
                if !same_url_origin(root, url) {
                    return Err(AppError::FailedPrecondition(format!(
                        "OpenAPI descriptor '{}' reference '{reference}' resolves outside descriptor origin '{}'",
                        self.root_location.display(),
                        url_origin_display(root)
                    )));
                }
            }
            (OpenApiDocumentLocation::Url(_), OpenApiDocumentLocation::File(_)) => {
                return Err(AppError::FailedPrecondition(format!(
                    "OpenAPI descriptor '{}' reference '{reference}' resolves to a local file",
                    self.root_location.display()
                )));
            }
            (OpenApiDocumentLocation::File(_), OpenApiDocumentLocation::Url(_)) => {}
        }
        Ok(())
    }

    fn document(&mut self, location: &OpenApiDocumentLocation) -> Result<&Value, AppError> {
        let effective_location = self.ensure_document(location)?;
        let key = effective_location.cache_key();
        self.documents.get(&key).ok_or_else(|| {
            AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' could not be loaded",
                effective_location.display()
            ))
        })
    }

    fn ensure_document(
        &mut self,
        location: &OpenApiDocumentLocation,
    ) -> Result<OpenApiDocumentLocation, AppError> {
        let key = location.cache_key();
        if let Some(effective_location) = self.document_locations.get(&key) {
            return Ok(effective_location.clone());
        }
        if self.documents.len() >= MAX_OPENAPI_REF_DOCUMENTS {
            return Err(AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' references more than {MAX_OPENAPI_REF_DOCUMENTS} documents",
                self.root_location.display()
            )));
        }
        let (effective_location, bytes) = match location {
            OpenApiDocumentLocation::Url(url) => {
                let allowed_origin = match &self.root_location {
                    OpenApiDocumentLocation::Url(root) => Some(root),
                    OpenApiDocumentLocation::File(_) => None,
                };
                let descriptor =
                    read_url_descriptor_with_allowed_origin(url.as_str(), allowed_origin)?;
                (
                    OpenApiDocumentLocation::Url(descriptor.final_url),
                    descriptor.bytes,
                )
            }
            OpenApiDocumentLocation::File(file) => (
                OpenApiDocumentLocation::File(file.clone()),
                read_file_descriptor(file)?,
            ),
        };
        self.reserve_loaded_ref_bytes(&effective_location, bytes.len())?;
        let effective_key = effective_location.cache_key();
        let value = parse_openapi_document(&effective_location, &bytes)?;
        self.documents.insert(effective_key.clone(), value);
        self.document_locations
            .insert(key, effective_location.clone());
        self.document_locations
            .insert(effective_key, effective_location.clone());
        if !self.documents.contains_key(&effective_location.cache_key()) {
            return Err(AppError::FailedPrecondition(format!(
                "OpenAPI descriptor '{}' could not be cached",
                effective_location.display()
            )));
        }
        Ok(effective_location)
    }
}

fn openapi_ref_target<'a>(document: &'a Value, pointer: Option<&str>) -> Option<&'a Value> {
    match pointer {
        Some(pointer) if !pointer.is_empty() => document.pointer(pointer),
        Some(_) | None => Some(document),
    }
}

fn estimated_json_value_bytes(value: &Value) -> usize {
    match value {
        Value::Null => 4,
        Value::Bool(_) => 5,
        Value::Number(number) => number.to_string().len(),
        Value::String(string) => string.len().saturating_add(2),
        Value::Array(items) => items.iter().fold(2usize, |total, item| {
            total
                .saturating_add(1)
                .saturating_add(estimated_json_value_bytes(item))
        }),
        Value::Object(object) => object.iter().fold(2usize, |total, (key, value)| {
            total
                .saturating_add(key.len())
                .saturating_add(4)
                .saturating_add(estimated_json_value_bytes(value))
        }),
    }
}

fn should_inline_openapi_ref(
    location: &OpenApiDocumentLocation,
    root_location: &OpenApiDocumentLocation,
    reference: &str,
) -> bool {
    !reference.starts_with('#') || location != root_location
}

#[derive(Debug)]
struct OpenApiResolvedRef {
    location: OpenApiDocumentLocation,
    pointer: Option<String>,
}

fn resolve_openapi_ref_location(
    base: &OpenApiDocumentLocation,
    reference: &str,
) -> Result<OpenApiResolvedRef, AppError> {
    let (document_ref, pointer) = split_openapi_ref(reference);
    let pointer = pointer
        .map(json_pointer_from_fragment)
        .transpose()
        .map_err(|error| {
            AppError::InvalidInput(format!(
                "OpenAPI descriptor reference '{reference}' has an invalid URI fragment: {error}"
            ))
        })?;
    let location = if document_ref.is_empty() {
        base.clone()
    } else if url::Url::parse(document_ref).is_ok() {
        let mut url = parse_descriptor_url(document_ref)?;
        url.set_fragment(None);
        OpenApiDocumentLocation::Url(url)
    } else {
        match base {
            OpenApiDocumentLocation::Url(url) => {
                let mut resolved = url.join(document_ref).map_err(|error| {
                    AppError::InvalidInput(format!(
                        "OpenAPI descriptor reference '{reference}' is invalid relative to '{}': {error}",
                        url.as_str()
                    ))
                })?;
                resolved.set_fragment(None);
                OpenApiDocumentLocation::Url(resolved)
            }
            OpenApiDocumentLocation::File(file) => {
                let path = file_path_from_openapi_document_ref(reference, document_ref)?;
                let candidate = if path.is_absolute() {
                    path
                } else {
                    file.parent().unwrap_or_else(|| Path::new(".")).join(path)
                };
                OpenApiDocumentLocation::File(canonicalize_file_descriptor(&candidate)?)
            }
        }
    };
    Ok(OpenApiResolvedRef { location, pointer })
}

fn split_openapi_ref(reference: &str) -> (&str, Option<&str>) {
    reference
        .split_once('#')
        .map_or((reference, None), |(document, pointer)| {
            (document, Some(pointer))
        })
}

fn should_skip_openapi_ref_walk(path: &[String], key: &str) -> bool {
    if path.is_empty() && matches!(key, "x-path-items" | "x-paths") {
        return false;
    }
    let named_map = is_openapi_named_map(path);
    (matches!(key, "example" | "examples") || key.starts_with("x-")) && !named_map
}

fn is_openapi_named_map(path: &[String]) -> bool {
    let Some(last) = path.last().map(String::as_str) else {
        return false;
    };
    if matches!(last, "properties" | "$defs") {
        return true;
    }
    matches!(
        path,
        [component, map]
            if component == "components"
                && matches!(
                    map.as_str(),
                    "callbacks"
                        | "examples"
                        | "headers"
                        | "links"
                        | "parameters"
                        | "pathItems"
                        | "requestBodies"
                        | "responses"
                        | "schemas"
                        | "securitySchemes"
                )
    )
}

fn same_url_origin(left: &url::Url, right: &url::Url) -> bool {
    left.scheme() == right.scheme()
        && left.host_str() == right.host_str()
        && left.port_or_known_default() == right.port_or_known_default()
}

fn url_origin_display(url: &url::Url) -> String {
    match (url.host_str(), url.port_or_known_default()) {
        (Some(host), Some(port)) => format!("{}://{}:{}", url.scheme(), host, port),
        (Some(host), None) => format!("{}://{}", url.scheme(), host),
        (None, _) => url.scheme().to_string(),
    }
}

fn file_path_from_openapi_document_ref(
    reference: &str,
    document_ref: &str,
) -> Result<PathBuf, AppError> {
    let decoded = percent_decode_str(document_ref)
        .decode_utf8()
        .map_err(|error| {
            AppError::InvalidInput(format!(
                "OpenAPI descriptor reference '{reference}' has an invalid URI path: {error}"
            ))
        })?;
    Ok(Path::new(decoded.as_ref()).to_path_buf())
}

fn json_pointer_from_fragment(fragment: &str) -> Result<String, std::str::Utf8Error> {
    let decoded = percent_decode_str(fragment).decode_utf8()?;
    Ok(if decoded.is_empty() {
        String::new()
    } else if decoded.starts_with('/') {
        decoded.into_owned()
    } else {
        format!("/{decoded}")
    })
}

pub(crate) fn canonicalize_file_descriptor(file: &Path) -> Result<PathBuf, AppError> {
    if !file.is_absolute() {
        return Err(AppError::InvalidInput(format!(
            "OpenAPI descriptor '{}' is relative, but imported DSL v4 manifests must use absolute file descriptors. Use `coral source add --file <manifest>` so Coral can resolve relative descriptors from the manifest directory.",
            file.display()
        )));
    }
    let metadata = std::fs::symlink_metadata(file)?;
    if metadata.file_type().is_symlink() {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' must not be a symlink",
            file.display()
        )));
    }
    if !metadata.file_type().is_file() {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' must be a regular file",
            file.display()
        )));
    }
    let canonical = file.canonicalize()?;
    Ok(canonical)
}

fn read_url_descriptor(url: &str) -> Result<OpenApiUrlDescriptor, AppError> {
    read_url_descriptor_with_allowed_origin(url, None)
}

fn read_url_descriptor_with_allowed_origin(
    url: &str,
    allowed_origin: Option<&url::Url>,
) -> Result<OpenApiUrlDescriptor, AppError> {
    let url = url.to_string();
    let panic_url = url.clone();
    let allowed_origin = allowed_origin.cloned();
    std::thread::spawn(move || read_url_descriptor_on_blocking_thread(&url, allowed_origin))
        .join()
        .map_err(|_panic| {
            AppError::Unavailable(format!(
                "failed to fetch OpenAPI descriptor '{panic_url}': fetch thread panicked"
            ))
        })?
}

fn read_url_descriptor_on_blocking_thread(
    url: &str,
    allowed_origin: Option<url::Url>,
) -> Result<OpenApiUrlDescriptor, AppError> {
    ensure_https_descriptor_url(url)?;
    let redirect_origin = allowed_origin;
    let client = reqwest::blocking::Client::builder()
        .timeout(DESCRIPTOR_FETCH_TIMEOUT)
        .https_only(true)
        .redirect(reqwest::redirect::Policy::custom(move |attempt| {
            if attempt.previous().len() >= 5 {
                return attempt.error("too many redirects");
            }
            if attempt.url().scheme() != "https" {
                return attempt.error("redirect target must use HTTPS");
            }
            if let Some(origin) = redirect_origin.as_ref()
                && !same_url_origin(origin, attempt.url())
            {
                return attempt.error("redirect target leaves OpenAPI descriptor origin");
            }
            attempt.follow()
        }))
        .user_agent(DESCRIPTOR_USER_AGENT)
        .build()
        .map_err(|error| {
            AppError::Unavailable(format!(
                "failed to build OpenAPI descriptor client for '{url}': {error}"
            ))
        })?;
    let mut response = client.get(url).send().map_err(|error| {
        AppError::Unavailable(format!(
            "failed to fetch OpenAPI descriptor '{url}': {error}"
        ))
    })?;
    if response.url().scheme() != "https" {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{url}' redirected to non-HTTPS URL '{}'",
            response.url()
        )));
    }
    if !response.status().is_success() {
        return Err(AppError::Unavailable(format!(
            "failed to fetch OpenAPI descriptor '{url}': HTTP {}",
            response.status()
        )));
    }
    let mut final_url = response.url().clone();
    final_url.set_fragment(None);
    if let Some(length) = response.content_length()
        && length > MAX_DESCRIPTOR_BYTES
    {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{url}' is too large: {length} bytes exceeds {MAX_DESCRIPTOR_BYTES}"
        )));
    }
    let mut bytes = Vec::new();
    let mut limited = response.by_ref().take(MAX_DESCRIPTOR_BYTES + 1);
    limited.read_to_end(&mut bytes).map_err(|error| {
        AppError::Unavailable(format!(
            "failed to read OpenAPI descriptor '{url}': {error}"
        ))
    })?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > MAX_DESCRIPTOR_BYTES {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{url}' is too large: exceeds {MAX_DESCRIPTOR_BYTES} bytes"
        )));
    }
    Ok(OpenApiUrlDescriptor { final_url, bytes })
}

fn ensure_https_descriptor_url(url: &str) -> Result<(), AppError> {
    parse_descriptor_url(url).map(|_| ())
}

fn parse_descriptor_url(url: &str) -> Result<url::Url, AppError> {
    let parsed = url::Url::parse(url).map_err(|error| {
        AppError::InvalidInput(format!(
            "OpenAPI descriptor URL '{url}' is invalid: {error}"
        ))
    })?;
    if parsed.scheme() != "https" {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor URL '{url}' must use HTTPS"
        )));
    }
    Ok(parsed)
}

fn stable_input_declarations_sha256(inputs: &[ManifestInputSpec]) -> Result<String, AppError> {
    let stable = inputs.iter().map(stable_input_spec).collect::<Vec<_>>();
    let bytes = serde_json::to_vec(&stable).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to encode DSL v4 input declarations fingerprint: {error}"
        ))
    })?;
    Ok(sha256_hex(&bytes))
}

fn stable_input_spec(input: &ManifestInputSpec) -> Value {
    json!({
        "key": &input.key,
        "kind": stable_input_kind(input.kind),
        "required": input.required,
        "default_value": &input.default_value,
        "hint": &input.hint,
        "credential": input.credential.as_ref().map(stable_credential_spec),
    })
}

fn stable_credential_spec(credential: &coral_spec::ManifestCredentialSpec) -> Value {
    json!({
        "methods": credential
            .methods
            .iter()
            .map(stable_credential_method)
            .collect::<Vec<_>>(),
    })
}

fn stable_credential_method(method: &ManifestCredentialMethod) -> Value {
    json!({
        "kind": stable_credential_method_kind(method.kind),
        "label": &method.label,
        "description": &method.description,
        "hint": &method.hint,
        "oauth": method.oauth.as_ref().map(stable_oauth_credential),
    })
}

fn stable_oauth_credential(oauth: &coral_spec::ManifestOAuthCredentialSpec) -> Value {
    json!({
        "flow": {
            "kind": stable_oauth_flow_kind(oauth.flow.kind),
            "pkce": stable_oauth_pkce_mode(oauth.flow.pkce),
        },
        "redirect_uri": &oauth.redirect_uri,
        "redirect_uri_port_mode": stable_redirect_uri_port_mode(oauth.redirect_uri_port_mode),
        "authorization_url": &oauth.authorization_url,
        "device_authorization_url": &oauth.device_authorization_url,
        "token_url": &oauth.token_url,
        "client": {
            "id": {
                "default": &oauth.client.id.default,
                "input": &oauth.client.id.input,
            },
            "secret": oauth.client.secret.as_ref().map(|secret| json!({
                "input": &secret.input,
                "transport": stable_client_secret_transport(secret.transport),
            })),
        },
        "scopes": oauth.scopes.as_ref().map(|scopes| json!({
            "scope": {
                "delimiter": stable_scope_delimiter(scopes.scope.delimiter),
                "values": &scopes.scope.values,
            },
        })),
    })
}

fn stable_input_kind(kind: ManifestInputKind) -> &'static str {
    match kind {
        ManifestInputKind::Variable => "variable",
        ManifestInputKind::Secret => "secret",
    }
}

fn stable_credential_method_kind(kind: ManifestCredentialMethodKind) -> &'static str {
    match kind {
        ManifestCredentialMethodKind::SourceConfig => "source_config",
        ManifestCredentialMethodKind::OAuth => "oauth",
    }
}

fn stable_oauth_flow_kind(kind: ManifestOAuthFlowKind) -> &'static str {
    match kind {
        ManifestOAuthFlowKind::AuthorizationCode => "authorization_code",
        ManifestOAuthFlowKind::DeviceCode => "device_code",
    }
}

fn stable_oauth_pkce_mode(mode: ManifestOAuthPkceMode) -> &'static str {
    match mode {
        ManifestOAuthPkceMode::Required => "required",
        ManifestOAuthPkceMode::Disabled => "disabled",
    }
}

fn stable_redirect_uri_port_mode(mode: ManifestOAuthRedirectUriPortMode) -> &'static str {
    match mode {
        ManifestOAuthRedirectUriPortMode::Fixed => "fixed",
        ManifestOAuthRedirectUriPortMode::Random => "random",
    }
}

fn stable_client_secret_transport(transport: ManifestOAuthClientSecretTransport) -> &'static str {
    match transport {
        ManifestOAuthClientSecretTransport::BasicAuth => "basic_auth",
        ManifestOAuthClientSecretTransport::RequestBody => "request_body",
    }
}

fn stable_scope_delimiter(delimiter: ManifestOAuthScopeDelimiter) -> &'static str {
    match delimiter {
        ManifestOAuthScopeDelimiter::Space => "space",
        ManifestOAuthScopeDelimiter::Comma => "comma",
    }
}

fn read_yaml<T: serde::de::DeserializeOwned>(path: &Path) -> Result<T, AppError> {
    let bytes = std::fs::read(path)?;
    serde_yaml::from_slice(&bytes).map_err(AppError::from)
}

fn read_artifact_yaml<T: serde::de::DeserializeOwned>(
    source_name: &SourceName,
    artifact: &str,
    path: &Path,
) -> Result<T, AppError> {
    read_yaml(path).map_err(|error| {
        incompatible_materialization_error(
            source_name,
            format!(
                "failed to read {artifact} artifact '{}': {error}",
                path.display()
            ),
        )
    })
}

fn write_yaml<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::ensure_private_dir(parent)?;
    }
    let bytes = serde_yaml::to_string(value)?;
    fs::write_atomic(path, bytes.as_bytes())?;
    Ok(())
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    format!("{:x}", hasher.finalize())
}

pub(crate) fn new_materialization_suffix(prefix: &str) -> String {
    format!("{prefix}.{}", Uuid::new_v4())
}

#[cfg(test)]
mod tests {
    use coral_spec::parse_source_manifest_yaml;
    use serde_json::{Value, json};
    use tempfile::TempDir;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::*;

    fn workspace_name() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn source_name() -> SourceName {
        SourceName::parse("github_v4_materialization_test").expect("source name")
    }

    fn openapi_fixture() -> &'static str {
        r"
openapi: 3.0.3
paths:
  /issues:
    get:
      operationId: issues/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: integer}
"
    }

    fn json_rpc_result_response(id: Value, result: Value) -> ResponseTemplate {
        let mut body = serde_json::Map::new();
        body.insert("jsonrpc".to_string(), Value::String("2.0".to_string()));
        body.insert("id".to_string(), id);
        body.insert("result".to_string(), result);
        ResponseTemplate::new(200)
            .append_header("Content-Type", "application/json")
            .set_body_json(Value::Object(body))
    }

    fn json_rpc_request_id(body: &Value) -> Option<Value> {
        body.get("id").cloned()
    }

    fn missing_json_rpc_request_id_response() -> ResponseTemplate {
        ResponseTemplate::new(400).set_body_string("JSON-RPC request is missing id")
    }

    async fn mount_mcp_materialization_server(server: &MockServer) {
        Mock::given(method("POST"))
            .respond_with(|request: &wiremock::Request| {
                let body: Value = request.body_json().expect("JSON-RPC request body");
                match body.get("method").and_then(Value::as_str) {
                    Some("initialize") => json_rpc_request_id(&body).map_or_else(
                        missing_json_rpc_request_id_response,
                        |id| {
                            json_rpc_result_response(
                                id,
                                json!({
                                    "protocolVersion": "2025-03-26",
                                    "capabilities": { "tools": {} },
                                    "serverInfo": { "name": "test-mcp", "version": "1.0.0" }
                                }),
                            )
                        },
                    ),
                    Some("notifications/initialized") => ResponseTemplate::new(202),
                    Some("tools/list") => json_rpc_request_id(&body).map_or_else(
                        missing_json_rpc_request_id_response,
                        |id| {
                            json_rpc_result_response(
                                id,
                                json!({
                                    "tools": [{
                                        "name": "list_items",
                                        "description": "List items",
                                        "inputSchema": {
                                            "type": "object",
                                            "properties": {
                                                "cursor": { "type": "string" }
                                            }
                                        },
                                        "outputSchema": {
                                            "type": "object",
                                            "properties": {
                                                "items": {
                                                    "type": "array",
                                                    "items": {
                                                        "type": "object",
                                                        "properties": {
                                                            "id": { "type": "string" }
                                                        }
                                                    }
                                                },
                                                "meta": {
                                                    "type": "object",
                                                    "properties": {
                                                        "nextCursor": { "type": ["string", "null"] }
                                                    }
                                                }
                                            }
                                        },
                                        "annotations": { "readOnlyHint": true }
                                    }]
                                }),
                            )
                        },
                    ),
                    other => ResponseTemplate::new(404)
                        .set_body_string(format!("unexpected MCP method {other:?}")),
                }
            })
            .mount(server)
            .await;
    }

    #[test]
    fn json_rpc_request_id_rejects_missing_request_id() {
        let result = json_rpc_request_id(&json!({
            "jsonrpc": "2.0",
            "method": "tools/list"
        }));

        assert!(result.is_none(), "request methods must include an id");
    }

    fn setup_materialization() -> (TempDir, TempDir, AppStateLayout, String, V4SourceManifest) {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("openapi.yaml");
        std::fs::write(&openapi_file, openapi_fixture()).expect("write descriptor");

        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: github_v4_materialization_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
",
            openapi_file.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build materialization");
        replace_v4_materialization(&layout, &workspace_name(), &source_name(), &build.temp_dir)
            .expect("install materialization");
        (state_temp, descriptor_temp, layout, manifest_yaml, manifest)
    }

    fn write_openapi_external_ref_fixture() -> (TempDir, PathBuf) {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let root = descriptor_temp.path().join("openapi.yaml");
        let paths_dir = descriptor_temp.path().join("paths");
        let parameters_dir = descriptor_temp.path().join("components").join("parameters");
        let schemas_dir = descriptor_temp.path().join("components").join("schemas");
        std::fs::create_dir_all(&paths_dir).expect("paths dir");
        std::fs::create_dir_all(&parameters_dir).expect("parameters dir");
        std::fs::create_dir_all(&schemas_dir).expect("schemas dir");
        std::fs::write(
            &root,
            r"
openapi: 3.0.3
paths:
  /items:
    $ref: paths/items.json
",
        )
        .expect("write root descriptor");
        std::fs::write(
            paths_dir.join("items.json"),
            r#"
{
  "get": {
    "operationId": "items/list",
    "parameters": [
      { "$ref": "../components/parameters/cursor.json#/Cursor" }
    ],
    "responses": {
      "200": {
        "content": {
          "application/json": {
            "schema": {
              "type": "array",
              "items": { "$ref": "../components/schemas/item%20schema.json#/Item%20Type" }
            }
          }
        }
      }
    }
  }
}
"#,
        )
        .expect("write path item");
        std::fs::write(
            parameters_dir.join("cursor.json"),
            r#"
{
  "Cursor": {
    "name": "cursor",
    "in": "query",
    "description": "Pagination cursor.",
    "schema": { "type": "string" }
  }
}
"#,
        )
        .expect("write parameter");
        std::fs::write(
            schemas_dir.join("item schema.json"),
            r#"
{
  "Item Type": {
    "type": "object",
    "required": ["id"],
    "properties": {
      "id": { "type": "string" },
      "name": { "type": "string" },
      "examples": { "$ref": "example%20field.json#/Example%20Field" }
    }
  }
}
"#,
        )
        .expect("write schema");
        std::fs::write(
            schemas_dir.join("example field.json"),
            r#"
{
  "Example Field": {
    "type": "string"
  }
}
"#,
        )
        .expect("write example property schema");
        (descriptor_temp, root)
    }

    #[test]
    fn resolve_openapi_ref_location_accepts_absolute_https_refs_from_file_base() {
        let base = OpenApiDocumentLocation::File(PathBuf::from("/tmp/openapi.yaml"));

        let resolved = resolve_openapi_ref_location(
            &base,
            "https://example.com/schemas.yaml#/components/schemas/Foo%20Bar",
        )
        .expect("resolve ref");

        let OpenApiDocumentLocation::Url(url) = resolved.location else {
            panic!("expected URL location");
        };
        assert_eq!(url.as_str(), "https://example.com/schemas.yaml");
        assert_eq!(
            resolved.pointer.as_deref(),
            Some("/components/schemas/Foo Bar")
        );
    }

    #[test]
    fn resolve_openapi_ref_location_rejects_non_https_absolute_refs() {
        let base = OpenApiDocumentLocation::File(PathBuf::from("/tmp/openapi.yaml"));

        let error = resolve_openapi_ref_location(
            &base,
            "http://example.com/schemas.yaml#/components/schemas/Foo",
        )
        .expect_err("non-HTTPS refs must fail");

        assert!(
            error.to_string().contains("must use HTTPS"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn json_pointer_from_fragment_decodes_uri_fragments() {
        assert_eq!(
            json_pointer_from_fragment("/components/schemas/Foo%20Bar").expect("decode space"),
            "/components/schemas/Foo Bar"
        );
        assert_eq!(
            json_pointer_from_fragment("/components/schemas/Foo+Bar").expect("preserve plus"),
            "/components/schemas/Foo+Bar"
        );
        assert!(
            json_pointer_from_fragment("/components/schemas/%FF").is_err(),
            "invalid UTF-8 escapes must fail"
        );
    }

    #[test]
    fn openapi_ref_bundler_rejects_expansion_budget_exhaustion() {
        let root = OpenApiDocumentLocation::File(PathBuf::from("/tmp/openapi.yaml"));
        let mut resolver = OpenApiExternalRefResolver::new(root.clone(), json!({}));
        resolver.expanded_nodes = MAX_OPENAPI_EXPANDED_NODES;
        let mut resolving = BTreeSet::new();
        let mut path = Vec::new();

        let error = resolver
            .bundle_value_at(&root, json!(null), 0, &mut resolving, &mut path, true)
            .expect_err("exhausted expansion budget must fail");

        assert!(
            error.to_string().contains("expands beyond"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openapi_ref_bundler_rejects_expanded_byte_budget_exhaustion() {
        let root = OpenApiDocumentLocation::File(PathBuf::from("/tmp/openapi.yaml"));
        let mut resolver = OpenApiExternalRefResolver::new(root.clone(), json!({}));
        resolver.expanded_bytes = MAX_OPENAPI_EXPANDED_BYTES - 1;

        let error = resolver
            .reserve_expanded_bytes(&root, 2)
            .expect_err("exhausted byte budget must fail");

        assert!(
            error.to_string().contains("expands beyond"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openapi_ref_bundler_rejects_loaded_ref_byte_budget_exhaustion() {
        let root = OpenApiDocumentLocation::File(PathBuf::from("/tmp/openapi.yaml"));
        let mut resolver = OpenApiExternalRefResolver::new(root.clone(), json!({}));
        resolver.loaded_ref_bytes = MAX_OPENAPI_REF_LOADED_BYTES - 1;

        let error = resolver
            .reserve_loaded_ref_bytes(&root, 2)
            .expect_err("exhausted loaded-byte budget must fail");

        assert!(
            error.to_string().contains("external $ref documents"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openapi_ref_boundary_rejects_file_refs_outside_root_dir() {
        let root = OpenApiDocumentLocation::File(PathBuf::from("/tmp/spec/openapi.yaml"));
        let resolver = OpenApiExternalRefResolver::new(root, json!({}));
        let outside = OpenApiDocumentLocation::File(PathBuf::from("/tmp/secrets.yaml"));

        let error = resolver
            .validate_ref_boundary("../secrets.yaml", &outside)
            .expect_err("outside file refs must fail");

        assert!(
            error.to_string().contains("outside descriptor directory"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn openapi_ref_boundary_rejects_url_refs_outside_root_origin() {
        let root = OpenApiDocumentLocation::Url(
            parse_descriptor_url("https://api.example.com/openapi.yaml").expect("root URL"),
        );
        let resolver = OpenApiExternalRefResolver::new(root, json!({}));
        let same_origin = OpenApiDocumentLocation::Url(
            parse_descriptor_url("https://api.example.com/components.yaml")
                .expect("same-origin URL"),
        );
        resolver
            .validate_ref_boundary("components.yaml", &same_origin)
            .expect("same-origin ref");
        let cross_origin = OpenApiDocumentLocation::Url(
            parse_descriptor_url("https://other.example.com/components.yaml")
                .expect("cross-origin URL"),
        );

        let error = resolver
            .validate_ref_boundary("https://other.example.com/components.yaml", &cross_origin)
            .expect_err("cross-origin refs must fail");

        assert!(
            error.to_string().contains("outside descriptor origin"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn build_v4_materialization_rejects_external_file_refs_outside_descriptor_dir() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let outside_temp = TempDir::new().expect("outside temp dir");
        let root = descriptor_temp.path().join("openapi.yaml");
        let outside = outside_temp.path().join("secret.yaml");
        std::fs::write(
            &outside,
            r"
Secret:
  type: object
  properties:
    value: {type: string}
",
        )
        .expect("write outside file");
        std::fs::write(
            &root,
            format!(
                r#"
openapi: 3.0.3
paths:
  /items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: "{}#/Secret"
"#,
                outside.display()
            ),
        )
        .expect("write root descriptor");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: openapi_ref_boundary_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
",
            root.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let source_name = SourceName::parse("openapi_ref_boundary_test").expect("source");

        let error = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect_err("outside file refs must fail");

        assert!(
            error.to_string().contains("outside descriptor directory"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn build_v4_materialization_ignores_ref_shaped_example_payloads() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let root = descriptor_temp.path().join("openapi.yaml");
        std::fs::write(
            &root,
            r#"
openapi: 3.0.3
x-example-payload:
  $ref: "not-a-file.yaml"
paths:
  /items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  type: object
                  properties:
                    id: {type: string}
                  example:
                    $ref: "also-not-a-file.yaml"
"#,
        )
        .expect("write root descriptor");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: openapi_ref_example_payload_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
",
            root.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let source_name = SourceName::parse("openapi_ref_example_payload_test").expect("source");

        build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build materialization");
    }

    #[test]
    fn build_v4_materialization_bundles_external_refs_in_x_path_items() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let root = descriptor_temp.path().join("openapi.yaml");
        let schemas_dir = descriptor_temp.path().join("components").join("schemas");
        std::fs::create_dir_all(&schemas_dir).expect("schemas dir");
        std::fs::write(
            &root,
            r"
openapi: 3.0.3
paths:
  /items:
    $ref: '#/x-path-items/items'
x-path-items:
  items:
    get:
      operationId: items/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: components/schemas/item.json#/Item
",
        )
        .expect("write root descriptor");
        std::fs::write(
            schemas_dir.join("item.json"),
            r#"
{
  "Item": {
    "type": "object",
    "properties": {
      "id": { "type": "string" },
      "name": { "type": "string" }
    }
  }
}
"#,
        )
        .expect("write schema");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: openapi_x_path_items_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
",
            root.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let source_name = SourceName::parse("openapi_x_path_items_test").expect("source");

        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build materialization");

        let projections: ProjectionCatalog =
            read_yaml(&build.temp_dir.join(PROJECTIONS_FILENAME)).expect("read projections");
        let projection = projections.projections.first().expect("projection");
        assert_eq!(
            projection
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name"]
        );
    }

    #[test]
    fn build_v4_materialization_allows_external_recursive_schemas() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let root = descriptor_temp.path().join("openapi.yaml");
        let schemas_dir = descriptor_temp.path().join("components").join("schemas");
        std::fs::create_dir_all(&schemas_dir).expect("schemas dir");
        std::fs::write(
            &root,
            r"
openapi: 3.0.3
paths:
  /trees:
    get:
      operationId: trees/list
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items:
                  $ref: components/schemas/tree.yaml#/Tree
",
        )
        .expect("write root descriptor");
        std::fs::write(
            schemas_dir.join("tree.yaml"),
            r"
Tree:
  type: object
  properties:
    id: {type: string}
    children:
      type: array
      items:
        $ref: '#/Tree'
",
        )
        .expect("write recursive schema");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: openapi_recursive_external_schema_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
",
            root.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let source_name =
            SourceName::parse("openapi_recursive_external_schema_test").expect("source");

        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build materialization");

        let projections: ProjectionCatalog =
            read_yaml(&build.temp_dir.join(PROJECTIONS_FILENAME)).expect("read projections");
        let projection = projections.projections.first().expect("projection");
        assert!(
            projection.columns.iter().any(|column| column.name == "id"),
            "{projection:#?}"
        );
    }

    #[test]
    fn build_v4_materialization_resolves_openapi_external_refs() {
        let (_descriptor_temp, root) = write_openapi_external_ref_fixture();
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: openapi_ref_materialization_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
",
            root.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let source_name = SourceName::parse("openapi_ref_materialization_test").expect("source");

        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build materialization");

        let semantic_ir: SemanticIr = read_yaml(
            &build
                .temp_dir
                .join("surfaces")
                .join("rest")
                .join("semantic-ir.yaml"),
        )
        .expect("read semantic IR");
        let operation = semantic_ir.operations.first().expect("operation");
        assert_eq!(operation.id, "items_list");
        assert!(
            operation
                .inputs
                .iter()
                .any(|input| input.name == "cursor" && !input.required),
            "expected resolved cursor input: {operation:#?}"
        );
        assert_eq!(
            operation.output.cardinality,
            coral_spec::v4::OutputCardinality::List
        );

        let projections: ProjectionCatalog =
            read_yaml(&build.temp_dir.join(PROJECTIONS_FILENAME)).expect("read projections");
        let projection = projections.projections.first().expect("projection");
        assert_eq!(projection.name, "items");
        assert_eq!(projection.namespace, "openapi_ref_materialization_test");
        assert_eq!(
            projection
                .columns
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
            ["id", "name", "examples"]
        );
        let raw = std::fs::read_to_string(
            build
                .temp_dir
                .join("surfaces")
                .join("rest")
                .join("source-document.raw"),
        )
        .expect("read resolved descriptor");
        assert!(!raw.contains("paths/items.json"));
        assert!(!raw.contains("../components"));
    }

    #[tokio::test]
    async fn build_v4_materialization_tmp_materializes_mcp_surface() {
        let server = MockServer::start().await;
        mount_mcp_materialization_server(&server).await;
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r#"
name: mcp_materialization_test
dsl_version: 4
surfaces:
  - id: mcp
    type: mcp
    server:
      transport: streamable_http
      url: "{}"
"#,
            server.uri()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();

        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &SourceName::parse("mcp_materialization_test").expect("source"),
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("build MCP materialization");

        let semantic_ir: SemanticIr = read_yaml(
            &build
                .temp_dir
                .join("surfaces")
                .join("mcp")
                .join("semantic-ir.yaml"),
        )
        .expect("read semantic IR");
        let operation = semantic_ir.operations.first().expect("operation");
        let coral_spec::v4::IrExecutionAttachment::Mcp(mcp) = &operation.execution else {
            panic!("expected MCP execution");
        };
        let pagination = mcp.pagination.as_ref().expect("pagination");
        assert_eq!(pagination.cursor_arg, "cursor");
        assert_eq!(
            pagination.response_cursor_path,
            vec!["meta".to_string(), "nextCursor".to_string()]
        );

        let projections: ProjectionCatalog =
            read_yaml(&build.temp_dir.join(PROJECTIONS_FILENAME)).expect("read projections");
        let projection = projections.projections.first().expect("projection");
        assert_eq!(projection.namespace, "mcp_materialization_test");
        let column_names = projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(column_names, ["result", "result_json"]);
    }

    #[test]
    fn build_v4_materialization_keeps_successful_surfaces_when_mcp_discovery_fails() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("openapi.yaml");
        std::fs::write(&openapi_file, openapi_fixture()).expect("write descriptor");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: mixed_materialization_test
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: rest
    type: openapi
    file: {}
    base_url: https://api.example.com
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: definitely-missing-coral-mcp-server
",
            openapi_file.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let source_name = SourceName::parse("mixed_materialization_test").expect("source");

        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("partial materialization should succeed");

        let fingerprint: Fingerprint =
            read_yaml(&build.temp_dir.join(FINGERPRINT_FILENAME)).expect("read fingerprint");
        let surface_ids = fingerprint
            .surfaces
            .iter()
            .map(|surface| surface.surface_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(surface_ids, ["rest"]);
        assert!(build.temp_dir.join("surfaces").join("rest").exists());
        assert!(!build.temp_dir.join("surfaces").join("mcp").exists());

        let diagnostics: Vec<Diagnostic> =
            read_yaml(&build.temp_dir.join(DIAGNOSTICS_FILENAME)).expect("read diagnostics");
        assert!(
            diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "SURFACE_MATERIALIZATION_FAILED"
                    && diagnostic.surface_id.as_deref() == Some("mcp")
            }),
            "expected MCP failure diagnostic: {diagnostics:#?}"
        );

        replace_v4_materialization(&layout, &workspace_name(), &source_name, &build.temp_dir)
            .expect("install partial materialization");
        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
        )
        .expect("load partial materialization");
        assert_eq!(materialized.surfaces.len(), 1);
        assert_eq!(
            materialized.surfaces.first().expect("surface").surface_id,
            "rest"
        );
        assert!(
            materialized
                .projections
                .projections
                .iter()
                .all(|projection| projection.surface_id == "rest")
        );
    }

    fn credential_method_hint_manifest(hint: &str) -> V4SourceManifest {
        let manifest_yaml = format!(
            r"
name: github_v4_materialization_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: /tmp/openapi.yaml
    inputs:
      ACCESS_TOKEN:
        kind: secret
        credential:
          methods:
            - type: source_config
              label: Paste token
              description: Configure a token manually.
              hint: {hint}
"
        );
        parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone()
    }

    #[test]
    fn input_declarations_fingerprint_includes_credential_method_hint() {
        let first = credential_method_hint_manifest("Use source config one.");
        let second = credential_method_hint_manifest("Use source config two.");

        let first_hash =
            stable_input_declarations_sha256(&first.surfaces.first().expect("surface").inputs)
                .expect("first hash");
        let second_hash =
            stable_input_declarations_sha256(&second.surfaces.first().expect("surface").inputs)
                .expect("second hash");

        assert_ne!(first_hash, second_hash);
    }

    #[test]
    fn load_v4_materialization_rejects_mismatched_manifest_hash() {
        let (_state, _descriptor, layout, manifest_yaml, _manifest) = setup_materialization();
        let changed_manifest_yaml = format!("description: changed\n{manifest_yaml}");
        let changed_manifest = parse_source_manifest_yaml(&changed_manifest_yaml)
            .expect("parse changed manifest")
            .as_v4()
            .expect("v4")
            .clone();

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &changed_manifest_yaml,
            &changed_manifest,
        )
        .expect_err("changed manifest hash should fail");

        assert!(
            error
                .to_string()
                .contains("manifest fingerprint does not match installed manifest"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn load_v4_materialization_rejects_corrupted_artifact_yaml_with_readd_guidance() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        std::fs::write(&fingerprint_path, b": not yaml").expect("corrupt fingerprint");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("corrupted artifact should fail");
        let message = error.to_string();

        assert!(
            message.contains("missing or incompatible DSL v4 materialized artifacts"),
            "unexpected error: {error}"
        );
        assert!(
            message.contains("Re-add the source"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn load_v4_materialization_rejects_extra_fingerprint_surface() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        let mut fingerprint: serde_yaml::Value =
            serde_yaml::from_slice(&std::fs::read(&fingerprint_path).expect("fingerprint"))
                .expect("fingerprint yaml");
        let surfaces = fingerprint
            .get_mut("surfaces")
            .and_then(serde_yaml::Value::as_sequence_mut)
            .expect("surfaces");
        let mut extra = surfaces.first().expect("first surface").clone();
        extra
            .as_mapping_mut()
            .expect("surface mapping")
            .insert("surface_id".into(), "extra".into());
        surfaces.push(extra);
        std::fs::write(
            &fingerprint_path,
            serde_yaml::to_string(&fingerprint).expect("encode fingerprint"),
        )
        .expect("write fingerprint");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("extra surface should fail");

        assert!(
            error
                .to_string()
                .contains("fingerprint surface set mismatch"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn load_v4_materialization_rejects_corrupted_raw_source_document() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let raw_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("source-document.raw");
        std::fs::write(&raw_path, b"corrupted").expect("corrupt raw descriptor");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("corrupted raw descriptor should fail");

        assert!(
            error
                .to_string()
                .contains("raw source document hash does not match"),
            "unexpected error: {error}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn load_v4_materialization_rejects_unreadable_raw_source_document_with_readd_guidance() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let raw_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("source-document.raw");
        let original_permissions = std::fs::metadata(&raw_path)
            .expect("raw descriptor metadata")
            .permissions();
        let mut unreadable_permissions = original_permissions.clone();
        unreadable_permissions.set_mode(0o000);
        std::fs::set_permissions(&raw_path, unreadable_permissions)
            .expect("make raw descriptor unreadable");
        if std::fs::read(&raw_path).is_ok() {
            std::fs::set_permissions(&raw_path, original_permissions)
                .expect("restore raw descriptor permissions");
            return;
        }

        let result = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        );

        std::fs::set_permissions(&raw_path, original_permissions)
            .expect("restore raw descriptor permissions");
        let message = result
            .expect_err("unreadable raw descriptor should fail")
            .to_string();
        assert!(
            message.contains("missing or incompatible DSL v4 materialized artifacts"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("failed to read raw source document artifact"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("Re-add the source"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn read_url_descriptor_rejects_non_https_urls() {
        let error = read_url_descriptor_on_blocking_thread("http://example.com/openapi.yaml", None)
            .expect_err("plain HTTP descriptor should fail");

        assert!(
            error.to_string().contains("must use HTTPS"),
            "unexpected error: {error}"
        );
    }
}
