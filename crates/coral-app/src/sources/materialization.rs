//! DSL v4 source materialization and artifact loading.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coral_spec::v4::{
    Diagnostic, DiagnosticSeverity, Fingerprint, FingerprintSurface, MCP_IMPORTER_VERSION,
    MaterializedSurface, McpToolCatalog, OPENAPI_IMPORTER_VERSION,
    OPERATION_METADATA_GENERATOR_VERSION, OperationMetadataCatalog, PROJECTION_GENERATOR_VERSION,
    ProjectionCatalog, ProjectionInputSyncMode, SURFACE_IMPORTER_VERSION, SemanticIr, SurfaceType,
    V4_ARTIFACT_SCHEMA_VERSION, V4MaterializedSource, V4SourceManifest, ValidatedSurfacePlan,
    generate_projection_catalog, import_mcp_surface, import_openapi_surface,
    normalize_mcp_tool_catalog, normalize_source_document, openapi_document_metadata,
    sync_projection_inputs, validate_materialized_source, validate_materialized_source_structure,
    validate_openapi_base_url_template, validate_semantic_ir_structure,
};
use coral_spec::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
    ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter, ParsedTemplate,
};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::hash::sha256_hex;
use crate::sources::SourceName;
use crate::state::{
    AppStateLayout, V4OperationMetadataFile, V4OperationMetadataOrigin, V4ProjectionCatalogFile,
    V4ProjectionCatalogOrigin,
};
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

const DESCRIPTOR_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DESCRIPTOR_BYTES: u64 = 64 * 1024 * 1024;
const DESCRIPTOR_USER_AGENT: &str = "coral-dsl-v4-materializer";
pub(crate) const PROJECTIONS_FILENAME: &str = "projections.yaml";
pub(crate) const FINGERPRINT_FILENAME: &str = "fingerprint.yaml";
pub(crate) const DIAGNOSTICS_FILENAME: &str = "diagnostics.yaml";
pub(crate) const OPERATION_METADATA_FILENAME: &str = "operation-metadata.yaml";

type ReportedDiagnosticKey = (String, String);
type ReportedDiagnosticStateKey = (String, String, String);
type ReportedDiagnostics = BTreeMap<ReportedDiagnosticStateKey, BTreeSet<ReportedDiagnosticKey>>;
type RawDocumentValidationKey = (String, String);

#[derive(Debug, Clone)]
struct RawDocumentValidation {
    expected_sha256: String,
    diagnostic: Option<Diagnostic>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct SourceDiagnosticReporter {
    reported: Arc<Mutex<ReportedDiagnostics>>,
    raw_document_validations: Arc<Mutex<BTreeMap<RawDocumentValidationKey, RawDocumentValidation>>>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SourceLoadDiagnosticStage {
    Query,
}

impl SourceLoadDiagnosticStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query-source",
        }
    }
}

impl SourceDiagnosticReporter {
    pub(crate) fn report_source_load_failure(
        &self,
        stage: SourceLoadDiagnosticStage,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        code: &str,
        detail: &str,
    ) {
        let diagnostic = materialization_warning(code, detail);
        self.report_source_diagnostics(
            workspace_name,
            source_name,
            stage.as_str(),
            std::iter::once(&diagnostic),
        );
    }

    pub(crate) fn clear_source_load_failure(
        &self,
        stage: SourceLoadDiagnosticStage,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        self.report_source_diagnostics(
            workspace_name,
            source_name,
            stage.as_str(),
            std::iter::empty::<&Diagnostic>(),
        );
    }

    pub(crate) fn clear_source(&self, workspace_name: &WorkspaceName, source_name: &SourceName) {
        self.reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(workspace, source, _stage), _diagnostics| {
                workspace != workspace_name.as_str() || source != source_name.as_str()
            });
        self.raw_document_validations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(workspace, source), _validation| {
                workspace != workspace_name.as_str() || source != source_name.as_str()
            });
    }

    pub(crate) fn clear_workspace(&self, workspace_name: &WorkspaceName) {
        self.reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(workspace, _source, _stage), _diagnostics| {
                workspace != workspace_name.as_str()
            });
        self.raw_document_validations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|(workspace, _source), _validation| workspace != workspace_name.as_str());
    }

    fn validate_raw_document_fingerprint(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        path: &Path,
        expected_sha256: &str,
    ) -> Option<Diagnostic> {
        let key = (workspace_name.to_string(), source_name.to_string());
        if let Some(validation) = self
            .raw_document_validations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&key)
            .filter(|validation| validation.expected_sha256 == expected_sha256)
        {
            return validation.diagnostic.clone();
        }

        let (diagnostic, cacheable) = match std::fs::read(path) {
            Ok(raw_bytes) if sha256_hex(&raw_bytes) != expected_sha256 => (
                Some(materialization_warning(
                    "V4_RAW_DOCUMENT_FINGERPRINT_MISMATCH",
                    "raw source document hash does not match",
                )),
                true,
            ),
            Ok(_) => (None, true),
            Err(error) => (
                Some(materialization_warning(
                    "V4_RAW_DOCUMENT_UNAVAILABLE",
                    format!(
                        "could not read raw source document '{}' for provenance validation: {error}",
                        path.display()
                    ),
                )),
                false,
            ),
        };
        if cacheable {
            self.raw_document_validations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(
                    key,
                    RawDocumentValidation {
                        expected_sha256: expected_sha256.to_string(),
                        diagnostic: diagnostic.clone(),
                    },
                );
        }
        diagnostic
    }

    fn report_source_diagnostics<'a>(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        stage: &str,
        diagnostics: impl IntoIterator<Item = &'a Diagnostic>,
    ) {
        let state_key = (
            workspace_name.to_string(),
            source_name.to_string(),
            stage.to_string(),
        );
        let diagnostics = diagnostics.into_iter().collect::<Vec<_>>();
        let current = diagnostics
            .iter()
            .map(|diagnostic| (diagnostic.code.clone(), diagnostic.message.clone()))
            .collect::<BTreeSet<_>>();
        let mut reported = self
            .reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = reported.get(&state_key).cloned().unwrap_or_default();
        for diagnostic in diagnostics {
            let key = (diagnostic.code.clone(), diagnostic.message.clone());
            if previous.contains(&key) {
                continue;
            }
            tracing::warn!(
                diagnostic.code = %diagnostic.code,
                diagnostic.stage = stage,
                workspace = %workspace_name,
                source = %source_name,
                operation = diagnostic.operation_id.as_deref().unwrap_or(""),
                detail = %diagnostic.message,
                "DSL v4 source load diagnostic"
            );
        }
        if current.is_empty() {
            reported.remove(&state_key);
        } else {
            reported.insert(state_key, current);
        }
    }

    #[cfg(test)]
    pub(crate) fn tracks_diagnostic(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        stage: &str,
        code: &str,
    ) -> bool {
        self.reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(
                workspace_name.to_string(),
                source_name.to_string(),
                stage.to_string(),
            ))
            .is_some_and(|diagnostics| {
                diagnostics
                    .iter()
                    .any(|(diagnostic_code, _message)| diagnostic_code == code)
            })
    }
}

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

pub(crate) fn load_v4_materialization_with_reporter(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<V4MaterializedSource, AppError> {
    let fingerprint_path = layout.v4_fingerprint_file(workspace_name, source_name);
    let projections_file = layout.v4_projection_catalog_file(workspace_name, source_name);
    let operation_metadata_file = layout.v4_operation_metadata_file(workspace_name, source_name)?;
    let diagnostics_path = layout.v4_diagnostics_file(workspace_name, source_name);
    if !projections_file.path.exists() {
        return Err(incompatible_materialization_error(
            source_name,
            format!(
                "required projection catalog '{}' is missing",
                projections_file.path.display()
            ),
        ));
    }
    let mut load_diagnostics = Vec::new();
    let fingerprint = load_optional_fingerprint(
        manifest_yaml,
        manifest,
        &fingerprint_path,
        &mut load_diagnostics,
    );
    let mut projections = load_projection_catalog(
        source_name,
        manifest,
        &projections_file,
        &mut load_diagnostics,
    )?;
    let mut diagnostics = load_optional_diagnostics(&diagnostics_path, &mut load_diagnostics);
    let materialized_dir = layout.v4_materialized_dir(workspace_name, source_name);
    let raw_source_document_path = materialized_dir.join("source-document.raw");
    let normalized_source_document_path = materialized_dir.join("source-document.yaml");
    let semantic_ir_path = materialized_dir.join("semantic-ir.yaml");
    let semantic_ir = read_validated_semantic_ir_with_reporter(
        manifest,
        workspace_name,
        source_name,
        &semantic_ir_path,
        &mut load_diagnostics,
        diagnostic_reporter,
    )?;
    let operation_metadata = read_operation_metadata_with_reporter(
        workspace_name,
        source_name,
        manifest,
        &operation_metadata_file,
        &mut load_diagnostics,
        diagnostic_reporter,
    )?;
    let plan = build_validated_plan_with_reporter(
        semantic_ir,
        operation_metadata,
        &operation_metadata_file,
        workspace_name,
        source_name,
        &mut load_diagnostics,
        diagnostic_reporter,
    )?;
    if let Some(fingerprint) = fingerprint.as_ref()
        && let Some(diagnostic) = diagnostic_reporter.validate_raw_document_fingerprint(
            workspace_name,
            source_name,
            &raw_source_document_path,
            &fingerprint.surface.descriptor_sha256,
        )
    {
        load_diagnostics.push(diagnostic);
    }
    let projection_sync_mode = match projections_file.origin {
        V4ProjectionCatalogOrigin::Materialized => ProjectionInputSyncMode::RecomputeInputExposure,
        V4ProjectionCatalogOrigin::Override => ProjectionInputSyncMode::PreserveExistingExposure,
    };
    sync_projection_inputs(&plan, &mut projections, projection_sync_mode).map_err(|error| {
        match projections_file.origin {
            V4ProjectionCatalogOrigin::Materialized => {
                incompatible_materialization_error(source_name, error.to_string())
            }
            V4ProjectionCatalogOrigin::Override => invalid_projection_override_error(
                source_name,
                &projections_file.path,
                error.to_string(),
            ),
        }
    })?;
    diagnostic_reporter.report_source_diagnostics(
        workspace_name,
        source_name,
        "materialization",
        load_diagnostics.iter(),
    );
    diagnostics.append(&mut load_diagnostics);
    let surface = MaterializedSurface {
        plan,
        source_document_sha256: fingerprint
            .as_ref()
            .map(|fingerprint| fingerprint.surface.descriptor_sha256.clone()),
        normalized_source_document_path,
        raw_source_document_path,
    };
    let materialized = V4MaterializedSource {
        fingerprint,
        surface,
        projections,
        diagnostics,
    };
    validate_loaded_materialization(source_name, manifest, &projections_file, &materialized)?;
    Ok(materialized)
}

fn report_materialization_failure(
    diagnostic_reporter: &SourceDiagnosticReporter,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    load_diagnostics: &mut Vec<Diagnostic>,
    code: &str,
    error: &AppError,
) {
    load_diagnostics.push(materialization_warning(code, error.to_string()));
    diagnostic_reporter.report_source_diagnostics(
        workspace_name,
        source_name,
        "materialization",
        load_diagnostics.iter(),
    );
}

fn validate_semantic_ir_structure_with_reporter(
    semantic_ir: &SemanticIr,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    load_diagnostics: &mut Vec<Diagnostic>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<(), AppError> {
    validate_semantic_ir_structure(semantic_ir).map_err(|error| {
        let error = incompatible_materialization_error(source_name, error.to_string());
        report_materialization_failure(
            diagnostic_reporter,
            workspace_name,
            source_name,
            load_diagnostics,
            "V4_SEMANTIC_IR_UNAVAILABLE",
            &error,
        );
        error
    })
}

fn read_validated_semantic_ir_with_reporter(
    manifest: &V4SourceManifest,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    path: &Path,
    load_diagnostics: &mut Vec<Diagnostic>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<SemanticIr, AppError> {
    let semantic_ir = read_semantic_ir_with_reporter(
        workspace_name,
        source_name,
        path,
        load_diagnostics,
        diagnostic_reporter,
    )?;
    if let Err(error) = validate_semantic_ir(manifest, &semantic_ir) {
        load_diagnostics.push(materialization_warning(
            "V4_SEMANTIC_IR_PROVENANCE_MISMATCH",
            error,
        ));
    }
    validate_semantic_ir_structure_with_reporter(
        &semantic_ir,
        workspace_name,
        source_name,
        load_diagnostics,
        diagnostic_reporter,
    )?;
    Ok(semantic_ir)
}

fn build_validated_plan_with_reporter(
    semantic_ir: SemanticIr,
    operation_metadata: OperationMetadataCatalog,
    metadata_file: &V4OperationMetadataFile,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    load_diagnostics: &mut Vec<Diagnostic>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<ValidatedSurfacePlan, AppError> {
    ValidatedSurfacePlan::new(semantic_ir, operation_metadata).map_err(|error| {
        let (error, code) = match metadata_file.origin {
            V4OperationMetadataOrigin::Materialized => (
                incompatible_materialization_error(source_name, error.to_string()),
                "V4_OPERATION_METADATA_UNAVAILABLE",
            ),
            V4OperationMetadataOrigin::Override => (
                invalid_operation_metadata_override_error(
                    source_name,
                    &metadata_file.path,
                    error.to_string(),
                ),
                "V4_OPERATION_METADATA_OVERRIDE_FAILED",
            ),
        };
        report_materialization_failure(
            diagnostic_reporter,
            workspace_name,
            source_name,
            load_diagnostics,
            code,
            &error,
        );
        error
    })
}

fn read_semantic_ir_with_reporter(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    path: &Path,
    load_diagnostics: &mut Vec<Diagnostic>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<SemanticIr, AppError> {
    read_artifact_yaml(source_name, "semantic IR", path).inspect_err(|error| {
        report_materialization_failure(
            diagnostic_reporter,
            workspace_name,
            source_name,
            load_diagnostics,
            "V4_SEMANTIC_IR_UNAVAILABLE",
            error,
        );
    })
}

fn read_operation_metadata_with_reporter(
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    metadata_file: &V4OperationMetadataFile,
    load_diagnostics: &mut Vec<Diagnostic>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<OperationMetadataCatalog, AppError> {
    load_operation_metadata(manifest, source_name, metadata_file, load_diagnostics).inspect_err(
        |error| {
            report_materialization_failure(
                diagnostic_reporter,
                workspace_name,
                source_name,
                load_diagnostics,
                match metadata_file.origin {
                    V4OperationMetadataOrigin::Materialized => "V4_OPERATION_METADATA_UNAVAILABLE",
                    V4OperationMetadataOrigin::Override => "V4_OPERATION_METADATA_OVERRIDE_FAILED",
                },
                error,
            );
        },
    )
}

#[cfg(test)]
fn load_v4_materialization(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
) -> Result<V4MaterializedSource, AppError> {
    load_v4_materialization_with_reporter(
        layout,
        workspace_name,
        source_name,
        manifest_yaml,
        manifest,
        &SourceDiagnosticReporter::default(),
    )
}

fn load_optional_fingerprint(
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    path: &Path,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<Fingerprint> {
    let fingerprint = match read_yaml::<Fingerprint>(path) {
        Ok(fingerprint) => fingerprint,
        Err(error) => {
            diagnostics.push(materialization_warning(
                "V4_FINGERPRINT_UNAVAILABLE",
                format!(
                    "could not read optional fingerprint '{}': {error}",
                    path.display()
                ),
            ));
            return None;
        }
    };
    if let Err(error) = validate_fingerprint_header(manifest, &fingerprint) {
        diagnostics.push(materialization_warning(
            "V4_FINGERPRINT_HEADER_MISMATCH",
            error,
        ));
    }
    if fingerprint.manifest_sha256 != sha256_hex(manifest_yaml.as_bytes()) {
        diagnostics.push(materialization_warning(
            "V4_MANIFEST_FINGERPRINT_MISMATCH",
            "manifest fingerprint does not match installed manifest",
        ));
    }
    if let Err(error) = validate_fingerprint_surface(manifest, &fingerprint) {
        diagnostics.push(materialization_warning(
            "V4_FINGERPRINT_SURFACE_MISMATCH",
            error,
        ));
    }
    Some(fingerprint)
}

fn validate_fingerprint_header(
    manifest: &V4SourceManifest,
    fingerprint: &Fingerprint,
) -> Result<(), String> {
    if fingerprint.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err("fingerprint artifact schema version mismatch".to_string());
    }
    if fingerprint.source_name != manifest.common.name {
        return Err("fingerprint source name does not match installed manifest".to_string());
    }
    if fingerprint.importer_version != SURFACE_IMPORTER_VERSION
        || fingerprint.operation_metadata_generator_version != OPERATION_METADATA_GENERATOR_VERSION
        || fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err("fingerprint importer or generator version mismatch".to_string());
    }
    Ok(())
}

fn validate_fingerprint_surface(
    manifest: &V4SourceManifest,
    fingerprint: &Fingerprint,
) -> Result<(), String> {
    let fingerprint_surface = &fingerprint.surface;
    let surface = &manifest.surface;
    if fingerprint_surface.surface_type != surface.surface_type {
        return Err("surface type fingerprint does not match".to_string());
    }
    if fingerprint_surface.descriptor_kind != surface.descriptor.kind()
        || fingerprint_surface.descriptor_location != surface.descriptor.location()
    {
        return Err("surface descriptor fingerprint does not match".to_string());
    }
    let expected = stable_input_declarations_sha256(&manifest.declared_inputs)
        .map_err(|error| error.to_string())?;
    if fingerprint_surface.input_declarations_sha256 != expected {
        return Err("input declarations fingerprint does not match".to_string());
    }
    Ok(())
}

fn load_projection_catalog(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    projections_file: &V4ProjectionCatalogFile,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<ProjectionCatalog, AppError> {
    let projections = match projections_file.origin {
        V4ProjectionCatalogOrigin::Materialized => {
            read_artifact_yaml(source_name, "projection catalog", &projections_file.path)?
        }
        V4ProjectionCatalogOrigin::Override => {
            read_projection_override_yaml(source_name, &projections_file.path)?
        }
    };
    if let Err(error) = validate_projection_catalog_header(manifest, &projections, projections_file)
    {
        diagnostics.push(materialization_warning(
            "V4_PROJECTION_CATALOG_PROVENANCE_MISMATCH",
            error,
        ));
    }
    Ok(projections)
}

fn load_optional_diagnostics(path: &Path, diagnostics: &mut Vec<Diagnostic>) -> Vec<Diagnostic> {
    match read_yaml(path) {
        Ok(diagnostics) => diagnostics,
        Err(error) => {
            diagnostics.push(materialization_warning(
                "V4_DIAGNOSTICS_UNAVAILABLE",
                format!(
                    "could not read optional diagnostics '{}': {error}",
                    path.display()
                ),
            ));
            Vec::new()
        }
    }
}

fn materialization_warning(code: &str, message: impl Into<String>) -> Diagnostic {
    Diagnostic {
        code: code.to_string(),
        severity: DiagnosticSeverity::Warning,
        message: message.into(),
        operation_id: None,
    }
}

fn validate_projection_catalog_header(
    manifest: &V4SourceManifest,
    projections: &ProjectionCatalog,
    projections_file: &V4ProjectionCatalogFile,
) -> Result<(), String> {
    if projections.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err("projection catalog artifact schema version mismatch".to_string());
    }
    if projections.source_name != manifest.common.name {
        return Err("projection catalog source name does not match installed manifest".to_string());
    }
    match projections_file.origin {
        V4ProjectionCatalogOrigin::Materialized => {
            if projections.generator_version.as_deref() != Some(PROJECTION_GENERATOR_VERSION) {
                return Err("projection catalog generator version mismatch".to_string());
            }
        }
        V4ProjectionCatalogOrigin::Override => {
            if let Some(generator_version) = projections.generator_version.as_deref()
                && generator_version != PROJECTION_GENERATOR_VERSION
            {
                return Err(format!(
                    "projection override was copied from generator version '{generator_version}', but this Coral build expects '{PROJECTION_GENERATOR_VERSION}'"
                ));
            }
        }
    }
    Ok(())
}

fn load_operation_metadata(
    manifest: &V4SourceManifest,
    source_name: &SourceName,
    metadata_file: &V4OperationMetadataFile,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<OperationMetadataCatalog, AppError> {
    let metadata = match read_yaml(&metadata_file.path) {
        Ok(metadata) => metadata,
        Err(AppError::Io(error)) if error.kind() != std::io::ErrorKind::NotFound => {
            return Err(AppError::Io(error));
        }
        Err(error) => {
            return Err(match metadata_file.origin {
                V4OperationMetadataOrigin::Materialized => incompatible_materialization_error(
                    source_name,
                    format!(
                        "failed to read operation metadata artifact '{}': {error}",
                        metadata_file.path.display()
                    ),
                ),
                V4OperationMetadataOrigin::Override => invalid_operation_metadata_override_error(
                    source_name,
                    &metadata_file.path,
                    format!("failed to read override artifact: {error}"),
                ),
            });
        }
    };
    if let Err(error) = validate_operation_metadata_header(manifest, &metadata, metadata_file) {
        diagnostics.push(materialization_warning(
            "V4_OPERATION_METADATA_PROVENANCE_MISMATCH",
            error,
        ));
    }
    Ok(metadata)
}

fn validate_operation_metadata_header(
    manifest: &V4SourceManifest,
    metadata: &OperationMetadataCatalog,
    metadata_file: &V4OperationMetadataFile,
) -> Result<(), String> {
    if metadata.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err("operation metadata artifact schema version mismatch".to_string());
    }
    if metadata.source_name != manifest.common.name {
        return Err("operation metadata source name does not match installed manifest".to_string());
    }
    match metadata_file.origin {
        V4OperationMetadataOrigin::Materialized => {
            if metadata.generator_version.as_deref() != Some(OPERATION_METADATA_GENERATOR_VERSION) {
                return Err("operation metadata generator version mismatch".to_string());
            }
        }
        V4OperationMetadataOrigin::Override => {
            if let Some(generator_version) = metadata.generator_version.as_deref()
                && generator_version != OPERATION_METADATA_GENERATOR_VERSION
            {
                return Err(format!(
                    "operation metadata override was copied from generator version '{generator_version}', but this Coral build expects '{OPERATION_METADATA_GENERATOR_VERSION}'"
                ));
            }
        }
    }
    Ok(())
}

fn validate_loaded_materialization(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    projections_file: &V4ProjectionCatalogFile,
    materialized: &V4MaterializedSource,
) -> Result<(), AppError> {
    validate_materialized_source_structure(manifest, materialized).map_err(|error| {
        match projections_file.origin {
            V4ProjectionCatalogOrigin::Materialized => {
                incompatible_materialization_error(source_name, error.to_string())
            }
            V4ProjectionCatalogOrigin::Override => invalid_projection_override_error(
                source_name,
                &projections_file.path,
                error.to_string(),
            ),
        }
    })?;
    let operations = materialized
        .surface
        .plan
        .semantic_ir()
        .operations
        .iter()
        .map(|operation| operation.id.as_str())
        .collect::<BTreeSet<_>>();
    validate_projection_references(source_name, projections_file, materialized, &operations)
}

fn validate_projection_references(
    source_name: &SourceName,
    projections_file: &V4ProjectionCatalogFile,
    materialized: &V4MaterializedSource,
    operations: &BTreeSet<&str>,
) -> Result<(), AppError> {
    for projection in &materialized.projections.projections {
        if !operations.contains(projection.operation_id.as_str()) {
            let detail = format!(
                "projection '{}' references missing operation '{}'",
                projection.name, projection.operation_id
            );
            return Err(match projections_file.origin {
                V4ProjectionCatalogOrigin::Materialized => {
                    incompatible_materialization_error(source_name, detail)
                }
                V4ProjectionCatalogOrigin::Override => {
                    invalid_projection_override_error(source_name, &projections_file.path, detail)
                }
            });
        }
    }
    Ok(())
}

fn validate_semantic_ir(
    manifest: &V4SourceManifest,
    semantic_ir: &SemanticIr,
) -> Result<(), String> {
    if semantic_ir.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err("semantic IR schema version mismatch".to_string());
    }
    if semantic_ir.source_name != manifest.common.name
        || semantic_ir.surface_type != manifest.surface.surface_type
    {
        return Err("semantic IR identity mismatch".to_string());
    }
    if semantic_ir.importer_version != expected_importer_version(manifest.surface.surface_type) {
        return Err("semantic IR importer version mismatch".to_string());
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
    let surface = &manifest.surface;
    let materialized_surface = materialize_surface(manifest, surface, inputs).map_err(|error| {
        AppError::Unavailable(format!(
            "failed to materialize source '{}': {error}",
            manifest.common.name
        ))
    })?;
    write_surface_artifacts(temp_dir, &materialized_surface)?;
    let projections = generate_projection_catalog(manifest, &materialized_surface.plan)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    let mut diagnostics = Vec::new();
    diagnostics.extend(projections.diagnostics.clone());
    diagnostics.extend(materialized_surface.plan.semantic_ir().diagnostics.clone());
    diagnostics.extend(
        materialized_surface
            .plan
            .semantic_ir()
            .operations
            .iter()
            .flat_map(|operation| operation.diagnostics.clone()),
    );
    let fingerprint = Fingerprint {
        artifact_schema_version: V4_ARTIFACT_SCHEMA_VERSION,
        source_name: manifest.common.name.clone(),
        manifest_sha256,
        surface: FingerprintSurface {
            surface_type: surface.surface_type,
            descriptor_kind: surface.descriptor.kind().to_string(),
            descriptor_location: surface.descriptor.location(),
            descriptor_sha256: materialized_surface.observed_sha256.clone(),
            input_declarations_sha256: stable_input_declarations_sha256(&manifest.declared_inputs)?,
        },
        importer_version: SURFACE_IMPORTER_VERSION.to_string(),
        operation_metadata_generator_version: OPERATION_METADATA_GENERATOR_VERSION.to_string(),
        projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
    };
    let materialized = V4MaterializedSource {
        fingerprint: Some(fingerprint.clone()),
        surface: MaterializedSurface {
            plan: materialized_surface.plan,
            source_document_sha256: Some(materialized_surface.observed_sha256),
            normalized_source_document_path: temp_dir.join("source-document.yaml"),
            raw_source_document_path: temp_dir.join("source-document.raw"),
        },
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
    plan: ValidatedSurfacePlan,
}

/// Writes the materialized surface documents, semantic IR, and operation
/// metadata.
fn write_surface_artifacts(
    materialized_dir: &Path,
    materialized_surface: &MaterializedSurfaceBuild,
) -> Result<(), AppError> {
    fs::ensure_private_dir(materialized_dir)?;
    std::fs::write(
        materialized_dir.join("source-document.raw"),
        &materialized_surface.raw_document,
    )?;
    std::fs::write(
        materialized_dir.join("source-document.yaml"),
        &materialized_surface.normalized_document,
    )?;
    write_yaml(
        &materialized_dir.join("semantic-ir.yaml"),
        materialized_surface.plan.semantic_ir(),
    )?;
    write_yaml(
        &materialized_dir.join(OPERATION_METADATA_FILENAME),
        materialized_surface.plan.operation_metadata(),
    )?;
    Ok(())
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
    let bytes = read_descriptor(surface)?;
    validate_materialized_surface_base_url(manifest, surface, &bytes)?;
    let observed_sha256 = sha256_hex(&bytes);
    let imported = import_openapi_surface(manifest, surface, &bytes).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to import source '{}' surface: {error}",
            manifest.common.name
        ))
    })?;
    let normalized_document = normalize_source_document(&bytes)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    let plan = imported
        .validated_plan()
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    Ok(MaterializedSurfaceBuild {
        raw_document: bytes,
        normalized_document: normalized_document.into_bytes(),
        observed_sha256,
        plan,
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
    let imported = import_mcp_surface(manifest, surface, &catalog).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to import source '{}' surface: {error}",
            manifest.common.name
        ))
    })?;
    let plan = imported
        .validated_plan()
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    Ok(MaterializedSurfaceBuild {
        raw_document: normalized_document.clone(),
        normalized_document,
        observed_sha256,
        plan,
    })
}

fn discover_mcp_tool_catalog(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    inputs: &MaterializationInputs,
) -> Result<McpToolCatalog, AppError> {
    let runtime = surface.mcp_runtime().ok_or_else(|| {
        AppError::FailedPrecondition("DSL v4 surface is not an MCP surface".to_string())
    })?;
    let source_name = manifest.common.name.clone();
    let server = runtime.server.clone();
    let declared_inputs = manifest.declared_inputs.clone();
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
            "failed to discover MCP tools for source '{}': discovery thread panicked",
            manifest.common.name
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
            "failed to derive base_url for DSL v4 surface: {error}"
        ))
    })?;
    let Some(server_url) = metadata.server_url.as_deref() else {
        return Ok(());
    };
    let base_url = ParsedTemplate::parse(server_url).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse derived base_url for DSL v4 surface: {error}"
        ))
    })?;
    validate_openapi_base_url_template(
        &manifest.common.name,
        &manifest.declared_inputs,
        &base_url,
        "derived OpenAPI server",
    )
    .map_err(|error| AppError::FailedPrecondition(error.to_string()))
}

fn read_descriptor(surface: &coral_spec::v4::V4Surface) -> Result<Vec<u8>, AppError> {
    match &surface.descriptor {
        coral_spec::v4::SurfaceDescriptor::File { file } => read_file_descriptor(file),
        coral_spec::v4::SurfaceDescriptor::Url { url } => read_url_descriptor(url),
        coral_spec::v4::SurfaceDescriptor::McpServer { .. } => Err(AppError::FailedPrecondition(
            "DSL v4 MCP surface does not have an OpenAPI descriptor".to_string(),
        )),
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

fn read_url_descriptor(url: &str) -> Result<Vec<u8>, AppError> {
    let url = url.to_string();
    let panic_url = url.clone();
    std::thread::spawn(move || read_url_descriptor_on_blocking_thread(&url))
        .join()
        .map_err(|_panic| {
            AppError::Unavailable(format!(
                "failed to fetch OpenAPI descriptor '{panic_url}': fetch thread panicked"
            ))
        })?
}

fn read_url_descriptor_on_blocking_thread(url: &str) -> Result<Vec<u8>, AppError> {
    ensure_https_descriptor_url(url)?;
    let client = reqwest::blocking::Client::builder()
        .timeout(DESCRIPTOR_FETCH_TIMEOUT)
        .https_only(true)
        .redirect(reqwest::redirect::Policy::limited(5))
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
    Ok(bytes)
}

fn ensure_https_descriptor_url(url: &str) -> Result<(), AppError> {
    let parsed = reqwest::Url::parse(url).map_err(|error| {
        AppError::InvalidInput(format!(
            "OpenAPI descriptor URL '{url}' is invalid: {error}"
        ))
    })?;
    if parsed.scheme() != "https" {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor URL '{url}' must use HTTPS"
        )));
    }
    Ok(())
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

fn read_projection_override_yaml(
    source_name: &SourceName,
    path: &Path,
) -> Result<ProjectionCatalog, AppError> {
    read_yaml(path).map_err(|error| {
        invalid_projection_override_error(
            source_name,
            path,
            format!("failed to read projection override artifact: {error}"),
        )
    })
}

fn invalid_projection_override_error(
    source_name: &SourceName,
    path: &Path,
    detail: impl AsRef<str>,
) -> AppError {
    AppError::InvalidV4ProjectionOverride {
        source_name: source_name.to_string(),
        override_path: path.display().to_string(),
        detail: detail.as_ref().to_string(),
    }
}

fn invalid_operation_metadata_override_error(
    source_name: &SourceName,
    path: &Path,
    detail: impl AsRef<str>,
) -> AppError {
    AppError::InvalidV4OperationMetadataOverride {
        source_name: source_name.to_string(),
        override_path: path.display().to_string(),
        detail: detail.as_ref().to_string(),
    }
}

fn write_yaml<T: serde::Serialize>(path: &Path, value: &T) -> Result<(), AppError> {
    if let Some(parent) = path.parent() {
        fs::ensure_private_dir(parent)?;
    }
    let bytes = serde_yaml::to_string(value)?;
    fs::write_atomic(path, bytes.as_bytes())?;
    Ok(())
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
      parameters:
        - {name: order_by, in: query, schema: {type: string}}
        - {name: q, in: query, schema: {type: string}}
        - {name: state, in: query, schema: {type: string}}
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

    fn unsupported_operation_ref_openapi_fixture() -> &'static str {
        r"
openapi: 3.0.3
paths:
  /account:
    get:
      $ref: resources/account/account_get.yml
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
surface:
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

    fn installed_projection_catalog_value(layout: &AppStateLayout) -> serde_yaml::Value {
        let path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join(PROJECTIONS_FILENAME);
        serde_yaml::from_slice(&std::fs::read(path).expect("read generated projections"))
            .expect("parse generated projections")
    }

    #[test]
    fn installed_v4_materialization_uses_the_root_level_singular_layout() {
        let (_state, _descriptor, layout, _manifest_yaml, _manifest) = setup_materialization();
        let materialized_dir = layout.v4_materialized_dir(&workspace_name(), &source_name());

        for filename in [
            "source-document.raw",
            "source-document.yaml",
            "semantic-ir.yaml",
            OPERATION_METADATA_FILENAME,
            FINGERPRINT_FILENAME,
            PROJECTIONS_FILENAME,
            DIAGNOSTICS_FILENAME,
        ] {
            assert!(
                materialized_dir.join(filename).is_file(),
                "missing root-level materialization artifact {filename}"
            );
        }
        assert!(
            !materialized_dir.join("surfaces").exists(),
            "singular materializations must not create a surfaces directory"
        );
    }

    #[test]
    fn failed_singular_materialization_removes_the_atomic_temp_directory() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let missing_openapi_file = descriptor_temp.path().join("missing-openapi.yaml");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let source_name = SourceName::parse("atomic_failure_test").expect("source name");
        let manifest_yaml = format!(
            r"
name: atomic_failure_test
dsl_version: 4
surface:
    type: openapi
    file: {}
",
            missing_openapi_file.display()
        );
        let manifest = parse_source_manifest_yaml(&manifest_yaml)
            .expect("parse v4 manifest")
            .as_v4()
            .expect("v4")
            .clone();
        let temp_suffix = "atomic-failure";
        let expected_temp_dir =
            layout.v4_materialized_tmp_dir(&workspace_name(), &source_name, temp_suffix);

        let error = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            temp_suffix,
        )
        .expect_err("missing descriptor should fail materialization");

        assert!(
            error
                .to_string()
                .contains("failed to materialize source 'atomic_failure_test'"),
            "unexpected error: {error}"
        );
        assert!(
            !expected_temp_dir.exists(),
            "failed singular materialization left partial artifacts at {}",
            expected_temp_dir.display()
        );
    }

    fn remove_generator_version(catalog: &mut serde_yaml::Value) {
        let key = serde_yaml::Value::String("generator_version".to_string());
        catalog
            .as_mapping_mut()
            .expect("projection catalog mapping")
            .remove(&key);
    }

    fn set_generator_version(catalog: &mut serde_yaml::Value, generator_version: &str) {
        let key = serde_yaml::Value::String("generator_version".to_string());
        catalog
            .as_mapping_mut()
            .expect("projection catalog mapping")
            .insert(
                key,
                serde_yaml::Value::String(generator_version.to_string()),
            );
    }

    fn write_projection_override(layout: &AppStateLayout, catalog: &serde_yaml::Value) -> PathBuf {
        let path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(PROJECTIONS_FILENAME);
        std::fs::create_dir_all(path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(
            &path,
            serde_yaml::to_string(catalog).expect("encode projection override"),
        )
        .expect("write projection override");
        path
    }

    #[test]
    fn build_v4_materialization_persists_lookup_keys_in_operation_metadata() {
        let (_state, _descriptor, layout, _manifest_yaml, _manifest) = setup_materialization();
        let surface_dir = layout.v4_materialized_dir(&workspace_name(), &source_name());
        let metadata: OperationMetadataCatalog =
            read_yaml(&surface_dir.join(OPERATION_METADATA_FILENAME))
                .expect("read operation metadata");
        let lookup_keys = match metadata
            .operations
            .values()
            .next()
            .expect("operation metadata")
        {
            coral_spec::v4::OperationMetadata::Rest { lookup_keys, .. } => lookup_keys,
            coral_spec::v4::OperationMetadata::Mcp { .. } => panic!("expected REST metadata"),
        };
        assert_eq!(lookup_keys, &["state"]);
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
surface:
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

        let metadata: OperationMetadataCatalog =
            read_yaml(&build.temp_dir.join(OPERATION_METADATA_FILENAME))
                .expect("read operation metadata");
        let pagination = match metadata
            .operations
            .values()
            .next()
            .expect("operation metadata")
        {
            coral_spec::v4::OperationMetadata::Mcp { pagination } => {
                pagination.cursor.as_ref().expect("pagination")
            }
            coral_spec::v4::OperationMetadata::Rest { .. } => panic!("expected MCP metadata"),
        };
        assert_eq!(pagination.cursor_arg, "cursor");
        assert_eq!(
            pagination.response_cursor_path,
            vec!["meta".to_string(), "nextCursor".to_string()]
        );

        let projections: ProjectionCatalog =
            read_yaml(&build.temp_dir.join(PROJECTIONS_FILENAME)).expect("read projections");
        let projection = projections.projections.first().expect("projection");
        let column_names = projection
            .columns
            .iter()
            .map(|column| column.name.as_str())
            .collect::<Vec<_>>();
        assert_eq!(column_names, ["result", "result_json"]);
    }

    #[test]
    fn build_v4_materialization_reports_unsupported_openapi_operation_refs() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("openapi.yaml");
        std::fs::write(&openapi_file, unsupported_operation_ref_openapi_fixture())
            .expect("write descriptor");
        let state_temp = TempDir::new().expect("state temp dir");
        let layout =
            AppStateLayout::discover(Some(state_temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manifest_yaml = format!(
            r"
name: unsupported_openapi_refs
dsl_version: 4
surface:
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
        let source_name = SourceName::parse("unsupported_openapi_refs").expect("source");

        let build = build_v4_materialization_tmp(
            &layout,
            &workspace_name(),
            &source_name,
            &manifest_yaml,
            &manifest,
            &MaterializationInputs::default(),
            "test",
        )
        .expect("unsupported operation refs should materialize with diagnostics");

        let semantic_ir: SemanticIr =
            read_yaml(&build.temp_dir.join("semantic-ir.yaml")).expect("read semantic IR");
        assert!(
            semantic_ir.operations.is_empty(),
            "unsupported refs should not import empty operations: {:?}",
            semantic_ir.operations
        );

        let projections: ProjectionCatalog =
            read_yaml(&build.temp_dir.join(PROJECTIONS_FILENAME)).expect("read projections");
        assert!(
            projections.projections.is_empty(),
            "unsupported refs should not create hidden projections: {:?}",
            projections.projections
        );

        let diagnostics: Vec<Diagnostic> =
            read_yaml(&build.temp_dir.join(DIAGNOSTICS_FILENAME)).expect("read diagnostics");
        let diagnostic = diagnostics
            .iter()
            .find(|diagnostic| diagnostic.code == "OPENAPI_EXTERNAL_REF_UNSUPPORTED")
            .expect("unsupported operation ref diagnostic");
        assert!(
            diagnostic
                .message
                .contains("resources/account/account_get.yml"),
            "{}",
            diagnostic.message
        );
        assert!(
            diagnostic
                .message
                .contains("dereferenced or bundled OpenAPI documents"),
            "{}",
            diagnostic.message
        );
    }

    fn credential_method_hint_manifest(hint: &str) -> V4SourceManifest {
        let manifest_yaml = format!(
            r"
name: github_v4_materialization_test
dsl_version: 4
inputs:
  ACCESS_TOKEN:
    kind: secret
    credential:
      methods:
        - type: source_config
          label: Paste token
          description: Configure a token manually.
          hint: {hint}
surface:
    type: openapi
    file: /tmp/openapi.yaml
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
            stable_input_declarations_sha256(&first.declared_inputs).expect("first hash");
        let second_hash =
            stable_input_declarations_sha256(&second.declared_inputs).expect("second hash");

        assert_ne!(first_hash, second_hash);
    }

    #[test]
    fn load_v4_materialization_warns_on_mismatched_manifest_hash() {
        let (_state, _descriptor, layout, manifest_yaml, _manifest) = setup_materialization();
        let changed_manifest_yaml = format!("description: changed\n{manifest_yaml}");
        let changed_manifest = parse_source_manifest_yaml(&changed_manifest_yaml)
            .expect("parse changed manifest")
            .as_v4()
            .expect("v4")
            .clone();

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &changed_manifest_yaml,
            &changed_manifest,
        )
        .expect("changed manifest hash is advisory");

        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "V4_MANIFEST_FINGERPRINT_MISMATCH")
        );
    }

    #[test]
    fn load_v4_materialization_warns_on_generated_catalog_without_generator_version() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let projection_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join(PROJECTIONS_FILENAME);
        let mut catalog = installed_projection_catalog_value(&layout);
        remove_generator_version(&mut catalog);
        std::fs::write(
            &projection_path,
            serde_yaml::to_string(&catalog).expect("encode projections"),
        )
        .expect("write generated projections");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("generated projection catalog provenance is advisory");

        assert!(
            materialized.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "V4_PROJECTION_CATALOG_PROVENANCE_MISMATCH"
            })
        );
    }

    #[test]
    fn load_v4_materialization_accepts_projection_override_without_generator_version() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let mut catalog = installed_projection_catalog_value(&layout);
        remove_generator_version(&mut catalog);
        write_projection_override(&layout, &catalog);

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("projection override without generator version should load");

        assert_eq!(materialized.projections.generator_version, None);
    }

    #[test]
    fn load_v4_materialization_accepts_projection_override_with_current_generator_version() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let catalog = installed_projection_catalog_value(&layout);
        write_projection_override(&layout, &catalog);

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("projection override with current generator version should load");

        assert_eq!(
            materialized.projections.generator_version.as_deref(),
            Some(PROJECTION_GENERATOR_VERSION)
        );
    }

    #[test]
    fn load_v4_materialization_warns_on_stale_projection_override_generator_version() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let mut catalog = installed_projection_catalog_value(&layout);
        set_generator_version(&mut catalog, "derive-read-v0");
        write_projection_override(&layout, &catalog);

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("stale projection generator provenance is advisory");

        assert!(
            materialized.diagnostics.iter().any(|diagnostic| {
                diagnostic.code == "V4_PROJECTION_CATALOG_PROVENANCE_MISMATCH"
            })
        );
    }

    #[test]
    fn load_v4_materialization_rejects_corrupted_projection_override_with_override_guidance() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(PROJECTIONS_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(&override_path, b": not yaml").expect("write corrupt projection override");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("corrupt projection override should fail");
        let message = error.to_string();

        assert!(
            matches!(error, AppError::InvalidV4ProjectionOverride { .. }),
            "unexpected error: {error:#}"
        );
        assert!(
            message.contains("failed to read projection override artifact"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("Edit or remove the override file"),
            "unexpected error: {message}"
        );
        assert!(
            !message.contains("Re-add the source"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn load_v4_materialization_warns_on_corrupted_optional_fingerprint() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        std::fs::write(&fingerprint_path, b": not yaml").expect("corrupt fingerprint");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("corrupted optional fingerprint is advisory");

        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "V4_FINGERPRINT_UNAVAILABLE")
        );
    }

    #[test]
    fn load_v4_materialization_reports_semantic_ir_failure_and_prior_warnings() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        std::fs::write(fingerprint_path, b": not yaml").expect("corrupt fingerprint");
        let semantic_ir_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join("semantic-ir.yaml");
        std::fs::write(semantic_ir_path, b": not yaml").expect("corrupt semantic IR");
        let reporter = SourceDiagnosticReporter::default();

        load_v4_materialization_with_reporter(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
            &reporter,
        )
        .expect_err("corrupt semantic IR should fail");

        assert!(reporter.tracks_diagnostic(
            &workspace_name(),
            &source_name(),
            "materialization",
            "V4_FINGERPRINT_UNAVAILABLE",
        ));
        assert!(reporter.tracks_diagnostic(
            &workspace_name(),
            &source_name(),
            "materialization",
            "V4_SEMANTIC_IR_UNAVAILABLE",
        ));
    }

    #[test]
    fn load_v4_materialization_warns_on_previous_fingerprint_schema() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        let mut fingerprint: serde_yaml::Value =
            serde_yaml::from_slice(&std::fs::read(&fingerprint_path).expect("fingerprint"))
                .expect("fingerprint yaml");
        fingerprint
            .as_mapping_mut()
            .expect("fingerprint mapping")
            .insert(
                "artifact_schema_version".into(),
                (V4_ARTIFACT_SCHEMA_VERSION - 1).into(),
            );
        std::fs::write(
            &fingerprint_path,
            serde_yaml::to_string(&fingerprint).expect("encode fingerprint"),
        )
        .expect("write old fingerprint");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("old fingerprint provenance is advisory");

        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "V4_FINGERPRINT_HEADER_MISMATCH")
        );
    }

    #[test]
    fn load_v4_materialization_applies_operation_metadata_override_without_rewriting_artifact() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let generated_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        let mut override_metadata: OperationMetadataCatalog =
            read_yaml(&generated_path).expect("generated operation metadata");
        let operation_metadata = override_metadata
            .operations
            .values_mut()
            .next()
            .expect("operation metadata");
        let coral_spec::v4::OperationMetadata::Rest { lookup_keys, .. } = operation_metadata else {
            panic!("expected REST metadata");
        };
        *lookup_keys = vec!["q".to_string()];
        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        write_yaml(&override_path, &override_metadata).expect("write operation metadata override");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("load materialization with override");
        let operation_id = materialized
            .surface
            .plan
            .semantic_ir()
            .operations
            .first()
            .expect("operation")
            .id
            .clone();
        assert!(
            materialized
                .surface
                .plan
                .input_is_lookup_key(&operation_id, "q")
        );
        assert!(
            !materialized
                .surface
                .plan
                .input_is_lookup_key(&operation_id, "state")
        );

        let artifact_metadata: OperationMetadataCatalog =
            read_yaml(&generated_path).expect("read persisted operation metadata");
        let coral_spec::v4::OperationMetadata::Rest { lookup_keys, .. } = artifact_metadata
            .operations
            .get(&operation_id)
            .expect("artifact operation metadata")
        else {
            panic!("expected REST metadata");
        };
        assert_eq!(lookup_keys, &["state"]);
    }

    #[test]
    fn full_operation_metadata_override_rescues_missing_generated_artifact() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let generated_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        let generated = std::fs::read(&generated_path).expect("generated operation metadata");
        std::fs::remove_file(&generated_path).expect("remove generated metadata");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("old materialization must require re-add");
        assert!(matches!(
            error,
            AppError::MissingOrIncompatibleV4Materialization { .. }
        ));

        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(override_path, generated).expect("write full override");

        load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("full valid override should rescue old materialization");
    }

    #[test]
    fn valid_metadata_override_does_not_misattribute_corrupt_semantic_ir() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let materialized_dir = layout.v4_materialized_dir(&workspace_name(), &source_name());
        let generated_metadata = materialized_dir.join(OPERATION_METADATA_FILENAME);
        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::copy(generated_metadata, override_path).expect("copy valid metadata override");

        let semantic_ir_path = materialized_dir.join("semantic-ir.yaml");
        let mut semantic_ir: SemanticIr = read_yaml(&semantic_ir_path).expect("read semantic IR");
        semantic_ir
            .operations
            .first_mut()
            .expect("operation")
            .output
            .type_ref = "missing_type".to_string();
        write_yaml(&semantic_ir_path, &semantic_ir).expect("write corrupt semantic IR");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("corrupt semantic IR should fail independently of the override");

        assert!(
            matches!(
                error,
                AppError::MissingOrIncompatibleV4Materialization { .. }
            ),
            "unexpected error: {error:#}"
        );
        assert!(error.to_string().contains("Re-add the source"));
    }

    #[test]
    fn legacy_parameter_metadata_override_is_silently_ignored() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let legacy_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join("parameter_metadata.yaml");
        std::fs::create_dir_all(legacy_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(&legacy_path, b": deliberately invalid legacy yaml")
            .expect("write legacy override");

        load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("legacy parameter metadata must be inert");
        assert!(legacy_path.exists(), "legacy file must remain untouched");
    }

    #[test]
    fn operation_metadata_operational_io_errors_propagate() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(&override_path).expect("create directory at override path");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("directory read should be an operational error");
        assert!(
            matches!(error, AppError::Io(_)),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn load_v4_materialization_rejects_invalid_operation_metadata_override_with_override_guidance()
    {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(&override_path, b": not yaml").expect("write corrupt override");

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("corrupt operation metadata override should fail");
        let message = error.to_string();

        assert!(
            matches!(error, AppError::InvalidV4OperationMetadataOverride { .. }),
            "unexpected error: {error:#}"
        );
        assert!(
            message.contains("failed to read override artifact"),
            "unexpected error: {message}"
        );
        assert!(
            message.contains("Edit or remove the override file"),
            "unexpected error: {message}"
        );
    }

    #[test]
    fn load_v4_materialization_reports_metadata_override_failure_and_prior_warnings() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        std::fs::write(fingerprint_path, b": not yaml").expect("corrupt fingerprint");
        let override_path = layout
            .v4_override_dir(&workspace_name(), &source_name())
            .join(OPERATION_METADATA_FILENAME);
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(override_path, b": not yaml").expect("write corrupt override");
        let reporter = SourceDiagnosticReporter::default();

        load_v4_materialization_with_reporter(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
            &reporter,
        )
        .expect_err("corrupt metadata override should fail");

        assert!(reporter.tracks_diagnostic(
            &workspace_name(),
            &source_name(),
            "materialization",
            "V4_FINGERPRINT_UNAVAILABLE",
        ));
        assert!(reporter.tracks_diagnostic(
            &workspace_name(),
            &source_name(),
            "materialization",
            "V4_OPERATION_METADATA_OVERRIDE_FAILED",
        ));
    }

    #[test]
    fn load_v4_materialization_warns_on_mismatched_fingerprint_surface_type() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        let mut fingerprint: serde_yaml::Value =
            serde_yaml::from_slice(&std::fs::read(&fingerprint_path).expect("fingerprint"))
                .expect("fingerprint yaml");
        fingerprint
            .get_mut("surface")
            .and_then(serde_yaml::Value::as_mapping_mut)
            .expect("surface")
            .insert("surface_type".into(), "mcp".into());
        std::fs::write(
            &fingerprint_path,
            serde_yaml::to_string(&fingerprint).expect("encode fingerprint"),
        )
        .expect("write fingerprint");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("mismatched fingerprint surface type is advisory");

        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "V4_FINGERPRINT_SURFACE_MISMATCH")
        );
    }

    #[test]
    fn load_v4_materialization_warns_on_corrupted_raw_source_document() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let raw_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join("source-document.raw");
        std::fs::write(&raw_path, b"corrupted").expect("corrupt raw descriptor");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("raw descriptor hash is advisory provenance");

        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "V4_RAW_DOCUMENT_FINGERPRINT_MISMATCH" })
        );
    }

    #[cfg(unix)]
    #[test]
    fn load_v4_materialization_warns_on_unreadable_raw_source_document() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let raw_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
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
        let materialized = result.expect("unreadable raw descriptor is advisory provenance");
        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "V4_RAW_DOCUMENT_UNAVAILABLE")
        );
    }

    #[test]
    fn read_url_descriptor_rejects_non_https_urls() {
        let error = read_url_descriptor_on_blocking_thread("http://example.com/openapi.yaml")
            .expect_err("plain HTTP descriptor should fail");

        assert!(
            error.to_string().contains("must use HTTPS"),
            "unexpected error: {error}"
        );
    }
}
