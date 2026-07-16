//! DSL v4 source materialization and artifact loading.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use coral_spec::v4::{
    Diagnostic, DiagnosticSeverity, Fingerprint, FingerprintSurface, MCP_IMPORTER_VERSION,
    MaterializedSurface, McpToolCatalog, OPENAPI_IMPORTER_VERSION, PROJECTION_GENERATOR_VERSION,
    ProjectionCatalog, ProjectionPaginationInputSyncMode, ProjectionVisibility,
    SURFACE_IMPORTER_VERSION, SemanticIr, SurfaceType, V4_ARTIFACT_SCHEMA_VERSION,
    V4MaterializedSource, V4SourceManifest, apply_parameter_metadata_overrides,
    generate_projection_catalog, import_mcp_surface, import_openapi_surface,
    normalize_mcp_tool_catalog, normalize_source_document, openapi_document_metadata,
    parse_parameter_metadata_overrides_yaml, sync_projection_pagination_inputs,
    validate_materialized_source, validate_materialized_source_structure,
    validate_openapi_base_url_template,
};
use coral_spec::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
    ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter, ParsedTemplate,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::state::{AppStateLayout, V4ProjectionCatalogFile, V4ProjectionCatalogOrigin};
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

const DESCRIPTOR_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DESCRIPTOR_BYTES: u64 = 64 * 1024 * 1024;
const DESCRIPTOR_USER_AGENT: &str = "coral-dsl-v4-materializer";
pub(crate) const PROJECTIONS_FILENAME: &str = "projections.yaml";
pub(crate) const FINGERPRINT_FILENAME: &str = "fingerprint.yaml";
pub(crate) const DIAGNOSTICS_FILENAME: &str = "diagnostics.yaml";
pub(crate) const PARAMETER_METADATA_OVERRIDE_FILENAME: &str = "parameter_metadata.yaml";

type ReportedDiagnosticKey = (String, String);
type ReportedDiagnosticStateKey = (String, String, String);
type ReportedDiagnostics = BTreeMap<ReportedDiagnosticStateKey, BTreeSet<ReportedDiagnosticKey>>;
type RawDocumentValidationKey = (String, String, String);

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
    Catalog,
}

impl SourceLoadDiagnosticStage {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Query => "query-source",
            Self::Catalog => "catalog-source",
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
        let diagnostic = Diagnostic {
            code: code.to_string(),
            severity: DiagnosticSeverity::Warning,
            message: detail.to_string(),
            surface_id: None,
            operation_id: None,
            projection_name: None,
        };
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

    pub(crate) fn report_runtime_surface_diagnostics(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        diagnostics: &[Diagnostic],
    ) {
        self.report_source_diagnostics(workspace_name, source_name, "runtime", diagnostics.iter());
    }

    pub(crate) fn clear_source(&self, workspace_name: &WorkspaceName, source_name: &SourceName) {
        {
            let mut reported = self
                .reported
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            reported.retain(|(workspace, source, _stage), _diagnostics| {
                workspace != workspace_name.as_str() || source != source_name.as_str()
            });
        }
        let mut raw_document_validations = self
            .raw_document_validations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        raw_document_validations.retain(|(workspace, source, _surface), _validation| {
            workspace != workspace_name.as_str() || source != source_name.as_str()
        });
    }

    fn validate_raw_document_fingerprint(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface: &coral_spec::v4::V4Surface,
        path: &Path,
        expected_sha256: &str,
    ) -> Option<Diagnostic> {
        let key = (
            workspace_name.to_string(),
            source_name.to_string(),
            surface.id.clone(),
        );
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
                    format!(
                        "raw source document hash does not match for surface '{}'",
                        surface.id
                    ),
                    Some(surface.id.clone()),
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
                    Some(surface.id.clone()),
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
                surface = diagnostic.surface_id.as_deref().unwrap_or(""),
                operation = diagnostic.operation_id.as_deref().unwrap_or(""),
                projection = diagnostic.projection_name.as_deref().unwrap_or(""),
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
    fn tracked_stage_count(&self) -> usize {
        self.reported
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .len()
    }

    #[cfg(test)]
    fn tracks_diagnostic(
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

pub(crate) fn load_v4_materialization(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<V4MaterializedSource, AppError> {
    let fingerprint_path = layout.v4_fingerprint_file(workspace_name, source_name);
    let projections_file = layout.v4_projection_catalog_file(workspace_name, source_name);
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
    let originally_published = projections
        .projections
        .iter()
        .filter(|projection| projection.visibility == ProjectionVisibility::Published)
        .count();
    let (surfaces, mut failed_surfaces, mut surface_diagnostics) = load_projected_surfaces(
        layout,
        workspace_name,
        source_name,
        manifest,
        &projections,
        fingerprint.as_ref(),
        diagnostic_reporter,
    );
    load_diagnostics.append(&mut surface_diagnostics);
    let projection_structure_failure = collect_projection_coherence_diagnostics(
        manifest,
        &projections,
        &surfaces,
        &mut failed_surfaces,
        &mut load_diagnostics,
    );
    projections
        .projections
        .retain(|projection| !failed_surfaces.contains(&projection.surface_id));
    let projection_sync_mode = match projections_file.origin {
        V4ProjectionCatalogOrigin::Materialized => {
            ProjectionPaginationInputSyncMode::RecomputeRestInputExposure
        }
        V4ProjectionCatalogOrigin::Override => {
            ProjectionPaginationInputSyncMode::PreserveExistingExposure
        }
    };
    sync_projection_pagination_inputs(
        surfaces.iter().map(|surface| &surface.semantic_ir),
        &mut projections,
        projection_sync_mode,
    );
    diagnostic_reporter.report_source_diagnostics(
        workspace_name,
        source_name,
        "materialization",
        load_diagnostics.iter(),
    );
    ensure_published_projection_survives(
        source_name,
        &projections_file,
        originally_published,
        &projections,
        projection_structure_failure,
    )?;
    diagnostics.append(&mut load_diagnostics);
    let materialized = V4MaterializedSource {
        fingerprint,
        surfaces,
        projections,
        diagnostics,
    };
    validate_loaded_materialized_source(source_name, manifest, &projections_file, &materialized)?;
    Ok(materialized)
}

fn ensure_published_projection_survives(
    source_name: &SourceName,
    projections_file: &V4ProjectionCatalogFile,
    originally_published: usize,
    projections: &ProjectionCatalog,
    structure_failure: Option<String>,
) -> Result<(), AppError> {
    if originally_published == 0
        || projections
            .projections
            .iter()
            .any(|projection| projection.visibility == ProjectionVisibility::Published)
    {
        return Ok(());
    }
    if let Some(detail) = structure_failure {
        return Err(match projections_file.origin {
            V4ProjectionCatalogOrigin::Materialized => {
                incompatible_materialization_error(source_name, detail)
            }
            V4ProjectionCatalogOrigin::Override => {
                invalid_projection_override_error(source_name, &projections_file.path, detail)
            }
        });
    }
    Err(incompatible_materialization_error(
        source_name,
        "no published projection surfaces could be loaded",
    ))
}

fn validate_loaded_materialized_source(
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
    })
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
                None,
            ));
            return None;
        }
    };
    if let Err(error) = validate_fingerprint_header(manifest, &fingerprint) {
        diagnostics.push(materialization_warning(
            "V4_FINGERPRINT_HEADER_MISMATCH",
            error,
            None,
        ));
    }
    if fingerprint.manifest_sha256 != sha256_hex(manifest_yaml.as_bytes()) {
        diagnostics.push(materialization_warning(
            "V4_MANIFEST_FINGERPRINT_MISMATCH",
            "manifest fingerprint does not match installed manifest",
            None,
        ));
    }
    if let Err(error) = validate_fingerprint_surfaces(manifest, &fingerprint) {
        diagnostics.push(materialization_warning(
            "V4_FINGERPRINT_SURFACE_MISMATCH",
            error,
            None,
        ));
    }
    Some(fingerprint)
}

fn load_projection_catalog(
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    projections_file: &V4ProjectionCatalogFile,
    diagnostics: &mut Vec<Diagnostic>,
) -> Result<ProjectionCatalog, AppError> {
    let mut projections = match projections_file.origin {
        V4ProjectionCatalogOrigin::Materialized => {
            read_artifact_yaml(source_name, "projection catalog", &projections_file.path)?
        }
        V4ProjectionCatalogOrigin::Override => {
            read_projection_override_yaml(source_name, &projections_file.path)?
        }
    };
    for projection in &mut projections.projections {
        if projection.namespace.is_empty()
            && let Some(surface) = manifest.surface(&projection.surface_id)
        {
            projection.namespace = surface.relation_namespace.clone();
        }
    }
    if let Err(error) = validate_projection_catalog_header(manifest, &projections, projections_file)
    {
        diagnostics.push(materialization_warning(
            "V4_PROJECTION_CATALOG_PROVENANCE_MISMATCH",
            error,
            None,
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
                None,
            ));
            Vec::new()
        }
    }
}

fn load_projected_surfaces(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    projections: &ProjectionCatalog,
    fingerprint: Option<&Fingerprint>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> (Vec<MaterializedSurface>, BTreeSet<String>, Vec<Diagnostic>) {
    let mut diagnostics = Vec::new();
    let projected_surface_ids = projections
        .projections
        .iter()
        .map(|projection| projection.surface_id.clone())
        .collect::<BTreeSet<_>>();
    let fingerprint_surfaces = fingerprint
        .map(|fingerprint| {
            fingerprint
                .surfaces
                .iter()
                .map(|surface| (surface.surface_id.clone(), surface))
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();
    let mut surfaces = Vec::new();
    let mut failed_surfaces = BTreeSet::new();
    for surface_id in projected_surface_ids {
        let Some(surface) = manifest.surface(&surface_id) else {
            diagnostics.push(materialization_warning(
                "V4_PROJECTED_SURFACE_NOT_DECLARED",
                format!("projection catalog references undeclared surface '{surface_id}'"),
                Some(surface_id.clone()),
            ));
            failed_surfaces.insert(surface_id);
            continue;
        };
        match load_projected_surface(
            layout,
            workspace_name,
            source_name,
            manifest,
            surface,
            fingerprint_surfaces.get(&surface.id).copied(),
            diagnostic_reporter,
        ) {
            Ok((materialized_surface, mut surface_diagnostics)) => {
                surfaces.push(materialized_surface);
                diagnostics.append(&mut surface_diagnostics);
            }
            Err(diagnostic) => {
                diagnostics.push(*diagnostic);
                failed_surfaces.insert(surface.id.clone());
            }
        }
    }
    (surfaces, failed_surfaces, diagnostics)
}

fn load_projected_surface(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    fingerprint: Option<&FingerprintSurface>,
    diagnostic_reporter: &SourceDiagnosticReporter,
) -> Result<(MaterializedSurface, Vec<Diagnostic>), Box<Diagnostic>> {
    let surface_dir = layout.v4_surface_dir(workspace_name, source_name, &surface.id);
    let raw_source_document_path = surface_dir.join("source-document.raw");
    let semantic_ir_path = surface_dir.join("semantic-ir.yaml");
    let mut semantic_ir: SemanticIr =
        read_artifact_yaml(source_name, "semantic IR", &semantic_ir_path).map_err(|error| {
            Box::new(materialization_warning(
                "V4_SEMANTIC_IR_UNAVAILABLE",
                error.to_string(),
                Some(surface.id.clone()),
            ))
        })?;
    let mut diagnostics = Vec::new();
    if let Err(error) = validate_semantic_ir(manifest, surface, &semantic_ir) {
        diagnostics.push(materialization_warning(
            "V4_SEMANTIC_IR_PROVENANCE_MISMATCH",
            error,
            Some(surface.id.clone()),
        ));
    }
    apply_parameter_metadata_override_file(
        layout,
        workspace_name,
        source_name,
        &surface.id,
        &mut semantic_ir,
    )
    .map_err(|error| {
        Box::new(materialization_warning(
            "V4_PARAMETER_METADATA_OVERRIDE_FAILED",
            error.to_string(),
            Some(surface.id.clone()),
        ))
    })?;
    if let Some(fingerprint) = fingerprint
        && let Some(diagnostic) = diagnostic_reporter.validate_raw_document_fingerprint(
            workspace_name,
            source_name,
            surface,
            &raw_source_document_path,
            &fingerprint.descriptor_sha256,
        )
    {
        diagnostics.push(diagnostic);
    }
    Ok((
        MaterializedSurface {
            surface_id: surface.id.clone(),
            semantic_ir,
            source_document_sha256: fingerprint
                .map(|fingerprint| fingerprint.descriptor_sha256.clone()),
            normalized_source_document_path: surface_dir.join("source-document.yaml"),
            raw_source_document_path,
        },
        diagnostics,
    ))
}

fn collect_projection_coherence_diagnostics(
    manifest: &V4SourceManifest,
    projections: &ProjectionCatalog,
    surfaces: &[MaterializedSurface],
    failed_surfaces: &mut BTreeSet<String>,
    diagnostics: &mut Vec<Diagnostic>,
) -> Option<String> {
    let mut projection_names = BTreeSet::new();
    let mut duplicate_projection_names = BTreeSet::new();
    let mut first_structure_failure = None;
    for projection in &projections.projections {
        let Some(surface) = manifest.surface(&projection.surface_id) else {
            continue;
        };
        let runtime_name = (projection.surface_id.as_str(), projection.name.as_str());
        if !projection_names.insert(runtime_name) && duplicate_projection_names.insert(runtime_name)
        {
            let detail = format!(
                "projection '{}' is repeated for surface '{}'",
                projection.name, projection.surface_id
            );
            diagnostics.push(materialization_warning(
                "V4_PROJECTION_NAME_REPEATED",
                detail.clone(),
                Some(surface.id.clone()),
            ));
            failed_surfaces.insert(surface.id.clone());
            first_structure_failure.get_or_insert(detail);
        }
        if projection.namespace != surface.relation_namespace {
            diagnostics.push(materialization_warning(
                "V4_PROJECTION_NAMESPACE_MISMATCH",
                format!(
                    "projection '{}' namespace '{}' does not match surface '{}' relation namespace '{}'",
                    projection.name,
                    projection.namespace,
                    projection.surface_id,
                    surface.relation_namespace
                ),
                Some(surface.id.clone()),
            ));
        }
        if let Some(materialized_surface) = surfaces
            .iter()
            .find(|candidate| candidate.surface_id == projection.surface_id)
            && !materialized_surface
                .semantic_ir
                .operations
                .iter()
                .any(|operation| operation.id == projection.operation_id)
        {
            diagnostics.push(materialization_warning(
                "V4_PROJECTION_OPERATION_MISSING",
                format!(
                    "projection '{}' references missing operation '{}'",
                    projection.name, projection.operation_id
                ),
                Some(surface.id.clone()),
            ));
            failed_surfaces.insert(surface.id.clone());
        }
    }
    first_structure_failure
}

fn materialization_warning(
    code: &str,
    message: impl Into<String>,
    surface_id: Option<String>,
) -> Diagnostic {
    Diagnostic {
        code: code.to_string(),
        severity: DiagnosticSeverity::Warning,
        message: message.into(),
        surface_id,
        operation_id: None,
        projection_name: None,
    }
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
        || fingerprint.projection_generator_version != PROJECTION_GENERATOR_VERSION
    {
        return Err("fingerprint importer or generator version mismatch".to_string());
    }
    Ok(())
}

fn validate_fingerprint_surfaces(
    manifest: &V4SourceManifest,
    fingerprint: &Fingerprint,
) -> Result<(), String> {
    let declared_ids = manifest
        .surfaces
        .iter()
        .map(|surface| surface.id.as_str())
        .collect::<BTreeSet<_>>();
    let mut seen_ids = BTreeSet::new();
    for surface in &fingerprint.surfaces {
        if !seen_ids.insert(surface.surface_id.as_str()) {
            return Err(format!(
                "fingerprint repeats surface '{}'",
                surface.surface_id
            ));
        }
        if !declared_ids.contains(surface.surface_id.as_str()) {
            return Err(format!(
                "fingerprint surface set mismatch; missing [], extra [{}]",
                surface.surface_id
            ));
        }
    }
    for fingerprint_surface in &fingerprint.surfaces {
        let surface = manifest
            .surface(&fingerprint_surface.surface_id)
            .ok_or_else(|| {
                format!(
                    "fingerprint references undeclared surface '{}'",
                    fingerprint_surface.surface_id
                )
            })?;
        if fingerprint_surface.surface_type != surface.surface_type {
            return Err(format!(
                "surface '{}' type fingerprint does not match",
                surface.id
            ));
        }
        if fingerprint_surface.descriptor_kind != surface.descriptor.kind()
            || fingerprint_surface.descriptor_location != surface.descriptor.location()
        {
            return Err(format!(
                "surface '{}' descriptor fingerprint does not match",
                surface.id
            ));
        }
        let expected =
            stable_input_declarations_sha256(&surface.inputs).map_err(|error| error.to_string())?;
        if fingerprint_surface.input_declarations_sha256 != expected {
            return Err(format!(
                "input declarations fingerprint does not match for surface '{}'",
                surface.id
            ));
        }
    }
    Ok(())
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

fn apply_parameter_metadata_override_file(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    surface_id: &str,
    semantic_ir: &mut SemanticIr,
) -> Result<(), AppError> {
    let path = layout.v4_parameter_metadata_override_file(workspace_name, source_name, surface_id);
    if !path.exists() {
        return Ok(());
    }

    let raw = std::fs::read_to_string(&path).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to read DSL v4 parameter metadata override '{}': {error}",
            path.display()
        ))
    })?;
    let overrides = parse_parameter_metadata_overrides_yaml(&raw).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to parse DSL v4 parameter metadata override '{}': {error}",
            path.display()
        ))
    })?;
    apply_parameter_metadata_overrides(semantic_ir, &overrides).map_err(|error| {
        AppError::FailedPrecondition(format!(
            "failed to apply DSL v4 parameter metadata override '{}': {error}",
            path.display()
        ))
    })?;
    Ok(())
}

fn validate_semantic_ir(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    semantic_ir: &SemanticIr,
) -> Result<(), String> {
    if semantic_ir.artifact_schema_version != V4_ARTIFACT_SCHEMA_VERSION {
        return Err(format!(
            "semantic IR schema version mismatch for surface '{}'",
            surface.id
        ));
    }
    if semantic_ir.source_name != manifest.common.name
        || semantic_ir.surface_id != surface.id
        || semantic_ir.surface_type != surface.surface_type
    {
        return Err(format!(
            "semantic IR identity mismatch for surface '{}'",
            surface.id
        ));
    }
    if semantic_ir.importer_version != expected_importer_version(surface.surface_type) {
        return Err(format!(
            "semantic IR importer version mismatch for surface '{}'",
            surface.id
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
        write_surface_artifacts(&surface_dir, &materialized_surface)?;
        materialized_surfaces.push(MaterializedSurface {
            surface_id: surface.id.clone(),
            semantic_ir: materialized_surface.semantic_ir.clone(),
            source_document_sha256: Some(materialized_surface.observed_sha256.clone()),
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
        fingerprint: Some(fingerprint.clone()),
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

/// Writes the per-surface materialized artifacts (source documents and
/// semantic IR).
fn write_surface_artifacts(
    surface_dir: &Path,
    materialized_surface: &MaterializedSurfaceBuild,
) -> Result<(), AppError> {
    fs::ensure_private_dir(surface_dir)?;
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

fn read_descriptor(surface: &coral_spec::v4::V4Surface) -> Result<Vec<u8>, AppError> {
    match &surface.descriptor {
        coral_spec::v4::SurfaceDescriptor::File { file } => read_file_descriptor(file),
        coral_spec::v4::SurfaceDescriptor::Url { url } => read_url_descriptor(url),
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

    fn load_v4_materialization(
        layout: &AppStateLayout,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: &str,
        manifest: &V4SourceManifest,
    ) -> Result<V4MaterializedSource, AppError> {
        super::load_v4_materialization(
            layout,
            workspace_name,
            source_name,
            manifest_yaml,
            manifest,
            &SourceDiagnosticReporter::default(),
        )
    }

    fn assert_load_diagnostic(materialized: &V4MaterializedSource, code: &str) {
        assert!(
            materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == code),
            "expected diagnostic {code}: {:#?}",
            materialized.diagnostics
        );
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

    #[test]
    fn diagnostic_reporter_owns_and_clears_source_state() {
        let reporter = SourceDiagnosticReporter::default();
        let independent_reporter = SourceDiagnosticReporter::default();
        let workspace_name = workspace_name();
        let source_name = source_name();
        reporter.report_source_load_failure(
            SourceLoadDiagnosticStage::Query,
            &workspace_name,
            &source_name,
            "SOURCE_LOAD_FAILED",
            "test failure",
        );
        reporter.report_source_load_failure(
            SourceLoadDiagnosticStage::Catalog,
            &workspace_name,
            &source_name,
            "SOURCE_LOAD_FAILED",
            "catalog failure",
        );
        reporter.report_runtime_surface_diagnostics(
            &workspace_name,
            &source_name,
            &[materialization_warning(
                "V4_RUNTIME_SURFACE_ASSEMBLY_FAILED",
                "test runtime failure",
                Some("rest".to_string()),
            )],
        );

        assert_eq!(reporter.tracked_stage_count(), 3);
        assert_eq!(reporter.clone().tracked_stage_count(), 3);
        assert_eq!(independent_reporter.tracked_stage_count(), 0);

        reporter.clear_source_load_failure(
            SourceLoadDiagnosticStage::Catalog,
            &workspace_name,
            &source_name,
        );

        assert_eq!(reporter.tracked_stage_count(), 2);
        assert!(reporter.tracks_diagnostic(
            &workspace_name,
            &source_name,
            SourceLoadDiagnosticStage::Query.as_str(),
            "SOURCE_LOAD_FAILED",
        ));

        reporter.clear_source(&workspace_name, &source_name);

        assert_eq!(reporter.tracked_stage_count(), 0);
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

    fn installed_projection_catalog_value(layout: &AppStateLayout) -> serde_yaml::Value {
        let path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join(PROJECTIONS_FILENAME);
        serde_yaml::from_slice(&std::fs::read(path).expect("read generated projections"))
            .expect("parse generated projections")
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

    fn set_yaml_string_field(path: &Path, field: &str, value: &str) {
        let mut artifact: serde_yaml::Value =
            serde_yaml::from_slice(&std::fs::read(path).expect("read YAML artifact"))
                .expect("parse YAML artifact");
        artifact
            .as_mapping_mut()
            .expect("artifact mapping")
            .insert(field.into(), value.into());
        std::fs::write(
            path,
            serde_yaml::to_string(&artifact).expect("encode YAML artifact"),
        )
        .expect("write YAML artifact");
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
    fn build_v4_materialization_persists_lookup_key_flags_in_semantic_ir() {
        let (_state, _descriptor, layout, _manifest_yaml, _manifest) = setup_materialization();
        let surface_dir = layout.v4_surface_dir(&workspace_name(), &source_name(), "rest");
        assert!(
            !surface_dir
                .join(PARAMETER_METADATA_OVERRIDE_FILENAME)
                .exists(),
            "generated lookup key metadata should live in semantic-ir.yaml"
        );

        let semantic_ir: SemanticIr =
            read_yaml(&surface_dir.join("semantic-ir.yaml")).expect("read semantic IR");
        let operation = semantic_ir.operations.first().expect("operation");
        let input_excluded = |name: &str| {
            operation
                .inputs
                .iter()
                .find(|input| input.name == name)
                .expect("input")
                .exclude_from_lookup_keys
        };
        assert!(input_excluded("order_by"));
        assert!(input_excluded("q"));
        assert!(!input_excluded("state"));
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

        let semantic_ir: SemanticIr = read_yaml(
            &build
                .temp_dir
                .join("surfaces")
                .join("rest")
                .join("semantic-ir.yaml"),
        )
        .expect("read semantic IR");
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
        assert_eq!(diagnostic.surface_id.as_deref(), Some("rest"));
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
        .expect("changed manifest hash should remain loadable");

        assert_load_diagnostic(&materialized, "V4_MANIFEST_FINGERPRINT_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_warns_on_fingerprint_identity_mismatch() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let fingerprint_path = layout.v4_fingerprint_file(&workspace_name(), &source_name());
        set_yaml_string_field(&fingerprint_path, "source_name", "stale_source_identity");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("fingerprint identity provenance should not block loading");

        assert_load_diagnostic(&materialized, "V4_FINGERPRINT_HEADER_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_warns_on_semantic_ir_identity_mismatch() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let semantic_ir_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("semantic-ir.yaml");
        set_yaml_string_field(&semantic_ir_path, "source_name", "stale_source_identity");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("semantic IR identity provenance should not block loading");

        assert_load_diagnostic(&materialized, "V4_SEMANTIC_IR_PROVENANCE_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_warns_on_semantic_ir_importer_version_mismatch() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let semantic_ir_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("semantic-ir.yaml");
        set_yaml_string_field(&semantic_ir_path, "importer_version", "openapi-v0");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("semantic IR producer provenance should not block loading");

        assert_load_diagnostic(&materialized, "V4_SEMANTIC_IR_PROVENANCE_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_reports_surface_diagnostics_before_fatal_degradation() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let semantic_ir_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("semantic-ir.yaml");
        std::fs::write(semantic_ir_path, b": not yaml").expect("corrupt semantic IR");
        let reporter = SourceDiagnosticReporter::default();

        let error = super::load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
            &reporter,
        )
        .expect_err("losing every published surface should fail");

        assert!(
            error
                .to_string()
                .contains("no published projection surfaces could be loaded"),
            "unexpected error: {error}"
        );
        assert!(reporter.tracks_diagnostic(
            &workspace_name(),
            &source_name(),
            "materialization",
            "V4_SEMANTIC_IR_UNAVAILABLE",
        ));
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
        .expect("generated catalog provenance should not block loading");

        assert_load_diagnostic(&materialized, "V4_PROJECTION_CATALOG_PROVENANCE_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_defaults_missing_projection_namespace() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let projection_path = layout
            .v4_materialized_dir(&workspace_name(), &source_name())
            .join(PROJECTIONS_FILENAME);
        let mut catalog = installed_projection_catalog_value(&layout);
        let first_projection = catalog
            .get_mut("projections")
            .and_then(serde_yaml::Value::as_sequence_mut)
            .and_then(|projections| projections.first_mut())
            .and_then(serde_yaml::Value::as_mapping_mut)
            .expect("first projection mapping");
        first_projection.remove(serde_yaml::Value::String("namespace".to_string()));
        std::fs::write(
            &projection_path,
            serde_yaml::to_string(&catalog).expect("encode projections"),
        )
        .expect("write projections without namespace");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("legacy projection namespace should be migrated");

        let projection = materialized
            .projections
            .projections
            .first()
            .expect("first projection");
        assert_eq!(
            projection.namespace,
            manifest
                .surface("rest")
                .expect("REST surface")
                .relation_namespace
        );
        assert!(
            !materialized
                .diagnostics
                .iter()
                .any(|diagnostic| { diagnostic.code == "V4_PROJECTION_NAMESPACE_MISMATCH" })
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
        .expect("stale override provenance should not block loading");

        assert_load_diagnostic(&materialized, "V4_PROJECTION_CATALOG_PROVENANCE_MISMATCH");
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
    fn load_v4_materialization_rejects_duplicate_projection_override_names() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let mut catalog = installed_projection_catalog_value(&layout);
        let projections = catalog
            .get_mut("projections")
            .and_then(serde_yaml::Value::as_sequence_mut)
            .expect("projection sequence");
        projections.push(projections.first().expect("first projection").clone());
        let override_path = write_projection_override(&layout, &catalog);

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("duplicate projection names should fail at load time");

        assert!(
            matches!(error, AppError::InvalidV4ProjectionOverride { .. }),
            "unexpected error: {error:#}"
        );
        assert!(
            error.to_string().contains("projection") && error.to_string().contains("is repeated"),
            "unexpected error: {error}"
        );
        assert!(
            error
                .to_string()
                .contains(&override_path.display().to_string()),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn load_v4_materialization_rejects_duplicate_runtime_names_across_namespaces() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let mut catalog = installed_projection_catalog_value(&layout);
        let projections = catalog
            .get_mut("projections")
            .and_then(serde_yaml::Value::as_sequence_mut)
            .expect("projection sequence");
        let mut duplicate = projections.first().expect("first projection").clone();
        let projection_name = duplicate
            .get("name")
            .and_then(serde_yaml::Value::as_str)
            .expect("projection name")
            .to_string();
        duplicate
            .as_mapping_mut()
            .expect("projection mapping")
            .insert("namespace".into(), "stale_namespace".into());
        projections.push(duplicate);
        write_projection_override(&layout, &catalog);

        let error = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect_err("duplicate runtime names should fail at load time");

        assert!(
            matches!(error, AppError::InvalidV4ProjectionOverride { .. }),
            "unexpected error: {error:#}"
        );
        assert!(
            error.to_string().contains(&format!(
                "projection '{projection_name}' is repeated for surface 'rest'"
            )),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn load_v4_materialization_ignores_corrupted_optional_fingerprint() {
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
        .expect("corrupted optional fingerprint should not fail");

        assert!(materialized.fingerprint.is_none());
        assert!(
            materialized
                .surfaces
                .iter()
                .all(|surface| surface.source_document_sha256.is_none())
        );
        assert_load_diagnostic(&materialized, "V4_FINGERPRINT_UNAVAILABLE");
    }

    #[test]
    fn load_v4_materialization_ignores_missing_optional_diagnostics() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        std::fs::remove_file(layout.v4_diagnostics_file(&workspace_name(), &source_name()))
            .expect("remove diagnostics");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("missing optional diagnostics should not fail");

        assert_load_diagnostic(&materialized, "V4_DIAGNOSTICS_UNAVAILABLE");
    }

    #[test]
    fn load_v4_materialization_applies_parameter_metadata_override_without_rewriting_artifact() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let override_path =
            layout.v4_parameter_metadata_override_file(&workspace_name(), &source_name(), "rest");
        std::fs::create_dir_all(override_path.parent().expect("override parent"))
            .expect("create override dir");
        std::fs::write(
            &override_path,
            r"
lookup_keys:
  enabled: true
  exclude: [state]
operation_overrides:
  issues/list:
    pagination:
      mode: page
      page_param: page_number
      page_start: 1
      page_size:
        default: 50
        max: 100
        query_param: per_page
",
        )
        .expect("write parameter metadata override");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("load materialization with override");
        let operation = materialized
            .surfaces
            .first()
            .expect("surface")
            .semantic_ir
            .operations
            .first()
            .expect("operation");
        let coral_spec::v4::IrExecutionAttachment::Rest(rest) = &operation.execution else {
            panic!("expected REST operation");
        };
        assert_eq!(rest.pagination.mode, coral_spec::PaginationMode::Page);
        assert_eq!(rest.pagination.page_param.as_deref(), Some("page_number"));
        let loaded_input_excluded = |name: &str| {
            operation
                .inputs
                .iter()
                .find(|input| input.name == name)
                .expect("input")
                .exclude_from_lookup_keys
        };
        assert!(!loaded_input_excluded("order_by"));
        assert!(!loaded_input_excluded("q"));
        assert!(loaded_input_excluded("state"));

        let semantic_ir_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("semantic-ir.yaml");
        let artifact_ir: SemanticIr =
            read_yaml(&semantic_ir_path).expect("read persisted semantic IR");
        let artifact_operation = artifact_ir.operations.first().expect("artifact operation");
        let coral_spec::v4::IrExecutionAttachment::Rest(artifact_rest) =
            &artifact_operation.execution
        else {
            panic!("expected REST operation");
        };
        assert_eq!(
            artifact_rest.pagination.mode,
            coral_spec::PaginationMode::None
        );
        let artifact_input_excluded = |name: &str| {
            artifact_operation
                .inputs
                .iter()
                .find(|input| input.name == name)
                .expect("input")
                .exclude_from_lookup_keys
        };
        assert!(artifact_input_excluded("order_by"));
        assert!(artifact_input_excluded("q"));
        assert!(!artifact_input_excluded("state"));
    }

    #[test]
    fn load_v4_materialization_warns_on_extra_fingerprint_surface() {
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

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("extra fingerprint surface should not fail");

        assert_load_diagnostic(&materialized, "V4_FINGERPRINT_SURFACE_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_warns_on_corrupted_raw_source_document() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let raw_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("source-document.raw");
        std::fs::write(&raw_path, b"corrupted").expect("corrupt raw descriptor");

        let materialized = load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
        )
        .expect("raw descriptor hash mismatch should not fail loading");

        assert_load_diagnostic(&materialized, "V4_RAW_DOCUMENT_FINGERPRINT_MISMATCH");
    }

    #[test]
    fn load_v4_materialization_caches_raw_document_fingerprint_validation() {
        let (_state, _descriptor, layout, manifest_yaml, manifest) = setup_materialization();
        let reporter = SourceDiagnosticReporter::default();
        super::load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
            &reporter,
        )
        .expect("initial materialization load");
        let raw_path = layout
            .v4_surface_dir(&workspace_name(), &source_name(), "rest")
            .join("source-document.raw");
        std::fs::remove_file(&raw_path).expect("remove raw descriptor after validation");

        let materialized = super::load_v4_materialization(
            &layout,
            &workspace_name(),
            &source_name(),
            &manifest_yaml,
            &manifest,
            &reporter,
        )
        .expect("cached provenance validation should not reread the raw descriptor");

        assert!(
            !materialized
                .diagnostics
                .iter()
                .any(|diagnostic| diagnostic.code == "V4_RAW_DOCUMENT_UNAVAILABLE"),
            "cached validation unexpectedly reread the raw descriptor"
        );
    }

    #[cfg(unix)]
    #[test]
    fn load_v4_materialization_warns_on_unreadable_raw_source_document() {
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
        let materialized = result.expect("unreadable optional raw descriptor should not fail");
        assert_load_diagnostic(&materialized, "V4_RAW_DOCUMENT_UNAVAILABLE");
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
