//! DSL v4 source materialization and artifact loading.

use std::collections::{BTreeMap, BTreeSet};
use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use coral_spec::v4::{
    Diagnostic, Fingerprint, FingerprintSurface, MaterializedSurface, OPENAPI_IMPORTER_VERSION,
    PROJECTION_GENERATOR_VERSION, ProjectionCatalog, SemanticIr, V4_ARTIFACT_SCHEMA_VERSION,
    V4MaterializedSource, V4SourceManifest, generate_projection_catalog, import_openapi_surface,
    normalize_source_document, openapi_document_metadata, validate_materialized_source,
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
use crate::state::AppStateLayout;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

const DESCRIPTOR_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;
const DESCRIPTOR_USER_AGENT: &str = "coral-dsl-v4-materializer";

#[derive(Debug)]
pub(crate) struct MaterializationBuild {
    pub(crate) temp_dir: PathBuf,
}

pub(crate) fn build_v4_materialization_tmp(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    temp_suffix: &str,
) -> Result<MaterializationBuild, AppError> {
    let temp_dir = layout.v4_materialized_tmp_dir(workspace_name, source_name, temp_suffix);
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    fs::ensure_private_dir(&temp_dir)?;

    match write_materialization(&temp_dir, manifest_yaml, manifest) {
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
    for surface in &manifest.surfaces {
        let surface_dir = layout.v4_surface_dir(workspace_name, source_name, &surface.id);
        let raw_source_document_path = surface_dir.join("source-document.raw");
        let normalized_source_document_path = surface_dir.join("source-document.yaml");
        let semantic_ir_path = surface_dir.join("semantic-ir.yaml");
        require_file(source_name, &raw_source_document_path)?;
        require_file(source_name, &normalized_source_document_path)?;
        require_file(source_name, &semantic_ir_path)?;
        let semantic_ir: SemanticIr =
            read_artifact_yaml(source_name, "semantic IR", &semantic_ir_path)?;
        let source_document_sha256 = fingerprint
            .surfaces
            .iter()
            .find(|entry| entry.surface_id == surface.id)
            .map(|entry| entry.descriptor_sha256.clone())
            .unwrap_or_default();
        surfaces.push(MaterializedSurface {
            surface_id: surface.id.clone(),
            semantic_ir,
            source_document_sha256,
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
    if fingerprint.importer_version != OPENAPI_IMPORTER_VERSION
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
    let expected_ids = manifest
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
        by_id.insert(surface.surface_id.clone(), surface.clone());
    }
    let actual_ids = seen_ids;
    if actual_ids != expected_ids {
        let missing = expected_ids
            .difference(&actual_ids)
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        let extra = actual_ids
            .difference(&expected_ids)
            .copied()
            .collect::<Vec<_>>()
            .join(", ");
        return Err(incompatible_materialization_error(
            source_name,
            format!("fingerprint surface set mismatch; missing [{missing}], extra [{extra}]"),
        ));
    }
    for surface in &manifest.surfaces {
        let fingerprint_surface = by_id.get(&surface.id).ok_or_else(|| {
            incompatible_materialization_error(
                source_name,
                format!("fingerprint is missing surface '{}'", surface.id),
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
    for surface in &manifest.surfaces {
        let Some(materialized_surface) = materialized
            .surfaces
            .iter()
            .find(|candidate| candidate.surface_id == surface.id)
        else {
            return Err(incompatible_materialization_error(
                source_name,
                format!("materialized surface '{}' is missing", surface.id),
            ));
        };
        let Some(fingerprint_surface) = fingerprint_surfaces.get(&surface.id) else {
            return Err(incompatible_materialization_error(
                source_name,
                format!("fingerprint is missing surface '{}'", surface.id),
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
            surface.id.as_str(),
            materialized_surface
                .semantic_ir
                .operations
                .iter()
                .map(|operation| operation.id.as_str())
                .collect(),
        );
    }
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
    if semantic_ir.importer_version != OPENAPI_IMPORTER_VERSION {
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
) -> Result<(), AppError> {
    let manifest_sha256 = sha256_hex(manifest_yaml.as_bytes());
    let mut materialized_surfaces = Vec::new();
    let mut semantic_irs = Vec::new();
    let mut fingerprint_surfaces = Vec::new();
    for surface in &manifest.surfaces {
        let bytes = read_descriptor(surface)?;
        validate_materialized_surface_base_url(manifest, surface, &bytes)?;
        let observed = sha256_hex(&bytes);
        let semantic_ir = import_openapi_surface(manifest, surface, &bytes).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "failed to import source '{}' surface '{}': {error}",
                manifest.common.name, surface.id
            ))
        })?;
        let surface_dir = temp_dir.join("surfaces").join(&surface.id);
        fs::ensure_private_dir(&surface_dir)?;
        std::fs::write(surface_dir.join("source-document.raw"), &bytes)?;
        std::fs::write(
            surface_dir.join("source-document.yaml"),
            normalize_source_document(&bytes)
                .map_err(|error| AppError::FailedPrecondition(error.to_string()))?,
        )?;
        write_yaml(&surface_dir.join("semantic-ir.yaml"), &semantic_ir)?;
        materialized_surfaces.push(MaterializedSurface {
            surface_id: surface.id.clone(),
            semantic_ir: semantic_ir.clone(),
            source_document_sha256: observed.clone(),
            normalized_source_document_path: surface_dir.join("source-document.yaml"),
            raw_source_document_path: surface_dir.join("source-document.raw"),
        });
        semantic_irs.push(semantic_ir);
        fingerprint_surfaces.push(FingerprintSurface {
            surface_id: surface.id.clone(),
            surface_type: surface.surface_type,
            descriptor_kind: surface.descriptor.kind().to_string(),
            descriptor_location: surface.descriptor.location(),
            descriptor_sha256: observed,
            input_declarations_sha256: stable_input_declarations_sha256(&surface.inputs)?,
        });
    }
    let projections = generate_projection_catalog(manifest, &semantic_irs)
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
    let mut diagnostics = projections.diagnostics.clone();
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
        importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
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
    write_yaml(&temp_dir.join("fingerprint.yaml"), &fingerprint)?;
    write_yaml(&temp_dir.join("projections.yaml"), &projections)?;
    write_yaml(&temp_dir.join("diagnostics.yaml"), &diagnostics)?;
    Ok(())
}

fn validate_materialized_surface_base_url(
    manifest: &V4SourceManifest,
    surface: &coral_spec::v4::V4Surface,
    bytes: &[u8],
) -> Result<(), AppError> {
    if !surface.openapi_runtime.base_url.raw().trim().is_empty() {
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
    use tempfile::TempDir;

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
            "test",
        )
        .expect("build materialization");
        replace_v4_materialization(&layout, &workspace_name(), &source_name(), &build.temp_dir)
            .expect("install materialization");
        (state_temp, descriptor_temp, layout, manifest_yaml, manifest)
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
        let error = read_url_descriptor_on_blocking_thread("http://example.com/openapi.yaml")
            .expect_err("plain HTTP descriptor should fail");

        assert!(
            error.to_string().contains("must use HTTPS"),
            "unexpected error: {error}"
        );
    }
}
