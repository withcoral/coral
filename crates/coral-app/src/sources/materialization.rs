//! DSL v4 source materialization and artifact loading.

use std::io::Read as _;
use std::path::{Path, PathBuf};
use std::time::Duration;

use coral_spec::v4::{
    Diagnostic, Fingerprint, FingerprintSurface, MaterializedSurface, OPENAPI_IMPORTER_VERSION,
    PROJECTION_GENERATOR_VERSION, ProjectionCatalog, SemanticIr, V4_ARTIFACT_SCHEMA_VERSION,
    V4MaterializedSource, V4SourceManifest, generate_projection_catalog, import_openapi_surface,
    normalize_source_document, validate_materialized_source,
};
use coral_spec::{
    ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestInputKind, ManifestInputSpec,
    ManifestOAuthClientSecretTransport, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
    ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter,
};
use serde_json::{Value, json};
use sha2::{Digest as _, Sha256};
use uuid::Uuid;

use crate::bootstrap::AppError;
use crate::sources::SourceName;
use crate::sources::catalog::BundledV4Descriptor;
use crate::sources::model::SourceOrigin;
use crate::state::AppStateLayout;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;

const DESCRIPTOR_FETCH_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_DESCRIPTOR_BYTES: u64 = 16 * 1024 * 1024;
const DESCRIPTOR_USER_AGENT: &str = "coral-dsl-v4-materializer";

#[derive(Debug, Clone)]
pub(crate) struct SourceMaterializationSummary {
    pub(crate) source_name: String,
    pub(crate) source_version: String,
    pub(crate) manifest_sha256: String,
    pub(crate) importer_version: String,
    pub(crate) projection_generator_version: String,
    pub(crate) surface_count: u32,
    pub(crate) projection_count: u32,
    pub(crate) published_projection_count: u32,
    pub(crate) hidden_projection_count: u32,
}

#[derive(Debug)]
pub(crate) struct MaterializationBuild {
    pub(crate) temp_dir: PathBuf,
    pub(crate) summary: SourceMaterializationSummary,
    pub(crate) diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct MaterializationDescriptorSource<'a> {
    pub(crate) origin: SourceOrigin,
    pub(crate) bundled_descriptors: &'a [BundledV4Descriptor],
}

pub(crate) fn build_v4_materialization_tmp(
    layout: &AppStateLayout,
    workspace_name: &WorkspaceName,
    source_name: &SourceName,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    descriptor_source: MaterializationDescriptorSource<'_>,
    temp_suffix: &str,
) -> Result<MaterializationBuild, AppError> {
    if matches!(descriptor_source.origin, SourceOrigin::Bundled)
        && manifest.surfaces.iter().any(|surface| {
            matches!(
                surface.descriptor,
                coral_spec::v4::SurfaceDescriptor::File { .. }
            ) && bundled_descriptor(surface, descriptor_source.bundled_descriptors).is_none()
        })
    {
        return Err(AppError::FailedPrecondition(format!(
            "bundled source '{}' uses local DSL v4 file descriptors, which are development-only",
            manifest.common.name
        )));
    }

    let temp_dir = layout.v4_materialized_tmp_dir(workspace_name, source_name, temp_suffix);
    if temp_dir.exists() {
        std::fs::remove_dir_all(&temp_dir)?;
    }
    fs::ensure_private_dir(&temp_dir)?;

    match write_materialization(
        &temp_dir,
        manifest_yaml,
        manifest,
        descriptor_source.bundled_descriptors,
    ) {
        Ok((summary, diagnostics)) => Ok(MaterializationBuild {
            temp_dir,
            summary,
            diagnostics,
        }),
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
) {
    let target = layout.v4_materialized_dir(workspace_name, source_name);
    if let Some(backup) = backup {
        if target.exists() {
            drop(std::fs::remove_dir_all(&target));
        }
        if backup.exists() {
            drop(std::fs::rename(backup, target));
        }
    } else if target.exists() {
        drop(std::fs::remove_dir_all(target));
    }
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
        return Err(stale_materialization_error(
            source_name,
            "required artifact is missing",
        ));
    }
    let fingerprint: Fingerprint = read_yaml(&fingerprint_path)?;
    if fingerprint.manifest_sha256 != sha256_hex(manifest_yaml.as_bytes()) {
        return Err(stale_materialization_error(
            source_name,
            "manifest fingerprint does not match installed manifest",
        ));
    }
    for surface in &manifest.surfaces {
        let Some(fingerprint_surface) = fingerprint
            .surfaces
            .iter()
            .find(|entry| entry.surface_id == surface.id)
        else {
            return Err(stale_materialization_error(
                source_name,
                format!("fingerprint is missing surface '{}'", surface.id),
            ));
        };
        let expected = stable_input_declarations_sha256(&surface.inputs)?;
        if fingerprint_surface.input_declarations_sha256 != expected {
            return Err(stale_materialization_error(
                source_name,
                format!(
                    "input declarations fingerprint does not match for surface '{}'",
                    surface.id
                ),
            ));
        }
    }
    let projections: ProjectionCatalog = read_yaml(&projections_path)?;
    let diagnostics: Vec<Diagnostic> = read_yaml(&diagnostics_path)?;
    let mut surfaces = Vec::new();
    for surface in &manifest.surfaces {
        let surface_dir = layout.v4_surface_dir(workspace_name, source_name, &surface.id);
        let semantic_ir: SemanticIr = read_yaml(&surface_dir.join("semantic-ir.yaml"))?;
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
            normalized_source_document_path: surface_dir.join("source-document.yaml"),
            raw_source_document_path: surface_dir.join("source-document.raw"),
        });
    }
    let materialized = V4MaterializedSource {
        fingerprint,
        surfaces,
        projections,
        diagnostics,
    };
    validate_materialized_source(manifest, &materialized).map_err(|error| {
        stale_materialization_error(source_name, format!("artifact validation failed: {error}"))
    })?;
    Ok(materialized)
}

pub(crate) fn stale_materialization_error(
    source_name: &SourceName,
    detail: impl AsRef<str>,
) -> AppError {
    AppError::FailedPrecondition(format!(
        "source '{source_name}' has stale or missing DSL v4 materialized artifacts: {}. Run `coral source refresh {source_name}` or reinstall the source.",
        detail.as_ref()
    ))
}

#[expect(
    clippy::too_many_lines,
    reason = "Materialization writes one transaction-shaped artifact set; splitting would obscure ordering."
)]
fn write_materialization(
    temp_dir: &Path,
    manifest_yaml: &str,
    manifest: &V4SourceManifest,
    bundled_descriptors: &[BundledV4Descriptor],
) -> Result<(SourceMaterializationSummary, Vec<Diagnostic>), AppError> {
    let manifest_sha256 = sha256_hex(manifest_yaml.as_bytes());
    let mut materialized_surfaces = Vec::new();
    let mut semantic_irs = Vec::new();
    let mut fingerprint_surfaces = Vec::new();
    for surface in &manifest.surfaces {
        let bytes = read_descriptor(surface, bundled_descriptors)?;
        let observed = sha256_hex(&bytes);
        if observed != surface.descriptor.sha256() {
            return Err(AppError::FailedPrecondition(format!(
                "descriptor hash mismatch for source '{}' surface '{}': expected {}, observed {}",
                manifest.common.name,
                surface.id,
                surface.descriptor.sha256(),
                observed
            )));
        }
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
        source_version: manifest.common.version.clone(),
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
    let published = projections
        .projections
        .iter()
        .filter(|projection| {
            projection.visibility == coral_spec::v4::ProjectionVisibility::Published
        })
        .count();
    let hidden = projections.projections.len().saturating_sub(published);
    Ok((
        SourceMaterializationSummary {
            source_name: manifest.common.name.clone(),
            source_version: manifest.common.version.clone(),
            manifest_sha256,
            importer_version: OPENAPI_IMPORTER_VERSION.to_string(),
            projection_generator_version: PROJECTION_GENERATOR_VERSION.to_string(),
            surface_count: u32::try_from(manifest.surfaces.len()).unwrap_or(u32::MAX),
            projection_count: u32::try_from(projections.projections.len()).unwrap_or(u32::MAX),
            published_projection_count: u32::try_from(published).unwrap_or(u32::MAX),
            hidden_projection_count: u32::try_from(hidden).unwrap_or(u32::MAX),
        },
        diagnostics,
    ))
}

fn read_descriptor(
    surface: &coral_spec::v4::V4Surface,
    bundled_descriptors: &[BundledV4Descriptor],
) -> Result<Vec<u8>, AppError> {
    match &surface.descriptor {
        coral_spec::v4::SurfaceDescriptor::File { file, .. } => {
            bundled_descriptor(surface, bundled_descriptors).map_or_else(
                || read_file_descriptor(file),
                |descriptor| Ok(descriptor.to_vec()),
            )
        }
        coral_spec::v4::SurfaceDescriptor::Url { url, .. } => read_url_descriptor(url),
    }
}

fn bundled_descriptor<'a>(
    surface: &coral_spec::v4::V4Surface,
    bundled_descriptors: &'a [BundledV4Descriptor],
) -> Option<&'a [u8]> {
    bundled_descriptors
        .iter()
        .find(|descriptor| descriptor.surface_id == surface.id)
        .map(|descriptor| descriptor.bytes)
}

fn read_file_descriptor(file: &Path) -> Result<Vec<u8>, AppError> {
    if std::fs::symlink_metadata(file)?.file_type().is_symlink() {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' must not be a symlink",
            file.display()
        )));
    }
    let canonical = file.canonicalize()?;
    let current_dir = std::env::current_dir()?.canonicalize()?;
    if !canonical.starts_with(&current_dir) {
        return Err(AppError::FailedPrecondition(format!(
            "OpenAPI descriptor '{}' must be under the current working directory '{}'",
            file.display(),
            current_dir.display()
        )));
    }
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
    let client = reqwest::blocking::Client::builder()
        .timeout(DESCRIPTOR_FETCH_TIMEOUT)
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
