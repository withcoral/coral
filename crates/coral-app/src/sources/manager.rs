//! Owns the source lifecycle workflow for the local app.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use coral_spec::v4::SurfaceDescriptor;
use serde_yaml::Value as YamlValue;

use crate::bootstrap::AppError;
use crate::credentials::oauth::{
    OAuthClientMaterialPersistence, OAuthCredentialMaterial, OAuthCredentialService,
    OAuthProgressEventSender, StartOAuthCredentialRequest, material_key_belongs_to_input,
};
use crate::credentials::{
    CORAL_INTERNAL_KEY_PREFIX, CredentialManager, CredentialMaterialGuard,
    CredentialMaterialSnapshot, CredentialSetId, CredentialStorageKind, CredentialsError,
};
use crate::source_registry::{
    SourceRegistry, installed_source_from_record, record_from_installed_source,
};
use crate::sources::SourceName;
use crate::sources::catalog::{
    describe_manifest, list_bundled_sources, load_bundled_source, resolve_installed_manifest,
};
use crate::sources::materialization::{
    MaterializationBuild, MaterializationInputs, build_v4_materialization_tmp,
    canonicalize_file_descriptor, cleanup_materialization_backup, cleanup_materialization_tmp,
    new_materialization_suffix, replace_v4_materialization, restore_materialization_backup,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::state::AppStateLayout;
#[cfg(test)]
use crate::state::ConfigStore;
use crate::storage::fs;
use crate::workspaces::WorkspaceName;
use coral_spec::{ManifestCredentialMethodKind, ManifestInputKind, ManifestOAuthCredentialSpec};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};
use tracing::warn;
use uuid::Uuid;

use crate::features::Features;
use crate::identity::{SourceIdentityBinding, SourceIdentityOwner};

#[derive(Clone)]
pub(crate) struct SourceManager {
    source_registry: Arc<dyn SourceRegistry>,
    credential_manager: CredentialManager,
    oauth_credential_service: OAuthCredentialService,
    layout: AppStateLayout,
    features: Features,
}

pub(crate) struct CreateBundledSourceCommand {
    pub(crate) name: SourceName,
    pub(crate) bindings: SourceBindings,
}

pub(crate) struct CreateBundledSourceWithOAuthCommand {
    pub(crate) name: SourceName,
    pub(crate) bindings: SourceBindings,
    pub(crate) oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
}

pub(crate) struct ImportSourceCommand {
    pub(crate) manifest_yaml: String,
    pub(crate) bindings: SourceBindings,
    pub(crate) identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    pub(crate) replace_identity_bindings: bool,
}

pub(crate) struct ImportSourceWithCredentialsCommand {
    pub(crate) manifest_yaml: String,
    pub(crate) bindings: SourceBindings,
    pub(crate) oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
    pub(crate) identity_bindings: BTreeMap<String, SourceIdentityBinding>,
    pub(crate) replace_identity_bindings: bool,
}

#[derive(Default)]
pub(crate) struct SourceBindings {
    pub(crate) variables: Vec<SourceBinding>,
    pub(crate) secrets: Vec<SourceBinding>,
}

pub(crate) struct SourceBinding {
    pub(crate) key: String,
    pub(crate) value: String,
}

pub(crate) struct SourceOAuthCredentialRetrieval {
    pub(crate) input_key: String,
    pub(crate) method_index: usize,
    pub(crate) credential_inputs: Vec<SourceBinding>,
}

struct SourceCredentialOAuthConfig<'a> {
    input_key: &'a str,
    oauth: &'a ManifestOAuthCredentialSpec,
}

struct ValidatedBindings {
    variables: BTreeMap<String, String>,
    secrets: BTreeMap<String, String>,
    replaced_oauth_inputs: BTreeSet<String>,
}

#[derive(Clone, Copy)]
struct InstallSourceRequest<'a> {
    candidate: &'a CandidateSource,
    bindings: &'a SourceBindings,
    identity_bindings: &'a BTreeMap<String, SourceIdentityBinding>,
    replace_identity_bindings: bool,
    manifest_yaml: Option<&'a str>,
    materialization_manifest_yaml: &'a str,
    origin: SourceOrigin,
}

struct PersistSourceRequest<'a> {
    candidate: &'a CandidateSource,
    manifest_yaml: Option<&'a str>,
    bindings: ValidatedBindings,
    identity_bindings: &'a BTreeMap<String, SourceIdentityBinding>,
    origin: SourceOrigin,
    credential_storage: Option<CredentialStorageKind>,
    materialization_tmp: Option<PathBuf>,
}

struct SourceRollbackState {
    source: InstalledSource,
    manifest_yaml: Option<String>,
    credential_material: Option<CredentialMaterialSnapshot>,
}

fn materialization_inputs_from_bindings(
    bindings: &ValidatedBindings,
    stored_material: &BTreeMap<String, String>,
) -> MaterializationInputs {
    let mut secrets = stored_material.clone();
    secrets.extend(bindings.secrets.clone());
    MaterializationInputs {
        variables: bindings.variables.clone(),
        secrets,
    }
}

impl SourceManager {
    #[cfg(test)]
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
    ) -> Self {
        Self::new_with_features(
            config_store,
            credential_manager,
            layout,
            Features::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new_with_features(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
        features: Features,
    ) -> Self {
        Self::new_with_features_and_source_registry(
            Arc::new(config_store),
            credential_manager,
            layout,
            features,
        )
    }

    pub(crate) fn new_with_features_and_source_registry(
        source_registry: Arc<dyn SourceRegistry>,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
        features: Features,
    ) -> Self {
        Self {
            source_registry,
            credential_manager,
            oauth_credential_service: OAuthCredentialService::new(),
            layout,
            features,
        }
    }

    fn list_registry_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        self.source_registry
            .list_workspace_sources(workspace_name.as_str())?
            .into_iter()
            .map(|record| installed_source_from_record(workspace_name, record))
            .collect()
    }

    fn get_registry_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<Option<InstalledSource>, AppError> {
        self.source_registry
            .get_source(workspace_name.as_str(), source_name.as_str())?
            .map(|record| installed_source_from_record(workspace_name, record))
            .transpose()
    }

    fn require_registry_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        self.get_registry_source(workspace_name, source_name)?
            .ok_or_else(|| AppError::SourceNotFound(format!("{workspace_name}:{source_name}")))
    }

    pub(crate) fn list_workspace_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        Ok(self
            .list_registry_sources(workspace_name)?
            .into_iter()
            .map(|source| self.populate_source_version_or_keep(workspace_name, source))
            .collect())
    }

    pub(crate) fn get_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        Ok(self.populate_source_version_or_keep(
            workspace_name,
            self.require_registry_source(workspace_name, source_name)?,
        ))
    }

    pub(crate) fn get_source_info(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<CandidateSource, AppError> {
        match self.get_registry_source(workspace_name, source_name) {
            Ok(Some(source)) => {
                return Ok(
                    resolve_installed_manifest(workspace_name, &source, &self.layout)?.candidate,
                );
            }
            Ok(None) => {}
            Err(error) => return Err(error),
        }

        match load_bundled_source(source_name) {
            Ok(bundled) => self.describe_bundled_source(workspace_name, &bundled.manifest_yaml),
            Err(AppError::InvalidInput(_)) => {
                Err(AppError::SourceNotFound(source_name.to_string()))
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) fn discover_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<CandidateSource>, AppError> {
        let installed_sources = self.list_registry_sources(workspace_name)?;
        let installed = installed_sources
            .iter()
            .map(|source| source.name.clone())
            .collect::<BTreeSet<_>>();
        let installed_storage = installed_sources
            .iter()
            .filter_map(|source| {
                source
                    .credential_storage_for_material()
                    .map(|storage| (source.name.clone(), storage))
            })
            .collect::<BTreeMap<_, _>>();
        let mut candidates = list_bundled_sources(&installed)?;
        for candidate in &mut candidates {
            if let Some(storage) = installed_storage.get(&candidate.name) {
                candidate.credential_storage = Some(*storage);
            }
        }

        Ok(candidates)
    }

    pub(crate) fn create_bundled_source(
        &self,
        workspace_name: &WorkspaceName,
        command: &CreateBundledSourceCommand,
    ) -> Result<InstalledSource, AppError> {
        let bundled = load_bundled_source(&command.name)?;
        let candidate = self.describe_bundled_source(workspace_name, &bundled.manifest_yaml)?;
        self.install_validated_source(
            workspace_name,
            InstallSourceRequest {
                candidate: &candidate,
                bindings: &command.bindings,
                identity_bindings: &BTreeMap::new(),
                replace_identity_bindings: false,
                manifest_yaml: None,
                materialization_manifest_yaml: &bundled.manifest_yaml,
                origin: SourceOrigin::Bundled,
            },
        )
    }

    pub(crate) async fn create_bundled_source_with_oauth(
        &self,
        workspace_name: &WorkspaceName,
        command: CreateBundledSourceWithOAuthCommand,
        events: OAuthProgressEventSender,
    ) -> Result<InstalledSource, AppError> {
        let bundled = load_bundled_source(&command.name)?;
        let candidate = self.describe_bundled_source(workspace_name, &bundled.manifest_yaml)?;
        self.install_source_with_oauth(
            workspace_name,
            InstallSourceRequest {
                candidate: &candidate,
                bindings: &command.bindings,
                identity_bindings: &BTreeMap::new(),
                replace_identity_bindings: false,
                manifest_yaml: None,
                materialization_manifest_yaml: &bundled.manifest_yaml,
                origin: SourceOrigin::Bundled,
            },
            command.oauth_credential_retrievals,
            events,
        )
        .await
    }

    pub(crate) fn import_source(
        &self,
        workspace_name: &WorkspaceName,
        command: &ImportSourceCommand,
    ) -> Result<InstalledSource, AppError> {
        let manifest = parse_source_manifest_yaml(&command.manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let manifest_yaml = durable_import_manifest_yaml(&command.manifest_yaml, &manifest)?;
        let mut candidate = describe_manifest(&manifest_yaml, SourceOrigin::Imported, false)?;
        candidate.installed = self.source_exists(workspace_name, &candidate.name)?;
        self.install_validated_source(
            workspace_name,
            InstallSourceRequest {
                candidate: &candidate,
                bindings: &command.bindings,
                identity_bindings: &command.identity_bindings,
                replace_identity_bindings: command.replace_identity_bindings,
                manifest_yaml: Some(&manifest_yaml),
                materialization_manifest_yaml: &manifest_yaml,
                origin: SourceOrigin::Imported,
            },
        )
    }

    pub(crate) async fn import_source_with_credentials(
        &self,
        workspace_name: &WorkspaceName,
        command: ImportSourceWithCredentialsCommand,
        events: OAuthProgressEventSender,
    ) -> Result<InstalledSource, AppError> {
        let manifest = parse_source_manifest_yaml(&command.manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let manifest_yaml = durable_import_manifest_yaml(&command.manifest_yaml, &manifest)?;
        let mut candidate = describe_manifest(&manifest_yaml, SourceOrigin::Imported, false)?;
        candidate.installed = self.source_exists(workspace_name, &candidate.name)?;
        self.install_source_with_oauth(
            workspace_name,
            InstallSourceRequest {
                candidate: &candidate,
                bindings: &command.bindings,
                identity_bindings: &command.identity_bindings,
                replace_identity_bindings: command.replace_identity_bindings,
                manifest_yaml: Some(&manifest_yaml),
                materialization_manifest_yaml: &manifest_yaml,
                origin: SourceOrigin::Imported,
            },
            command.oauth_credential_retrievals,
            events,
        )
        .await
    }

    /// Validates `bindings` against any stored credential material and persists
    /// the source. Shared tail of the non-OAuth install entry points; the
    /// caller supplies the resolved `candidate` plus the per-origin
    /// `manifest_yaml`/`origin`.
    fn install_validated_source(
        &self,
        workspace_name: &WorkspaceName,
        request: InstallSourceRequest<'_>,
    ) -> Result<InstalledSource, AppError> {
        let manifest = parse_source_manifest_yaml(request.materialization_manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        self.ensure_dsl_v4_feature_enabled(&manifest)?;
        self.validate_runtime_schema_names_available(
            workspace_name,
            &request.candidate.name,
            &manifest,
        )?;
        let identity_bindings = self.effective_source_identity_bindings(
            workspace_name,
            &request.candidate.name,
            request.identity_bindings,
            request.replace_identity_bindings,
        )?;
        validate_import_identity_bindings(&manifest, &identity_bindings)?;
        let stored_material = self.source_stored_material_for_validation(
            workspace_name,
            request.candidate,
            request.bindings,
            &BTreeSet::new(),
        )?;
        let bindings = validate_bindings(request.candidate, request.bindings, &stored_material)?;
        let materialization_inputs =
            materialization_inputs_from_bindings(&bindings, &stored_material);
        let credential_storage = self.source_persist_storage(
            workspace_name,
            &request.candidate.name,
            &bindings,
            !stored_material.is_empty(),
        )?;
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate: request.candidate,
                manifest_yaml: request.manifest_yaml,
                bindings,
                identity_bindings: &identity_bindings,
                origin: request.origin,
                credential_storage,
                materialization_tmp: self
                    .prepare_v4_materialization(
                        workspace_name,
                        request.candidate,
                        &manifest,
                        request.materialization_manifest_yaml,
                        &materialization_inputs,
                        request.origin,
                        "tmp",
                    )?
                    .map(|build| build.temp_dir),
            },
        )
    }

    /// Resolves OAuth credential material (driving the authorization flow over
    /// `events`), then validates and persists the source. Shared tail of the
    /// OAuth install entry points; the caller supplies the resolved `candidate`
    /// plus the per-origin `manifest_yaml`/`origin`.
    async fn install_source_with_oauth(
        &self,
        workspace_name: &WorkspaceName,
        request: InstallSourceRequest<'_>,
        oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
        events: OAuthProgressEventSender,
    ) -> Result<InstalledSource, AppError> {
        let manifest = parse_source_manifest_yaml(request.materialization_manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        self.ensure_dsl_v4_feature_enabled(&manifest)?;
        self.validate_runtime_schema_names_available(
            workspace_name,
            &request.candidate.name,
            &manifest,
        )?;
        let identity_bindings = self.effective_source_identity_bindings(
            workspace_name,
            &request.candidate.name,
            request.identity_bindings,
            request.replace_identity_bindings,
        )?;
        validate_import_identity_bindings(&manifest, &identity_bindings)?;
        let oauth_input_keys = oauth_credential_retrievals
            .iter()
            .map(|credential| credential.input_key.clone())
            .collect::<BTreeSet<_>>();
        let stored_material = self.source_stored_material_for_validation(
            workspace_name,
            request.candidate,
            request.bindings,
            &oauth_input_keys,
        )?;
        let has_stored_material = !stored_material.is_empty();
        let stored_material_for_materialization = stored_material.clone();
        let bindings = self
            .bindings_with_oauth_material(
                request.candidate,
                request.bindings,
                stored_material,
                oauth_credential_retrievals,
                events,
            )
            .await?;
        let materialization_inputs =
            materialization_inputs_from_bindings(&bindings, &stored_material_for_materialization);
        let credential_storage = self.source_persist_storage(
            workspace_name,
            &request.candidate.name,
            &bindings,
            has_stored_material,
        )?;
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate: request.candidate,
                manifest_yaml: request.manifest_yaml,
                bindings,
                identity_bindings: &identity_bindings,
                origin: request.origin,
                credential_storage,
                materialization_tmp: self
                    .prepare_v4_materialization(
                        workspace_name,
                        request.candidate,
                        &manifest,
                        request.materialization_manifest_yaml,
                        &materialization_inputs,
                        request.origin,
                        "tmp",
                    )?
                    .map(|build| build.temp_dir),
            },
        )
    }

    pub(crate) fn delete_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        let stored = self.require_registry_source(workspace_name, source_name)?;
        let removed = self.populate_source_version_or_keep(workspace_name, stored.clone());
        let source_dir = self.layout.source_dir(workspace_name, source_name);
        let credential_set_id = CredentialSetId::for_source(source_name);
        let credential_guard = self
            .credential_manager
            .material_guard(workspace_name, &credential_set_id)?;
        let credential_storage = stored.credential_storage_for_material();
        let credential_material = credential_storage
            .map(|storage| credential_guard.snapshot_material(storage))
            .transpose()?;
        let previous = SourceRollbackState {
            source: stored,
            manifest_yaml: match removed.origin {
                SourceOrigin::Bundled => None,
                SourceOrigin::Imported => Some(std::fs::read_to_string(
                    self.layout.manifest_file(workspace_name, source_name),
                )?),
            },
            credential_material,
        };
        if let Some(credential_storage) = credential_storage
            && let Err(error) = credential_guard.remove_material(credential_storage)
        {
            self.restore_source_rollback_state(
                workspace_name,
                source_name,
                Some(previous),
                None,
                &credential_guard,
            );
            return Err(error);
        }
        let source_dir_backup =
            source_dir.with_file_name(format!("{source_name}.delete.rollback.{}", Uuid::new_v4()));
        let had_source_dir = source_dir.exists();
        if had_source_dir {
            if source_dir_backup.exists() {
                std::fs::remove_dir_all(&source_dir_backup)?;
            }
            if let Err(error) = std::fs::rename(&source_dir, &source_dir_backup) {
                self.restore_source_rollback_state(
                    workspace_name,
                    source_name,
                    Some(previous),
                    None,
                    &credential_guard,
                );
                return Err(error.into());
            }
        }
        if let Err(error) = self
            .source_registry
            .remove_source(workspace_name.as_str(), source_name.as_str())
        {
            if had_source_dir
                && source_dir_backup.exists()
                && let Err(restore_error) = std::fs::rename(&source_dir_backup, &source_dir)
            {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to remove source '{source_name}': {error}; failed to restore source directory from '{}': {restore_error}",
                    source_dir_backup.display()
                )));
            }
            self.restore_source_rollback_state(
                workspace_name,
                source_name,
                Some(previous),
                None,
                &credential_guard,
            );
            return Err(error);
        }
        if source_dir_backup.exists() {
            std::fs::remove_dir_all(&source_dir_backup)?;
        }
        cleanup_empty_parent(&self.layout.workspaces_root(), source_dir.parent());
        cleanup_empty_parent(
            &self.layout.workspaces_root(),
            self.layout.workspace_dir(workspace_name).parent(),
        );
        Ok(removed)
    }

    fn describe_bundled_source(
        &self,
        workspace_name: &WorkspaceName,
        manifest_yaml: &str,
    ) -> Result<CandidateSource, AppError> {
        let mut candidate = describe_manifest(manifest_yaml, SourceOrigin::Bundled, false)?;
        candidate.installed = self.source_exists(workspace_name, &candidate.name)?;
        Ok(candidate)
    }

    #[expect(
        clippy::too_many_lines,
        reason = "Source persistence keeps rollback steps together so failure ordering is visible."
    )]
    fn persist_source(
        &self,
        workspace_name: &WorkspaceName,
        request: PersistSourceRequest<'_>,
    ) -> Result<InstalledSource, AppError> {
        let source_name = request.candidate.name.clone();
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let credential_guard = self
            .credential_manager
            .material_guard(workspace_name, &credential_set_id)?;
        let previous =
            self.load_source_rollback_state(workspace_name, &source_name, &credential_guard)?;
        if let Err(error) =
            self.persist_manifest_artifact(workspace_name, &source_name, request.manifest_yaml)
        {
            cleanup_materialization_tmp(request.materialization_tmp.as_deref());
            self.restore_source_rollback_state(
                workspace_name,
                &source_name,
                previous,
                None,
                &credential_guard,
            );
            return Err(error);
        }

        let ValidatedBindings {
            variables,
            secrets,
            replaced_oauth_inputs,
        } = request.bindings;
        let (visible_secret_keys, credential_storage) =
            if let Some(requested_storage) = request.credential_storage {
                let expected_secret_keys = request
                    .candidate
                    .inputs
                    .iter()
                    .filter(|input| input.kind == ManifestInputKind::Secret)
                    .map(|input| input.key.clone())
                    .collect::<BTreeSet<_>>();
                let credential_write = match credential_guard.update_material_or_empty_on_parse(
                    requested_storage,
                    |mut credential_material| {
                        credential_material.retain(|key, _| {
                            material_key_belongs_to_source_secret(key, &expected_secret_keys)
                        });
                        for input_key in &replaced_oauth_inputs {
                            credential_material
                                .retain(|key, _| !material_key_belongs_to_input(key, input_key));
                        }
                        credential_material.extend(secrets.clone());
                        Ok(credential_material)
                    },
                ) {
                    Ok(outcome) => outcome,
                    Err(error) => {
                        cleanup_materialization_tmp(request.materialization_tmp.as_deref());
                        self.restore_source_rollback_state(
                            workspace_name,
                            &source_name,
                            previous,
                            Some(requested_storage),
                            &credential_guard,
                        );
                        return Err(error);
                    }
                };
                let credential_storage = if credential_write.visible_keys.is_empty() {
                    None
                } else {
                    Some(credential_write.storage)
                };
                (credential_write.visible_keys, credential_storage)
            } else {
                (Vec::new(), None)
            };

        let materialization_backup =
            if let Some(materialization_tmp) = request.materialization_tmp.as_ref() {
                match replace_v4_materialization(
                    &self.layout,
                    workspace_name,
                    &source_name,
                    materialization_tmp,
                ) {
                    Ok(backup) => backup,
                    Err(error) => {
                        cleanup_materialization_tmp(request.materialization_tmp.as_deref());
                        self.restore_source_rollback_state(
                            workspace_name,
                            &source_name,
                            previous,
                            credential_storage,
                            &credential_guard,
                        );
                        return Err(error);
                    }
                }
            } else {
                None
            };

        let persisted_version = match request.origin {
            SourceOrigin::Bundled => None,
            SourceOrigin::Imported => request.candidate.version.clone(),
        };
        let stored = InstalledSource {
            name: source_name.clone(),
            version: persisted_version,
            variables,
            secrets: visible_secret_keys,
            credential_storage,
            identity_bindings: request.identity_bindings.clone(),
            origin: request.origin,
        };
        let mut record = record_from_installed_source(workspace_name, stored.clone());
        record.manifest_yaml = request.manifest_yaml.map(ToString::to_string);
        if let Err(error) = self.source_registry.upsert_source(record) {
            let restore_result = restore_materialization_backup(
                &self.layout,
                workspace_name,
                &source_name,
                materialization_backup,
            );
            self.restore_source_rollback_state(
                workspace_name,
                &source_name,
                previous,
                credential_storage,
                &credential_guard,
            );
            if let Err(restore_error) = restore_result {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to persist source '{source_name}': {error}; failed to restore previous DSL v4 materialization: {restore_error}"
                )));
            }
            return Err(error);
        }
        cleanup_materialization_backup(materialization_backup);
        let mut resolved = stored;
        resolved.version.clone_from(&request.candidate.version);
        Ok(resolved)
    }

    fn prepare_v4_materialization(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        manifest: &ValidatedSourceManifest,
        manifest_yaml: &str,
        inputs: &MaterializationInputs,
        origin: SourceOrigin,
        suffix_prefix: &str,
    ) -> Result<Option<MaterializationBuild>, AppError> {
        let Some(v4) = manifest.as_v4() else {
            return Ok(None);
        };
        self.features.ensure_dsl_v4_enabled()?;
        if matches!(origin, SourceOrigin::Bundled)
            && v4.surfaces.iter().any(|surface| {
                matches!(
                    surface.descriptor,
                    coral_spec::v4::SurfaceDescriptor::File { .. }
                )
            })
        {
            return Err(AppError::FailedPrecondition(format!(
                "bundled source '{}' uses local DSL v4 file descriptors, which are development-only",
                v4.common.name
            )));
        }
        build_v4_materialization_tmp(
            &self.layout,
            workspace_name,
            &candidate.name,
            manifest_yaml,
            v4,
            inputs,
            &new_materialization_suffix(suffix_prefix),
        )
        .map(Some)
    }

    fn validate_runtime_schema_names_available(
        &self,
        workspace_name: &WorkspaceName,
        candidate_name: &SourceName,
        candidate_manifest: &ValidatedSourceManifest,
    ) -> Result<(), AppError> {
        let candidate_schema_names = runtime_schema_names(&candidate_manifest);
        for installed in self.list_registry_sources(workspace_name)? {
            if installed.name == *candidate_name {
                continue;
            }
            let installed_manifest =
                resolve_installed_manifest(workspace_name, &installed, &self.layout)?;
            let installed_schema_names = runtime_schema_names(&installed_manifest.source_spec);
            if let Some(schema_name) = candidate_schema_names
                .intersection(&installed_schema_names)
                .next()
            {
                return Err(AppError::InvalidInput(format!(
                    "source '{candidate_name}' runtime schema name '{schema_name}' conflicts with installed source '{}'",
                    installed.name
                )));
            }
        }
        Ok(())
    }

    fn ensure_dsl_v4_feature_enabled(
        &self,
        manifest: &ValidatedSourceManifest,
    ) -> Result<(), AppError> {
        if manifest.as_v4().is_some() {
            self.features.ensure_dsl_v4_enabled()?;
        }
        Ok(())
    }

    fn source_exists(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<bool, AppError> {
        self.get_registry_source(workspace_name, source_name)
            .map(|source| source.is_some())
    }

    pub(crate) fn effective_source_identity_bindings_for_import(
        &self,
        workspace_name: &WorkspaceName,
        manifest_yaml: &str,
        requested: &BTreeMap<String, SourceIdentityBinding>,
        replace_identity_bindings: bool,
    ) -> Result<BTreeMap<String, SourceIdentityBinding>, AppError> {
        let manifest = parse_source_manifest_yaml(manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let source_name = SourceName::parse(manifest.schema_name())?;
        self.effective_source_identity_bindings(
            workspace_name,
            &source_name,
            requested,
            replace_identity_bindings,
        )
    }

    fn effective_source_identity_bindings(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        requested: &BTreeMap<String, SourceIdentityBinding>,
        replace_identity_bindings: bool,
    ) -> Result<BTreeMap<String, SourceIdentityBinding>, AppError> {
        if replace_identity_bindings || !requested.is_empty() {
            return Ok(requested.clone());
        }
        self.get_registry_source(workspace_name, source_name)
            .map(|source| {
                source
                    .map(|source| source.identity_bindings)
                    .unwrap_or_default()
            })
    }

    fn read_source_material(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        credential_storage: CredentialStorageKind,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let credential_set_id = CredentialSetId::for_source(source_name);
        match self.credential_manager.read_material(
            workspace_name,
            &credential_set_id,
            credential_storage,
        ) {
            Ok(material) => Ok(material),
            Err(AppError::Credentials(CredentialsError::Parse(_)))
                if credential_storage == CredentialStorageKind::File =>
            {
                Ok(BTreeMap::new())
            }
            Err(error) => Err(error),
        }
    }

    fn source_stored_material_for_validation(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        filled_secret_keys: &BTreeSet<String>,
    ) -> Result<BTreeMap<String, String>, AppError> {
        let (credential_storage, persisted_secret_keys) =
            match self.get_registry_source(workspace_name, &candidate.name)? {
                Some(source) => (
                    source.credential_storage_for_material(),
                    Some(source.secrets.iter().cloned().collect::<BTreeSet<_>>()),
                ),
                None if self
                    .layout
                    .secret_file(workspace_name, &candidate.name)
                    .exists() =>
                {
                    (Some(CredentialStorageKind::File), None)
                }
                None => (None, Some(BTreeSet::new())),
            };

        if !source_needs_stored_material_for_validation(
            candidate,
            bindings,
            filled_secret_keys,
            persisted_secret_keys.as_ref(),
        )? {
            return Ok(BTreeMap::new());
        }

        match credential_storage {
            Some(credential_storage) => {
                self.read_source_material(workspace_name, &candidate.name, credential_storage)
            }
            None => Ok(BTreeMap::new()),
        }
    }

    fn source_persist_storage(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        bindings: &ValidatedBindings,
        has_stored_material: bool,
    ) -> Result<Option<CredentialStorageKind>, AppError> {
        match self.get_registry_source(workspace_name, source_name)? {
            Some(source) if !source.secrets.is_empty() => {
                Ok(Some(source.effective_credential_storage()))
            }
            Some(_) | None if bindings.secrets.is_empty() && !has_stored_material => Ok(None),
            Some(_) | None => self.credential_manager.default_write_storage().map(Some),
        }
    }

    fn validate_oauth_import_preflight(
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        stored_material: &BTreeMap<String, String>,
        oauth_credential_retrievals: &[SourceOAuthCredentialRetrieval],
    ) -> Result<ValidatedBindings, AppError> {
        let mut seen = BTreeSet::new();
        let mut validation_material = stored_material.clone();
        for retrieval in oauth_credential_retrievals {
            if !seen.insert(retrieval.input_key.clone()) {
                return Err(AppError::InvalidInput(format!(
                    "OAuth credential retrieval for source input '{}' is repeated",
                    retrieval.input_key
                )));
            }
            let config =
                source_oauth_config(candidate, &retrieval.input_key, retrieval.method_index)?;
            validation_material.insert(config.input_key.to_string(), String::new());
        }

        let bindings = validate_bindings(candidate, bindings, &validation_material)?;
        for retrieval in oauth_credential_retrievals {
            let config =
                source_oauth_config(candidate, &retrieval.input_key, retrieval.method_index)?;
            let credential_inputs = retrieval
                .credential_inputs
                .iter()
                .map(|input| (input.key.clone(), input.value.clone()))
                .collect();
            OAuthCredentialService::validate_credential_inputs(
                config.oauth,
                &bindings.variables,
                credential_inputs,
            )?;
        }
        for input_key in seen {
            if bindings.secrets.contains_key(&input_key) {
                return Err(AppError::InvalidInput(format!(
                    "source secret '{input_key}' was provided by both source config and OAuth"
                )));
            }
        }
        Ok(bindings)
    }

    async fn retrieve_oauth_material(
        &self,
        candidate: &CandidateSource,
        source_inputs: &BTreeMap<String, String>,
        oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
        events: OAuthProgressEventSender,
    ) -> Result<Vec<OAuthCredentialMaterial>, AppError> {
        let mut seen = BTreeSet::new();
        let mut materials = Vec::new();
        for retrieval in oauth_credential_retrievals {
            if !seen.insert(retrieval.input_key.clone()) {
                return Err(AppError::InvalidInput(format!(
                    "OAuth credential retrieval for source input '{}' is repeated",
                    retrieval.input_key
                )));
            }
            let config =
                source_oauth_config(candidate, &retrieval.input_key, retrieval.method_index)?;
            let input_key = config.input_key.to_string();
            let credential_inputs = retrieval
                .credential_inputs
                .into_iter()
                .map(|input| (input.key, input.value))
                .collect();
            let material = self
                .oauth_credential_service
                .authorize_with_progress(
                    StartOAuthCredentialRequest {
                        input_key: &input_key,
                        oauth: config.oauth,
                        source_inputs,
                        credential_inputs,
                        client_material_persistence: OAuthClientMaterialPersistence::All,
                    },
                    input_key.clone(),
                    &events,
                )
                .await?;
            materials.push(material);
        }
        Ok(materials)
    }

    async fn bindings_with_oauth_material(
        &self,
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        stored_material: BTreeMap<String, String>,
        oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
        events: OAuthProgressEventSender,
    ) -> Result<ValidatedBindings, AppError> {
        let preflight_bindings = Self::validate_oauth_import_preflight(
            candidate,
            bindings,
            &stored_material,
            &oauth_credential_retrievals,
        )?;
        let oauth_material = self
            .retrieve_oauth_material(
                candidate,
                &preflight_bindings.variables,
                oauth_credential_retrievals,
                events,
            )
            .await?;
        let mut validation_material = stored_material;
        for material in &oauth_material {
            validation_material.insert(material.input_key.clone(), material.access_token.clone());
        }
        let mut bindings = validate_bindings(candidate, bindings, &validation_material)?;
        merge_oauth_material_into_bindings(&mut bindings, oauth_material)?;
        Ok(bindings)
    }

    fn load_source_rollback_state(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        credential_material: &CredentialMaterialGuard<'_>,
    ) -> Result<Option<SourceRollbackState>, AppError> {
        let Some(source) = self.get_registry_source(workspace_name, source_name)? else {
            return Ok(None);
        };
        let credential_material = source
            .credential_storage_for_material()
            .map(|credential_storage| credential_material.snapshot_material(credential_storage))
            .transpose()?;
        Ok(Some(SourceRollbackState {
            manifest_yaml: match source.origin {
                SourceOrigin::Bundled => None,
                SourceOrigin::Imported => Some(std::fs::read_to_string(
                    self.layout.manifest_file(workspace_name, source_name),
                )?),
            },
            source,
            credential_material,
        }))
    }

    fn restore_source_rollback_state(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        previous: Option<SourceRollbackState>,
        new_material_storage: Option<CredentialStorageKind>,
        credential_material: &CredentialMaterialGuard<'_>,
    ) {
        if let Some(previous) = previous {
            let manifest_path = self.layout.manifest_file(workspace_name, source_name);
            let previous_manifest_yaml = previous.manifest_yaml.clone();
            match previous.manifest_yaml {
                Some(manifest_yaml) => {
                    if let Some(parent) = manifest_path.parent()
                        && let Err(e) = fs::ensure_dir(parent)
                    {
                        warn!("rollback: failed to create manifest parent dir: {e}");
                    }
                    if let Err(e) = fs::write_atomic(&manifest_path, manifest_yaml.as_bytes()) {
                        warn!("rollback: failed to restore manifest file: {e}");
                    }
                }
                None if manifest_path.exists() => {
                    if let Err(e) = std::fs::remove_file(&manifest_path) {
                        warn!("rollback: failed to remove manifest file: {e}");
                    }
                }
                None => {}
            }
            match previous.credential_material {
                Some(snapshot) => {
                    if let Err(e) = credential_material.restore_material(&snapshot) {
                        warn!("rollback: failed to restore source credential material: {e}");
                    }
                }
                None => {
                    if let Some(storage) = new_material_storage
                        && let Err(e) = credential_material.remove_material(storage)
                    {
                        warn!("rollback: failed to remove new source credential material: {e}");
                    }
                }
            }
            let mut record = record_from_installed_source(workspace_name, previous.source);
            record.manifest_yaml = previous_manifest_yaml;
            if let Err(e) = self.source_registry.upsert_source(record) {
                warn!("rollback: failed to restore source config: {e}");
            }
        } else {
            let source_dir = self.layout.source_dir(workspace_name, source_name);
            if source_dir.exists()
                && let Err(e) = std::fs::remove_dir_all(&source_dir)
            {
                warn!("rollback: failed to remove source directory: {e}");
            }
            if let Some(storage) = new_material_storage
                && let Err(e) = credential_material.remove_material(storage)
            {
                warn!("rollback: failed to remove source credential material: {e}");
            }
        }
    }

    fn persist_manifest_artifact(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        manifest_yaml: Option<&str>,
    ) -> Result<(), AppError> {
        let manifest_path = self.layout.manifest_file(workspace_name, source_name);
        match manifest_yaml {
            Some(manifest_yaml) => {
                if let Some(parent) = manifest_path.parent() {
                    fs::ensure_dir(parent)?;
                }
                fs::write_atomic(&manifest_path, manifest_yaml.as_bytes())?;
            }
            None if manifest_path.exists() => {
                std::fs::remove_file(&manifest_path)?;
            }
            None => {}
        }
        cleanup_empty_parent(&self.layout.workspaces_root(), manifest_path.parent());
        Ok(())
    }

    fn populate_source_version(
        &self,
        workspace_name: &WorkspaceName,
        mut source: InstalledSource,
    ) -> Result<InstalledSource, AppError> {
        source.version = resolve_installed_manifest(workspace_name, &source, &self.layout)?
            .candidate
            .version;
        Ok(source)
    }

    fn populate_source_version_or_keep(
        &self,
        workspace_name: &WorkspaceName,
        source: InstalledSource,
    ) -> InstalledSource {
        self.populate_source_version(workspace_name, source.clone())
            .unwrap_or(source)
    }
}

fn validate_bindings(
    candidate: &CandidateSource,
    bindings: &SourceBindings,
    stored_material: &BTreeMap<String, String>,
) -> Result<ValidatedBindings, AppError> {
    let mut variable_values = collect_unique_variables(&bindings.variables)?;
    let secret_values = collect_unique_secrets(&bindings.secrets)?;
    let expected_variables = candidate
        .inputs
        .iter()
        .filter(|input| input.kind == ManifestInputKind::Variable)
        .map(|input| input.key.clone())
        .collect::<BTreeSet<_>>();
    let expected_secrets = candidate
        .inputs
        .iter()
        .filter(|input| input.kind == ManifestInputKind::Secret)
        .map(|input| input.key.clone())
        .collect::<BTreeSet<_>>();

    for key in variable_values.keys() {
        if !expected_variables.contains(key) {
            return Err(AppError::InvalidInput(format!(
                "unknown source variable '{key}'"
            )));
        }
    }
    for key in secret_values.keys() {
        if !expected_secrets.contains(key) {
            return Err(AppError::InvalidInput(format!(
                "unknown source secret '{key}'"
            )));
        }
    }

    for input in &candidate.inputs {
        if input.kind == ManifestInputKind::Variable
            && !variable_values.contains_key(&input.key)
            && !input.default_value.is_empty()
        {
            variable_values.insert(input.key.clone(), input.default_value.clone());
        }
    }

    for input in &candidate.inputs {
        match input.kind {
            ManifestInputKind::Variable
                if input.required && !variable_values.contains_key(&input.key) =>
            {
                return Err(AppError::InvalidInput(format!(
                    "missing required source variable '{}'",
                    input.key
                )));
            }
            ManifestInputKind::Secret
                if input.required
                    && !secret_values.contains_key(&input.key)
                    && !stored_material.contains_key(&input.key) =>
            {
                return Err(AppError::InvalidInput(format!(
                    "missing required source secret '{}'",
                    input.key
                )));
            }
            _ => {}
        }
    }

    Ok(ValidatedBindings {
        variables: variable_values,
        replaced_oauth_inputs: secret_values.keys().cloned().collect(),
        secrets: secret_values,
    })
}

fn validate_import_identity_bindings(
    manifest: &ValidatedSourceManifest,
    bindings: &BTreeMap<String, SourceIdentityBinding>,
) -> Result<(), AppError> {
    let Some(v4) = manifest.as_v4() else {
        if bindings.is_empty() {
            return Ok(());
        }
        return Err(AppError::InvalidInput(
            "source identity bindings can only be configured for DSL v4 sources".to_string(),
        ));
    };
    for surface in v4
        .surfaces
        .iter()
        .filter(|surface| surface.identity_requirements.is_some())
    {
        if !bindings.contains_key(&surface.id) {
            return Err(AppError::InvalidInput(format!(
                "source '{}' surface '{}' declares identity_requirements but no identity binding was provided",
                manifest.schema_name(),
                surface.id
            )));
        }
    }
    for (surface_id, binding) in bindings {
        binding.validate()?;
        let surface = v4.surface(surface_id).ok_or_else(|| {
            AppError::InvalidInput(format!(
                "source '{}' identity binding targets unknown surface '{surface_id}'",
                manifest.schema_name()
            ))
        })?;
        let requirements = surface.identity_requirements.as_ref().ok_or_else(|| {
            AppError::InvalidInput(format!(
                "source '{}' surface '{surface_id}' does not declare identity_requirements",
                manifest.schema_name()
            ))
        })?;
        if binding.owner == SourceIdentityOwner::Workspace {
            match binding.accepted_identity.as_deref() {
                Some(accepted_identity)
                    if requirements
                        .accepts
                        .iter()
                        .any(|accepted| accepted.id == accepted_identity) => {}
                Some(accepted_identity) => {
                    return Err(AppError::InvalidInput(format!(
                        "source '{}' surface '{surface_id}' identity binding references unknown accepted_identity '{accepted_identity}'",
                        manifest.schema_name()
                    )));
                }
                None if requirements.accepts.len() > 1 => {
                    return Err(AppError::InvalidInput(format!(
                        "source '{}' surface '{surface_id}' workspace-owned identity binding must include accepted_identity because the surface accepts multiple identities",
                        manifest.schema_name()
                    )));
                }
                None => {}
            }
        }
    }
    Ok(())
}

fn source_needs_stored_material_for_validation(
    candidate: &CandidateSource,
    bindings: &SourceBindings,
    filled_secret_keys: &BTreeSet<String>,
    persisted_secret_keys: Option<&BTreeSet<String>>,
) -> Result<bool, AppError> {
    let supplied_secrets = collect_unique_secrets(&bindings.secrets)?;
    Ok(candidate.inputs.iter().any(|input| {
        input.kind == ManifestInputKind::Secret
            && !supplied_secrets.contains_key(&input.key)
            && !filled_secret_keys.contains(&input.key)
            && persisted_secret_keys.is_none_or(|keys| keys.contains(&input.key))
    }))
}

fn material_key_belongs_to_source_secret(
    key: &str,
    expected_secret_keys: &BTreeSet<String>,
) -> bool {
    if expected_secret_keys.contains(key) {
        return true;
    }
    expected_secret_keys
        .iter()
        .any(|secret_key| material_key_belongs_to_input(key, secret_key))
}

fn source_oauth_config<'a>(
    candidate: &'a CandidateSource,
    input_key: &str,
    method_index: usize,
) -> Result<SourceCredentialOAuthConfig<'a>, AppError> {
    let input = candidate
        .inputs
        .iter()
        .find(|input| input.key == input_key)
        .ok_or_else(|| {
            AppError::InvalidInput(format!(
                "source '{}' has no input '{input_key}'",
                candidate.name
            ))
        })?;
    if input.kind != ManifestInputKind::Secret {
        return Err(AppError::InvalidInput(format!(
            "source input '{}' is not a secret",
            input.key
        )));
    }
    let credential = input.credential.as_ref().ok_or_else(|| {
        AppError::InvalidInput(format!(
            "source input '{}' does not declare credential methods",
            input.key
        ))
    })?;
    let method = credential.methods.get(method_index).ok_or_else(|| {
        AppError::InvalidInput(format!(
            "source input '{}' credential method index {method_index} is out of range",
            input.key
        ))
    })?;
    if method.kind != ManifestCredentialMethodKind::OAuth {
        return Err(AppError::InvalidInput(format!(
            "source input '{}' credential method index {method_index} is not oauth",
            input.key
        )));
    }
    let oauth = method.oauth.as_ref().ok_or_else(|| {
        AppError::InvalidInput(format!(
            "source input '{}' oauth credential method is missing oauth config",
            input.key
        ))
    })?;
    Ok(SourceCredentialOAuthConfig {
        input_key: &input.key,
        oauth,
    })
}

fn merge_oauth_material_into_bindings(
    bindings: &mut ValidatedBindings,
    materials: Vec<OAuthCredentialMaterial>,
) -> Result<(), AppError> {
    for material in materials {
        let OAuthCredentialMaterial {
            input_key,
            access_token,
            internal_metadata,
            safe_metadata: _,
        } = material;
        if bindings.secrets.contains_key(&input_key) {
            return Err(AppError::InvalidInput(format!(
                "source secret '{input_key}' was provided by both source config and OAuth"
            )));
        }
        bindings.replaced_oauth_inputs.insert(input_key.clone());
        bindings.secrets.insert(input_key, access_token);
        bindings.secrets.extend(internal_metadata);
    }
    Ok(())
}

fn collect_unique_variables(
    variables: &[SourceBinding],
) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    for variable in variables {
        let key = normalize_binding_key("source variable key", &variable.key)?;
        if values.insert(key.clone(), variable.value.clone()).is_some() {
            return Err(AppError::InvalidInput(format!(
                "source variable '{key}' is repeated"
            )));
        }
    }
    Ok(values)
}

fn collect_unique_secrets(secrets: &[SourceBinding]) -> Result<BTreeMap<String, String>, AppError> {
    let mut values = BTreeMap::new();
    for secret in secrets {
        let key = normalize_binding_key("source secret key", &secret.key)?;
        if values.insert(key.clone(), secret.value.clone()).is_some() {
            return Err(AppError::InvalidInput(format!(
                "source secret '{key}' is repeated"
            )));
        }
    }
    Ok(values)
}

fn normalize_binding_key(label: &str, value: &str) -> Result<String, AppError> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(AppError::InvalidInput(format!("missing {label}")));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(AppError::InvalidInput(format!(
            "{label} must not contain '/' or '\\\\'"
        )));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(AppError::InvalidInput(format!(
            "{label} must not contain '=', '\\n', or '\\r'"
        )));
    }
    if trimmed.starts_with('#') {
        return Err(AppError::InvalidInput(format!(
            "{label} must not start with '#'"
        )));
    }
    if trimmed.starts_with(CORAL_INTERNAL_KEY_PREFIX) {
        return Err(AppError::InvalidInput(format!(
            "{label} must not start with reserved prefix '{CORAL_INTERNAL_KEY_PREFIX}'"
        )));
    }
    Ok(trimmed.to_string())
}

fn runtime_schema_names(manifest: &ValidatedSourceManifest) -> BTreeSet<String> {
    if let Some(v4) = manifest.as_v4() {
        return v4
            .surfaces
            .iter()
            .map(|surface| surface.relation_namespace.clone())
            .collect();
    }
    BTreeSet::from([manifest.schema_name().to_string()])
}

fn durable_import_manifest_yaml(
    manifest_yaml: &str,
    manifest: &ValidatedSourceManifest,
) -> Result<String, AppError> {
    let Some(v4) = manifest.as_v4() else {
        return Ok(manifest_yaml.to_string());
    };
    let mut replacement_files = BTreeMap::new();
    for surface in &v4.surfaces {
        let SurfaceDescriptor::File { file, .. } = &surface.descriptor else {
            continue;
        };
        let canonical = canonicalize_file_descriptor(file)?;
        if canonical != *file {
            replacement_files.insert(surface.id.as_str(), canonical);
        }
    }
    if replacement_files.is_empty() {
        return Ok(manifest_yaml.to_string());
    }

    let mut value: YamlValue = serde_yaml::from_str(manifest_yaml)?;
    let surfaces_key = YamlValue::String("surfaces".to_string());
    let id_key = YamlValue::String("id".to_string());
    let file_key = YamlValue::String("file".to_string());
    let surfaces = value
        .as_mapping_mut()
        .and_then(|mapping| mapping.get_mut(&surfaces_key))
        .and_then(YamlValue::as_sequence_mut)
        .ok_or_else(|| AppError::InvalidInput("DSL v4 manifest is missing surfaces".to_string()))?;
    for surface in surfaces {
        let Some(mapping) = surface.as_mapping_mut() else {
            continue;
        };
        let Some(surface_id) = mapping.get(&id_key).and_then(YamlValue::as_str) else {
            continue;
        };
        let Some(file) = replacement_files.get(surface_id) else {
            continue;
        };
        mapping.insert(
            file_key.clone(),
            YamlValue::String(file.display().to_string()),
        );
    }
    serde_yaml::to_string(&value).map_err(AppError::from)
}

fn cleanup_empty_parent(root: &std::path::Path, path: Option<&std::path::Path>) {
    let Some(mut current) = path.map(std::path::Path::to_path_buf) else {
        return;
    };
    while current.starts_with(root) && current != root {
        let Ok(mut entries) = std::fs::read_dir(&current) else {
            break;
        };
        if entries.next().is_some() {
            break;
        }
        let next = current.parent().unwrap_or(root).to_path_buf();
        if std::fs::remove_dir(&current).is_err() {
            break;
        }
        current = next;
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener as StdTcpListener;
    use std::path::{Path, PathBuf};
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use url::Url;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{
        ImportSourceCommand, ImportSourceWithCredentialsCommand, SourceBinding, SourceBindings,
        SourceManager, SourceOAuthCredentialRetrieval, ValidatedBindings,
        materialization_inputs_from_bindings, normalize_binding_key,
        source_needs_stored_material_for_validation,
    };
    use crate::bootstrap::AppError;
    use crate::credentials::oauth::{
        OAuthProgressEvent, OAuthProgressEventSender, PendingOAuthProgressEvent,
    };
    use crate::credentials::{
        CredentialManager, CredentialSetId, CredentialStorageKind, CredentialStoragePreference,
        CredentialStore, CredentialsError,
    };
    use crate::features::{Features, dsl_v4_features};
    use crate::identity::{SourceIdentityBinding, SourceIdentityOwner};
    use crate::sources::SourceName;
    use crate::sources::materialization::sha256_hex;
    use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;
    use coral_spec::{ManifestInputKind, ManifestInputSpec};

    fn default_workspace() -> WorkspaceName {
        WorkspaceName::default()
    }

    /// dsl_version-3 HTTP manifest exposing a single `messages` table;
    /// `head_yaml` supplies the `name/version/inputs/base_url/auth` prefix.
    fn http_manifest(head_yaml: &str, table_description: &str) -> String {
        format!(
            r"{head_yaml}tables:
  - name: messages
    description: {table_description}
    request:
      method: GET
      path: /messages
    response: {{}}
    columns:
      - name: id
        type: Utf8
"
        )
    }

    /// Templated `base_url` plus bearer-token auth shared by the secret-bearing
    /// v3 manifests.
    const V3_BEARER_AUTH_YAML: &str = r#"base_url: "{{input.API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.API_TOKEN}}
"#;

    fn manifest_with_secret() -> String {
        http_manifest(
            &format!(
                r"
name: secured_messages
version: 0.1.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
    default: https://example.com
  API_TOKEN:
    kind: secret
{V3_BEARER_AUTH_YAML}"
            ),
            "Secured messages",
        )
    }

    fn manifest_without_secrets() -> String {
        http_manifest(
            r#"
name: public_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: "https://example.com"
"#,
            "Public messages",
        )
    }

    fn manifest_with_oauth_secret(token_url: &str, redirect_port: u16) -> String {
        http_manifest(
            &format!(
                r"
name: secured_messages
version: 0.2.0
dsl_version: 3
backend: http
inputs:
  API_BASE:
    kind: variable
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect
          description: Use OAuth.
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://127.0.0.1:{redirect_port}/oauth/callback
            endpoints:
              authorization_url: https://provider.example.com/oauth/authorize
              token_url: {token_url}
            client:
              id:
                default: default-client
{V3_BEARER_AUTH_YAML}"
            ),
            "Secured messages",
        )
    }

    /// Shared `OpenAPI` operations document body for the v4 import fixtures.
    const V4_OPENAPI_OPERATIONS: &str = r"paths:
  /repos/{owner}/{repo}/issues:
    get:
      operationId: issues/list-for-repo
      parameters:
        - {name: owner, in: path, required: true, schema: {type: string}}
        - {name: repo, in: path, required: true, schema: {type: string}}
      responses:
        '200':
          content:
            application/json:
              schema:
                type: array
                items: {$ref: '#/components/schemas/issue'}
components:
  schemas:
    issue:
      type: object
      properties:
        id: {type: integer}
        title: {type: string}
";

    fn v4_openapi_fixture() -> String {
        format!("\nopenapi: 3.0.3\n{V4_OPENAPI_OPERATIONS}")
    }

    fn v4_openapi_fixture_with_metadata() -> String {
        format!(
            r"
openapi: 3.0.3
info:
  title: GitHub
  description: Query GitHub issues.
servers:
  - url: https://api.github.test
{V4_OPENAPI_OPERATIONS}"
        )
    }

    fn v4_openapi_fixture_with_defaulted_input_server_url() -> String {
        format!(
            r#"
openapi: 3.0.3
servers:
  - url: "{{apiBase}}"
    variables:
      apiBase:
        default: "{{{{input.API_BASE|https://fallback.example.com}}}}"
{V4_OPENAPI_OPERATIONS}"#
        )
    }

    /// Renders a v4 manifest whose `rest` surface points at the authored
    /// descriptor, with `extra_surface_yaml` appended to the surface entry.
    fn manifest_v4(openapi_file: &Path, sha256: &str, extra_surface_yaml: &str) -> String {
        format!(
            r"
name: github_v4_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    sha256: {}
{extra_surface_yaml}",
            openapi_file.display(),
            sha256
        )
    }

    const V4_LOCAL_BASE_URL_YAML: &str = r#"    inputs:
      API_BASE:
        kind: variable
        default: http://127.0.0.1:1
    base_url: "{{input.API_BASE}}"
"#;

    fn manifest_v4_with_file_descriptor(openapi_file: &Path, sha256: &str) -> String {
        manifest_v4(openapi_file, sha256, V4_LOCAL_BASE_URL_YAML)
    }

    fn manifest_v4_with_identity_requirement(openapi_file: &Path, sha256: &str) -> String {
        manifest_v4(
            openapi_file,
            sha256,
            &format!(
                r"{V4_LOCAL_BASE_URL_YAML}    identity_requirements:
      accepts:
        - id: github-rest-read
          identity_specs:
            - github_oauth
          audience:
            host: github.com
"
            ),
        )
    }

    fn manifest_v4_with_input_and_derived_base_url(openapi_file: &Path, sha256: &str) -> String {
        manifest_v4(
            openapi_file,
            sha256,
            r"    inputs:
      API_BASE:
        kind: variable
        default: https://api.example.com
",
        )
    }

    fn manifest_v4_without_description_or_base_url(openapi_file: &Path, sha256: &str) -> String {
        manifest_v4(openapi_file, sha256, "")
    }

    /// Authored-descriptor state plus a dsl_v4-enabled source manager over a
    /// fresh app layout.
    struct V4ImportFixture {
        _temp: TempDir,
        _descriptor_temp: TempDir,
        layout: AppStateLayout,
        openapi_file: PathBuf,
        openapi_sha256: String,
        manager: SourceManager,
    }

    impl V4ImportFixture {
        /// Renders `manifest` against the fixture descriptor.
        fn manifest(&self, manifest: fn(&Path, &str) -> String) -> String {
            manifest(&self.openapi_file, &self.openapi_sha256)
        }

        /// Imports the rendered `manifest` with no bindings into the default
        /// workspace.
        fn import(&self, manifest: fn(&Path, &str) -> String) -> Result<InstalledSource, AppError> {
            import_manifest(
                &self.manager,
                self.manifest(manifest),
                SourceBindings::default(),
            )
        }
    }

    fn v4_import_fixture(openapi_yaml: &str) -> V4ImportFixture {
        v4_import_fixture_with_features(openapi_yaml, dsl_v4_features())
    }

    /// As [`v4_import_fixture`], but with the given feature set (so tests can
    /// exercise the `dsl_v4` feature gate).
    fn v4_import_fixture_with_features(openapi_yaml: &str, features: Features) -> V4ImportFixture {
        let temp = TempDir::new().expect("temp dir");
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(&openapi_file, openapi_yaml).expect("write fixture");
        let manager = SourceManager::new_with_features(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
            features,
        );
        V4ImportFixture {
            _temp: temp,
            _descriptor_temp: descriptor_temp,
            layout,
            openapi_file,
            openapi_sha256: sha256_hex(openapi_yaml.as_bytes()),
            manager,
        }
    }

    /// An [`ImportSourceCommand`] with no identity bindings.
    fn import_command(manifest_yaml: String, bindings: SourceBindings) -> ImportSourceCommand {
        ImportSourceCommand {
            manifest_yaml,
            bindings,
            identity_bindings: BTreeMap::new(),
            replace_identity_bindings: false,
        }
    }

    /// An [`ImportSourceWithCredentialsCommand`] with no identity bindings.
    fn credentials_import_command(
        manifest_yaml: String,
        bindings: SourceBindings,
        oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
    ) -> ImportSourceWithCredentialsCommand {
        ImportSourceWithCredentialsCommand {
            manifest_yaml,
            bindings,
            identity_bindings: BTreeMap::new(),
            replace_identity_bindings: false,
            oauth_credential_retrievals,
        }
    }

    /// A credentials-import command for `manifest_with_oauth_secret` whose
    /// token endpoint is unreachable; for tests that must fail preflight
    /// before OAuth retrieval starts.
    fn unreachable_oauth_import_command(
        bindings: SourceBindings,
    ) -> ImportSourceWithCredentialsCommand {
        credentials_import_command(
            manifest_with_oauth_secret("http://127.0.0.1:1/token", free_loopback_port()),
            bindings,
            api_token_oauth_retrieval(),
        )
    }

    fn api_token_oauth_retrieval() -> Vec<SourceOAuthCredentialRetrieval> {
        vec![SourceOAuthCredentialRetrieval {
            input_key: "API_TOKEN".to_string(),
            method_index: 0,
            credential_inputs: Vec::new(),
        }]
    }

    fn binding(key: &str, value: &str) -> SourceBinding {
        SourceBinding {
            key: key.to_string(),
            value: value.to_string(),
        }
    }

    /// [`SourceBindings`] built from `(key, value)` variable and secret pairs.
    fn bindings(variables: &[(&str, &str)], secrets: &[(&str, &str)]) -> SourceBindings {
        SourceBindings {
            variables: variables
                .iter()
                .map(|(key, value)| binding(key, value))
                .collect(),
            secrets: secrets
                .iter()
                .map(|(key, value)| binding(key, value))
                .collect(),
        }
    }

    fn api_token_bindings(value: &str) -> SourceBindings {
        bindings(&[], &[("API_TOKEN", value)])
    }

    /// Imports `manifest_yaml` with `bindings` into the default workspace.
    fn import_manifest(
        manager: &SourceManager,
        manifest_yaml: String,
        bindings: SourceBindings,
    ) -> Result<InstalledSource, AppError> {
        manager.import_source(
            &default_workspace(),
            &import_command(manifest_yaml, bindings),
        )
    }

    /// Imports [`manifest_with_secret`] into the default workspace.
    fn import_secured(
        manager: &SourceManager,
        bindings: SourceBindings,
    ) -> Result<InstalledSource, AppError> {
        import_manifest(manager, manifest_with_secret(), bindings)
    }

    /// The source name shared by the secret-bearing v3 manifests, with its
    /// credential set id.
    fn secured_messages_ids() -> (SourceName, CredentialSetId) {
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        (source_name, credential_set_id)
    }

    /// Asserts `map[key] == expected`, labeling failures with the key.
    #[track_caller]
    fn assert_map_entry(map: &BTreeMap<String, String>, key: &str, expected: &str) {
        assert_eq!(
            map.get(key).map(String::as_str),
            Some(expected),
            "entry '{key}'"
        );
    }

    /// Asserts the error's message contains `expected`.
    #[track_caller]
    fn assert_error_contains(error: &AppError, expected: &str) {
        let message = error.to_string();
        assert!(message.contains(expected), "unexpected error: {message}");
    }

    /// A plain (no v4 features) source manager over a fresh app layout.
    struct ManagerFixture {
        _temp: TempDir,
        layout: AppStateLayout,
        credential_store: CredentialStore,
        credential_manager: CredentialManager,
        manager: SourceManager,
    }

    impl ManagerFixture {
        /// Path of `source_name`'s file-backed secret material.
        fn secret_path(&self, source_name: &SourceName) -> PathBuf {
            self.layout.secret_file(&default_workspace(), source_name)
        }

        /// Reads stored credential material of `kind` for `credential_set_id`.
        fn material(
            &self,
            credential_set_id: &CredentialSetId,
            kind: CredentialStorageKind,
        ) -> BTreeMap<String, String> {
            self.credential_manager
                .read_material(&default_workspace(), credential_set_id, kind)
                .expect("read material")
        }

        /// Seeds file-backed credential material for `credential_set_id`.
        fn seed_file_material(
            &self,
            credential_set_id: &CredentialSetId,
            entries: &[(&str, &str)],
        ) {
            self.credential_manager
                .replace_material(
                    &default_workspace(),
                    credential_set_id,
                    CredentialStorageKind::File,
                    &entries
                        .iter()
                        .map(|(key, value)| ((*key).to_string(), (*value).to_string()))
                        .collect::<BTreeMap<_, _>>(),
                )
                .expect("seed credential material");
        }
    }

    fn manager_fixture() -> ManagerFixture {
        manager_fixture_with_store(|layout| CredentialStore::new(layout.clone()))
    }

    fn manager_fixture_with_store(
        credential_store: impl FnOnce(&AppStateLayout) -> CredentialStore,
    ) -> ManagerFixture {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let credential_store = credential_store(&layout);
        let credential_manager = CredentialManager::new(credential_store.clone());
        let manager = SourceManager::new(
            ConfigStore::new(layout.clone()),
            credential_manager.clone(),
            layout.clone(),
        );
        ManagerFixture {
            _temp: temp,
            layout,
            credential_store,
            credential_manager,
            manager,
        }
    }

    fn manifest_v4_with_variable_input() -> String {
        r#"
name: secured_messages_v4
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
    sha256: 0000000000000000000000000000000000000000000000000000000000000000
    inputs:
      API_BASE:
        kind: variable
    base_url: "{{input.API_BASE}}"
"#
        .to_string()
    }

    fn manifest_with_templated_oauth_endpoints(
        token_url: &str,
        redirect_port: u16,
    ) -> (String, String) {
        let token_url_template = token_url.replace("/token", "/{{input.OUTLOOK_TENANT_ID}}/token");
        let rendered_token_url = token_url.replace("/token", "/organizations/token");
        let manifest = manifest_with_oauth_secret(&token_url_template, redirect_port)
            .replace(
                "base_url: \"{{input.API_BASE}}\"",
                "  OUTLOOK_TENANT_ID:\n    kind: variable\nbase_url: \"{{input.API_BASE}}\"",
            )
            .replace(
                "authorization_url: https://provider.example.com/oauth/authorize",
                "authorization_url: https://provider.example.com/{{input.OUTLOOK_TENANT_ID}}/oauth/authorize",
            );
        (manifest, rendered_token_url)
    }

    fn oauth_import_bindings_with_tenant() -> SourceBindings {
        bindings(
            &[
                ("API_BASE", "https://api.example.test"),
                ("OUTLOOK_TENANT_ID", "organizations"),
            ],
            &[],
        )
    }

    fn candidate_with_secret(key: &str, required: bool) -> CandidateSource {
        CandidateSource {
            name: SourceName::parse("secured_messages").expect("source"),
            description: String::new(),
            version: None,
            inputs: vec![ManifestInputSpec {
                key: key.to_string(),
                kind: ManifestInputKind::Secret,
                required,
                default_value: String::new(),
                hint: None,
                credential: None,
            }],
            installed: true,
            origin: SourceOrigin::Imported,
            credential_storage: Some(CredentialStorageKind::File),
        }
    }

    #[test]
    fn materialization_inputs_include_persisted_optional_secrets() {
        let candidate = candidate_with_secret("OPTIONAL_TOKEN", false);
        let persisted_secret_keys = BTreeSet::from(["OPTIONAL_TOKEN".to_string()]);
        let needs_stored = source_needs_stored_material_for_validation(
            &candidate,
            &SourceBindings::default(),
            &BTreeSet::new(),
            Some(&persisted_secret_keys),
        )
        .expect("stored material check");
        assert!(
            needs_stored,
            "optional persisted secrets can affect v4 materialization and should be loaded"
        );

        let bindings = ValidatedBindings {
            variables: BTreeMap::new(),
            secrets: BTreeMap::new(),
            replaced_oauth_inputs: BTreeSet::new(),
        };
        let stored_material =
            BTreeMap::from([("OPTIONAL_TOKEN".to_string(), "persisted-secret".to_string())]);

        let inputs = materialization_inputs_from_bindings(&bindings, &stored_material);
        assert_eq!(
            inputs.secrets.get("OPTIONAL_TOKEN").map(String::as_str),
            Some("persisted-secret")
        );
    }

    #[test]
    fn unsupplied_optional_secret_without_persisted_material_skips_keychain_read() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::with_unavailable_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Keychain,
        );
        let credential_manager = CredentialManager::new(credential_store);
        let manager = SourceManager::new(config_store.clone(), credential_manager, layout);
        let candidate = candidate_with_secret("OPTIONAL_TOKEN", false);

        config_store
            .upsert_source(
                &default_workspace(),
                InstalledSource {
                    name: candidate.name.clone(),
                    version: None,
                    variables: BTreeMap::new(),
                    secrets: vec!["OTHER_TOKEN".to_string()],
                    credential_storage: Some(CredentialStorageKind::Keychain),
                    origin: SourceOrigin::Imported,
                },
            )
            .expect("persist source metadata");

        let stored_material = manager
            .source_stored_material_for_validation(
                &default_workspace(),
                &candidate,
                &SourceBindings::default(),
                &BTreeSet::new(),
            )
            .expect("optional secret should not force keychain read");

        assert!(stored_material.is_empty());
    }

    #[test]
    fn unsupplied_optional_secret_with_persisted_material_needs_stored_material() {
        let candidate = candidate_with_secret("OPTIONAL_TOKEN", false);
        let persisted_secret_keys = BTreeSet::from(["OPTIONAL_TOKEN".to_string()]);
        let needs_stored = source_needs_stored_material_for_validation(
            &candidate,
            &SourceBindings::default(),
            &BTreeSet::new(),
            Some(&persisted_secret_keys),
        )
        .expect("stored material check");

        assert!(needs_stored);
    }

    #[test]
    fn discover_sources_omits_core_v4_preview_sources() {
        let ManagerFixture { _temp, manager, .. } = manager_fixture();

        let disabled = manager
            .discover_sources(&default_workspace())
            .expect("discover sources");
        assert!(
            !disabled
                .iter()
                .any(|source| source.name.as_str() == "github_v4")
        );
    }

    #[test]
    fn import_v4_source_writes_materialized_artifacts() {
        let fixture = v4_import_fixture(&v4_openapi_fixture());

        let installed = fixture
            .import(manifest_v4_with_file_descriptor)
            .expect("import v4 source");

        assert_eq!(installed.name.as_str(), "github_v4_test");
        let source_name = SourceName::parse("github_v4_test").expect("source");
        let materialized = fixture
            .layout
            .v4_materialized_dir(&default_workspace(), &source_name);
        assert!(materialized.join("fingerprint.yaml").exists());
        assert!(materialized.join("projections.yaml").exists());
        assert!(
            materialized
                .join("surfaces")
                .join("rest")
                .join("semantic-ir.yaml")
                .exists()
        );

        let info = fixture
            .manager
            .get_source_info(&default_workspace(), &source_name)
            .expect("installed v4 source should be usable");
        assert_eq!(info.name.as_str(), "github_v4_test");
    }

    #[test]
    fn import_v4_source_rejects_runtime_schema_collision_before_persistence() {
        let fixture = v4_import_fixture(&v4_openapi_fixture());
        fixture
            .manager
            .import_source(
                &default_workspace(),
                &import_command(
                    manifest_without_secrets().replace("public_messages", "github_v4_rest"),
                    SourceBindings::default(),
                ),
            )
            .expect("install existing source");

        let colliding_manifest = format!(
            r"
name: github_v4
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: rest
    type: openapi
    file: {}
    sha256: {}
{V4_LOCAL_BASE_URL_YAML}",
            fixture.openapi_file.display(),
            fixture.openapi_sha256
        );
        let error = import_manifest(
            &fixture.manager,
            colliding_manifest,
            SourceBindings::default(),
        )
        .expect_err("surface namespace should collide with installed source schema");

        let message = error.to_string();
        assert!(message.contains("runtime schema name 'github_v4_rest'"));
        assert!(message.contains("conflicts with installed source 'github_v4_rest'"));
        let rejected_source = SourceName::parse("github_v4").expect("source");
        assert!(
            manager
                .get_source(&default_workspace(), &rejected_source)
                .is_err(),
            "rejected source should not be persisted"
        );
        assert!(
            !fixture
                .layout
                .v4_materialized_dir(&default_workspace(), &rejected_source)
                .exists(),
            "rejected source should not materialize artifacts"
        );
    }

    #[test]
    fn import_v4_source_persists_identity_bindings() {
        let fixture = v4_import_fixture(&v4_openapi_fixture());
        let mut command = import_command(
            fixture.manifest(manifest_v4_with_identity_requirement),
            SourceBindings::default(),
        );
        command
            .identity_bindings
            .insert("rest".to_string(), SourceIdentityBinding::user_owned());

        let installed = fixture
            .manager
            .import_source(&default_workspace(), &command)
            .expect("import v4 source with identity binding");

        let binding = installed
            .identity_bindings
            .get("rest")
            .expect("rest identity binding");
        assert_eq!(binding.identity, None);
        assert_eq!(binding.owner, SourceIdentityOwner::User);
        assert_eq!(binding.accepted_identity, None);
        let source_name = SourceName::parse("github_v4_test").expect("source");
        let stored = fixture
            .manager
            .get_source(&default_workspace(), &source_name)
            .expect("stored source");
        assert_eq!(stored.identity_bindings, installed.identity_bindings);
    }

    #[test]
    fn import_v4_source_requires_identity_binding_when_declared() {
        let fixture = v4_import_fixture(&v4_openapi_fixture());

        let error = fixture
            .import(manifest_v4_with_identity_requirement)
            .expect_err("identity-backed v4 source should require a binding");

        assert_error_contains(&error, "no identity binding was provided");
    }

    #[test]
    fn import_v4_source_rejects_derived_base_url_input_token_defaults() {
        let fixture = v4_import_fixture(&v4_openapi_fixture_with_defaulted_input_server_url());

        let error = fixture
            .import(manifest_v4_with_input_and_derived_base_url)
            .expect_err("source add should reject derived base_url input token defaults");

        assert_error_contains(&error, "derived OpenAPI server base_url input token");
        assert!(
            !fixture
                .layout
                .v4_materialized_dir(
                    &default_workspace(),
                    &SourceName::parse("github_v4_test").expect("source")
                )
                .exists(),
            "failed materialization should not install artifacts"
        );
    }

    #[test]
    fn import_v4_source_requires_dsl_v4_feature() {
        let fixture = v4_import_fixture_with_features(&v4_openapi_fixture(), Features::default());

        let error = fixture
            .import(manifest_v4_with_file_descriptor)
            .expect_err("disabled v4 feature should reject import");

        assert_error_contains(&error, "dsl_v4");
    }

    #[tokio::test]
    async fn import_v4_with_credentials_requires_feature_before_import() {
        let ManagerFixture { _temp, manager, .. } = manager_fixture();
        let (event_tx, mut event_rx) = import_event_channel();

        let error = manager
            .import_source_with_credentials(
                &default_workspace(),
                credentials_import_command(
                    manifest_v4_with_variable_input(),
                    bindings(&[("API_BASE", "https://api.example.test")], &[]),
                    Vec::new(),
                ),
                event_tx,
            )
            .await
            .expect_err("disabled v4 feature should reject before import");

        assert_error_contains(&error, "dsl_v4");
        assert!(
            event_rx.try_recv().is_err(),
            "feature gate should fail before import events"
        );
    }

    #[test]
    fn import_v4_source_rejects_unresolved_relative_descriptor() {
        let fixture = v4_import_fixture(&v4_openapi_fixture());

        let error = import_manifest(
            &fixture.manager,
            manifest_v4_with_file_descriptor(Path::new("openapi.yaml"), &"0".repeat(64)),
            SourceBindings::default(),
        )
        .expect_err("raw relative descriptors should fail in app import");

        assert_error_contains(
            &error,
            "imported DSL v4 manifests must use absolute file descriptors",
        );
    }

    #[test]
    fn import_v4_source_preserves_intent_yaml_without_openapi_metadata() {
        let fixture = v4_import_fixture(&v4_openapi_fixture_with_metadata());

        fixture
            .import(manifest_v4_without_description_or_base_url)
            .expect("import v4 source");

        let source_name = SourceName::parse("github_v4_test").expect("source");
        let stored_manifest = std::fs::read_to_string(
            fixture
                .layout
                .manifest_file(&default_workspace(), &source_name),
        )
        .expect("stored manifest");
        assert!(
            !stored_manifest.contains("description: Query GitHub issues."),
            "expected stored manifest not to contain OpenAPI description: {stored_manifest}"
        );
        assert!(
            !stored_manifest.contains("base_url: https://api.github.test"),
            "expected stored manifest not to contain OpenAPI server URL: {stored_manifest}"
        );
    }

    #[test]
    fn import_restores_prior_state_when_secret_persistence_fails() {
        let fixture = manager_fixture();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let source_dir = fixture
            .layout
            .source_dir(&default_workspace(), &source_name);
        std::fs::create_dir_all(&source_dir).expect("create source dir");
        std::fs::create_dir(source_dir.join("secrets.env"))
            .expect("create blocking secrets directory");

        let error = import_secured(
            &fixture.manager,
            bindings(
                &[("API_BASE", "https://example.com")],
                &[("API_TOKEN", "secret-token")],
            ),
        )
        .expect_err("secret persistence should fail");

        assert!(
            matches!(error, AppError::Credentials(CredentialsError::Io(_))),
            "unexpected error: {error:#}"
        );
        assert!(
            !fixture
                .layout
                .source_dir(&default_workspace(), &source_name)
                .exists(),
            "source dir should be cleaned up after secret persistence failure"
        );
        assert!(
            fixture
                .manager
                .list_workspace_sources(&default_workspace())
                .expect("list sources")
                .is_empty(),
            "source config should not be persisted after rollback"
        );
    }

    #[test]
    fn logical_binding_keys_allow_dot_segments() {
        assert_eq!(
            normalize_binding_key("source variable key", "..").expect("key"),
            ".."
        );
    }

    /// Labeled reject cases for [`normalize_binding_key`]: env-file breaking
    /// characters, comment markers, and reserved internal prefixes.
    #[test]
    fn rejects_invalid_binding_keys() {
        for (label, key, expected) in [
            (
                "'=' should be rejected",
                "API=TOKEN",
                "must not contain '=', '\\n', or '\\r'",
            ),
            (
                "newlines should be rejected",
                "API\nTOKEN",
                "must not contain '=', '\\n', or '\\r'",
            ),
            (
                "leading comment markers should be rejected",
                " #comment",
                "must not start with '#'",
            ),
            (
                "reserved prefix should be rejected",
                "__coral.API_TOKEN",
                "must not start with reserved prefix '__coral'",
            ),
        ] {
            let error = normalize_binding_key("source secret key", key).expect_err(label);
            assert!(
                error.to_string().contains(expected),
                "{label}: unexpected error: {error}"
            );
        }
    }

    #[test]
    fn import_materializes_variable_defaults_server_side() {
        let ManagerFixture { _temp, manager, .. } = manager_fixture();

        let source =
            import_secured(&manager, api_token_bindings("secret-token")).expect("import source");

        assert_map_entry(&source.variables, "API_BASE", "https://example.com");
    }

    #[test]
    fn import_new_source_uses_keychain_when_auto_probe_succeeds() {
        let fixture = manager_fixture_with_store(|layout| {
            CredentialStore::with_available_keychain_for_test(
                layout.clone(),
                CredentialStoragePreference::Auto,
            )
        });
        let (source_name, credential_set_id) = secured_messages_ids();

        let source = import_secured(&fixture.manager, api_token_bindings("secret-token"))
            .expect("import source");

        assert_eq!(
            source.credential_storage,
            Some(CredentialStorageKind::Keychain)
        );
        assert!(
            !fixture.secret_path(&source_name).exists(),
            "keychain-routed install should not create plaintext material"
        );
        let stored = fixture.material(&credential_set_id, CredentialStorageKind::Keychain);
        assert_map_entry(&stored, "API_TOKEN", "secret-token");

        fixture
            .manager
            .delete_source(&default_workspace(), &source_name)
            .expect("delete source");
        assert!(
            fixture
                .material(&credential_set_id, CredentialStorageKind::Keychain)
                .is_empty(),
            "delete should remove keychain-routed material"
        );
    }

    #[test]
    fn import_source_without_secret_material_does_not_probe_keychain() {
        let fixture = manager_fixture_with_store(|layout| {
            CredentialStore::with_unavailable_keychain_for_test(
                layout.clone(),
                CredentialStoragePreference::Keychain,
            )
        });
        let source_name = SourceName::parse("public_messages").expect("source");

        let source = import_manifest(
            &fixture.manager,
            manifest_without_secrets(),
            SourceBindings::default(),
        )
        .expect("import source");

        assert!(source.secrets.is_empty());
        assert_eq!(source.credential_storage, None);
        assert!(
            !fixture.secret_path(&source_name).exists(),
            "credential material should not be created for a source without secrets"
        );
        let config_raw =
            std::fs::read_to_string(fixture.layout.config_file()).expect("read rendered config");
        assert!(
            !config_raw.contains("credential_storage"),
            "source without credential material should not persist a storage route"
        );
    }

    #[test]
    fn import_missing_secret_does_not_probe_keychain_for_new_source() {
        let ManagerFixture { _temp, manager, .. } = manager_fixture_with_store(|layout| {
            CredentialStore::with_unavailable_keychain_for_test(
                layout.clone(),
                CredentialStoragePreference::Keychain,
            )
        });

        let error = import_secured(&manager, SourceBindings::default())
            .expect_err("missing required secret should fail validation");

        assert!(
            error
                .to_string()
                .contains("missing required source secret 'API_TOKEN'"),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn import_replaces_malformed_existing_credential_material() {
        let fixture = manager_fixture();
        let source_name = SourceName::parse("secured_messages").expect("source");
        import_secured(&fixture.manager, api_token_bindings("old-token")).expect("initial import");

        let secret_path = fixture.secret_path(&source_name);
        std::fs::write(&secret_path, "BROKEN\n").expect("write malformed credential material");

        import_secured(&fixture.manager, api_token_bindings("new-token"))
            .expect("replace malformed credential material");

        assert_eq!(
            std::fs::read_to_string(&secret_path).expect("read replaced credential material"),
            "API_TOKEN=new-token\n"
        );
    }

    #[test]
    fn delete_removes_source_with_malformed_credential_material() {
        let fixture = manager_fixture();
        let source_name = SourceName::parse("secured_messages").expect("source");
        import_secured(&fixture.manager, api_token_bindings("secret-token"))
            .expect("initial import");

        let secret_path = fixture.secret_path(&source_name);
        std::fs::write(&secret_path, "BROKEN\n").expect("write malformed credential material");

        fixture
            .manager
            .delete_source(&default_workspace(), &source_name)
            .expect("delete source with malformed credential material");

        assert!(
            !secret_path.exists(),
            "delete should remove malformed credential material"
        );
        assert!(
            fixture
                .manager
                .list_workspace_sources(&default_workspace())
                .expect("list sources")
                .is_empty(),
            "source config should be removed"
        );
    }

    #[test]
    fn import_accepts_secret_already_populated_in_credential_material() {
        let fixture = manager_fixture();
        let (_, credential_set_id) = secured_messages_ids();
        fixture.seed_file_material(
            &credential_set_id,
            &[
                ("API_TOKEN", "oauth-token"),
                ("__coral_oauth.QVBJX1RPS0VO.method", "oauth"),
            ],
        );

        let source =
            import_secured(&fixture.manager, SourceBindings::default()).expect("import source");

        assert_eq!(source.secrets, vec!["API_TOKEN"]);
        let material = fixture.material(&credential_set_id, CredentialStorageKind::File);
        assert_map_entry(&material, "API_TOKEN", "oauth-token");
        assert_map_entry(&material, "__coral_oauth.QVBJX1RPS0VO.method", "oauth");
    }

    #[test]
    fn import_preserves_credential_store_io_errors_when_material_is_needed() {
        let fixture = manager_fixture();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let secret_path = fixture.secret_path(&source_name);
        std::fs::create_dir_all(&secret_path).expect("create blocking secret directory");

        let error = import_secured(&fixture.manager, SourceBindings::default())
            .expect_err("stored material I/O error should fail import");

        assert!(
            matches!(error, AppError::Credentials(CredentialsError::Io(_))),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn manual_secret_reimport_clears_prior_oauth_material() {
        let fixture = manager_fixture();
        let (_, credential_set_id) = secured_messages_ids();
        fixture.seed_file_material(
            &credential_set_id,
            &[
                ("API_TOKEN", "oauth-token"),
                ("__coral_oauth.QVBJX1RPS0VO.refresh_token", "refresh-token"),
                ("__coral_oauth.QVBJX1RPS0VO.method", "oauth"),
            ],
        );

        import_secured(&fixture.manager, api_token_bindings("manual-token"))
            .expect("import source");

        let material = fixture.material(&credential_set_id, CredentialStorageKind::File);
        assert_map_entry(&material, "API_TOKEN", "manual-token");
        assert!(
            !material
                .keys()
                .any(|key| key.starts_with("__coral_oauth.QVBJX1RPS0VO.")),
            "manual secret replacement should clear stale OAuth metadata"
        );
    }

    #[test]
    fn source_rollback_snapshots_credentials_after_refresh_lock() {
        let fixture = manager_fixture();
        let workspace_name = default_workspace();
        let (_, credential_set_id) = secured_messages_ids();
        import_secured(&fixture.manager, api_token_bindings("old-token")).expect("install source");
        let refresh_lock = fixture
            .credential_store
            .credential_refresh_lock(&workspace_name, &credential_set_id)
            .expect("hold refresh lock");
        let config_temp_path = fixture
            .layout
            .config_file()
            .with_file_name(format!("config.toml.tmp.{}", std::process::id()));
        std::fs::create_dir_all(&config_temp_path).expect("block config save temp path");
        let (started_tx, started_rx) = std_mpsc::channel();
        let import_manager = fixture.manager.clone();
        let import_handle = thread::spawn(move || {
            started_tx.send(()).expect("signal import start");
            import_secured(&import_manager, api_token_bindings("manual-token"))
        });
        started_rx.recv().expect("wait for import thread");
        thread::sleep(Duration::from_millis(50));
        fixture
            .credential_store
            .replace_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([
                    ("API_TOKEN".to_string(), "refreshed-token".to_string()),
                    (
                        "__coral_oauth.QVBJX1RPS0VO.refresh_token".to_string(),
                        "refreshed-refresh-token".to_string(),
                    ),
                ]),
            )
            .expect("simulate persisted refresh while lock is held");
        drop(refresh_lock);
        import_handle
            .join()
            .expect("import thread")
            .expect_err("blocked config save should fail import");
        drop(std::fs::remove_dir_all(&config_temp_path));

        let material = fixture.material(&credential_set_id, CredentialStorageKind::File);
        assert_map_entry(&material, "API_TOKEN", "refreshed-token");
        assert_map_entry(
            &material,
            "__coral_oauth.QVBJX1RPS0VO.refresh_token",
            "refreshed-refresh-token",
        );
    }

    #[tokio::test]
    async fn import_with_oauth_persists_retrieved_material() {
        let fixture = manager_fixture();
        let (_, credential_set_id) = secured_messages_ids();
        let token_server = MockServer::start().await;
        Mock::given(method("POST"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                r#"{"access_token":"access-token","token_type":"Bearer"}"#,
                "application/json",
            ))
            .mount(&token_server)
            .await;
        let redirect_port = free_loopback_port();
        let (manifest_yaml, rendered_token_url) = manifest_with_templated_oauth_endpoints(
            &format!("{}/token", token_server.uri()),
            redirect_port,
        );
        assert!(
            manifest_yaml.find("  API_TOKEN:").expect("API_TOKEN input")
                < manifest_yaml
                    .find("  OUTLOOK_TENANT_ID:")
                    .expect("tenant input"),
            "tenant variable should exercise manifest order after the OAuth secret"
        );
        let (event_tx, mut event_rx) = import_event_channel();
        let workspace_name = default_workspace();
        let import = fixture.manager.import_source_with_credentials(
            &workspace_name,
            credentials_import_command(
                manifest_yaml,
                oauth_import_bindings_with_tenant(),
                api_token_oauth_retrieval(),
            ),
            event_tx,
        );
        let callback = async {
            let event = event_rx
                .recv()
                .await
                .expect("authorization event")
                .into_event();
            let OAuthProgressEvent::OAuthAuthorization {
                input_key,
                authorization_url,
                ..
            } = event
            else {
                panic!("unexpected import event");
            };
            assert_eq!(input_key, "API_TOKEN");
            let parsed = Url::parse(&authorization_url).expect("authorization url");
            assert_eq!(parsed.path(), "/organizations/oauth/authorize");
            callback(&authorization_url, redirect_port).await;
            let event = event_rx
                .recv()
                .await
                .expect("completion event")
                .into_event();
            let OAuthProgressEvent::OAuthCompleted { input_key, .. } = event else {
                panic!("unexpected import event");
            };
            assert_eq!(input_key, "API_TOKEN");
        };

        let (source, ()) = tokio::join!(import, callback);
        let source = source.expect("import source with OAuth");
        assert_eq!(source.secrets, vec!["API_TOKEN"]);
        let token_request = token_server
            .received_requests()
            .await
            .expect("recorded token requests")
            .into_iter()
            .next()
            .expect("token request");
        let form: BTreeMap<String, String> = url::form_urlencoded::parse(&token_request.body)
            .into_owned()
            .collect();
        assert_eq!(form.get("code").map(String::as_str), Some("test-code"));
        let material = fixture.material(&credential_set_id, CredentialStorageKind::File);
        assert_map_entry(&material, "API_TOKEN", "access-token");
        assert_map_entry(&material, "__coral_oauth.QVBJX1RPS0VO.method", "oauth");
        assert_map_entry(
            &material,
            "__coral_oauth.QVBJX1RPS0VO.token_url",
            &rendered_token_url,
        );
    }

    #[tokio::test]
    async fn import_with_oauth_does_not_overwrite_installed_credentials_when_validation_fails() {
        let fixture = manager_fixture();
        let (_, credential_set_id) = secured_messages_ids();
        import_secured(&fixture.manager, api_token_bindings("old-token")).expect("install source");

        let (event_tx, mut event_rx) = import_event_channel();
        let error = fixture
            .manager
            .import_source_with_credentials(
                &default_workspace(),
                unreachable_oauth_import_command(SourceBindings::default()),
                event_tx,
            )
            .await
            .expect_err("missing API_BASE should fail validation");
        assert_error_contains(&error, "missing required source variable 'API_BASE'");
        assert!(
            event_rx.try_recv().is_err(),
            "preflight validation should fail before OAuth retrieval starts"
        );
        let material = fixture.material(&credential_set_id, CredentialStorageKind::File);
        assert_map_entry(&material, "API_TOKEN", "old-token");
        assert!(
            !material.values().any(|value| value == "access-token"),
            "candidate OAuth material should not be persisted on validation failure"
        );
    }

    #[tokio::test]
    async fn import_with_oauth_rejects_source_config_conflict_before_authorization() {
        let ManagerFixture { _temp, manager, .. } = manager_fixture();
        let (event_tx, mut event_rx) = import_event_channel();

        let error = manager
            .import_source_with_credentials(
                &default_workspace(),
                unreachable_oauth_import_command(bindings(
                    &[("API_BASE", "https://api.example.test")],
                    &[("API_TOKEN", "manual-token")],
                )),
                event_tx,
            )
            .await
            .expect_err("source config and OAuth should conflict");
        assert_error_contains(
            &error,
            "source secret 'API_TOKEN' was provided by both source config and OAuth",
        );
        assert!(
            event_rx.try_recv().is_err(),
            "preflight validation should fail before OAuth retrieval starts"
        );
    }

    async fn callback(authorization_url: &str, redirect_port: u16) {
        let authorization_url = Url::parse(authorization_url).expect("authorization url");
        let state = authorization_url
            .query_pairs()
            .find_map(|(key, value)| (key == "state").then(|| value.into_owned()))
            .expect("state");
        let callback_url =
            format!("http://127.0.0.1:{redirect_port}/oauth/callback?state={state}&code=test-code");
        reqwest::get(callback_url)
            .await
            .expect("callback response")
            .error_for_status()
            .expect("callback success");
    }

    fn import_event_channel() -> (
        OAuthProgressEventSender,
        mpsc::Receiver<PendingOAuthProgressEvent>,
    ) {
        let (tx, rx) = mpsc::channel(4);
        (
            OAuthProgressEventSender::new(tx, "source import stream closed"),
            rx,
        )
    }

    fn free_loopback_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0")
            .expect("bind free port")
            .local_addr()
            .expect("addr")
            .port()
    }
}
