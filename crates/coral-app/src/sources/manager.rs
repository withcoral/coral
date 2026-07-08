//! Owns the source lifecycle workflow for the local app.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use coral_spec::v4::SurfaceDescriptor;
use serde_yaml::Value as YamlValue;

use crate::bootstrap::AppError;
use crate::credentials::oauth::{
    OAuthCredentialMaterial, OAuthCredentialService, StartOAuthCredentialRequest,
    material_key_belongs_to_input,
};
use crate::credentials::{
    CORAL_INTERNAL_KEY_PREFIX, CredentialManager, CredentialMaterialGuard,
    CredentialMaterialSnapshot, CredentialSetId, CredentialStorageKind, CredentialsError,
};
use crate::search::observed::SearchObservationHandle;
use crate::search::sqlite_store::SqliteSearchStore;
use crate::sources::SourceName;
use crate::sources::catalog::{
    describe_manifest, list_bundled_sources, load_bundled_source, resolve_installed_manifest,
};
use crate::sources::materialization::{
    MaterializationBuild, MaterializationInputs, SourceDiagnosticReporter,
    build_v4_materialization_tmp, canonicalize_file_descriptor, cleanup_materialization_backup,
    cleanup_materialization_tmp, new_materialization_suffix, replace_v4_materialization,
    restore_materialization_backup,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::state::{AppStateLayout, ConfigStore};
use crate::storage::fs;
use crate::workspaces::{WorkspaceLifecycleLock, WorkspaceName};
use coral_spec::{ManifestCredentialMethodKind, ManifestInputKind, ManifestOAuthCredentialSpec};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

#[derive(Clone)]
pub(crate) struct SourceManager {
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    oauth_credential_service: OAuthCredentialService,
    layout: AppStateLayout,
    lifecycle_lock: WorkspaceLifecycleLock,
    diagnostic_reporter: SourceDiagnosticReporter,
    search_observations: Option<SearchObservationHandle>,
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
}

pub(crate) struct ImportSourceWithCredentialsCommand {
    pub(crate) manifest_yaml: String,
    pub(crate) bindings: SourceBindings,
    pub(crate) oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
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

pub(crate) enum ImportSourceWithCredentialsEvent {
    Authorization {
        input_key: String,
        authorization_url: String,
        expires_in_seconds: u64,
        user_code: Option<String>,
        verification_uri: Option<String>,
        verification_uri_complete: Option<String>,
    },
    CallbackReceived {
        input_key: String,
    },
    Completed {
        input_key: String,
        metadata: BTreeMap<String, String>,
    },
}

#[derive(Clone)]
pub(crate) struct ImportSourceEventSender {
    tx: mpsc::Sender<PendingImportSourceWithCredentialsEvent>,
}

pub(crate) struct PendingImportSourceWithCredentialsEvent {
    event: ImportSourceWithCredentialsEvent,
    delivered: oneshot::Sender<()>,
}

impl ImportSourceEventSender {
    pub(crate) fn new(tx: mpsc::Sender<PendingImportSourceWithCredentialsEvent>) -> Self {
        Self { tx }
    }

    async fn send(&self, event: ImportSourceWithCredentialsEvent) -> Result<(), AppError> {
        let (delivered, delivered_rx) = oneshot::channel();
        self.tx
            .send(PendingImportSourceWithCredentialsEvent { event, delivered })
            .await
            .map_err(|_closed| AppError::FailedPrecondition(import_stream_closed_message()))?;
        delivered_rx
            .await
            .map_err(|_closed| AppError::FailedPrecondition(import_stream_closed_message()))
    }
}

impl PendingImportSourceWithCredentialsEvent {
    pub(crate) fn into_event(self) -> ImportSourceWithCredentialsEvent {
        let _delivery = self.delivered.send(());
        self.event
    }
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

struct PersistSourceRequest<'a> {
    candidate: &'a CandidateSource,
    manifest_yaml: Option<&'a str>,
    bindings: ValidatedBindings,
    origin: SourceOrigin,
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
    pub(crate) fn new_for_tests(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
    ) -> Self {
        Self::new(
            config_store,
            credential_manager,
            layout,
            crate::workspaces::WorkspaceLifecycleLock::default(),
        )
    }

    #[cfg(test)]
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
        lifecycle_lock: WorkspaceLifecycleLock,
    ) -> Self {
        Self::with_diagnostic_reporter(
            config_store,
            credential_manager,
            layout,
            lifecycle_lock,
            SourceDiagnosticReporter::default(),
        )
    }

    pub(crate) fn with_diagnostic_reporter(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
        lifecycle_lock: WorkspaceLifecycleLock,
        diagnostic_reporter: SourceDiagnosticReporter,
    ) -> Self {
        Self {
            config_store,
            credential_manager,
            oauth_credential_service: OAuthCredentialService::new(),
            layout,
            lifecycle_lock,
            diagnostic_reporter,
            search_observations: None,
        }
    }

    pub(crate) fn with_search_observation_handle(
        mut self,
        search_observations: SearchObservationHandle,
    ) -> Self {
        self.search_observations = Some(search_observations);
        self
    }

    pub(crate) fn list_workspace_sources(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<InstalledSource>, AppError> {
        Ok(self
            .config_store
            .list_workspace_sources(workspace_name)?
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
            self.config_store.get_source(workspace_name, source_name)?,
        ))
    }

    pub(crate) fn get_source_info(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<CandidateSource, AppError> {
        match self.config_store.get_source(workspace_name, source_name) {
            Ok(source) => {
                return Ok(
                    resolve_installed_manifest(workspace_name, &source, &self.layout)?.candidate,
                );
            }
            Err(AppError::SourceNotFound(_)) => {}
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
        let installed_sources = self.config_store.list_workspace_sources(workspace_name)?;
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
            &candidate,
            &command.bindings,
            None,
            &bundled.manifest_yaml,
            SourceOrigin::Bundled,
        )
    }

    pub(crate) async fn create_bundled_source_with_oauth(
        &self,
        workspace_name: &WorkspaceName,
        command: CreateBundledSourceWithOAuthCommand,
        events: ImportSourceEventSender,
    ) -> Result<InstalledSource, AppError> {
        let bundled = load_bundled_source(&command.name)?;
        let candidate = self.describe_bundled_source(workspace_name, &bundled.manifest_yaml)?;
        self.install_source_with_oauth(
            workspace_name,
            &candidate,
            &command.bindings,
            command.oauth_credential_retrievals,
            events,
            None,
            &bundled.manifest_yaml,
            SourceOrigin::Bundled,
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
            &candidate,
            &command.bindings,
            Some(&manifest_yaml),
            &manifest_yaml,
            SourceOrigin::Imported,
        )
    }

    pub(crate) async fn import_source_with_credentials(
        &self,
        workspace_name: &WorkspaceName,
        command: ImportSourceWithCredentialsCommand,
        events: ImportSourceEventSender,
    ) -> Result<InstalledSource, AppError> {
        let manifest = parse_source_manifest_yaml(&command.manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let manifest_yaml = durable_import_manifest_yaml(&command.manifest_yaml, &manifest)?;
        let mut candidate = describe_manifest(&manifest_yaml, SourceOrigin::Imported, false)?;
        candidate.installed = self.source_exists(workspace_name, &candidate.name)?;
        self.install_source_with_oauth(
            workspace_name,
            &candidate,
            &command.bindings,
            command.oauth_credential_retrievals,
            events,
            Some(&manifest_yaml),
            &manifest_yaml,
            SourceOrigin::Imported,
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
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        manifest_yaml: Option<&str>,
        materialization_manifest_yaml: &str,
        origin: SourceOrigin,
    ) -> Result<InstalledSource, AppError> {
        let _lifecycle_guard = self.lifecycle_lock.lock();
        self.validate_runtime_schema_names_available(
            workspace_name,
            &candidate.name,
            materialization_manifest_yaml,
        )?;
        let stored_material = self.source_stored_material_for_validation(
            workspace_name,
            candidate,
            bindings,
            &BTreeSet::new(),
        )?;
        let bindings = validate_bindings(candidate, bindings, &stored_material)?;
        let materialization_inputs =
            materialization_inputs_from_bindings(&bindings, &stored_material);
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate,
                manifest_yaml,
                bindings,
                origin,
                materialization_tmp: self
                    .prepare_v4_materialization(
                        workspace_name,
                        candidate,
                        materialization_manifest_yaml,
                        &materialization_inputs,
                        origin,
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
    #[expect(
        clippy::too_many_arguments,
        reason = "Shared OAuth install tail for the source-lifecycle entry points; the parameters are the irreducible per-call inputs and a grouping struct would only relocate the list."
    )]
    async fn install_source_with_oauth(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        oauth_credential_retrievals: Vec<SourceOAuthCredentialRetrieval>,
        events: ImportSourceEventSender,
        manifest_yaml: Option<&str>,
        materialization_manifest_yaml: &str,
        origin: SourceOrigin,
    ) -> Result<InstalledSource, AppError> {
        self.validate_runtime_schema_names_available(
            workspace_name,
            &candidate.name,
            materialization_manifest_yaml,
        )?;
        let oauth_input_keys = oauth_credential_retrievals
            .iter()
            .map(|credential| credential.input_key.clone())
            .collect::<BTreeSet<_>>();
        let stored_material = self.source_stored_material_for_validation(
            workspace_name,
            candidate,
            bindings,
            &oauth_input_keys,
        )?;
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
        let _lifecycle_guard = self.lifecycle_lock.lock();
        self.validate_runtime_schema_names_available(
            workspace_name,
            &candidate.name,
            materialization_manifest_yaml,
        )?;
        let stored_material = self.source_stored_material_for_validation(
            workspace_name,
            candidate,
            bindings,
            &oauth_input_keys,
        )?;
        let mut validation_material = stored_material.clone();
        for material in &oauth_material {
            validation_material.insert(material.input_key.clone(), material.access_token.clone());
        }
        let mut bindings = validate_bindings(candidate, bindings, &validation_material)?;
        merge_oauth_material_into_bindings(&mut bindings, oauth_material)?;
        let materialization_inputs =
            materialization_inputs_from_bindings(&bindings, &stored_material);
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate,
                manifest_yaml,
                bindings,
                origin,
                materialization_tmp: self
                    .prepare_v4_materialization(
                        workspace_name,
                        candidate,
                        materialization_manifest_yaml,
                        &materialization_inputs,
                        origin,
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
        let _lifecycle_guard = self.lifecycle_lock.lock();
        let source_dir = self.layout.source_dir(workspace_name, source_name);
        let credential_set_id = CredentialSetId::for_source(source_name);
        let credential_guard = self
            .credential_manager
            .material_guard(workspace_name, &credential_set_id)?;
        let state_lock = self.config_store.state_lock_exclusive()?;
        let stored = self
            .config_store
            .get_source_unlocked(workspace_name, source_name)?;
        let removed = self.populate_source_version_or_keep(workspace_name, stored.clone());
        let credential_storage = stored.credential_storage_for_material();
        let credential_material = credential_storage
            .map(|storage| credential_guard.snapshot_material_with_state_lock_held(storage))
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
        let source_dir_backup = fs::DirectoryBackup::move_for_delete(&source_dir, source_name)?;
        if let Some(credential_storage) = credential_storage
            && let Err(error) =
                credential_guard.remove_material_with_state_lock_held(credential_storage)
        {
            let restore_dir_result = source_dir_backup.restore();
            self.restore_source_rollback_state_with_state_lock_held(
                workspace_name,
                source_name,
                Some(previous),
                None,
                &credential_guard,
            );
            if let Err(restore_error) = restore_dir_result {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to remove source credentials for '{source_name}': {error}; failed to restore source directory from '{}': {restore_error}",
                    source_dir_backup.backup_path().display()
                )));
            }
            return Err(error);
        }
        if let Err(error) = self
            .config_store
            .remove_source_unlocked(workspace_name, source_name)
        {
            let restore_dir_result = source_dir_backup.restore();
            self.restore_source_rollback_state_with_state_lock_held(
                workspace_name,
                source_name,
                Some(previous),
                None,
                &credential_guard,
            );
            if let Err(restore_error) = restore_dir_result {
                return Err(AppError::FailedPrecondition(format!(
                    "failed to remove source '{source_name}': {error}; failed to restore source directory from '{}': {restore_error}",
                    source_dir_backup.backup_path().display()
                )));
            }
            return Err(error);
        }
        source_dir_backup.commit()?;
        cleanup_empty_parent(&self.layout.workspaces_root(), source_dir.parent());
        cleanup_empty_parent(
            &self.layout.workspaces_root(),
            self.layout.workspace_dir(workspace_name).parent(),
        );
        drop(state_lock);
        self.diagnostic_reporter
            .clear_source(workspace_name, source_name);
        self.clear_source_lifecycle_search_state_best_effort(workspace_name, source_name);
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
        let state_lock = self.config_store.state_lock_exclusive()?;
        let credential_storage = match self.source_persist_storage_with_state_lock_held(
            workspace_name,
            request.candidate,
            &request.bindings,
        ) {
            Ok(storage) => storage,
            Err(error) => {
                cleanup_materialization_tmp(request.materialization_tmp.as_deref());
                return Err(error);
            }
        };
        let previous =
            self.load_source_rollback_state(workspace_name, &source_name, &credential_guard)?;
        if let Err(error) =
            self.persist_manifest_artifact(workspace_name, &source_name, request.manifest_yaml)
        {
            cleanup_materialization_tmp(request.materialization_tmp.as_deref());
            self.restore_source_rollback_state_with_state_lock_held(
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
            if let Some(requested_storage) = credential_storage {
                let expected_secret_keys = request
                    .candidate
                    .inputs
                    .iter()
                    .filter(|input| input.kind == ManifestInputKind::Secret)
                    .map(|input| input.key.clone())
                    .collect::<BTreeSet<_>>();
                let credential_write = match credential_guard.update_material_with_state_lock(
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
                        self.restore_source_rollback_state_with_state_lock_held(
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
                        self.restore_source_rollback_state_with_state_lock_held(
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
            origin: request.origin,
        };
        if let Err(error) = self
            .config_store
            .upsert_source_unlocked(workspace_name, stored.clone())
        {
            let restore_result = restore_materialization_backup(
                &self.layout,
                workspace_name,
                &source_name,
                materialization_backup,
            );
            self.restore_source_rollback_state_with_state_lock_held(
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
        drop(state_lock);
        self.clear_source_lifecycle_search_state_best_effort(workspace_name, &source_name);
        Ok(resolved)
    }

    fn clear_source_lifecycle_search_state_best_effort(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        self.clear_observed_values_for_source_lifecycle_best_effort(workspace_name, source_name);
        self.clear_catalog_projection_for_source_lifecycle_best_effort(workspace_name, source_name);
    }

    fn clear_observed_values_for_source_lifecycle_best_effort(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let Some(search_observations) = &self.search_observations else {
            return;
        };
        if !self.layout.search_sqlite_file(workspace_name).exists() {
            return;
        }
        if let Err(error) = search_observations.clear_source(workspace_name, source_name.as_str()) {
            warn!(
                workspace = %workspace_name,
                source = %source_name,
                "source lifecycle changed, but failed to clear observed-values state: {error}"
            );
        }
    }

    fn clear_catalog_projection_for_source_lifecycle_best_effort(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) {
        let search_sqlite_file = self.layout.search_sqlite_file(workspace_name);
        if !search_sqlite_file.exists() {
            return;
        }
        match SqliteSearchStore::open_workspace(&self.layout, workspace_name)
            .and_then(|store| store.clear_catalog_workspace())
        {
            Ok(result) => {
                tracing::debug!(
                    workspace = %workspace_name,
                    source = %source_name,
                    deleted_document_count = result.deleted_document_count,
                    "cleared SQLite catalog projection for source lifecycle change"
                );
            }
            Err(error) => {
                warn!(
                    workspace = %workspace_name,
                    source = %source_name,
                    search_sqlite_file = %search_sqlite_file.display(),
                    "source lifecycle changed, but failed to clear SQLite catalog projection: {error}"
                );
            }
        }
    }

    fn prepare_v4_materialization(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        manifest_yaml: &str,
        inputs: &MaterializationInputs,
        origin: SourceOrigin,
        suffix_prefix: &str,
    ) -> Result<Option<MaterializationBuild>, AppError> {
        let manifest = parse_source_manifest_yaml(manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let Some(v4) = manifest.as_v4() else {
            return Ok(None);
        };
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
        manifest_yaml: &str,
    ) -> Result<(), AppError> {
        let candidate_manifest = parse_source_manifest_yaml(manifest_yaml)
            .map_err(|error| AppError::InvalidInput(error.to_string()))?;
        let candidate_schema_names = runtime_schema_names(&candidate_manifest);
        for installed in self.config_store.list_workspace_sources(workspace_name)? {
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

    fn source_exists(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<bool, AppError> {
        Ok(self
            .config_store
            .load_catalog()?
            .contains(workspace_name, source_name))
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
        let (credential_storage, persisted_secret_keys) = match self
            .config_store
            .get_source(workspace_name, &candidate.name)
        {
            Ok(source) => (
                source.credential_storage_for_material(),
                Some(source.secrets.iter().cloned().collect::<BTreeSet<_>>()),
            ),
            Err(AppError::SourceNotFound(_))
                if self
                    .layout
                    .secret_file(workspace_name, &candidate.name)
                    .exists() =>
            {
                (Some(CredentialStorageKind::File), None)
            }
            Err(AppError::SourceNotFound(_)) => (None, Some(BTreeSet::new())),
            Err(error) => return Err(error),
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

    fn source_persist_storage_with_state_lock_held(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        bindings: &ValidatedBindings,
    ) -> Result<Option<CredentialStorageKind>, AppError> {
        let needs_stored_material = candidate.inputs.iter().any(|input| {
            input.kind == ManifestInputKind::Secret
                && input.required
                && !bindings.secrets.contains_key(&input.key)
        });
        let existing_storage = match self
            .config_store
            .get_source_unlocked(workspace_name, &candidate.name)
        {
            Ok(source) => source.credential_storage_for_material(),
            Err(AppError::SourceNotFound(_)) if needs_stored_material => {
                let legacy_secret_file = self.layout.secret_file(workspace_name, &candidate.name);
                if legacy_secret_file.is_file() {
                    Some(CredentialStorageKind::File)
                } else {
                    None
                }
            }
            Err(AppError::SourceNotFound(_)) => None,
            Err(error) => return Err(error),
        };
        let stored_material = match existing_storage {
            Some(storage) => self.read_source_material(workspace_name, &candidate.name, storage)?,
            None => BTreeMap::new(),
        };
        validate_required_secret_material(candidate, bindings, &stored_material)?;

        if existing_storage.is_some() {
            return Ok(existing_storage);
        }
        if bindings.secrets.is_empty() && stored_material.is_empty() {
            Ok(None)
        } else {
            self.credential_manager.default_write_storage().map(Some)
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
        events: ImportSourceEventSender,
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
            let authorization_input_key = input_key.clone();
            let authorization_events = events.clone();
            let callback_input_key = input_key.clone();
            let callback_events = events.clone();
            let material = self
                .oauth_credential_service
                .authorize_with_callback(
                    StartOAuthCredentialRequest {
                        input_key: &input_key,
                        oauth: config.oauth,
                        source_inputs,
                        credential_inputs,
                    },
                    move |authorization| {
                        let events = authorization_events;
                        async move {
                            events
                                .send(ImportSourceWithCredentialsEvent::Authorization {
                                    input_key: authorization_input_key,
                                    authorization_url: authorization.authorization_url,
                                    expires_in_seconds: authorization.expires_in_seconds,
                                    user_code: authorization.user_code,
                                    verification_uri: authorization.verification_uri,
                                    verification_uri_complete: authorization
                                        .verification_uri_complete,
                                })
                                .await
                        }
                    },
                    move || {
                        let events = callback_events;
                        async move {
                            events
                                .send(ImportSourceWithCredentialsEvent::CallbackReceived {
                                    input_key: callback_input_key,
                                })
                                .await?;
                            tokio::task::yield_now().await;
                            Ok(())
                        }
                    },
                )
                .await?;
            events
                .send(ImportSourceWithCredentialsEvent::Completed {
                    input_key: material.input_key.clone(),
                    metadata: material.safe_metadata.clone(),
                })
                .await?;
            // Let the streaming response flush the OAuth completion event before
            // this task continues into synchronous source installation work.
            tokio::task::yield_now().await;
            materials.push(material);
        }
        Ok(materials)
    }

    fn load_source_rollback_state(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        credential_material: &CredentialMaterialGuard<'_>,
    ) -> Result<Option<SourceRollbackState>, AppError> {
        let source = match self
            .config_store
            .get_source_unlocked(workspace_name, source_name)
        {
            Ok(source) => source,
            Err(AppError::SourceNotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let credential_material = source
            .credential_storage_for_material()
            .map(|credential_storage| {
                credential_material.snapshot_material_with_state_lock_held(credential_storage)
            })
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

    fn restore_source_rollback_state_with_state_lock_held(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        previous: Option<SourceRollbackState>,
        new_material_storage: Option<CredentialStorageKind>,
        credential_material: &CredentialMaterialGuard<'_>,
    ) {
        if let Some(previous) = previous {
            let manifest_path = self.layout.manifest_file(workspace_name, source_name);
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
                    if let Err(e) =
                        credential_material.restore_material_with_state_lock_held(&snapshot)
                    {
                        warn!("rollback: failed to restore source credential material: {e}");
                    }
                }
                None => {
                    if let Some(storage) = new_material_storage
                        && let Err(e) =
                            credential_material.remove_material_with_state_lock_held(storage)
                    {
                        warn!("rollback: failed to remove new source credential material: {e}");
                    }
                }
            }
            if let Err(e) = self
                .config_store
                .upsert_source_unlocked(workspace_name, previous.source)
            {
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
                && let Err(e) = credential_material.remove_material_with_state_lock_held(storage)
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

fn validate_required_secret_material(
    candidate: &CandidateSource,
    bindings: &ValidatedBindings,
    stored_material: &BTreeMap<String, String>,
) -> Result<(), AppError> {
    for input in &candidate.inputs {
        if input.kind == ManifestInputKind::Secret
            && input.required
            && !bindings.secrets.contains_key(&input.key)
            && !stored_material.contains_key(&input.key)
        {
            return Err(AppError::InvalidInput(format!(
                "missing required source secret '{}'",
                input.key
            )));
        }
    }
    Ok(())
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

fn import_stream_closed_message() -> String {
    "source import stream closed".to_string()
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
        let SurfaceDescriptor::File { file } = &surface.descriptor else {
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
    use std::path::Path;
    use std::sync::mpsc as std_mpsc;
    use std::thread;
    use std::time::Duration;

    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use url::Url;

    use super::{
        ImportSourceCommand, ImportSourceEventSender, ImportSourceWithCredentialsCommand,
        ImportSourceWithCredentialsEvent, PendingImportSourceWithCredentialsEvent,
        PersistSourceRequest, SourceBinding, SourceBindings, SourceManager,
        SourceOAuthCredentialRetrieval, ValidatedBindings, materialization_inputs_from_bindings,
        normalize_binding_key, source_needs_stored_material_for_validation,
    };
    use crate::credentials::{
        CredentialManager, CredentialSetId, CredentialStorageKind, CredentialStoragePreference,
        CredentialStore,
    };
    use crate::sources::SourceName;
    use crate::sources::catalog::describe_manifest;
    use crate::sources::materialization::{FINGERPRINT_FILENAME, PROJECTIONS_FILENAME};
    use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;
    use coral_spec::{ManifestInputKind, ManifestInputSpec};

    fn default_workspace() -> WorkspaceName {
        WorkspaceName::default()
    }

    fn manifest_with_secret() -> String {
        r#"
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
base_url: "{{input.API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.API_TOKEN}}
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#
        .to_string()
    }

    fn v4_openapi_fixture() -> &'static str {
        r"
openapi: 3.0.3
paths:
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
"
    }

    fn v4_openapi_fixture_with_metadata() -> &'static str {
        r"
openapi: 3.0.3
info:
  title: GitHub
  description: Query GitHub issues.
servers:
  - url: https://api.github.test
paths:
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
"
    }

    fn v4_openapi_fixture_with_defaulted_input_server_url() -> &'static str {
        r#"
openapi: 3.0.3
servers:
  - url: "{apiBase}"
    variables:
      apiBase:
        default: "{{input.API_BASE|https://fallback.example.com}}"
paths:
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
"#
    }

    fn manifest_v4_with_file_descriptor(openapi_file: &std::path::Path) -> String {
        format!(
            r#"
name: github_v4_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    inputs:
      API_BASE:
        kind: variable
        default: http://127.0.0.1:1
    base_url: "{{{{input.API_BASE}}}}"
"#,
            openapi_file.display()
        )
    }

    fn manifest_v4_with_surface_namespace(
        openapi_file: &std::path::Path,
        source_name: &str,
        namespace_suffix: &str,
    ) -> String {
        format!(
            r#"
name: {source_name}
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: {namespace_suffix}
    type: openapi
    file: {}
    inputs:
      API_BASE:
        kind: variable
        default: http://127.0.0.1:1
    base_url: "{{{{input.API_BASE}}}}"
"#,
            openapi_file.display()
        )
    }

    fn manifest_v4_with_input_and_derived_base_url(openapi_file: &std::path::Path) -> String {
        format!(
            r"
name: github_v4_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    inputs:
      API_BASE:
        kind: variable
        default: https://api.example.com
",
            openapi_file.display()
        )
    }

    fn manifest_v4_without_description_or_base_url(openapi_file: &std::path::Path) -> String {
        format!(
            r"
name: github_v4_test
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
",
            openapi_file.display()
        )
    }

    fn manifest_without_secrets() -> String {
        r#"
name: public_messages
version: 0.1.0
dsl_version: 3
backend: http
base_url: "https://example.com"
tables:
  - name: messages
    description: Public messages
    request:
      method: GET
      path: /messages
    response: {}
    columns:
      - name: id
        type: Utf8
"#
        .to_string()
    }

    #[test]
    fn readd_source_waits_for_state_lock_before_replacing_manifest() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let manager = SourceManager::new(
            config_store.clone(),
            credential_manager,
            layout.clone(),
            crate::workspaces::WorkspaceLifecycleLock::default(),
        );

        let workspace_name = default_workspace();
        let original_manifest = manifest_without_secrets();
        manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: original_manifest.clone(),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("initial import");

        let source_name = SourceName::parse("public_messages").expect("source");
        let manifest_path = layout.manifest_file(&workspace_name, &source_name);
        let stored_before = std::fs::read_to_string(&manifest_path).expect("stored manifest");
        let state_lock = config_store.state_lock_shared().expect("shared state lock");

        let updated_manifest =
            original_manifest.replace("https://example.com", "https://replacement.example.com");
        let persist_manager = manager.clone();
        let persist_workspace_name = workspace_name.clone();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (done_tx, done_rx) = std_mpsc::channel();
        let handle = thread::spawn(move || {
            let candidate = describe_manifest(&updated_manifest, SourceOrigin::Imported, false)
                .expect("describe manifest");
            let bindings = ValidatedBindings {
                variables: BTreeMap::new(),
                secrets: BTreeMap::new(),
                replaced_oauth_inputs: BTreeSet::new(),
            };
            started_tx.send(()).expect("send started");
            let result = persist_manager
                .persist_source(
                    &persist_workspace_name,
                    PersistSourceRequest {
                        candidate: &candidate,
                        manifest_yaml: Some(&updated_manifest),
                        bindings,
                        origin: SourceOrigin::Imported,
                        materialization_tmp: None,
                    },
                )
                .map(|source| source.name.as_str().to_string())
                .map_err(|error| error.to_string());
            done_tx.send(result).expect("send import result");
        });

        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("import thread should start");
        match done_rx.recv_timeout(Duration::from_millis(300)) {
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                panic!("source re-add thread exited before sending a result")
            }
            Ok(result) => {
                panic!("source re-add completed while shared state lock was held: {result:?}")
            }
        }
        let stored_during_lock = std::fs::read_to_string(&manifest_path).expect("stored manifest");
        assert_eq!(stored_during_lock, stored_before);

        drop(state_lock);
        let result = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("source re-add should finish after releasing the state lock")
            .expect("source re-add should succeed");
        assert_eq!(result, "public_messages");
        handle.join().expect("join source re-add thread");

        let stored_after = std::fs::read_to_string(&manifest_path).expect("stored manifest");
        assert!(stored_after.contains("https://replacement.example.com"));
    }

    #[test]
    fn readd_source_revalidates_required_secret_material_under_state_lock() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let manager = SourceManager::new(
            config_store,
            credential_manager.clone(),
            layout,
            crate::workspaces::WorkspaceLifecycleLock::default(),
        );

        let workspace_name = default_workspace();
        manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: Vec::new(),
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect("initial import");

        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        credential_manager
            .material_guard(&workspace_name, &credential_set_id)
            .expect("credential guard")
            .remove_material(CredentialStorageKind::File)
            .expect("remove stored material");
        let manifest = manifest_with_secret();
        let candidate =
            describe_manifest(&manifest, SourceOrigin::Imported, false).expect("describe manifest");
        let bindings = ValidatedBindings {
            variables: BTreeMap::new(),
            secrets: BTreeMap::new(),
            replaced_oauth_inputs: BTreeSet::new(),
        };

        let error = manager
            .persist_source(
                &workspace_name,
                PersistSourceRequest {
                    candidate: &candidate,
                    manifest_yaml: Some(&manifest),
                    bindings,
                    origin: SourceOrigin::Imported,
                    materialization_tmp: None,
                },
            )
            .expect_err("re-add without current required material should fail");

        assert!(
            error
                .to_string()
                .contains("missing required source secret 'API_TOKEN'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn delete_source_waits_for_state_lock_before_removing_credentials() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let manager = SourceManager::new(
            config_store.clone(),
            credential_manager.clone(),
            layout,
            crate::workspaces::WorkspaceLifecycleLock::default(),
        );

        let workspace_name = default_workspace();
        manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: Vec::new(),
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect("initial import");

        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let state_lock = config_store.state_lock_shared().expect("shared state lock");
        let delete_manager = manager.clone();
        let delete_workspace_name = workspace_name.clone();
        let delete_source_name = source_name.clone();
        let (started_tx, started_rx) = std_mpsc::channel();
        let (done_tx, done_rx) = std_mpsc::channel();
        let handle = thread::spawn(move || {
            started_tx.send(()).expect("send started");
            let result = delete_manager
                .delete_source(&delete_workspace_name, &delete_source_name)
                .map(|source| source.name.as_str().to_string())
                .map_err(|error| error.to_string());
            done_tx.send(result).expect("send delete result");
        });

        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("delete thread should start");
        match done_rx.recv_timeout(Duration::from_millis(300)) {
            Err(std_mpsc::RecvTimeoutError::Timeout) => {}
            Err(std_mpsc::RecvTimeoutError::Disconnected) => {
                panic!("source delete thread exited before sending a result")
            }
            Ok(result) => {
                panic!("source delete completed while shared state lock was held: {result:?}")
            }
        }
        let material = credential_manager
            .read_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material during shared lock");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("secret-token")
        );

        drop(state_lock);
        let result = done_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("source delete should finish after releasing the state lock")
            .expect("source delete should succeed");
        assert_eq!(result, "secured_messages");
        handle.join().expect("join source delete thread");
        let material = credential_manager
            .read_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material after delete");
        assert!(material.is_empty());
    }

    #[cfg(unix)]
    #[test]
    fn delete_source_preserves_credentials_when_directory_staging_fails() {
        use std::os::unix::fs::PermissionsExt as _;

        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let manager = SourceManager::new(
            config_store.clone(),
            credential_manager.clone(),
            layout,
            crate::workspaces::WorkspaceLifecycleLock::default(),
        );

        let workspace_name = default_workspace();
        manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: Vec::new(),
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect("initial import");

        let source_name = SourceName::parse("secured_messages").expect("source");
        let source_dir = manager.layout.source_dir(&workspace_name, &source_name);
        let source_parent = source_dir.parent().expect("source parent");
        let original_permissions = std::fs::metadata(source_parent)
            .expect("source parent metadata")
            .permissions();
        let mut readonly_permissions = original_permissions.clone();
        readonly_permissions.set_mode(0o500);
        std::fs::set_permissions(source_parent, readonly_permissions)
            .expect("make source parent unwritable");

        let delete_result = manager.delete_source(&workspace_name, &source_name);

        std::fs::set_permissions(source_parent, original_permissions)
            .expect("restore source parent permissions");
        let error = delete_result.expect_err("directory staging should fail");
        assert!(
            matches!(error, crate::bootstrap::AppError::Io(_)),
            "unexpected delete error: {error}"
        );
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let material = credential_manager
            .read_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read credential material after staging failure");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("secret-token"),
            "credential material should be preserved when directory staging fails"
        );
        assert!(
            config_store
                .get_source(&workspace_name, &source_name)
                .is_ok(),
            "source config should be preserved when directory staging fails"
        );
        assert!(
            source_dir.exists(),
            "source directory should remain when staging fails"
        );
    }

    fn manifest_with_oauth_secret(token_url: &str, redirect_port: u16) -> String {
        format!(
            r#"
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
base_url: "{{{{input.API_BASE}}}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{{{input.API_TOKEN}}}}
tables:
  - name: messages
    description: Secured messages
    request:
      method: GET
      path: /messages
    response: {{}}
    columns:
      - name: id
        type: Utf8
"#
        )
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

    fn v4_manifest_with_templated_oauth_endpoints(
        openapi_file: &std::path::Path,
        token_url: &str,
        redirect_port: u16,
    ) -> (String, String) {
        let token_url_template = token_url.replace("/token", "/{{input.OUTLOOK_TENANT_ID}}/token");
        let rendered_token_url = token_url.replace("/token", "/organizations/token");
        let manifest = format!(
            r#"
name: secured_messages
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    file: {}
    inputs:
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
                  authorization_url: https://provider.example.com/{{{{input.OUTLOOK_TENANT_ID}}}}/oauth/authorize
                  token_url: {token_url_template}
                client:
                  id:
                    default: default-client
      OUTLOOK_TENANT_ID:
        kind: variable
      API_BASE:
        kind: variable
    base_url: "{{{{input.API_BASE}}}}"
    auth:
      type: HeaderAuth
      headers:
        - name: Authorization
          from: template
          template: Bearer {{{{input.API_TOKEN}}}}
"#,
            openapi_file.display()
        );
        (manifest, rendered_token_url)
    }

    fn oauth_import_bindings_with_tenant() -> SourceBindings {
        SourceBindings {
            variables: vec![
                SourceBinding {
                    key: "API_BASE".to_string(),
                    value: "https://api.example.test".to_string(),
                },
                SourceBinding {
                    key: "OUTLOOK_TENANT_ID".to_string(),
                    value: "organizations".to_string(),
                },
            ],
            secrets: Vec::new(),
        }
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
        let manager =
            SourceManager::new_for_tests(config_store.clone(), credential_manager, layout);
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
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());

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
        let temp = TempDir::new().expect("temp dir");
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(&openapi_file, v4_openapi_fixture()).expect("write fixture");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());

        let installed = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_v4_with_file_descriptor(&openapi_file),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import v4 source");

        assert_eq!(installed.name.as_str(), "github_v4_test");
        let source_name = SourceName::parse("github_v4_test").expect("source");
        let materialized = layout.v4_materialized_dir(&default_workspace(), &source_name);
        assert!(materialized.join(FINGERPRINT_FILENAME).exists());
        assert!(materialized.join(PROJECTIONS_FILENAME).exists());
        assert!(
            materialized
                .join("surfaces")
                .join("rest")
                .join("semantic-ir.yaml")
                .exists()
        );

        let info = manager
            .get_source_info(&default_workspace(), &source_name)
            .expect("installed v4 source should be usable");
        assert_eq!(info.name.as_str(), "github_v4_test");
    }

    #[test]
    fn import_v4_source_rejects_runtime_schema_collision_before_persistence() {
        let temp = TempDir::new().expect("temp dir");
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(&openapi_file, v4_openapi_fixture()).expect("write fixture");
        let manager = SourceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
        );

        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_without_secrets()
                        .replace("public_messages", "github_v4_rest"),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("install existing source");

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_v4_with_surface_namespace(
                        &openapi_file,
                        "github_v4",
                        "rest",
                    ),
                    bindings: SourceBindings::default(),
                },
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
            !layout
                .v4_materialized_dir(&default_workspace(), &rejected_source)
                .exists(),
            "rejected source should not materialize artifacts"
        );
    }

    #[test]
    fn import_v4_source_rejects_derived_base_url_input_token_defaults() {
        let temp = TempDir::new().expect("temp dir");
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(
            &openapi_file,
            v4_openapi_fixture_with_defaulted_input_server_url(),
        )
        .expect("write fixture");
        let manager = SourceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
        );

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_v4_with_input_and_derived_base_url(&openapi_file),
                    bindings: SourceBindings::default(),
                },
            )
            .expect_err("source add should reject derived base_url input token defaults");

        let message = error.to_string();
        assert!(
            message.contains("derived OpenAPI server base_url input token"),
            "unexpected error: {message}"
        );
        assert!(
            !layout
                .v4_materialized_dir(
                    &default_workspace(),
                    &SourceName::parse("github_v4_test").expect("source")
                )
                .exists(),
            "failed materialization should not install artifacts"
        );
    }

    #[test]
    fn import_v4_source_rejects_unresolved_relative_descriptor() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let manager = SourceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout,
        );

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_v4_with_file_descriptor(Path::new("openapi.yaml")),
                    bindings: SourceBindings::default(),
                },
            )
            .expect_err("raw relative descriptors should fail in app import");

        assert!(
            error
                .to_string()
                .contains("imported DSL v4 manifests must use absolute file descriptors"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn import_v4_source_preserves_intent_yaml_without_openapi_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let openapi_file = descriptor_temp.path().join("github-openapi.yaml");
        std::fs::write(&openapi_file, v4_openapi_fixture_with_metadata()).expect("write fixture");
        let manager = SourceManager::new_for_tests(
            ConfigStore::new(layout.clone()),
            CredentialManager::new(CredentialStore::new(layout.clone())),
            layout.clone(),
        );

        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_v4_without_description_or_base_url(&openapi_file),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import v4 source");

        let source_name = SourceName::parse("github_v4_test").expect("source");
        let stored_manifest =
            std::fs::read_to_string(layout.manifest_file(&default_workspace(), &source_name))
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
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());

        let source_name = SourceName::parse("secured_messages").expect("source");
        let source_dir = layout.source_dir(&default_workspace(), &source_name);
        std::fs::create_dir_all(&source_dir).expect("create source dir");
        std::fs::create_dir(source_dir.join("secrets.env"))
            .expect("create blocking secrets directory");

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![SourceBinding {
                            key: "API_BASE".to_string(),
                            value: "https://example.com".to_string(),
                        }],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect_err("secret persistence should fail");

        assert!(
            matches!(
                error,
                crate::bootstrap::AppError::Credentials(crate::credentials::CredentialsError::Io(
                    _
                ))
            ),
            "unexpected error: {error:#}"
        );
        assert!(
            !layout
                .source_dir(&default_workspace(), &source_name)
                .exists(),
            "source dir should be cleaned up after secret persistence failure"
        );
        assert!(
            manager
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

    #[test]
    fn rejects_env_file_breaking_binding_keys() {
        let error = normalize_binding_key("source secret key", "API=TOKEN")
            .expect_err("'=' should be rejected");
        assert!(
            error
                .to_string()
                .contains("must not contain '=', '\\n', or '\\r'")
        );

        let error = normalize_binding_key("source secret key", "API\nTOKEN")
            .expect_err("newlines should be rejected");
        assert!(
            error
                .to_string()
                .contains("must not contain '=', '\\n', or '\\r'")
        );

        let error = normalize_binding_key("source secret key", " #comment")
            .expect_err("leading comment markers should be rejected");
        assert!(error.to_string().contains("must not start with '#'"));
    }

    #[test]
    fn rejects_reserved_internal_binding_keys() {
        let error = normalize_binding_key("source secret key", "__coral.API_TOKEN")
            .expect_err("reserved prefix should be rejected");
        assert!(
            error
                .to_string()
                .contains("must not start with reserved prefix '__coral'")
        );
    }

    #[test]
    fn import_materializes_variable_defaults_server_side() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager = SourceManager::new_for_tests(config_store, credential_manager, layout);

        let source = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect("import source");

        assert_eq!(
            source.variables.get("API_BASE").map(String::as_str),
            Some("https://example.com")
        );
    }

    #[test]
    fn import_new_source_uses_keychain_when_auto_probe_succeeds() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::with_available_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Auto,
        );
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout.clone());
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);

        let source = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect("import source");

        assert_eq!(
            source.credential_storage,
            Some(CredentialStorageKind::Keychain)
        );
        assert!(
            !layout
                .secret_file(&default_workspace(), &source_name)
                .exists(),
            "keychain-routed install should not create plaintext material"
        );
        let stored = credential_manager
            .read_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::Keychain,
            )
            .expect("read keychain material");
        assert_eq!(
            stored.get("API_TOKEN").map(String::as_str),
            Some("secret-token")
        );

        manager
            .delete_source(&default_workspace(), &source_name)
            .expect("delete source");
        assert!(
            credential_manager
                .read_material(
                    &default_workspace(),
                    &credential_set_id,
                    CredentialStorageKind::Keychain,
                )
                .expect("read removed keychain material")
                .is_empty(),
            "delete should remove keychain-routed material"
        );
    }

    #[test]
    fn import_source_without_secret_material_does_not_probe_keychain() {
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
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());
        let source_name = SourceName::parse("public_messages").expect("source");

        let source = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_without_secrets(),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import source");

        assert!(source.secrets.is_empty());
        assert_eq!(source.credential_storage, None);
        assert!(
            !layout
                .secret_file(&default_workspace(), &source_name)
                .exists(),
            "credential material should not be created for a source without secrets"
        );
        let config_raw =
            std::fs::read_to_string(layout.config_file()).expect("read rendered config");
        assert!(
            !config_raw.contains("credential_storage"),
            "source without credential material should not persist a storage route"
        );
    }

    #[test]
    fn import_missing_secret_does_not_probe_keychain_for_new_source() {
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
        let manager = SourceManager::new_for_tests(config_store, credential_manager, layout);

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings::default(),
                },
            )
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
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());

        let source_name = SourceName::parse("secured_messages").expect("source");
        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "old-token".to_string(),
                        }],
                    },
                },
            )
            .expect("initial import");

        let secret_path = layout.secret_file(&default_workspace(), &source_name);
        std::fs::write(&secret_path, "BROKEN\n").expect("write malformed credential material");

        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "new-token".to_string(),
                        }],
                    },
                },
            )
            .expect("replace malformed credential material");

        assert_eq!(
            std::fs::read_to_string(&secret_path).expect("read replaced credential material"),
            "API_TOKEN=new-token\n"
        );
    }

    #[test]
    fn delete_removes_source_with_malformed_credential_material() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());

        let source_name = SourceName::parse("secured_messages").expect("source");
        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "secret-token".to_string(),
                        }],
                    },
                },
            )
            .expect("initial import");

        let secret_path = layout.secret_file(&default_workspace(), &source_name);
        std::fs::write(&secret_path, "BROKEN\n").expect("write malformed credential material");

        manager
            .delete_source(&default_workspace(), &source_name)
            .expect("delete source with malformed credential material");

        assert!(
            !secret_path.exists(),
            "delete should remove malformed credential material"
        );
        assert!(
            manager
                .list_workspace_sources(&default_workspace())
                .expect("list sources")
                .is_empty(),
            "source config should be removed"
        );
    }

    #[test]
    fn import_accepts_secret_already_populated_in_credential_material() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout);
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        credential_manager
            .replace_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([
                    ("API_TOKEN".to_string(), "oauth-token".to_string()),
                    (
                        "__coral_oauth.QVBJX1RPS0VO.method".to_string(),
                        "oauth".to_string(),
                    ),
                ]),
            )
            .expect("seed credential material");

        let source = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings::default(),
                },
            )
            .expect("import source");

        assert_eq!(source.secrets, vec!["API_TOKEN"]);
        let material = credential_manager
            .read_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("oauth-token")
        );
        assert_eq!(
            material
                .get("__coral_oauth.QVBJX1RPS0VO.method")
                .map(String::as_str),
            Some("oauth")
        );
    }

    #[test]
    fn import_preserves_credential_store_io_errors_when_material_is_needed() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager, layout.clone());
        let source_name = SourceName::parse("secured_messages").expect("source");
        let secret_path = layout.secret_file(&default_workspace(), &source_name);
        std::fs::create_dir_all(&secret_path).expect("create blocking secret directory");

        let error = manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings::default(),
                },
            )
            .expect_err("stored material I/O error should fail import");

        assert!(
            matches!(
                error,
                crate::bootstrap::AppError::Credentials(crate::credentials::CredentialsError::Io(
                    _
                ))
            ),
            "unexpected error: {error:#}"
        );
    }

    #[test]
    fn manual_secret_reimport_clears_prior_oauth_material() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout);
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        credential_manager
            .replace_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
                &BTreeMap::from([
                    ("API_TOKEN".to_string(), "oauth-token".to_string()),
                    (
                        "__coral_oauth.QVBJX1RPS0VO.refresh_token".to_string(),
                        "refresh-token".to_string(),
                    ),
                    (
                        "__coral_oauth.QVBJX1RPS0VO.method".to_string(),
                        "oauth".to_string(),
                    ),
                ]),
            )
            .expect("seed credential material");

        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: Vec::new(),
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "manual-token".to_string(),
                        }],
                    },
                },
            )
            .expect("import source");

        let material = credential_manager
            .read_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("manual-token")
        );
        assert!(
            !material
                .keys()
                .any(|key| key.starts_with("__coral_oauth.QVBJX1RPS0VO.")),
            "manual secret replacement should clear stale OAuth metadata"
        );
    }

    #[test]
    fn source_rollback_snapshots_credentials_after_refresh_lock() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store.clone());
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout.clone());
        let workspace_name = default_workspace();
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        manager
            .import_source(
                &workspace_name,
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: Vec::new(),
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "old-token".to_string(),
                        }],
                    },
                },
            )
            .expect("install source");
        let refresh_lock = credential_store
            .credential_refresh_lock(&workspace_name, &credential_set_id)
            .expect("hold refresh lock");
        let config_temp_path = layout
            .config_file()
            .with_file_name(format!("config.toml.tmp.{}", std::process::id()));
        std::fs::create_dir_all(&config_temp_path).expect("block config save temp path");
        let (started_tx, started_rx) = std_mpsc::channel();
        let import_manager = manager.clone();
        let import_workspace = workspace_name.clone();
        let import_handle = thread::spawn(move || {
            started_tx.send(()).expect("signal import start");
            import_manager.import_source(
                &import_workspace,
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: Vec::new(),
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "manual-token".to_string(),
                        }],
                    },
                },
            )
        });
        started_rx.recv().expect("wait for import thread");
        thread::sleep(Duration::from_millis(50));
        credential_store
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

        let material = credential_manager
            .read_material(
                &workspace_name,
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("refreshed-token")
        );
        assert_eq!(
            material
                .get("__coral_oauth.QVBJX1RPS0VO.refresh_token")
                .map(String::as_str),
            Some("refreshed-refresh-token")
        );
    }

    #[tokio::test]
    async fn import_with_oauth_persists_retrieved_material() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout);
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let fixture = OAuthFixture::new();
        let redirect_port = free_loopback_port();
        let (manifest_yaml, rendered_token_url) =
            manifest_with_templated_oauth_endpoints(&fixture.token_url, redirect_port);
        assert!(
            manifest_yaml.find("  API_TOKEN:").expect("API_TOKEN input")
                < manifest_yaml
                    .find("  OUTLOOK_TENANT_ID:")
                    .expect("tenant input"),
            "tenant variable should exercise manifest order after the OAuth secret"
        );
        let (event_tx, mut event_rx) = import_event_channel();
        let workspace_name = default_workspace();
        let import = manager.import_source_with_credentials(
            &workspace_name,
            ImportSourceWithCredentialsCommand {
                manifest_yaml,
                bindings: oauth_import_bindings_with_tenant(),
                oauth_credential_retrievals: vec![SourceOAuthCredentialRetrieval {
                    input_key: "API_TOKEN".to_string(),
                    method_index: 0,
                    credential_inputs: Vec::new(),
                }],
            },
            event_tx,
        );
        let callback = authorize_oauth_import(&mut event_rx, redirect_port);

        let (source, ()) = tokio::join!(import, callback);
        let source = source.expect("import source with OAuth");
        assert_eq!(source.secrets, vec!["API_TOKEN"]);
        let captured = fixture.token_server.await.expect("token server");
        assert_eq!(
            captured.form.get("code").map(String::as_str),
            Some("test-code")
        );
        let material = credential_manager
            .read_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("access-token")
        );
        assert_eq!(
            material
                .get("__coral_oauth.QVBJX1RPS0VO.method")
                .map(String::as_str),
            Some("oauth")
        );
        assert_eq!(
            material
                .get("__coral_oauth.QVBJX1RPS0VO.token_url")
                .map(String::as_str),
            Some(rendered_token_url.as_str())
        );
    }

    #[tokio::test]
    async fn import_v4_with_oauth_persists_retrieved_material() {
        let descriptor_temp = TempDir::new().expect("descriptor temp dir");
        let openapi_file = descriptor_temp.path().join("openapi.yaml");
        std::fs::write(&openapi_file, v4_openapi_fixture()).expect("write descriptor");

        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout);
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let fixture = OAuthFixture::new();
        let redirect_port = free_loopback_port();
        let (manifest_yaml, rendered_token_url) = v4_manifest_with_templated_oauth_endpoints(
            &openapi_file,
            &fixture.token_url,
            redirect_port,
        );
        assert!(
            manifest_yaml
                .find("      API_TOKEN:")
                .expect("API_TOKEN input")
                < manifest_yaml
                    .find("      OUTLOOK_TENANT_ID:")
                    .expect("tenant input"),
            "tenant variable should exercise v4 surface input order after the OAuth secret"
        );
        let (event_tx, mut event_rx) = import_event_channel();
        let workspace_name = default_workspace();
        let import = manager.import_source_with_credentials(
            &workspace_name,
            ImportSourceWithCredentialsCommand {
                manifest_yaml,
                bindings: oauth_import_bindings_with_tenant(),
                oauth_credential_retrievals: vec![SourceOAuthCredentialRetrieval {
                    input_key: "API_TOKEN".to_string(),
                    method_index: 0,
                    credential_inputs: Vec::new(),
                }],
            },
            event_tx,
        );
        let callback = authorize_oauth_import(&mut event_rx, redirect_port);

        let (source, ()) = tokio::join!(import, callback);
        let source = source.expect("import v4 source with OAuth");
        assert_eq!(source.secrets, vec!["API_TOKEN"]);
        let captured = fixture.token_server.await.expect("token server");
        assert_eq!(
            captured.form.get("code").map(String::as_str),
            Some("test-code")
        );
        let material = credential_manager
            .read_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("access-token")
        );
        assert_eq!(
            material
                .get("__coral_oauth.QVBJX1RPS0VO.method")
                .map(String::as_str),
            Some("oauth")
        );
        assert_eq!(
            material
                .get("__coral_oauth.QVBJX1RPS0VO.token_url")
                .map(String::as_str),
            Some(rendered_token_url.as_str())
        );
    }

    #[tokio::test]
    async fn import_with_oauth_does_not_overwrite_installed_credentials_when_validation_fails() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager =
            SourceManager::new_for_tests(config_store, credential_manager.clone(), layout);
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        manager
            .import_source(
                &default_workspace(),
                &ImportSourceCommand {
                    manifest_yaml: manifest_with_secret(),
                    bindings: SourceBindings {
                        variables: vec![],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "old-token".to_string(),
                        }],
                    },
                },
            )
            .expect("install source");

        let redirect_port = free_loopback_port();
        let (event_tx, mut event_rx) = import_event_channel();
        let workspace_name = default_workspace();
        let error = manager
            .import_source_with_credentials(
                &workspace_name,
                ImportSourceWithCredentialsCommand {
                    manifest_yaml: manifest_with_oauth_secret(
                        "http://127.0.0.1:1/token",
                        redirect_port,
                    ),
                    bindings: SourceBindings::default(),
                    oauth_credential_retrievals: vec![SourceOAuthCredentialRetrieval {
                        input_key: "API_TOKEN".to_string(),
                        method_index: 0,
                        credential_inputs: Vec::new(),
                    }],
                },
                event_tx,
            )
            .await
            .expect_err("missing API_BASE should fail validation");
        assert!(
            error
                .to_string()
                .contains("missing required source variable 'API_BASE'")
        );
        assert!(
            event_rx.try_recv().is_err(),
            "preflight validation should fail before OAuth retrieval starts"
        );
        let material = credential_manager
            .read_material(
                &default_workspace(),
                &credential_set_id,
                CredentialStorageKind::File,
            )
            .expect("read material");
        assert_eq!(
            material.get("API_TOKEN").map(String::as_str),
            Some("old-token")
        );
        assert!(
            !material.values().any(|value| value == "access-token"),
            "candidate OAuth material should not be persisted on validation failure"
        );
    }

    #[tokio::test]
    async fn import_with_oauth_rejects_source_config_conflict_before_authorization() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager = SourceManager::new_for_tests(config_store, credential_manager, layout);
        let redirect_port = free_loopback_port();
        let (event_tx, mut event_rx) = import_event_channel();

        let error = manager
            .import_source_with_credentials(
                &default_workspace(),
                ImportSourceWithCredentialsCommand {
                    manifest_yaml: manifest_with_oauth_secret(
                        "http://127.0.0.1:1/token",
                        redirect_port,
                    ),
                    bindings: SourceBindings {
                        variables: vec![SourceBinding {
                            key: "API_BASE".to_string(),
                            value: "https://api.example.test".to_string(),
                        }],
                        secrets: vec![SourceBinding {
                            key: "API_TOKEN".to_string(),
                            value: "manual-token".to_string(),
                        }],
                    },
                    oauth_credential_retrievals: vec![SourceOAuthCredentialRetrieval {
                        input_key: "API_TOKEN".to_string(),
                        method_index: 0,
                        credential_inputs: Vec::new(),
                    }],
                },
                event_tx,
            )
            .await
            .expect_err("source config and OAuth should conflict");
        assert!(
            error
                .to_string()
                .contains("source secret 'API_TOKEN' was provided by both source config and OAuth")
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

    async fn authorize_oauth_import(
        event_rx: &mut mpsc::Receiver<PendingImportSourceWithCredentialsEvent>,
        redirect_port: u16,
    ) {
        let event = event_rx
            .recv()
            .await
            .expect("authorization event")
            .into_event();
        let ImportSourceWithCredentialsEvent::Authorization {
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
            .expect("callback received event")
            .into_event();
        let ImportSourceWithCredentialsEvent::CallbackReceived { input_key } = event else {
            panic!("unexpected import event");
        };
        assert_eq!(input_key, "API_TOKEN");

        let event = event_rx
            .recv()
            .await
            .expect("completion event")
            .into_event();
        let ImportSourceWithCredentialsEvent::Completed { input_key, .. } = event else {
            panic!("unexpected import event");
        };
        assert_eq!(input_key, "API_TOKEN");
    }

    fn import_event_channel() -> (
        ImportSourceEventSender,
        mpsc::Receiver<PendingImportSourceWithCredentialsEvent>,
    ) {
        let (tx, rx) = mpsc::channel(4);
        (ImportSourceEventSender::new(tx), rx)
    }

    fn free_loopback_port() -> u16 {
        StdTcpListener::bind("127.0.0.1:0")
            .expect("bind free port")
            .local_addr()
            .expect("addr")
            .port()
    }

    struct OAuthFixture {
        token_url: String,
        token_server: JoinHandle<CapturedTokenRequest>,
    }

    impl OAuthFixture {
        fn new() -> Self {
            let token_listener = StdTcpListener::bind("127.0.0.1:0").expect("token listener");
            let token_url = format!(
                "http://{}/token",
                token_listener.local_addr().expect("addr")
            );
            let token_server = tokio::task::spawn_blocking(move || {
                let (mut stream, _) = token_listener.accept().expect("accept token request");
                let request = read_http_request(&mut stream);
                let response_body = r#"{"access_token":"access-token","token_type":"Bearer"}"#;
                let response = format!(
                    "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response_body}",
                    response_body.len()
                );
                stream
                    .write_all(response.as_bytes())
                    .expect("write token response");
                request
            });
            Self {
                token_url,
                token_server,
            }
        }
    }

    struct CapturedTokenRequest {
        form: BTreeMap<String, String>,
    }

    fn read_http_request(stream: &mut std::net::TcpStream) -> CapturedTokenRequest {
        let mut buffer = Vec::new();
        let mut temp = [0_u8; 1024];
        loop {
            let read = stream.read(&mut temp).expect("read token request");
            if read == 0 {
                break;
            }
            buffer.extend_from_slice(temp.get(..read).expect("read length is in buffer bounds"));
            if buffer.windows(4).any(|window| window == b"\r\n\r\n") {
                let header_end = buffer
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .expect("header end")
                    + 4;
                let headers = String::from_utf8_lossy(
                    buffer
                        .get(..header_end)
                        .expect("header end is in buffer bounds"),
                );
                let content_length = headers
                    .lines()
                    .find_map(|line| line.strip_prefix("content-length: "))
                    .or_else(|| {
                        headers
                            .lines()
                            .find_map(|line| line.strip_prefix("Content-Length: "))
                    })
                    .and_then(|value| value.parse::<usize>().ok())
                    .unwrap_or(0);
                while buffer.len() < header_end + content_length {
                    let read = stream.read(&mut temp).expect("read token body");
                    if read == 0 {
                        break;
                    }
                    buffer.extend_from_slice(temp.get(..read).expect("read length is in bounds"));
                }
                break;
            }
        }
        let raw = String::from_utf8_lossy(&buffer);
        let (_headers, body) = raw.split_once("\r\n\r\n").expect("split request");
        let form = url::form_urlencoded::parse(body.as_bytes())
            .into_owned()
            .collect();
        CapturedTokenRequest { form }
    }
}
