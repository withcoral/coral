//! Owns the source lifecycle workflow for the local app.

use std::collections::{BTreeMap, BTreeSet};

use crate::bootstrap::AppError;
use crate::credentials::oauth::{
    OAuthCredentialManager, OAuthCredentialMaterial, StartOAuthCredentialRequest,
    material_key_belongs_to_input,
};
use crate::credentials::{
    CORAL_INTERNAL_KEY_PREFIX, CredentialManager, CredentialMaterialSnapshot, CredentialSetId,
    CredentialStorageKind, CredentialsError,
};
use crate::sources::SourceName;
use crate::sources::catalog::{
    describe_manifest, list_bundled_sources, load_bundled_source, resolve_installed_manifest,
};
use crate::sources::model::{CandidateSource, InstalledSource, SourceOrigin};
use crate::state::{AppStateLayout, ConfigStore};
use crate::storage::fs;
use crate::workspaces::WorkspaceName;
use coral_spec::{ManifestCredentialMethodKind, ManifestInputKind, ManifestOAuthCredentialSpec};
use tokio::sync::{mpsc, oneshot};
use tracing::warn;

#[derive(Clone)]
pub(crate) struct SourceManager {
    config_store: ConfigStore,
    credential_manager: CredentialManager,
    oauth_manager: OAuthCredentialManager,
    layout: AppStateLayout,
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
    OAuthAuthorization {
        input_key: String,
        authorization_url: String,
        expires_in_seconds: u64,
        user_code: Option<String>,
        verification_uri: Option<String>,
        verification_uri_complete: Option<String>,
    },
    OAuthCompleted {
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
    credential_storage: Option<CredentialStorageKind>,
}

struct SourceRollbackState {
    source: InstalledSource,
    manifest_yaml: Option<String>,
    credential_material: Option<CredentialMaterialSnapshot>,
}

impl SourceManager {
    pub(crate) fn new(
        config_store: ConfigStore,
        credential_manager: CredentialManager,
        layout: AppStateLayout,
    ) -> Self {
        Self {
            config_store,
            credential_manager,
            oauth_manager: OAuthCredentialManager::new(),
            layout,
        }
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
        let validation_storage = self.source_validation_storage(
            workspace_name,
            &candidate,
            &command.bindings,
            &BTreeSet::new(),
        )?;
        let stored_material = self.source_material_for_validation(
            workspace_name,
            &candidate,
            &command.bindings,
            &BTreeSet::new(),
            validation_storage,
        )?;
        let bindings = validate_bindings(&candidate, &command.bindings, &stored_material)?;
        let credential_storage = self.source_persist_storage(
            workspace_name,
            &candidate.name,
            &bindings,
            !stored_material.is_empty(),
        )?;
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate: &candidate,
                manifest_yaml: None,
                bindings,
                origin: SourceOrigin::Bundled,
                credential_storage,
            },
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
        let oauth_input_keys = command
            .oauth_credential_retrievals
            .iter()
            .map(|credential| credential.input_key.clone())
            .collect::<BTreeSet<_>>();
        let validation_storage = self.source_validation_storage(
            workspace_name,
            &candidate,
            &command.bindings,
            &oauth_input_keys,
        )?;
        let stored_material = self.source_material_for_validation(
            workspace_name,
            &candidate,
            &command.bindings,
            &oauth_input_keys,
            validation_storage,
        )?;
        let has_stored_material = !stored_material.is_empty();
        let bindings = self
            .bindings_with_oauth_material(
                &candidate,
                &command.bindings,
                stored_material,
                command.oauth_credential_retrievals,
                events,
            )
            .await?;
        let credential_storage = self.source_persist_storage(
            workspace_name,
            &candidate.name,
            &bindings,
            has_stored_material,
        )?;
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate: &candidate,
                manifest_yaml: None,
                bindings,
                origin: SourceOrigin::Bundled,
                credential_storage,
            },
        )
    }

    pub(crate) fn import_source(
        &self,
        workspace_name: &WorkspaceName,
        command: &ImportSourceCommand,
    ) -> Result<InstalledSource, AppError> {
        let mut candidate =
            describe_manifest(&command.manifest_yaml, SourceOrigin::Imported, false)?;
        candidate.installed = self.source_exists(workspace_name, &candidate.name)?;
        let validation_storage = self.source_validation_storage(
            workspace_name,
            &candidate,
            &command.bindings,
            &BTreeSet::new(),
        )?;
        let stored_material = self.source_material_for_validation(
            workspace_name,
            &candidate,
            &command.bindings,
            &BTreeSet::new(),
            validation_storage,
        )?;
        let bindings = validate_bindings(&candidate, &command.bindings, &stored_material)?;
        let credential_storage = self.source_persist_storage(
            workspace_name,
            &candidate.name,
            &bindings,
            !stored_material.is_empty(),
        )?;
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate: &candidate,
                manifest_yaml: Some(&command.manifest_yaml),
                bindings,
                origin: SourceOrigin::Imported,
                credential_storage,
            },
        )
    }

    pub(crate) async fn import_source_with_credentials(
        &self,
        workspace_name: &WorkspaceName,
        command: ImportSourceWithCredentialsCommand,
        events: ImportSourceEventSender,
    ) -> Result<InstalledSource, AppError> {
        let mut candidate =
            describe_manifest(&command.manifest_yaml, SourceOrigin::Imported, false)?;
        candidate.installed = self.source_exists(workspace_name, &candidate.name)?;
        let oauth_input_keys = command
            .oauth_credential_retrievals
            .iter()
            .map(|credential| credential.input_key.clone())
            .collect::<BTreeSet<_>>();
        let validation_storage = self.source_validation_storage(
            workspace_name,
            &candidate,
            &command.bindings,
            &oauth_input_keys,
        )?;
        let stored_material = self.source_material_for_validation(
            workspace_name,
            &candidate,
            &command.bindings,
            &oauth_input_keys,
            validation_storage,
        )?;
        let has_stored_material = !stored_material.is_empty();
        let bindings = self
            .bindings_with_oauth_material(
                &candidate,
                &command.bindings,
                stored_material,
                command.oauth_credential_retrievals,
                events,
            )
            .await?;
        let credential_storage = self.source_persist_storage(
            workspace_name,
            &candidate.name,
            &bindings,
            has_stored_material,
        )?;
        self.persist_source(
            workspace_name,
            PersistSourceRequest {
                candidate: &candidate,
                manifest_yaml: Some(&command.manifest_yaml),
                bindings,
                origin: SourceOrigin::Imported,
                credential_storage,
            },
        )
    }

    pub(crate) fn delete_source(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
    ) -> Result<InstalledSource, AppError> {
        let stored = self.config_store.get_source(workspace_name, source_name)?;
        let removed = self.populate_source_version_or_keep(workspace_name, stored.clone());
        let source_dir = self.layout.source_dir(workspace_name, source_name);
        let credential_set_id = CredentialSetId::for_source(source_name);
        let credential_storage = stored.credential_storage_for_material();
        let credential_material = credential_storage
            .map(|storage| {
                self.credential_manager.snapshot_material(
                    workspace_name,
                    &credential_set_id,
                    storage,
                )
            })
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
            && let Err(error) = self.credential_manager.remove_material(
                workspace_name,
                &credential_set_id,
                credential_storage,
            )
        {
            self.restore_source_rollback_state(workspace_name, source_name, Some(previous), None);
            return Err(error);
        }
        if source_dir.exists()
            && let Err(error) = std::fs::remove_dir_all(&source_dir)
        {
            self.restore_source_rollback_state(workspace_name, source_name, Some(previous), None);
            return Err(error.into());
        }
        if let Err(error) = self.config_store.remove_source(workspace_name, source_name) {
            self.restore_source_rollback_state(workspace_name, source_name, Some(previous), None);
            return Err(error);
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

    fn persist_source(
        &self,
        workspace_name: &WorkspaceName,
        request: PersistSourceRequest<'_>,
    ) -> Result<InstalledSource, AppError> {
        let source_name = request.candidate.name.clone();
        let previous = self.load_source_rollback_state(workspace_name, &source_name)?;
        if let Err(error) =
            self.persist_manifest_artifact(workspace_name, &source_name, request.manifest_yaml)
        {
            self.restore_source_rollback_state(workspace_name, &source_name, previous, None);
            return Err(error);
        }

        let (visible_secret_keys, credential_storage) = if let Some(requested_storage) =
            request.credential_storage
        {
            let credential_set_id = CredentialSetId::for_source(&source_name);
            let mut credential_material = match self.credential_manager.read_material(
                workspace_name,
                &credential_set_id,
                requested_storage,
            ) {
                Ok(material) => material,
                Err(AppError::Credentials(CredentialsError::Parse(_)))
                    if requested_storage == CredentialStorageKind::File =>
                {
                    BTreeMap::new()
                }
                Err(error) => {
                    self.restore_source_rollback_state(
                        workspace_name,
                        &source_name,
                        previous,
                        None,
                    );
                    return Err(error);
                }
            };
            let expected_secret_keys = request
                .candidate
                .inputs
                .iter()
                .filter(|input| input.kind == ManifestInputKind::Secret)
                .map(|input| input.key.clone())
                .collect::<BTreeSet<_>>();
            credential_material
                .retain(|key, _| material_key_belongs_to_source_secret(key, &expected_secret_keys));
            for input_key in &request.bindings.replaced_oauth_inputs {
                credential_material.retain(|key, _| !material_key_belongs_to_input(key, input_key));
            }
            credential_material.extend(request.bindings.secrets);
            let credential_write = match self.credential_manager.replace_material(
                workspace_name,
                &credential_set_id,
                requested_storage,
                &credential_material,
            ) {
                Ok(outcome) => outcome,
                Err(error) => {
                    self.restore_source_rollback_state(
                        workspace_name,
                        &source_name,
                        previous,
                        Some(requested_storage),
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

        let persisted_version = match request.origin {
            SourceOrigin::Bundled => None,
            SourceOrigin::Imported => Some(request.candidate.version.clone()),
        };
        let stored = InstalledSource {
            name: source_name.clone(),
            version: persisted_version,
            variables: request.bindings.variables,
            secrets: visible_secret_keys,
            credential_storage,
            origin: request.origin,
        };
        if let Err(error) = self
            .config_store
            .upsert_source(workspace_name, stored.clone())
        {
            self.restore_source_rollback_state(
                workspace_name,
                &source_name,
                previous,
                credential_storage,
            );
            return Err(error);
        }
        let mut resolved = stored;
        resolved.version = Some(request.candidate.version.clone());
        Ok(resolved)
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

    fn source_material_for_validation(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        filled_secret_keys: &BTreeSet<String>,
        credential_storage: Option<CredentialStorageKind>,
    ) -> Result<BTreeMap<String, String>, AppError> {
        if !source_needs_stored_material_for_validation(candidate, bindings, filled_secret_keys)? {
            return Ok(BTreeMap::new());
        }

        match credential_storage {
            Some(credential_storage) => {
                self.read_source_material(workspace_name, &candidate.name, credential_storage)
            }
            None => Ok(BTreeMap::new()),
        }
    }

    fn source_validation_storage(
        &self,
        workspace_name: &WorkspaceName,
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        filled_secret_keys: &BTreeSet<String>,
    ) -> Result<Option<CredentialStorageKind>, AppError> {
        if !source_needs_stored_material_for_validation(candidate, bindings, filled_secret_keys)? {
            return Ok(None);
        }

        match self
            .config_store
            .get_source(workspace_name, &candidate.name)
        {
            Ok(source) => Ok(source.credential_storage_for_material()),
            Err(AppError::SourceNotFound(_))
                if self
                    .layout
                    .secret_file(workspace_name, &candidate.name)
                    .exists() =>
            {
                Ok(Some(CredentialStorageKind::File))
            }
            Err(AppError::SourceNotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    fn source_persist_storage(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        bindings: &ValidatedBindings,
        has_stored_material: bool,
    ) -> Result<Option<CredentialStorageKind>, AppError> {
        match self.config_store.get_source(workspace_name, source_name) {
            Ok(source) if !source.secrets.is_empty() => {
                Ok(Some(source.effective_credential_storage()))
            }
            Ok(_) | Err(AppError::SourceNotFound(_))
                if bindings.secrets.is_empty() && !has_stored_material =>
            {
                Ok(None)
            }
            Ok(_) | Err(AppError::SourceNotFound(_)) => {
                self.credential_manager.default_write_storage().map(Some)
            }
            Err(error) => Err(error),
        }
    }

    fn validate_oauth_import_preflight(
        candidate: &CandidateSource,
        bindings: &SourceBindings,
        stored_material: &BTreeMap<String, String>,
        oauth_credential_retrievals: &[SourceOAuthCredentialRetrieval],
    ) -> Result<(), AppError> {
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
            let credential_inputs = retrieval
                .credential_inputs
                .iter()
                .map(|input| (input.key.clone(), input.value.clone()))
                .collect();
            OAuthCredentialManager::validate_credential_inputs(config.oauth, credential_inputs)?;
            validation_material.insert(config.input_key.to_string(), String::new());
        }

        let bindings = validate_bindings(candidate, bindings, &validation_material)?;
        for input_key in seen {
            if bindings.secrets.contains_key(&input_key) {
                return Err(AppError::InvalidInput(format!(
                    "source secret '{input_key}' was provided by both source config and OAuth"
                )));
            }
        }
        Ok(())
    }

    async fn retrieve_oauth_material(
        &self,
        candidate: &CandidateSource,
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
            let material = self
                .oauth_manager
                .authorize(
                    StartOAuthCredentialRequest {
                        input_key: &input_key,
                        oauth: config.oauth,
                        credential_inputs,
                    },
                    move |authorization| {
                        let events = authorization_events;
                        async move {
                            send_import_event(
                                &events,
                                ImportSourceWithCredentialsEvent::OAuthAuthorization {
                                    input_key: authorization_input_key,
                                    authorization_url: authorization.authorization_url,
                                    expires_in_seconds: authorization.expires_in_seconds,
                                    user_code: authorization.user_code,
                                    verification_uri: authorization.verification_uri,
                                    verification_uri_complete: authorization
                                        .verification_uri_complete,
                                },
                            )
                            .await
                        }
                    },
                )
                .await?;
            send_import_event(
                &events,
                ImportSourceWithCredentialsEvent::OAuthCompleted {
                    input_key: material.input_key.clone(),
                    metadata: material.safe_metadata.clone(),
                },
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
        events: ImportSourceEventSender,
    ) -> Result<ValidatedBindings, AppError> {
        Self::validate_oauth_import_preflight(
            candidate,
            bindings,
            &stored_material,
            &oauth_credential_retrievals,
        )?;
        let oauth_material = self
            .retrieve_oauth_material(candidate, oauth_credential_retrievals, events)
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
    ) -> Result<Option<SourceRollbackState>, AppError> {
        let source = match self.config_store.get_source(workspace_name, source_name) {
            Ok(source) => source,
            Err(AppError::SourceNotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let credential_set_id = CredentialSetId::for_source(source_name);
        let credential_material = source
            .credential_storage_for_material()
            .map(|credential_storage| {
                self.credential_manager.snapshot_material(
                    workspace_name,
                    &credential_set_id,
                    credential_storage,
                )
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

    fn restore_source_rollback_state(
        &self,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        previous: Option<SourceRollbackState>,
        new_material_storage: Option<CredentialStorageKind>,
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
            let credential_set_id = CredentialSetId::for_source(source_name);
            match previous.credential_material {
                Some(credential_material) => {
                    if let Err(e) = self.credential_manager.restore_material(
                        workspace_name,
                        &credential_set_id,
                        &credential_material,
                    ) {
                        warn!("rollback: failed to restore source credential material: {e}");
                    }
                }
                None => {
                    if let Some(storage) = new_material_storage
                        && let Err(e) = self.credential_manager.remove_material(
                            workspace_name,
                            &credential_set_id,
                            storage,
                        )
                    {
                        warn!("rollback: failed to remove new source credential material: {e}");
                    }
                }
            }
            if let Err(e) = self
                .config_store
                .upsert_source(workspace_name, previous.source)
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
            let credential_set_id = CredentialSetId::for_source(source_name);
            if let Some(storage) = new_material_storage
                && let Err(e) = self.credential_manager.remove_material(
                    workspace_name,
                    &credential_set_id,
                    storage,
                )
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
        source.version = Some(
            resolve_installed_manifest(workspace_name, &source, &self.layout)?
                .candidate
                .version,
        );
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
) -> Result<bool, AppError> {
    let supplied_secrets = collect_unique_secrets(&bindings.secrets)?;
    Ok(candidate.inputs.iter().any(|input| {
        input.kind == ManifestInputKind::Secret
            && input.required
            && !supplied_secrets.contains_key(&input.key)
            && !filled_secret_keys.contains(&input.key)
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

async fn send_import_event(
    events: &ImportSourceEventSender,
    event: ImportSourceWithCredentialsEvent,
) -> Result<(), AppError> {
    events.send(event).await
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
    use std::collections::BTreeMap;
    use std::io::{Read as _, Write as _};
    use std::net::TcpListener as StdTcpListener;

    use tempfile::TempDir;
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;
    use url::Url;

    use super::{
        ImportSourceCommand, ImportSourceEventSender, ImportSourceWithCredentialsCommand,
        ImportSourceWithCredentialsEvent, PendingImportSourceWithCredentialsEvent, SourceBinding,
        SourceBindings, SourceManager, SourceOAuthCredentialRetrieval, normalize_binding_key,
    };
    use crate::credentials::{
        CredentialManager, CredentialSetId, CredentialStorageKind, CredentialStoragePreference,
        CredentialStore,
    };
    use crate::sources::SourceName;
    use crate::state::{AppStateLayout, ConfigStore};
    use crate::workspaces::WorkspaceName;

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

    #[test]
    fn import_restores_prior_state_when_secret_persistence_fails() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager = SourceManager::new(config_store, credential_manager, layout.clone());

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
        let manager = SourceManager::new(config_store, credential_manager, layout);

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
        let manager = SourceManager::new(config_store, credential_manager.clone(), layout.clone());
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
        let manager = SourceManager::new(config_store, credential_manager, layout.clone());
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
        let manager = SourceManager::new(config_store, credential_manager, layout);

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
        let manager = SourceManager::new(config_store, credential_manager, layout.clone());

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
        let manager = SourceManager::new(config_store, credential_manager, layout.clone());

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
        let manager = SourceManager::new(config_store, credential_manager.clone(), layout);
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
        let manager = SourceManager::new(config_store, credential_manager, layout.clone());
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
        let manager = SourceManager::new(config_store, credential_manager.clone(), layout);
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

    #[tokio::test]
    async fn import_with_oauth_persists_retrieved_material() {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("ensure layout");
        let config_store = ConfigStore::new(layout.clone());
        let credential_store = CredentialStore::new(layout.clone());
        let credential_manager = CredentialManager::new(credential_store);
        let manager = SourceManager::new(config_store, credential_manager.clone(), layout);
        let source_name = SourceName::parse("secured_messages").expect("source");
        let credential_set_id = CredentialSetId::for_source(&source_name);
        let fixture = OAuthFixture::new();
        let redirect_port = free_loopback_port();
        let (event_tx, mut event_rx) = import_event_channel();
        let workspace_name = default_workspace();
        let import = manager.import_source_with_credentials(
            &workspace_name,
            ImportSourceWithCredentialsCommand {
                manifest_yaml: manifest_with_oauth_secret(&fixture.token_url, redirect_port),
                bindings: SourceBindings {
                    variables: vec![SourceBinding {
                        key: "API_BASE".to_string(),
                        value: "https://api.example.test".to_string(),
                    }],
                    secrets: Vec::new(),
                },
                oauth_credential_retrievals: vec![SourceOAuthCredentialRetrieval {
                    input_key: "API_TOKEN".to_string(),
                    method_index: 0,
                    credential_inputs: Vec::new(),
                }],
            },
            event_tx,
        );
        let callback = async {
            let event = event_rx
                .recv()
                .await
                .expect("authorization event")
                .into_event();
            let ImportSourceWithCredentialsEvent::OAuthAuthorization {
                input_key,
                authorization_url,
                ..
            } = event
            else {
                panic!("unexpected import event");
            };
            assert_eq!(input_key, "API_TOKEN");
            callback(&authorization_url, redirect_port).await;
            let event = event_rx
                .recv()
                .await
                .expect("completion event")
                .into_event();
            let ImportSourceWithCredentialsEvent::OAuthCompleted { input_key, .. } = event else {
                panic!("unexpected import event");
            };
            assert_eq!(input_key, "API_TOKEN");
        };

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
        let manager = SourceManager::new(config_store, credential_manager.clone(), layout);
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
        let manager = SourceManager::new(config_store, credential_manager, layout);
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
