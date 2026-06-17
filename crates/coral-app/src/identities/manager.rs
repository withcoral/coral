//! Storage seam types and traits for provider identities.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use coral_engine::RequestIdentitySelectionContext;
use coral_spec::v4::IdentityRequirements;
use coral_spec::{
    IdentityManifest, IdentitySpecConfig, IdentitySpecType, ManifestOAuthCredentialSpec,
};
use serde::{Deserialize, Serialize};
use tracing::info_span;

use crate::bootstrap::AppError;
use crate::credentials::oauth::{
    OAuthCredentialMaterial, OAuthCredentialService, OAuthProgressEventSender,
    OAuthRefreshMaterialPersistence, StartOAuthCredentialRequest,
};
use crate::identities::runtime::{
    FIXED_TOKEN_MATERIAL_KEY, IdentityRuntimeServices, OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
    StoredIdentityRuntimeData,
};
use crate::identity::{
    IdentityOwnerKind, RuntimeSourceIdentity, SourceIdentityProvider,
    SourceIdentityResolutionRequest, SourceIdentitySelection, SourceIdentitySelectionRequest,
    UserPrincipal, parse_path_segment, unique_input_map,
};
use crate::identity_specs::{
    IdentitySpecManager, IdentitySpecRecord, identity_spec_fingerprint, validate_identity_spec_name,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::env_file::{parse_env_file, render_env_file};
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

const IDENTITY_DOCUMENT_VERSION: u32 = 1;
const USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION: u32 = 1;

/// One stored provider identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityRecord {
    /// Principal or workspace that owns this identity.
    pub owner: IdentityOwner,
    /// Stable identity name used by source identity bindings.
    pub name: IdentityName,
    /// Installed identity spec used to instantiate this identity.
    pub identity_spec: String,
    /// Fingerprint of the identity spec at creation time.
    pub identity_spec_fingerprint: Option<String>,
    /// Provider or issuer copied from the identity spec.
    pub issuer: String,
    /// Identity type copied from the identity spec.
    pub identity_type: String,
    /// Safe provider metadata, never credential material.
    pub metadata: BTreeMap<String, String>,
}

/// Validated storage name for one provider identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityName(String);

impl IdentityName {
    /// Builds an identity name from a storage-safe string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the name is empty or contains path separators.
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity", &value.into()).map(Self)
    }

    /// Returns the storage name string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdentityName {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdentityName {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Explicit owner of stored provider-facing identity material.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityOwner {
    kind: IdentityOwnerKind,
    key: String,
}

impl IdentityOwner {
    /// Builds an owner from its explicit kind and storage-safe key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the key is empty or contains path separators.
    pub fn new(kind: IdentityOwnerKind, key: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity owner", &key.into()).map(|key| Self { kind, key })
    }

    /// Builds a user-owned identity owner.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the key is empty or contains path separators.
    pub fn user(key: impl Into<String>) -> Result<Self, AppError> {
        Self::new(IdentityOwnerKind::User, key)
    }

    /// Builds a workspace-owned identity owner.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the key is empty or contains path separators.
    pub fn workspace(key: impl Into<String>) -> Result<Self, AppError> {
        Self::new(IdentityOwnerKind::Workspace, key)
    }

    pub(crate) fn for_user_principal(principal: &UserPrincipal) -> Result<Self, AppError> {
        Self::user(principal.user_id())
    }

    /// Returns the explicit owner kind.
    #[must_use]
    pub const fn kind(&self) -> IdentityOwnerKind {
        self.kind
    }

    /// Returns the storage key string.
    #[must_use]
    pub fn key(&self) -> &str {
        &self.key
    }

    pub(crate) const fn storage_segment(&self) -> &'static str {
        match self.kind {
            IdentityOwnerKind::User => "users",
            IdentityOwnerKind::Workspace => "workspaces",
        }
    }
}

impl fmt::Display for IdentityOwner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.kind.as_config_value(), self.key)
    }
}

/// Request to create or replace an OAuth identity.
#[derive(Debug, Clone)]
pub struct CreateOAuthIdentityCommand {
    /// Stable identity name used by source identity bindings.
    pub name: String,
    /// Installed OAuth identity spec name.
    pub identity_spec: String,
    /// Method-specific OAuth credential inputs.
    pub credential_inputs: Vec<IdentityCredentialInput>,
}

/// Request to create or replace a fixed-token identity.
#[derive(Debug, Clone)]
pub struct CreateFixedTokenIdentityCommand {
    /// Stable identity name used by source identity bindings.
    pub name: String,
    /// Installed fixed-token identity spec name.
    pub identity_spec: String,
    /// Bearer token to store.
    pub token: String,
}

/// Locked access to credential material for one provider identity.
/// OAuth method input supplied while creating an identity.
#[derive(Debug, Clone)]
pub struct IdentityCredentialInput {
    /// Input key.
    pub key: String,
    /// Input value.
    pub value: String,
}

/// Locked access to credential material for one provider identity.
#[tonic::async_trait]
pub trait IdentityMaterialGuard: Send {
    /// Reads the identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when material cannot be read or decoded.
    async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError>;

    /// Replaces the identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when material cannot be written.
    async fn write_material(&self, material: &BTreeMap<String, String>) -> Result<(), AppError>;
}

/// Durable storage backend for provider identities.
#[tonic::async_trait]
pub trait IdentityStore: Send + Sync + std::fmt::Debug + 'static {
    /// Lists identities owned by one owner.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn list_identities(&self, owner: &IdentityOwner)
    -> Result<Vec<IdentityRecord>, AppError>;

    /// Loads one identity owned by one owner.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<Option<IdentityRecord>, AppError>;

    /// Replaces one identity and its credential material atomically.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn replace_identity(
        &self,
        record: &IdentityRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError>;

    /// Deletes one identity and its credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn delete_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<bool, AppError>;

    /// Returns locked access to one identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the material lock cannot be acquired.
    async fn material_guard(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<Box<dyn IdentityMaterialGuard>, AppError>;

    /// Replaces one per-user source identity selection.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn replace_source_identity_binding(
        &self,
        _user_id: &str,
        _workspace_name: &str,
        _source_name: &str,
        _surface_id: &str,
        _selection: &SourceIdentitySelection,
    ) -> Result<(), AppError> {
        Err(source_identity_binding_store_unsupported())
    }

    /// Loads one per-user source identity selection if present.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_optional_source_identity_binding(
        &self,
        _user_id: &str,
        _workspace_name: &str,
        _source_name: &str,
        _surface_id: &str,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        Err(source_identity_binding_store_unsupported())
    }

    /// Loads one per-user source identity selection.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_source_identity_binding(
        &self,
        user_id: &str,
        workspace_name: &str,
        source_name: &str,
        surface_id: &str,
    ) -> Result<SourceIdentitySelection, AppError> {
        self.load_optional_source_identity_binding(user_id, workspace_name, source_name, surface_id)
            .await?
            .ok_or_else(|| {
                AppError::FailedPrecondition(format!(
                    "user '{user_id}' has no selected identity for source '{source_name}' surface '{surface_id}'"
                ))
            })
    }

    /// Deletes one per-user source identity selection if present.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn delete_source_identity_binding(
        &self,
        _user_id: &str,
        _workspace_name: &str,
        _source_name: &str,
        _surface_id: &str,
    ) -> Result<bool, AppError> {
        Err(source_identity_binding_store_unsupported())
    }

    /// Counts identities that reference `identity_spec_name`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot inspect its records.
    fn count_identities_for_spec(&self, identity_spec_name: &str) -> Result<u32, AppError>;
}

/// Manages provider-facing identities keyed by their explicit owner.
#[derive(Debug, Clone)]
pub(crate) struct IdentityManager {
    identity_specs: IdentitySpecManager,
    oauth_credential_service: OAuthCredentialService,
    store: Arc<dyn IdentityStore>,
}

/// Product-facing handle for stored provider identity management.
///
/// OSS callers keep using user-owned gRPC methods, while other runtimes can
/// pass explicit workspace or user owners.
#[derive(Debug, Clone)]
pub struct IdentityManagementHandle {
    manager: IdentityManager,
}

impl IdentityManagementHandle {
    /// Creates or replaces an OAuth identity under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the identity spec is invalid, OAuth
    /// authorization fails, or credential material cannot be stored.
    pub async fn create_oauth_identity(
        &self,
        owner: &IdentityOwner,
        command: CreateOAuthIdentityCommand,
        events: OAuthProgressEventSender,
    ) -> Result<IdentityRecord, AppError> {
        self.manager
            .create_oauth_identity(owner, command, events)
            .await
    }

    /// Creates or replaces a fixed-token identity under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when validation or storage fails.
    pub async fn create_fixed_token_identity(
        &self,
        owner: &IdentityOwner,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<IdentityRecord, AppError> {
        self.manager
            .create_fixed_token_identity(owner, command)
            .await
    }

    /// Lists identities stored under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    pub async fn list_identities(
        &self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        self.manager.list_identities(owner).await
    }

    /// Validates that an identity exists under `owner` and still matches its
    /// installed identity spec without reading credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read or the identity is
    /// orphaned because its installed spec is missing, changed, or has a
    /// different type.
    pub async fn validate_identity_metadata(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<Option<IdentityRecord>, AppError> {
        self.manager
            .validate_identity_metadata(owner, identity_name)
            .await
    }

    /// Deletes an identity stored under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    pub async fn delete_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<bool, AppError> {
        self.manager.delete_identity(owner, identity_name).await
    }

    /// Resolves an identity under `owner` into runtime credential material,
    /// refreshing OAuth material if necessary.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the stored identity is invalid or refresh
    /// fails.
    pub async fn resolve_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
        let identity_name = validate_identity_name(identity_name)?;
        self.manager.resolve_identity(owner, &identity_name).await
    }
}

impl IdentityManager {
    pub(crate) fn new(layout: AppStateLayout, identity_specs: IdentitySpecManager) -> Self {
        Self::new_with_store(
            identity_specs.clone(),
            Arc::new(FileIdentityStore::new(layout, identity_specs)),
        )
    }

    pub(crate) fn new_with_store(
        identity_specs: IdentitySpecManager,
        store: Arc<dyn IdentityStore>,
    ) -> Self {
        Self {
            identity_specs,
            oauth_credential_service: OAuthCredentialService::new(),
            store,
        }
    }

    pub(crate) fn handle(&self) -> IdentityManagementHandle {
        IdentityManagementHandle {
            manager: self.clone(),
        }
    }

    fn prepare_identity_creation(
        &self,
        owner: &IdentityOwner,
        name: &str,
        identity_spec: &str,
        expected: IdentitySpecType,
    ) -> Result<(IdentityOwner, IdentityName, IdentitySpecRecord), AppError> {
        self.identity_specs.ensure_dsl_v4_enabled()?;
        let name = validate_identity_name(name)?;
        let identity_spec_name = validate_identity_spec_name(identity_spec)?;
        let spec = self
            .identity_specs
            .get_identity_spec(identity_spec_name.as_str())?;
        if spec.manifest.identity_type != expected {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{identity_spec_name}' has type '{}'; expected {}",
                spec.manifest.identity_type.label(),
                expected.label()
            )));
        }
        Ok((owner.clone(), name, spec))
    }

    async fn create_oauth_identity(
        &self,
        owner: &IdentityOwner,
        command: CreateOAuthIdentityCommand,
        events: OAuthProgressEventSender,
    ) -> Result<IdentityRecord, AppError> {
        let span = info_span!("coral.app.identities.create_oauth");
        let _guard = span.enter();
        let (owner, name, spec) = self.prepare_identity_creation(
            owner,
            &command.name,
            &command.identity_spec,
            IdentitySpecType::OAuth,
        )?;
        let identity_spec_name = spec.manifest.name.clone();
        let oauth = oauth_method(&identity_spec_name, &spec.manifest.config)?;
        let provided_inputs = unique_input_map(
            command
                .credential_inputs
                .into_iter()
                .map(|input| (input.key, input.value)),
            "credential input",
        )?;
        reject_identity_owned_inputs(
            name.as_str(),
            &identity_spec_name,
            &spec.manifest,
            oauth,
            &provided_inputs,
        )?;
        let identity_inputs = self
            .identity_specs
            .resolve_identity_spec_inputs(&spec.manifest)?;
        let credential_inputs =
            oauth_credential_inputs_from_identity_inputs(oauth, &identity_inputs, &provided_inputs);
        OAuthCredentialService::validate_credential_inputs(
            oauth,
            &identity_inputs,
            credential_inputs.clone(),
        )?;

        let refresh_material_persistence =
            oauth_refresh_material_persistence_for_identity(oauth, &provided_inputs);
        let material = self
            .oauth_credential_service
            .authorize_with_progress(
                StartOAuthCredentialRequest {
                    input_key: OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
                    oauth,
                    source_inputs: &identity_inputs,
                    credential_inputs,
                    refresh_material_persistence,
                },
                name.to_string(),
                &events,
            )
            .await?;

        let record = IdentityRecord {
            owner,
            name,
            identity_spec: identity_spec_name,
            identity_spec_fingerprint: Some(identity_spec_fingerprint(&spec.manifest)?),
            issuer: spec.manifest.issuer,
            identity_type: spec.manifest.identity_type.label().to_string(),
            metadata: material.safe_metadata.clone(),
        };
        let material = material_values(material);
        self.store.replace_identity(&record, &material).await?;
        Ok(record)
    }

    async fn create_fixed_token_identity(
        &self,
        owner: &IdentityOwner,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<IdentityRecord, AppError> {
        let span = info_span!("coral.app.identities.create_fixed_token");
        let _guard = span.enter();
        let (owner, name, spec) = self.prepare_identity_creation(
            owner,
            &command.name,
            &command.identity_spec,
            IdentitySpecType::FixedToken,
        )?;
        let token = command.token.trim().to_string();
        if token.is_empty() {
            return Err(AppError::InvalidInput(
                "fixed token identity token must not be empty".to_string(),
            ));
        }
        let record = IdentityRecord {
            owner,
            name,
            identity_spec: spec.manifest.name.clone(),
            identity_spec_fingerprint: Some(identity_spec_fingerprint(&spec.manifest)?),
            issuer: spec.manifest.issuer,
            identity_type: spec.manifest.identity_type.label().to_string(),
            metadata: BTreeMap::new(),
        };
        let material = BTreeMap::from([(FIXED_TOKEN_MATERIAL_KEY.to_string(), token)]);
        self.store.replace_identity(&record, &material).await?;
        Ok(record)
    }

    pub(crate) async fn create_user_owned_oauth_identity(
        &self,
        principal: &UserPrincipal,
        command: CreateOAuthIdentityCommand,
        events: OAuthProgressEventSender,
    ) -> Result<IdentityRecord, AppError> {
        let owner = IdentityOwner::for_user_principal(principal)?;
        self.create_oauth_identity(&owner, command, events).await
    }

    pub(crate) async fn create_user_owned_fixed_token_identity(
        &self,
        principal: &UserPrincipal,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<IdentityRecord, AppError> {
        let owner = IdentityOwner::for_user_principal(principal)?;
        self.create_fixed_token_identity(&owner, command).await
    }

    async fn list_identities(
        &self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        self.store.list_identities(owner).await
    }

    async fn validate_identity_metadata(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<Option<IdentityRecord>, AppError> {
        let identity_name = validate_identity_name(identity_name)?;
        let Some(record) = self.store.load_identity(owner, &identity_name).await? else {
            return Ok(None);
        };
        self.load_spec_manifest_for_record(&record)?;
        Ok(Some(record))
    }

    pub(crate) async fn list_user_owned_identities(
        &self,
        principal: &UserPrincipal,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        let span = info_span!("coral.app.identities.list_user_owned");
        let _guard = span.enter();
        let owner = IdentityOwner::for_user_principal(principal)?;
        self.list_identities(&owner).await
    }

    async fn delete_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<bool, AppError> {
        let identity_name = validate_identity_name(identity_name)?;
        self.store.delete_identity(owner, &identity_name).await
    }

    pub(crate) async fn replace_user_owned_source_identity_binding(
        &self,
        principal: &UserPrincipal,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
        selection: &SourceIdentitySelection,
    ) -> Result<(), AppError> {
        let span = info_span!("coral.app.identities.replace_user_owned_source_binding");
        let _guard = span.enter();
        let user_id = validate_user_id(principal.user_id())?;
        let surface_id = validate_source_surface_id(surface_id)?;
        selection.validate()?;
        self.store
            .replace_source_identity_binding(
                &user_id,
                workspace_name.as_str(),
                source_name.as_str(),
                &surface_id,
                selection,
            )
            .await
    }

    pub(crate) async fn load_user_owned_source_identity_binding(
        &self,
        principal: &UserPrincipal,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        let user_id = validate_user_id(principal.user_id())?;
        let surface_id = validate_source_surface_id(surface_id)?;
        self.store
            .load_optional_source_identity_binding(
                &user_id,
                workspace_name.as_str(),
                source_name.as_str(),
                &surface_id,
            )
            .await
    }

    pub(crate) async fn delete_user_owned_source_identity_binding(
        &self,
        principal: &UserPrincipal,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
    ) -> Result<bool, AppError> {
        let user_id = validate_user_id(principal.user_id())?;
        let surface_id = validate_source_surface_id(surface_id)?;
        self.store
            .delete_source_identity_binding(
                &user_id,
                workspace_name.as_str(),
                source_name.as_str(),
                &surface_id,
            )
            .await
    }

    pub(crate) async fn validate_user_owned_source_identity_selection(
        &self,
        principal: &UserPrincipal,
        source_name: &SourceName,
        surface_id: &str,
        selection: &SourceIdentitySelection,
        requirements: &IdentityRequirements,
    ) -> Result<(), AppError> {
        let owner = IdentityOwner::for_user_principal(principal)?;
        let identity_name = validate_identity_name(&selection.identity)?;
        let record = self
            .store
            .load_identity(&owner, &identity_name)
            .await?
            .ok_or_else(|| AppError::IdentityNotFound(identity_name.to_string()))?;
        let spec = self.load_spec_for_record(&record)?;
        let context = RequestIdentitySelectionContext::new(
            source_name.as_str().to_string(),
            surface_id.to_string(),
            requirements.clone(),
        );
        if !context.accepts_identity(&record.identity_spec, &spec.manifest.audience) {
            return Err(AppError::FailedPrecondition(format!(
                "identity '{}' does not satisfy selected identity requirements for source '{}' surface '{}'",
                selection.identity,
                source_name.as_str(),
                surface_id
            )));
        }
        Ok(())
    }

    /// Loads the installed identity spec backing `record`, reporting orphaned
    /// identities and rejecting records that no longer match the spec.
    fn load_spec_for_record(
        &self,
        record: &IdentityRecord,
    ) -> Result<IdentitySpecRecord, AppError> {
        Self::validate_loaded_spec_for_record(
            record,
            self.identity_specs.get_identity_spec(&record.identity_spec),
        )
    }

    fn load_spec_manifest_for_record(
        &self,
        record: &IdentityRecord,
    ) -> Result<IdentitySpecRecord, AppError> {
        Self::validate_loaded_spec_for_record(
            record,
            self.identity_specs
                .get_identity_spec_manifest(&record.identity_spec),
        )
    }

    fn validate_loaded_spec_for_record(
        record: &IdentityRecord,
        loaded: Result<IdentitySpecRecord, AppError>,
    ) -> Result<IdentitySpecRecord, AppError> {
        let spec = match loaded {
            Ok(spec) => spec,
            Err(AppError::IdentitySpecNotFound(_)) => {
                return Err(AppError::FailedPrecondition(format!(
                    "identity '{}' is orphaned because identity spec '{}' is not installed",
                    record.name, record.identity_spec
                )));
            }
            Err(error) => return Err(error),
        };
        validate_record_identity_type(record, spec.manifest.identity_type)?;
        validate_record_identity_spec_fingerprint(record, &spec.manifest)?;
        Ok(spec)
    }

    async fn resolve_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
        let material_guard = self.store.material_guard(owner, identity_name).await?;
        let record = self.store.load_identity(owner, identity_name).await?;
        let Some(record) = record else {
            return Ok(None);
        };
        let spec = self.load_spec_for_record(&record)?;
        let identity_inputs = self
            .identity_specs
            .resolve_identity_spec_inputs(&spec.manifest)?;
        let material = material_guard.read_material().await?;
        let prepared = StoredIdentityRuntimeData::new(
            record.name.to_string(),
            spec.manifest,
            identity_inputs,
            material,
        )
        .prepare(IdentityRuntimeServices {
            oauth_credential_service: &self.oauth_credential_service,
        })
        .await?;
        if let Some(updated_material) = prepared.updated_material {
            material_guard.write_material(&updated_material).await?;
        }
        Ok(Some(prepared.identity))
    }
}

#[derive(Debug, Clone)]
struct FileIdentityStore {
    layout: AppStateLayout,
    identity_specs: IdentitySpecManager,
}

impl FileIdentityStore {
    fn new(layout: AppStateLayout, identity_specs: IdentitySpecManager) -> Self {
        Self {
            layout,
            identity_specs,
        }
    }

    fn load_identity_unlocked(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
    ) -> Result<IdentityRecord, AppError> {
        let path = self.layout.identity_manifest_file(owner, name);
        if !path.exists() {
            return Err(AppError::IdentityNotFound(name.to_string()));
        }
        let raw = fs::read_to_string(&path)?;
        let document: IdentityDocument = serde_yaml::from_str(&raw)?;
        document.into_record(owner, name)
    }

    fn material_lock_for_layout(
        layout: &AppStateLayout,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<FileLock, AppError> {
        FileLock::exclusive(&layout.identity_refresh_lock_file(owner, identity_name))
            .map_err(Into::into)
    }

    fn count_identity_spec_references_unlocked(
        layout: &AppStateLayout,
        identity_spec_name: &str,
    ) -> Result<u32, AppError> {
        let identities_root = layout.identities_root();
        if !identities_root.exists() {
            return Ok(0);
        }
        let mut count = 0u32;
        for owner_kind_entry in fs::read_dir(identities_root)? {
            let owner_kind_entry = owner_kind_entry?;
            if !owner_kind_entry.file_type()?.is_dir() {
                continue;
            }
            for owner_entry in fs::read_dir(owner_kind_entry.path())? {
                let owner_entry = owner_entry?;
                if !owner_entry.file_type()?.is_dir() {
                    continue;
                }
                for identity_entry in fs::read_dir(owner_entry.path())? {
                    let identity_entry = identity_entry?;
                    if !identity_entry.file_type()?.is_dir() {
                        continue;
                    }
                    let manifest_path = identity_entry
                        .path()
                        .join(crate::state::INSTALLED_IDENTITY_FILE_NAME);
                    if !manifest_path.exists() {
                        continue;
                    }
                    let raw = fs::read_to_string(&manifest_path)?;
                    let reference: IdentitySpecReference = serde_yaml::from_str(&raw)?;
                    if reference.identity_spec == identity_spec_name {
                        count = checked_add_identity_count(count, 1, identity_spec_name)?;
                    }
                }
            }
        }
        Ok(count)
    }
}

#[tonic::async_trait]
impl IdentityStore for FileIdentityStore {
    async fn list_identities(
        &self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        let store = self.clone();
        let owner = owner.clone();
        tokio::task::spawn_blocking(move || {
            let _lock = FileLock::shared(store.layout.state_lock())?;
            let root = store.layout.identity_owner_root(&owner);
            if !root.exists() {
                return Ok(Vec::new());
            }
            let mut records = Vec::new();
            for entry in fs::read_dir(root)? {
                let entry = entry?;
                if !entry.file_type()?.is_dir() {
                    continue;
                }
                let Some(name) = entry.file_name().to_str().map(ToString::to_string) else {
                    continue;
                };
                let name = validate_identity_name(&name)?;
                records.push(store.load_identity_unlocked(&owner, &name)?);
            }
            records.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(records)
        })
        .await?
    }

    async fn load_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<Option<IdentityRecord>, AppError> {
        let store = self.clone();
        let owner = owner.clone();
        let identity_name = identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let _lock = FileLock::shared(store.layout.state_lock())?;
            match store.load_identity_unlocked(&owner, &identity_name) {
                Ok(record) => Ok(Some(record)),
                Err(AppError::IdentityNotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await?
    }

    async fn replace_identity(
        &self,
        record: &IdentityRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        let store = self.clone();
        let record = record.clone();
        let material = material.clone();
        tokio::task::spawn_blocking(move || {
            let owner = record.owner.clone();
            let _material_lock =
                Self::material_lock_for_layout(&store.layout, &owner, &record.name)?;
            let _state_lock = FileLock::exclusive(store.layout.state_lock())?;
            let manifest_path = store.layout.identity_manifest_file(&owner, &record.name);
            let material_path = store.layout.identity_material_file(&owner, &record.name);
            if let Some(parent) = manifest_path.parent() {
                storage_fs::ensure_private_dir(parent)?;
            }
            validate_identity_spec_reference_unlocked(&store.identity_specs, &record)?;
            write_files_transactionally(&[&manifest_path, &material_path], || {
                let document = IdentityDocument::from_record(&record);
                write_file_unlocked(&manifest_path, serde_yaml::to_string(&document)?.as_bytes())?;
                write_file_unlocked(&material_path, render_env_file(&material).as_bytes())?;
                Ok(())
            })
        })
        .await?
    }

    async fn delete_identity(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<bool, AppError> {
        let store = self.clone();
        let owner = owner.clone();
        let identity_name = identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let _material_lock =
                Self::material_lock_for_layout(&store.layout, &owner, &identity_name)?;
            let _state_lock = FileLock::exclusive(store.layout.state_lock())?;
            let manifest_path = store.layout.identity_manifest_file(&owner, &identity_name);
            let material_path = store.layout.identity_material_file(&owner, &identity_name);
            if !manifest_path.exists() {
                return Ok(false);
            }
            remove_file_if_exists_unlocked(&material_path)?;
            remove_file_if_exists_unlocked(&manifest_path)?;
            let identity_dir = store.layout.identity_dir(&owner, &identity_name);
            if identity_dir.exists() {
                match fs::remove_dir(&identity_dir) {
                    Ok(()) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) if error.kind() == std::io::ErrorKind::DirectoryNotEmpty => {}
                    Err(error) => return Err(error.into()),
                }
            }
            Ok(true)
        })
        .await?
    }

    async fn material_guard(
        &self,
        owner: &IdentityOwner,
        identity_name: &IdentityName,
    ) -> Result<Box<dyn IdentityMaterialGuard>, AppError> {
        let layout = self.layout.clone();
        let owner = owner.clone();
        let identity_name = identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let lock = Self::material_lock_for_layout(&layout, &owner, &identity_name)?;
            Ok(Box::new(FileIdentityMaterialGuard {
                layout,
                owner,
                identity_name,
                _lock: lock,
            }) as Box<dyn IdentityMaterialGuard>)
        })
        .await?
    }

    async fn replace_source_identity_binding(
        &self,
        user_id: &str,
        workspace_name: &str,
        source_name: &str,
        surface_id: &str,
        selection: &SourceIdentitySelection,
    ) -> Result<(), AppError> {
        let store = self.clone();
        let user_id = user_id.to_string();
        let workspace_name = workspace_name.to_string();
        let source_name = source_name.to_string();
        let surface_id = surface_id.to_string();
        let selection = selection.clone();
        tokio::task::spawn_blocking(move || {
            let user_id = validate_user_id(&user_id)?;
            let workspace_name = WorkspaceName::parse(&workspace_name)?;
            let source_name = SourceName::parse(&source_name)?;
            let surface_id = validate_source_surface_id(&surface_id)?;
            selection.validate()?;
            let _lock = FileLock::exclusive(store.layout.state_lock())?;
            let path = store.layout.user_owned_source_identity_binding_file(
                &user_id,
                &workspace_name,
                &source_name,
                &surface_id,
            );
            if let Some(parent) = path.parent() {
                storage_fs::ensure_private_dir(parent)?;
            }
            write_files_transactionally(&[&path], || {
                let document = UserSourceIdentityBindingDocument::from_selection(&selection);
                write_file_unlocked(&path, serde_yaml::to_string(&document)?.as_bytes())?;
                Ok(())
            })
        })
        .await?
    }

    async fn load_optional_source_identity_binding(
        &self,
        user_id: &str,
        workspace_name: &str,
        source_name: &str,
        surface_id: &str,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        let store = self.clone();
        let user_id = user_id.to_string();
        let workspace_name = workspace_name.to_string();
        let source_name = source_name.to_string();
        let surface_id = surface_id.to_string();
        tokio::task::spawn_blocking(move || {
            let user_id = validate_user_id(&user_id)?;
            let workspace_name = WorkspaceName::parse(&workspace_name)?;
            let source_name = SourceName::parse(&source_name)?;
            let surface_id = validate_source_surface_id(&surface_id)?;
            let _lock = FileLock::shared(store.layout.state_lock())?;
            let path = store.layout.user_owned_source_identity_binding_file(
                &user_id,
                &workspace_name,
                &source_name,
                &surface_id,
            );
            if !path.exists() {
                return Ok(None);
            }
            let raw = fs::read_to_string(&path)?;
            let document: UserSourceIdentityBindingDocument = serde_yaml::from_str(&raw)?;
            document.into_selection().map(Some)
        })
        .await?
    }

    async fn load_source_identity_binding(
        &self,
        user_id: &str,
        workspace_name: &str,
        source_name: &str,
        surface_id: &str,
    ) -> Result<SourceIdentitySelection, AppError> {
        let selection = self
            .load_optional_source_identity_binding(user_id, workspace_name, source_name, surface_id)
            .await?;
        selection.ok_or_else(|| {
            AppError::FailedPrecondition(format!(
                "user '{user_id}' has no selected identity for source '{source_name}' surface '{surface_id}'"
            ))
        })
    }

    async fn delete_source_identity_binding(
        &self,
        user_id: &str,
        workspace_name: &str,
        source_name: &str,
        surface_id: &str,
    ) -> Result<bool, AppError> {
        let store = self.clone();
        let user_id = user_id.to_string();
        let workspace_name = workspace_name.to_string();
        let source_name = source_name.to_string();
        let surface_id = surface_id.to_string();
        tokio::task::spawn_blocking(move || {
            let user_id = validate_user_id(&user_id)?;
            let workspace_name = WorkspaceName::parse(&workspace_name)?;
            let source_name = SourceName::parse(&source_name)?;
            let surface_id = validate_source_surface_id(&surface_id)?;
            let _lock = FileLock::exclusive(store.layout.state_lock())?;
            let path = store.layout.user_owned_source_identity_binding_file(
                &user_id,
                &workspace_name,
                &source_name,
                &surface_id,
            );
            if !path.exists() {
                return Ok(false);
            }
            fs::remove_file(path)?;
            Ok(true)
        })
        .await?
    }

    fn count_identities_for_spec(&self, identity_spec_name: &str) -> Result<u32, AppError> {
        let identity_spec_name = validate_identity_spec_name(identity_spec_name)?;
        Self::count_identity_spec_references_unlocked(&self.layout, identity_spec_name.as_str())
    }
}

struct FileIdentityMaterialGuard {
    layout: AppStateLayout,
    owner: IdentityOwner,
    identity_name: IdentityName,
    _lock: FileLock,
}

#[tonic::async_trait]
impl IdentityMaterialGuard for FileIdentityMaterialGuard {
    async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError> {
        let path = self
            .layout
            .identity_material_file(&self.owner, &self.identity_name);
        let identity_name = self.identity_name.to_string();
        tokio::task::spawn_blocking(move || match fs::read_to_string(path) {
            Ok(raw) => parse_env_file(&raw).map_err(|error| {
                AppError::Credentials(crate::credentials::CredentialsError::from(error))
            }),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                Err(AppError::FailedPrecondition(format!(
                    "identity '{identity_name}' is missing credential material"
                )))
            }
            Err(error) => Err(error.into()),
        })
        .await?
    }

    async fn write_material(&self, material: &BTreeMap<String, String>) -> Result<(), AppError> {
        let layout = self.layout.clone();
        let owner = self.owner.clone();
        let identity_name = self.identity_name.clone();
        let material = material.clone();
        tokio::task::spawn_blocking(move || {
            let path = layout.identity_material_file(&owner, &identity_name);
            let _state_lock = FileLock::exclusive(layout.state_lock())?;
            let manifest_path = layout.identity_manifest_file(&owner, &identity_name);
            if !manifest_path.exists() {
                return Err(AppError::IdentityNotFound(identity_name.to_string()));
            }
            write_file_unlocked(&path, render_env_file(&material).as_bytes())
        })
        .await?
    }
}

#[tonic::async_trait]
impl SourceIdentityProvider for IdentityManager {
    async fn resolve_source_identity_selection(
        &self,
        request: &SourceIdentitySelectionRequest,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        if request.binding.owner != IdentityOwnerKind::User {
            return Ok(None);
        }
        request.binding.validate()?;
        let user_id = validate_user_id(&request.user_id)?;
        let workspace_name = WorkspaceName::parse(&request.workspace_name)?;
        let source_name = SourceName::parse(&request.source_name)?;
        let selection = self
            .store
            .load_source_identity_binding(
                &user_id,
                workspace_name.as_str(),
                source_name.as_str(),
                &request.surface_id,
            )
            .await?;
        Ok(Some(selection))
    }

    async fn resolve_source_identity(
        &self,
        request: &SourceIdentityResolutionRequest,
    ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
        request.binding.validate()?;
        request.selection.validate()?;
        let owner = match request.binding.owner {
            IdentityOwnerKind::User => {
                let Some(user_id) = request.user_id.as_deref() else {
                    return Ok(None);
                };
                IdentityOwner::user(user_id)?
            }
            IdentityOwnerKind::Workspace => {
                let workspace_name = WorkspaceName::parse(&request.workspace_name)?;
                IdentityOwner::workspace(workspace_name.as_str())?
            }
        };
        let identity_name = validate_identity_name(&request.selection.identity)?;
        self.resolve_identity(&owner, &identity_name).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityDocument {
    version: u32,
    #[serde(default = "default_identity_owner_kind")]
    owner: IdentityOwnerKind,
    name: String,
    identity_spec: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity_spec_fingerprint: Option<String>,
    issuer: String,
    identity_type: String,
    #[serde(default)]
    metadata: BTreeMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserSourceIdentityBindingDocument {
    version: u32,
    identity: String,
}

impl UserSourceIdentityBindingDocument {
    fn from_selection(selection: &SourceIdentitySelection) -> Self {
        Self {
            version: USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION,
            identity: selection.identity.clone(),
        }
    }

    fn into_selection(self) -> Result<SourceIdentitySelection, AppError> {
        if self.version != USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION {
            return Err(AppError::FailedPrecondition(format!(
                "source identity binding for identity '{}' has unsupported document version {}; expected {}",
                self.identity, self.version, USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION
            )));
        }
        SourceIdentitySelection::new(self.identity)
    }
}

impl IdentityDocument {
    fn from_record(record: &IdentityRecord) -> Self {
        Self {
            version: IDENTITY_DOCUMENT_VERSION,
            owner: record.owner.kind(),
            name: record.name.to_string(),
            identity_spec: record.identity_spec.clone(),
            identity_spec_fingerprint: record.identity_spec_fingerprint.clone(),
            issuer: record.issuer.clone(),
            identity_type: record.identity_type.clone(),
            metadata: record.metadata.clone(),
        }
    }

    fn into_record(
        self,
        expected_owner: &IdentityOwner,
        expected_name: &IdentityName,
    ) -> Result<IdentityRecord, AppError> {
        if self.version != IDENTITY_DOCUMENT_VERSION {
            return Err(AppError::FailedPrecondition(format!(
                "identity '{}' has unsupported document version {}; expected {}",
                self.name, self.version, IDENTITY_DOCUMENT_VERSION
            )));
        }
        let name = validate_identity_name(&self.name)?;
        if &name != expected_name {
            return Err(AppError::FailedPrecondition(format!(
                "identity file for '{expected_name}' contains identity '{name}'"
            )));
        }
        if self.owner != expected_owner.kind() {
            return Err(AppError::FailedPrecondition(format!(
                "identity file for '{expected_name}' is owned by '{}', but storage path is for '{}'",
                self.owner.as_config_value(),
                expected_owner.kind().as_config_value()
            )));
        }
        Ok(IdentityRecord {
            owner: expected_owner.clone(),
            name,
            identity_spec: validate_identity_spec_name(&self.identity_spec)?.to_string(),
            identity_spec_fingerprint: self.identity_spec_fingerprint,
            issuer: self.issuer,
            identity_type: self.identity_type,
            metadata: self.metadata,
        })
    }
}

const fn default_identity_owner_kind() -> IdentityOwnerKind {
    IdentityOwnerKind::User
}

#[derive(Debug, Deserialize)]
struct IdentitySpecReference {
    identity_spec: String,
}

fn oauth_method<'a>(
    identity_spec_name: &str,
    config: &'a IdentitySpecConfig,
) -> Result<&'a ManifestOAuthCredentialSpec, AppError> {
    let IdentitySpecConfig::OAuth(oauth) = config else {
        return Err(AppError::InvalidInput(format!(
            "identity spec '{identity_spec_name}' is not oauth"
        )));
    };
    Ok(&oauth.method.oauth)
}

fn material_values(material: OAuthCredentialMaterial) -> BTreeMap<String, String> {
    let mut values = material.internal_metadata;
    values.insert(
        OAUTH_ACCESS_TOKEN_MATERIAL_KEY.to_string(),
        material.access_token,
    );
    values
}

fn validate_record_identity_type(
    record: &IdentityRecord,
    identity_spec_type: IdentitySpecType,
) -> Result<(), AppError> {
    let expected = identity_spec_type.label();
    if record.identity_type == expected {
        return Ok(());
    }
    Err(AppError::FailedPrecondition(format!(
        "identity '{}' is orphaned because identity spec '{}' has type '{}', but the stored identity has type '{}'",
        record.name, record.identity_spec, expected, record.identity_type
    )))
}

fn validate_record_identity_spec_fingerprint(
    record: &IdentityRecord,
    identity_spec: &IdentityManifest,
) -> Result<(), AppError> {
    let Some(stored_fingerprint) = record.identity_spec_fingerprint.as_deref() else {
        return Err(AppError::FailedPrecondition(format!(
            "identity '{}' is orphaned because it was created before identity spec fingerprinting; recreate it from identity spec '{}'",
            record.name, record.identity_spec
        )));
    };
    let current_fingerprint = identity_spec_fingerprint(identity_spec)?;
    if stored_fingerprint == current_fingerprint {
        return Ok(());
    }
    Err(AppError::FailedPrecondition(format!(
        "identity '{}' is orphaned because identity spec '{}' has changed since the identity was created; recreate the identity from the current spec",
        record.name, record.identity_spec
    )))
}

fn reject_identity_owned_inputs(
    identity_name: &str,
    identity_spec_name: &str,
    manifest: &IdentityManifest,
    oauth: &ManifestOAuthCredentialSpec,
    provided: &BTreeMap<String, String>,
) -> Result<(), AppError> {
    let declared = manifest
        .inputs
        .iter()
        .map(|input| input.key.as_str())
        .collect::<BTreeSet<_>>();
    for key in provided.keys() {
        if declared.contains(key.as_str()) {
            return Err(AppError::InvalidInput(format!(
                "identity input '{key}' belongs to identity spec '{identity_spec_name}', not identity '{identity_name}'; provide it when installing the identity spec"
            )));
        }
        if legacy_oauth_client_input(oauth, key) {
            continue;
        }
        return Err(AppError::InvalidInput(format!(
            "unknown OAuth credential input '{key}' for identity '{identity_name}'"
        )));
    }
    Ok(())
}

fn legacy_oauth_client_input(oauth: &ManifestOAuthCredentialSpec, key: &str) -> bool {
    oauth.client.id.input.as_deref() == Some(key)
        || oauth
            .client
            .secret
            .as_ref()
            .is_some_and(|secret| secret.input == key)
}

fn oauth_credential_inputs_from_identity_inputs(
    oauth: &ManifestOAuthCredentialSpec,
    identity_inputs: &BTreeMap<String, String>,
    provided: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    let mut values = Vec::new();
    if let Some(input_key) = oauth.client.id.input.as_deref()
        && let Some(value) = identity_inputs
            .get(input_key)
            .or_else(|| provided.get(input_key))
            .filter(|value| !value.is_empty())
    {
        values.push((input_key.to_string(), value.clone()));
    }
    if let Some(secret) = oauth.client.secret.as_ref()
        && let Some(value) = identity_inputs
            .get(&secret.input)
            .or_else(|| provided.get(&secret.input))
            .filter(|value| !value.is_empty())
    {
        values.push((secret.input.clone(), value.clone()));
    }
    values
}

fn oauth_refresh_material_persistence_for_identity(
    oauth: &ManifestOAuthCredentialSpec,
    provided: &BTreeMap<String, String>,
) -> OAuthRefreshMaterialPersistence {
    let client_id = oauth
        .client
        .id
        .input
        .as_deref()
        .is_some_and(|input| provided.contains_key(input));
    let client_secret = oauth
        .client
        .secret
        .as_ref()
        .is_some_and(|secret| provided.contains_key(&secret.input));
    if client_id || client_secret {
        OAuthRefreshMaterialPersistence::PartialRefreshContext {
            client_id,
            client_secret,
        }
    } else {
        OAuthRefreshMaterialPersistence::None
    }
}

fn validate_identity_name(name: &str) -> Result<IdentityName, AppError> {
    IdentityName::new(name)
}

fn validate_user_id(user_id: &str) -> Result<String, AppError> {
    parse_path_segment("user", user_id)
}

fn source_identity_binding_store_unsupported() -> AppError {
    AppError::FailedPrecondition(
        "identity store does not support source identity bindings".to_string(),
    )
}

fn validate_identity_spec_reference_unlocked(
    identity_specs: &IdentitySpecManager,
    record: &IdentityRecord,
) -> Result<(), AppError> {
    let spec = identity_specs
        .load_identity_spec_manifest_unlocked_for_state_lock(&record.identity_spec)?;
    validate_record_identity_type(record, spec.manifest.identity_type)?;
    validate_record_identity_spec_fingerprint(record, &spec.manifest)
}

fn checked_add_identity_count(
    current: u32,
    additional: u32,
    identity_spec_name: &str,
) -> Result<u32, AppError> {
    current.checked_add(additional).ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "too many stored identities reference identity spec '{identity_spec_name}'"
        ))
    })
}

fn validate_source_surface_id(surface_id: &str) -> Result<String, AppError> {
    parse_path_segment("source surface", surface_id)
}

/// Snapshots `paths`, runs `write`, and restores every file to its prior
/// contents if `write` fails, so a partial write never leaves files mutated.
fn write_files_transactionally(
    paths: &[&std::path::Path],
    write: impl FnOnce() -> Result<(), AppError>,
) -> Result<(), AppError> {
    let snapshots = paths
        .iter()
        .map(|&path| snapshot_file_unlocked(path))
        .collect::<Result<Vec<_>, _>>()?;
    let result = write();
    if result.is_err() {
        for (&path, snapshot) in paths.iter().zip(snapshots) {
            restore_file_unlocked(path, snapshot)?;
        }
    }
    result
}

fn snapshot_file_unlocked(path: &std::path::Path) -> Result<Option<Vec<u8>>, AppError> {
    match fs::read(path) {
        Ok(bytes) => Ok(Some(bytes)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn restore_file_unlocked(
    path: &std::path::Path,
    snapshot: Option<Vec<u8>>,
) -> Result<(), AppError> {
    match snapshot {
        Some(bytes) => write_file_unlocked(path, &bytes),
        None => remove_file_if_exists_unlocked(path).map_err(Into::into),
    }
}

fn write_file_unlocked(path: &Path, bytes: &[u8]) -> Result<(), AppError> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    storage_fs::ensure_dir(parent)?;
    storage_fs::write_atomic(path, bytes)?;
    Ok(())
}

fn remove_file_if_exists_unlocked(path: &Path) -> Result<(), std::io::Error> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::features::{Features, dsl_v4_features};
    use crate::identity::{
        SourceIdentityBinding, SourceIdentitySelection, SourceIdentitySelectionRequest,
    };
    use crate::identity_specs::{IdentitySpecRegistry, IdentitySpecRegistryRecord};
    use tempfile::TempDir;

    #[test]
    fn identity_owner_rejects_storage_unsafe_keys() {
        IdentityOwner::user(" ").unwrap_err();
        IdentityOwner::user("a/b").unwrap_err();
        IdentityOwner::user("a\\b").unwrap_err();
        IdentityOwner::user("..").unwrap_err();
    }

    #[test]
    fn identity_owner_exposes_kind_and_storage_key() {
        let owner = IdentityOwner::user("member-123").expect("owner");

        assert_eq!(owner.kind(), IdentityOwnerKind::User);
        assert_eq!(owner.key(), "member-123");
        assert_eq!(owner.to_string(), "user:member-123");
    }

    #[test]
    fn identity_name_rejects_storage_unsafe_values() {
        IdentityName::new(" ").unwrap_err();
        IdentityName::new("a/b").unwrap_err();
        IdentityName::new("a\\b").unwrap_err();
        IdentityName::new("..").unwrap_err();
    }

    #[test]
    fn identity_name_round_trips_storage_name() {
        let identity_name = IdentityName::new("github-primary").expect("identity name");

        assert_eq!(identity_name.as_str(), "github-primary");
        assert_eq!(identity_name.to_string(), "github-primary");
        assert_eq!(
            "github-primary".parse::<IdentityName>().expect("parse"),
            identity_name
        );
    }

    #[derive(Debug)]
    struct MinimalIdentityStore;

    #[tonic::async_trait]
    impl IdentityStore for MinimalIdentityStore {
        async fn list_identities(
            &self,
            _owner: &IdentityOwner,
        ) -> Result<Vec<IdentityRecord>, AppError> {
            Ok(Vec::new())
        }

        async fn load_identity(
            &self,
            _owner: &IdentityOwner,
            _identity_name: &IdentityName,
        ) -> Result<Option<IdentityRecord>, AppError> {
            Ok(None)
        }

        async fn replace_identity(
            &self,
            _record: &IdentityRecord,
            _material: &BTreeMap<String, String>,
        ) -> Result<(), AppError> {
            Ok(())
        }

        async fn delete_identity(
            &self,
            _owner: &IdentityOwner,
            _identity_name: &IdentityName,
        ) -> Result<bool, AppError> {
            Ok(false)
        }

        async fn material_guard(
            &self,
            _owner: &IdentityOwner,
            _identity_name: &IdentityName,
        ) -> Result<Box<dyn IdentityMaterialGuard>, AppError> {
            Err(AppError::FailedPrecondition(
                "minimal store has no material guard".to_string(),
            ))
        }

        fn count_identities_for_spec(&self, _identity_spec_name: &str) -> Result<u32, AppError> {
            Ok(0)
        }
    }

    #[tokio::test]
    async fn identity_store_source_binding_optional_methods_fail_closed() {
        let store = MinimalIdentityStore;

        let load_error = store
            .load_optional_source_identity_binding("user", "workspace", "source", "surface")
            .await
            .expect_err("optional load should fail closed");
        assert!(matches!(
            load_error,
            AppError::FailedPrecondition(message)
                if message.contains("does not support source identity bindings")
        ));

        let delete_error = store
            .delete_source_identity_binding("user", "workspace", "source", "surface")
            .await
            .expect_err("delete should fail closed");
        assert!(matches!(
            delete_error,
            AppError::FailedPrecondition(message)
                if message.contains("does not support source identity bindings")
        ));
    }

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout directories");
        layout
    }

    fn manager() -> (TempDir, IdentityManager, IdentitySpecManager) {
        manager_with_features(dsl_v4_features())
    }

    fn manager_with_features(
        features: Features,
    ) -> (TempDir, IdentityManager, IdentitySpecManager) {
        let temp = TempDir::new().expect("tempdir");
        let layout = test_layout(&temp);
        let identity_specs =
            IdentitySpecManager::new_with_usage_providers(layout.clone(), features, Vec::new());
        (
            temp,
            IdentityManager::new(layout, identity_specs.clone()),
            identity_specs,
        )
    }

    fn manager_with_github_pat_spec() -> (TempDir, IdentityManager, IdentitySpecManager) {
        let (temp, manager, identity_specs) = manager();
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml())
            .expect("add identity spec");
        (temp, manager, identity_specs)
    }

    fn fixed_identity_spec_yaml() -> String {
        fixed_identity_spec_yaml_with_issuer("github")
    }

    fn fixed_identity_spec_yaml_with_issuer(issuer: &str) -> String {
        fixed_identity_spec_yaml_with(issuer, "github.com")
    }

    fn fixed_identity_spec_yaml_for_host(host: &str) -> String {
        fixed_identity_spec_yaml_with("github", host)
    }

    fn fixed_identity_spec_yaml_with(issuer: &str, host: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: github_pat
version: 0.1.0
issuer: {issuer}
type: fixed_token
audience:
  host: {host}
"
        )
    }

    fn github_local_command(token: &str) -> CreateFixedTokenIdentityCommand {
        CreateFixedTokenIdentityCommand {
            name: "github_local".to_string(),
            identity_spec: "github_pat".to_string(),
            token: token.to_string(),
        }
    }

    fn github_local_name() -> IdentityName {
        IdentityName::new("github_local").expect("github_local identity name")
    }

    fn local_owner() -> IdentityOwner {
        IdentityOwner::user("local").expect("local owner")
    }

    fn default_workspace_owner() -> IdentityOwner {
        IdentityOwner::workspace("default").expect("workspace owner")
    }

    fn material(key: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(key.to_string(), value.to_string())])
    }
    fn resolution_request(identity_name: &str) -> SourceIdentityResolutionRequest {
        SourceIdentityResolutionRequest {
            workspace_name: "default".to_string(),
            user_id: Some("local".to_string()),
            source_name: "github".to_string(),
            surface_id: "rest".to_string(),
            binding: SourceIdentityBinding::user_owned(),
            selection: SourceIdentitySelection::new(identity_name).expect("selection"),
            identity_requirements: coral_spec::v4::IdentityRequirements {
                accepts: Vec::new(),
            },
        }
    }

    #[tokio::test]
    async fn create_user_owned_identity_requires_dsl_v4_feature() {
        let (_temp, manager, _identity_specs) = manager_with_features(Features::default());

        let error = manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("token"),
            )
            .await
            .expect_err("identity creation requires the dsl_v4 feature");

        assert!(
            matches!(&error, AppError::SourceUnservable(message) if message.contains("dsl_v4")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn create_user_owned_fixed_token_identity_stores_record_and_material() {
        let (temp, manager, _identity_specs) = manager_with_github_pat_spec();

        let record = manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("  ghp_token  "),
            )
            .await
            .expect("create identity");

        assert_eq!(record.name.as_str(), "github_local");
        assert_eq!(record.identity_spec, "github_pat");
        assert_eq!(record.issuer, "github");
        assert_eq!(record.identity_type, "fixed_token");
        assert!(
            record.identity_spec_fingerprint.is_some(),
            "identity should pin the identity spec fingerprint"
        );
        assert!(record.metadata.is_empty());
        assert_eq!(record.owner, local_owner());

        let layout = test_layout(&temp);
        let raw =
            fs::read_to_string(layout.identity_manifest_file(&local_owner(), &github_local_name()))
                .expect("identity manifest");
        assert!(raw.contains("identity_spec: github_pat"));
        assert!(raw.contains("owner: user"));
        let material =
            fs::read_to_string(layout.identity_material_file(&local_owner(), &github_local_name()))
                .expect("identity material");
        assert!(material.contains("TOKEN=ghp_token"));
        assert!(!material.contains("  ghp_token  "));
    }

    #[tokio::test]
    async fn create_workspace_fixed_token_identity_stores_record_and_material() {
        let (temp, manager, _identity_specs) = manager_with_github_pat_spec();
        let owner = default_workspace_owner();

        let record = manager
            .create_fixed_token_identity(&owner, github_local_command("workspace-token"))
            .await
            .expect("create workspace identity");

        assert_eq!(record.owner, owner);
        assert_eq!(record.name.as_str(), "github_local");

        let layout = test_layout(&temp);
        let raw = fs::read_to_string(
            layout.identity_manifest_file(&default_workspace_owner(), &github_local_name()),
        )
        .expect("workspace identity manifest");
        assert!(raw.contains("owner: workspace"));
        let material = fs::read_to_string(
            layout.identity_material_file(&default_workspace_owner(), &github_local_name()),
        )
        .expect("workspace identity material");
        assert!(material.contains("TOKEN=workspace-token"));
        assert!(
            !layout
                .identity_manifest_file(&local_owner(), &github_local_name())
                .exists(),
            "workspace identity should not be stored in the user bucket"
        );
    }

    #[tokio::test]
    async fn create_user_owned_fixed_token_identity_rejects_whitespace_token() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();

        let error = manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("   "),
            )
            .await
            .expect_err("whitespace-only token should be rejected");

        assert!(
            matches!(error, AppError::InvalidInput(ref message) if message.contains("must not be empty")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn custom_identity_store_source_binding_defaults_fail_closed() {
        let store = MinimalIdentityStore;
        let selection = SourceIdentitySelection::new("github_local").expect("selection");

        let replace_error = store
            .replace_source_identity_binding("local", "default", "github_v4", "rest", &selection)
            .await
            .expect_err("default source binding writes should fail closed");
        assert!(
            matches!(replace_error, AppError::FailedPrecondition(ref message) if message.contains("does not support source identity bindings")),
            "unexpected error: {replace_error:?}"
        );

        let optional_error = store
            .load_optional_source_identity_binding("local", "default", "github_v4", "rest")
            .await
            .expect_err("default optional source binding reads should fail closed");
        assert!(
            matches!(optional_error, AppError::FailedPrecondition(ref message) if message.contains("does not support source identity bindings")),
            "unexpected error: {optional_error:?}"
        );

        let load_error = store
            .load_source_identity_binding("local", "default", "github_v4", "rest")
            .await
            .expect_err("default source binding reads should fail closed");
        assert!(
            matches!(load_error, AppError::FailedPrecondition(ref message) if message.contains("does not support source identity bindings")),
            "unexpected error: {load_error:?}"
        );

        let delete_error = store
            .delete_source_identity_binding("local", "default", "github_v4", "rest")
            .await
            .expect_err("default source binding deletes should fail closed");
        assert!(
            matches!(delete_error, AppError::FailedPrecondition(ref message) if message.contains("does not support source identity bindings")),
            "unexpected error: {delete_error:?}"
        );
    }

    #[tokio::test]
    async fn identity_write_rejects_identity_spec_deleted_after_validation() {
        let (_temp, manager, identity_specs) = manager_with_github_pat_spec();
        let original_spec = identity_specs
            .get_identity_spec("github_pat")
            .expect("original identity spec");
        let record = IdentityRecord {
            owner: local_owner(),
            name: github_local_name(),
            identity_spec: "github_pat".to_string(),
            identity_spec_fingerprint: Some(
                identity_spec_fingerprint(&original_spec.manifest).expect("fingerprint"),
            ),
            issuer: "github".to_string(),
            identity_type: IdentitySpecType::FixedToken.label().to_string(),
            metadata: BTreeMap::new(),
        };
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("remove original spec");

        let error = manager
            .store
            .replace_identity(&record, &material(FIXED_TOKEN_MATERIAL_KEY, "ghp_token"))
            .await
            .expect_err("deleted identity spec reference must be rejected");

        assert!(matches!(error, AppError::IdentitySpecNotFound(name) if name == "github_pat"));
    }

    #[tokio::test]
    async fn identity_write_rejects_identity_spec_changed_after_validation() {
        let (temp, manager, identity_specs) = manager_with_github_pat_spec();
        let original_spec = identity_specs
            .get_identity_spec("github_pat")
            .expect("original identity spec");
        let record = IdentityRecord {
            owner: local_owner(),
            name: github_local_name(),
            identity_spec: "github_pat".to_string(),
            identity_spec_fingerprint: Some(
                identity_spec_fingerprint(&original_spec.manifest).expect("fingerprint"),
            ),
            issuer: "github".to_string(),
            identity_type: IdentitySpecType::FixedToken.label().to_string(),
            metadata: BTreeMap::new(),
        };
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("remove original spec");
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml_with_issuer("github_enterprise"))
            .expect("replace with changed spec");

        let error = manager
            .store
            .replace_identity(&record, &material(FIXED_TOKEN_MATERIAL_KEY, "ghp_token"))
            .await
            .expect_err("stale identity spec reference must be rejected");

        assert!(
            matches!(error, AppError::FailedPrecondition(ref message) if message.contains("has changed since the identity was created")),
            "unexpected error: {error:?}"
        );
        let layout = test_layout(&temp);
        assert!(
            !layout
                .identity_manifest_file(&local_owner(), &github_local_name())
                .exists(),
            "rejected stale identity should not create a manifest"
        );
        assert!(
            !layout
                .identity_material_file(&local_owner(), &github_local_name())
                .exists(),
            "rejected stale identity should not create material"
        );
    }

    #[tokio::test]
    async fn identity_management_handle_resolves_fixed_token_identity() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        let principal = UserPrincipal::local();
        manager
            .create_user_owned_fixed_token_identity(&principal, github_local_command("ghp_token"))
            .await
            .expect("create identity");

        let identity = manager
            .handle()
            .resolve_identity(&local_owner(), "github_local")
            .await
            .expect("resolve identity")
            .expect("identity exists");
        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.github.com/user".parse().expect("request url"),
        );
        let headers = identity
            .resolve_headers(
                &coral_engine::SelectedRequestIdentity::new(
                    "github_local".to_string(),
                    identity.identity_spec_id().to_string(),
                    identity.audience().clone(),
                ),
                &request,
                &BTreeMap::new(),
            )
            .await
            .expect("identity headers");

        assert_eq!(
            headers,
            vec![(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_static("Bearer ghp_token")
            )]
        );
    }

    #[tokio::test]
    async fn handle_validates_identity_metadata_without_credential_material() {
        let (_temp, manager, identity_specs) = manager_with_github_pat_spec();
        manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("identity-token"),
            )
            .await
            .expect("create identity");
        let owner = local_owner();
        let identity_name = github_local_name();
        let record = manager
            .store
            .load_identity(&owner, &identity_name)
            .await
            .expect("load identity")
            .expect("identity");
        manager
            .store
            .replace_identity(&record, &BTreeMap::new())
            .await
            .expect("remove credential material");
        let handle = manager.handle();

        let validated = handle
            .validate_identity_metadata(&owner, "github_local")
            .await
            .expect("validate metadata")
            .expect("identity");

        assert_eq!(validated.name, identity_name);
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("force remove spec");
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml_for_host("api.github.com"))
            .expect("add changed spec");
        let error = handle
            .validate_identity_metadata(&owner, "github_local")
            .await
            .expect_err("changed identity spec should fail metadata validation");
        assert!(
            error
                .to_string()
                .contains("has changed since the identity was created"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn handle_validates_identity_metadata_from_manifest_only_registry_path() {
        let temp = TempDir::new().expect("tempdir");
        let layout = test_layout(&temp);
        let manifest_yaml = fixed_identity_spec_yaml();
        let manifest = coral_spec::parse_identity_manifest_yaml(&manifest_yaml).expect("manifest");
        let identity_specs = IdentitySpecManager::new_with_registry(
            layout.clone(),
            Arc::new(ManifestOnlyIdentitySpecRegistry { manifest_yaml }),
            dsl_v4_features(),
            Vec::new(),
        );
        let manager = IdentityManager::new(layout, identity_specs);
        let owner = local_owner();
        let identity_name = github_local_name();
        let record = IdentityRecord {
            owner: owner.clone(),
            name: identity_name.clone(),
            identity_spec: manifest.name.clone(),
            identity_spec_fingerprint: Some(
                identity_spec_fingerprint(&manifest).expect("fingerprint"),
            ),
            issuer: manifest.issuer.clone(),
            identity_type: manifest.identity_type.label().to_string(),
            metadata: BTreeMap::new(),
        };
        manager
            .store
            .replace_identity(&record, &BTreeMap::new())
            .await
            .expect("store identity");

        let validated = manager
            .handle()
            .validate_identity_metadata(&owner, "github_local")
            .await
            .expect("validate metadata")
            .expect("identity");

        assert_eq!(validated, record);
    }

    #[derive(Debug)]
    struct ManifestOnlyIdentitySpecRegistry {
        manifest_yaml: String,
    }

    impl IdentitySpecRegistry for ManifestOnlyIdentitySpecRegistry {
        fn list_identity_specs(&self) -> Result<Vec<IdentitySpecRegistryRecord>, AppError> {
            Err(unexpected_manifest_only_registry_call(
                "list identity specs",
            ))
        }

        fn get_identity_spec(
            &self,
            _name: &str,
        ) -> Result<Option<IdentitySpecRegistryRecord>, AppError> {
            Err(unexpected_manifest_only_registry_call(
                "hydrate identity spec input material",
            ))
        }

        fn get_identity_spec_manifest_yaml(&self, name: &str) -> Result<Option<String>, AppError> {
            if name != "github_pat" {
                return Err(AppError::FailedPrecondition(format!(
                    "unexpected identity spec lookup '{name}'"
                )));
            }
            Ok(Some(self.manifest_yaml.clone()))
        }

        fn upsert_identity_spec(
            &self,
            _name: &str,
            _record: IdentitySpecRegistryRecord,
        ) -> Result<(), AppError> {
            Err(unexpected_manifest_only_registry_call(
                "upsert identity specs",
            ))
        }

        fn remove_identity_spec(&self, _name: &str) -> Result<(), AppError> {
            Err(unexpected_manifest_only_registry_call(
                "remove identity specs",
            ))
        }
    }

    fn unexpected_manifest_only_registry_call(operation: &str) -> AppError {
        AppError::FailedPrecondition(format!("metadata validation should not {operation}"))
    }

    #[tokio::test]
    async fn built_in_provider_resolves_workspace_owned_bindings() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        manager
            .create_fixed_token_identity(
                &default_workspace_owner(),
                github_local_command("ghp_token"),
            )
            .await
            .expect("create workspace identity");
        let request = SourceIdentityResolutionRequest {
            user_id: None,
            binding: SourceIdentityBinding::workspace_owned("github_local").expect("binding"),
            ..resolution_request("github_local")
        };

        let identity = manager
            .resolve_source_identity(&request)
            .await
            .expect("resolve workspace binding")
            .expect("workspace identity");
        let request = reqwest::Request::new(
            reqwest::Method::GET,
            "https://api.github.com/user".parse().expect("request url"),
        );
        let headers = identity
            .resolve_headers(
                &coral_engine::SelectedRequestIdentity::new(
                    "github_local".to_string(),
                    identity.identity_spec_id().to_string(),
                    identity.audience().clone(),
                ),
                &request,
                &BTreeMap::new(),
            )
            .await
            .expect("identity headers");

        assert_eq!(
            headers,
            vec![(
                reqwest::header::AUTHORIZATION,
                reqwest::header::HeaderValue::from_static("Bearer ghp_token")
            )]
        );
    }

    #[tokio::test]
    async fn resolves_user_owned_source_identity_selection_per_user() {
        let (_temp, manager, _identity_specs) = manager();
        let principal = UserPrincipal::for_user("saul").expect("principal");
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github_v4").expect("source");
        let selection = SourceIdentitySelection::new("github_saul").expect("selection");
        manager
            .store
            .replace_source_identity_binding(
                principal.user_id(),
                workspace_name.as_str(),
                source_name.as_str(),
                "rest",
                &selection,
            )
            .await
            .expect("write source identity binding");

        let request = SourceIdentitySelectionRequest {
            workspace_name: "default".to_string(),
            user_id: "saul".to_string(),
            source_name: "github_v4".to_string(),
            surface_id: "rest".to_string(),
            binding: SourceIdentityBinding::user_owned(),
        };

        let resolved = manager
            .resolve_source_identity_selection(&request)
            .await
            .expect("resolve source identity binding")
            .expect("selection");
        assert_eq!(resolved, selection);

        let missing_user = SourceIdentitySelectionRequest {
            user_id: "tina".to_string(),
            ..request
        };
        let error = manager
            .resolve_source_identity_selection(&missing_user)
            .await
            .expect_err("selection should be per user");
        assert!(
            error
                .to_string()
                .contains("user 'tina' has no selected identity")
        );
    }

    #[tokio::test]
    async fn lists_user_owned_identity_records() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        let principal = UserPrincipal::local();
        manager
            .create_user_owned_fixed_token_identity(&principal, github_local_command("ghp_token"))
            .await
            .expect("create identity");

        let listed = manager
            .list_user_owned_identities(&principal)
            .await
            .expect("list identities");

        assert_eq!(listed.len(), 1);
        assert_eq!(
            listed.first().expect("listed identity").name.as_str(),
            "github_local"
        );
    }

    #[tokio::test]
    async fn identity_replacement_waits_for_in_flight_material_refresh() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("old-token"),
            )
            .await
            .expect("create identity");

        let refresh_guard = manager
            .store
            .material_guard(&local_owner(), &github_local_name())
            .await
            .expect("hold refresh lock");
        let replacement_manager = manager.clone();
        let mut replacement = tokio::spawn(async move {
            replacement_manager
                .create_user_owned_fixed_token_identity(
                    &UserPrincipal::local(),
                    github_local_command("replacement-token"),
                )
                .await
        });

        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut replacement)
                .await
                .is_err(),
            "identity replacement must wait behind an in-flight material refresh"
        );
        refresh_guard
            .write_material(&material(FIXED_TOKEN_MATERIAL_KEY, "stale-refreshed-token"))
            .await
            .expect("write stale refreshed material while refresh lock is held");
        drop(refresh_guard);
        replacement
            .await
            .expect("replacement task")
            .expect("replacement should succeed");

        let final_material = manager
            .store
            .material_guard(&local_owner(), &github_local_name())
            .await
            .expect("open final material guard")
            .read_material()
            .await
            .expect("read final material");
        assert_eq!(
            final_material
                .get(FIXED_TOKEN_MATERIAL_KEY)
                .map(String::as_str),
            Some("replacement-token"),
            "replacement material should win over stale in-flight refresh material"
        );
    }

    #[tokio::test]
    async fn refreshed_material_write_rejects_deleted_identity() {
        let (temp, manager, _identity_specs) = manager_with_github_pat_spec();
        manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command("old-token"),
            )
            .await
            .expect("create identity");
        let layout = test_layout(&temp);
        fs::remove_dir_all(layout.identity_dir(&local_owner(), &github_local_name()))
            .expect("delete identity");

        let error = manager
            .store
            .material_guard(&local_owner(), &github_local_name())
            .await
            .expect("open material guard")
            .write_material(&material(FIXED_TOKEN_MATERIAL_KEY, "new-token"))
            .await
            .expect_err("deleted identity material must not be recreated");

        assert!(matches!(error, AppError::IdentityNotFound(name) if name == "github_local"));
        assert!(
            !layout
                .identity_material_file(&local_owner(), &github_local_name())
                .exists(),
            "deleted identity material should stay deleted"
        );
    }
}
