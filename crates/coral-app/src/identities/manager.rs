//! Filesystem-backed user-owned provider identity registry and runtime provider.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;

use coral_engine::RequestIdentityResolutionContext;
use coral_spec::parse_identity_manifest_yaml;
use coral_spec::v4::IdentityRequirements;
use coral_spec::{
    IdentityManifest, IdentitySpecConfig, IdentitySpecType, ManifestOAuthCredentialSpec,
};
use serde::{Deserialize, Serialize};
use tracing::info_span;

use crate::bootstrap::AppError;
use crate::credentials::oauth::{
    OAuthClientMaterialPersistence, OAuthCredentialMaterial, OAuthCredentialService,
    OAuthProgressEventSender, StartOAuthCredentialRequest,
};
use crate::credentials::{
    CredentialSetId, CredentialStorageKind, CredentialStore, remove_file_if_exists_unlocked,
    write_file_unlocked,
};
use crate::identities::runtime::{
    FIXED_TOKEN_MATERIAL_KEY, IdentityRuntimeServices, OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
    StoredIdentityRuntimeData,
};
use crate::identity::{
    RuntimeSourceIdentity, SourceIdentityProvider, SourceIdentityResolutionRequest,
    SourceIdentitySelection, SourceIdentitySelectionRequest, UserPrincipal, parse_path_segment,
    unique_input_map,
};
use crate::identity_specs::{
    IdentitySpecManager, IdentitySpecRecord, identity_spec_fingerprint, validate_identity_spec_name,
};
use crate::sources::SourceName;
use crate::state::AppStateLayout;
use crate::storage::fs::{self as storage_fs, FileLock};
use crate::workspaces::WorkspaceName;

const USER_OWNED_IDENTITY_DOCUMENT_VERSION: u32 = 1;
const USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION: u32 = 1;

/// One stored user-owned identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserOwnedIdentityRecord {
    /// Stable identity name used by source identity bindings.
    pub name: String,
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

/// Opaque durable owner key for stored provider-facing identity material.
///
/// OSS Coral only constructs this from the request user principal. Product
/// runtimes can use their own stable owner keys without adding product-specific
/// ownership concepts to OSS identity management.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct IdentityOwnerKey(String);

impl IdentityOwnerKey {
    /// Builds an owner key from a storage-safe string.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the key is empty or contains path separators.
    pub fn new(value: impl Into<String>) -> Result<Self, AppError> {
        parse_path_segment("identity owner", &value.into()).map(Self)
    }

    pub(crate) fn for_user_principal(principal: &UserPrincipal) -> Result<Self, AppError> {
        Self::new(principal.user_id())
    }

    /// Returns the storage key string.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for IdentityOwnerKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for IdentityOwnerKey {
    type Err = AppError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        Self::new(value)
    }
}

impl TryFrom<String> for IdentityOwnerKey {
    type Error = AppError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

/// Request to create or replace an OAuth identity.
#[derive(Clone)]
pub struct CreateOAuthIdentityCommand {
    /// Stable identity name used by source identity bindings.
    pub name: String,
    /// Installed OAuth identity spec name.
    pub identity_spec: String,
    /// Method-specific OAuth credential inputs.
    pub credential_inputs: Vec<IdentityCredentialInput>,
}

impl fmt::Debug for CreateOAuthIdentityCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let credential_input_keys = self
            .credential_inputs
            .iter()
            .map(|input| input.key.as_str())
            .collect::<Vec<_>>();
        f.debug_struct("CreateOAuthIdentityCommand")
            .field("name", &self.name)
            .field("identity_spec", &self.identity_spec)
            .field("credential_input_keys", &credential_input_keys)
            .finish()
    }
}

/// Request to create or replace a fixed-token identity.
#[derive(Clone)]
pub struct CreateFixedTokenIdentityCommand {
    /// Stable identity name used by source identity bindings.
    pub name: String,
    /// Installed fixed-token identity spec name.
    pub identity_spec: String,
    /// Bearer token to store.
    pub token: String,
}

impl fmt::Debug for CreateFixedTokenIdentityCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CreateFixedTokenIdentityCommand")
            .field("name", &self.name)
            .field("identity_spec", &self.identity_spec)
            .field("token", &"<redacted>")
            .finish()
    }
}

/// OAuth method input supplied while creating an identity.
#[derive(Clone)]
pub struct IdentityCredentialInput {
    /// Input key.
    pub key: String,
    /// Input value.
    pub value: String,
}

impl fmt::Debug for IdentityCredentialInput {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IdentityCredentialInput")
            .field("key", &self.key)
            .field("value", &"<redacted>")
            .finish()
    }
}

/// Locked access to credential material for one user-owned identity.
#[tonic::async_trait]
pub trait UserOwnedIdentityMaterialGuard: Send {
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

/// Durable storage backend for user-owned identities and their source bindings.
#[tonic::async_trait]
pub trait UserOwnedIdentityStore: Send + Sync + std::fmt::Debug + 'static {
    /// Lists identities owned by one opaque owner key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<UserOwnedIdentityRecord>, AppError>;

    /// Loads one identity owned by one opaque owner key.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<Option<UserOwnedIdentityRecord>, AppError>;

    /// Replaces one identity and its credential material atomically.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn replace_identity(
        &self,
        owner: &IdentityOwnerKey,
        record: &UserOwnedIdentityRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError>;

    /// Deletes one identity and its credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<bool, AppError>;

    /// Returns locked access to one identity's credential material.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the material lock cannot be acquired.
    async fn material_guard(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<Box<dyn UserOwnedIdentityMaterialGuard>, AppError>;

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
        Err(AppError::FailedPrecondition(
            "user-owned source identity binding storage is not supported by this identity store"
                .to_string(),
        ))
    }

    /// Snapshots one per-user source identity selection, returning `None` when
    /// no selection has been stored.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn snapshot_source_identity_binding(
        &self,
        _user_id: &str,
        _workspace_name: &str,
        _source_name: &str,
        _surface_id: &str,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        Err(AppError::FailedPrecondition(
            "user-owned source identity binding storage is not supported by this identity store"
                .to_string(),
        ))
    }

    /// Restores one per-user source identity selection from a snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    async fn restore_source_identity_binding(
        &self,
        _user_id: &str,
        _workspace_name: &str,
        _source_name: &str,
        _surface_id: &str,
        _selection: Option<&SourceIdentitySelection>,
    ) -> Result<(), AppError> {
        Err(AppError::FailedPrecondition(
            "user-owned source identity binding storage is not supported by this identity store"
                .to_string(),
        ))
    }

    /// Loads one per-user source identity selection.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be read.
    async fn load_source_identity_binding(
        &self,
        _user_id: &str,
        _workspace_name: &str,
        _source_name: &str,
        _surface_id: &str,
    ) -> Result<SourceIdentitySelection, AppError> {
        Err(AppError::FailedPrecondition(
            "user-owned source identity binding storage is not supported by this identity store"
                .to_string(),
        ))
    }
}

/// Manages provider-facing identities owned by Coral user principals.
#[derive(Debug, Clone)]
pub(crate) struct UserOwnedIdentityManager {
    identity_specs: IdentitySpecManager,
    oauth_credential_service: OAuthCredentialService,
    store: Arc<dyn UserOwnedIdentityStore>,
}

/// Product-facing handle for stored provider identity management.
///
/// The handle intentionally deals only in opaque owner keys. OSS callers keep
/// using user-owned gRPC methods, while product runtimes can map their own
/// owner metadata into stable keys.
#[derive(Debug, Clone)]
pub struct IdentityManagementHandle {
    manager: UserOwnedIdentityManager,
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
        owner: &IdentityOwnerKey,
        command: CreateOAuthIdentityCommand,
        events: OAuthProgressEventSender,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
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
        owner: &IdentityOwnerKey,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
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
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<UserOwnedIdentityRecord>, AppError> {
        self.manager.list_identities(owner).await
    }

    /// Deletes an identity stored under `owner`.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the store cannot be written.
    pub async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
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
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
        self.manager.resolve_identity(owner, identity_name).await
    }
}

impl UserOwnedIdentityManager {
    pub(crate) fn new(
        layout: AppStateLayout,
        identity_specs: IdentitySpecManager,
        credential_store: CredentialStore,
    ) -> Self {
        Self::new_with_store(
            identity_specs,
            Arc::new(FileUserOwnedIdentityStore::new(layout, credential_store)),
        )
    }

    pub(crate) fn new_with_store(
        identity_specs: IdentitySpecManager,
        store: Arc<dyn UserOwnedIdentityStore>,
    ) -> Self {
        Self {
            identity_specs,
            oauth_credential_service: OAuthCredentialService::new(),
            store,
        }
    }

    #[expect(
        dead_code,
        reason = "consumed by the server extension context (ServerExtensionContext) in a later PR"
    )]
    pub(crate) fn handle(&self) -> IdentityManagementHandle {
        IdentityManagementHandle {
            manager: self.clone(),
        }
    }

    /// Gates, validates, and loads the identity spec shared by both identity
    /// creation paths, erroring when the spec's type is not `expected`.
    fn prepare_identity_creation(
        &self,
        owner: &IdentityOwnerKey,
        name: &str,
        identity_spec: &str,
        expected: IdentitySpecType,
    ) -> Result<(IdentityOwnerKey, String, IdentitySpecRecord), AppError> {
        self.identity_specs.ensure_dsl_v4_enabled()?;
        let name = validate_identity_name(name)?;
        let identity_spec_name = validate_identity_spec_name(identity_spec)?;
        let spec = self.identity_specs.get_identity_spec(&identity_spec_name)?;
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
        owner: &IdentityOwnerKey,
        command: CreateOAuthIdentityCommand,
        events: OAuthProgressEventSender,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
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
            &name,
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

        let client_material_persistence =
            oauth_client_material_persistence_for_identity(oauth, &provided_inputs);
        let material = self
            .oauth_credential_service
            .authorize_with_progress(
                StartOAuthCredentialRequest {
                    input_key: OAUTH_ACCESS_TOKEN_MATERIAL_KEY,
                    oauth,
                    source_inputs: &identity_inputs,
                    credential_inputs,
                    client_material_persistence,
                },
                name.clone(),
                &events,
            )
            .await?;

        let record = UserOwnedIdentityRecord {
            name,
            identity_spec: identity_spec_name,
            identity_spec_fingerprint: Some(identity_spec_fingerprint(&spec.manifest)?),
            issuer: spec.manifest.issuer,
            identity_type: spec.manifest.identity_type.label().to_string(),
            metadata: material.safe_metadata.clone(),
        };
        let material = material_values(material);
        self.store
            .replace_identity(&owner, &record, &material)
            .await?;
        Ok(record)
    }

    async fn create_fixed_token_identity(
        &self,
        owner: &IdentityOwnerKey,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
        let span = info_span!("coral.app.identities.create_fixed_token");
        let _guard = span.enter();
        let (owner, name, spec) = self.prepare_identity_creation(
            owner,
            &command.name,
            &command.identity_spec,
            IdentitySpecType::FixedToken,
        )?;
        if command.token.is_empty() {
            return Err(AppError::InvalidInput(
                "fixed token identity token must not be empty".to_string(),
            ));
        }
        let record = UserOwnedIdentityRecord {
            name,
            identity_spec: spec.manifest.name.clone(),
            identity_spec_fingerprint: Some(identity_spec_fingerprint(&spec.manifest)?),
            issuer: spec.manifest.issuer,
            identity_type: spec.manifest.identity_type.label().to_string(),
            metadata: BTreeMap::new(),
        };
        let material = BTreeMap::from([(FIXED_TOKEN_MATERIAL_KEY.to_string(), command.token)]);
        self.store
            .replace_identity(&owner, &record, &material)
            .await?;
        Ok(record)
    }

    pub(crate) async fn create_user_owned_oauth_identity(
        &self,
        principal: &UserPrincipal,
        command: CreateOAuthIdentityCommand,
        events: OAuthProgressEventSender,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
        let owner = IdentityOwnerKey::for_user_principal(principal)?;
        self.create_oauth_identity(&owner, command, events).await
    }

    pub(crate) async fn create_user_owned_fixed_token_identity(
        &self,
        principal: &UserPrincipal,
        command: CreateFixedTokenIdentityCommand,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
        let owner = IdentityOwnerKey::for_user_principal(principal)?;
        self.create_fixed_token_identity(&owner, command).await
    }

    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<UserOwnedIdentityRecord>, AppError> {
        self.identity_specs.ensure_dsl_v4_enabled()?;
        self.store.list_identities(owner).await
    }

    pub(crate) async fn list_user_owned_identities(
        &self,
        principal: &UserPrincipal,
    ) -> Result<Vec<UserOwnedIdentityRecord>, AppError> {
        let span = info_span!("coral.app.identities.list_user_owned");
        let _guard = span.enter();
        let owner = IdentityOwnerKey::for_user_principal(principal)?;
        self.list_identities(&owner).await
    }

    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
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

    pub(crate) async fn snapshot_user_owned_source_identity_binding(
        &self,
        principal: &UserPrincipal,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        let user_id = validate_user_id(principal.user_id())?;
        let surface_id = validate_source_surface_id(surface_id)?;
        self.store
            .snapshot_source_identity_binding(
                &user_id,
                workspace_name.as_str(),
                source_name.as_str(),
                &surface_id,
            )
            .await
    }

    pub(crate) async fn restore_user_owned_source_identity_binding(
        &self,
        principal: &UserPrincipal,
        workspace_name: &WorkspaceName,
        source_name: &SourceName,
        surface_id: &str,
        selection: Option<&SourceIdentitySelection>,
    ) -> Result<(), AppError> {
        let user_id = validate_user_id(principal.user_id())?;
        let surface_id = validate_source_surface_id(surface_id)?;
        if let Some(selection) = selection {
            selection.validate()?;
        }
        self.store
            .restore_source_identity_binding(
                &user_id,
                workspace_name.as_str(),
                source_name.as_str(),
                &surface_id,
                selection,
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
        let owner = IdentityOwnerKey::for_user_principal(principal)?;
        let identity_name = validate_identity_name(&selection.identity)?;
        let record = self
            .store
            .load_identity(&owner, &identity_name)
            .await?
            .ok_or_else(|| AppError::IdentityNotFound(identity_name.clone()))?;
        let spec = self.load_spec_for_record(&record)?;
        let context = RequestIdentityResolutionContext::new(
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
        record: &UserOwnedIdentityRecord,
    ) -> Result<IdentitySpecRecord, AppError> {
        let spec = match self.identity_specs.get_identity_spec(&record.identity_spec) {
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
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<Option<Arc<dyn RuntimeSourceIdentity>>, AppError> {
        let identity_name = validate_identity_name(identity_name)?;
        let material_guard = match self.store.material_guard(owner, &identity_name).await {
            Ok(material_guard) => material_guard,
            Err(AppError::IdentityNotFound(_)) => return Ok(None),
            Err(error) => return Err(error),
        };
        let record = self.store.load_identity(owner, &identity_name).await?;
        let Some(record) = record else {
            return Ok(None);
        };
        let spec = self.load_spec_for_record(&record)?;
        let identity_inputs = self
            .identity_specs
            .resolve_identity_spec_inputs(&spec.manifest)?;
        let material = material_guard.read_material().await?;
        let prepared = StoredIdentityRuntimeData::new(
            record.name.clone(),
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

#[derive(Clone)]
struct FileUserOwnedIdentityStore {
    layout: AppStateLayout,
    credential_store: CredentialStore,
}

impl fmt::Debug for FileUserOwnedIdentityStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FileUserOwnedIdentityStore")
            .field("layout", &self.layout)
            .finish_non_exhaustive()
    }
}

impl FileUserOwnedIdentityStore {
    fn new(layout: AppStateLayout, credential_store: CredentialStore) -> Self {
        Self {
            layout,
            credential_store,
        }
    }

    fn load_identity_unlocked(
        &self,
        owner_key: &str,
        name: &str,
    ) -> Result<UserOwnedIdentityRecord, AppError> {
        let document = self.load_identity_document_unlocked(owner_key, name)?;
        document.into_record(name)
    }

    fn load_identity_document_unlocked(
        &self,
        owner_key: &str,
        name: &str,
    ) -> Result<UserOwnedIdentityDocument, AppError> {
        let owner_key = validate_identity_owner_key(owner_key)?;
        let name = validate_identity_name(name)?;
        let path = self
            .layout
            .user_owned_identity_manifest_file(&owner_key, &name);
        if !path.exists() {
            return Err(AppError::IdentityNotFound(name));
        }
        let raw = fs::read_to_string(&path)?;
        serde_yaml::from_str(&raw).map_err(Into::into)
    }

    fn material_lock_for_layout(
        layout: &AppStateLayout,
        owner_key: &str,
        identity_name: &str,
    ) -> Result<FileLock, AppError> {
        FileLock::exclusive(&layout.user_owned_identity_refresh_lock_file(owner_key, identity_name))
            .map_err(Into::into)
    }

    fn validate_identity_spec_write_precondition_unlocked(
        layout: &AppStateLayout,
        record: &UserOwnedIdentityRecord,
    ) -> Result<(), AppError> {
        let Some(expected_fingerprint) = record.identity_spec_fingerprint.as_deref() else {
            return Ok(());
        };
        let identity_spec_name = validate_identity_spec_name(&record.identity_spec)?;
        let manifest_path = layout.identity_spec_manifest_file(&identity_spec_name);
        let manifest_yaml = match fs::read_to_string(&manifest_path) {
            Ok(manifest_yaml) => manifest_yaml,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Err(AppError::FailedPrecondition(format!(
                    "identity '{}' cannot be written because identity spec '{}' is not installed; retry identity creation",
                    record.name, identity_spec_name
                )));
            }
            Err(error) => return Err(error.into()),
        };
        let manifest = parse_identity_manifest_yaml(&manifest_yaml).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "identity '{}' cannot be written because identity spec '{}' could not be parsed: {error}",
                record.name, identity_spec_name
            ))
        })?;
        if manifest.name != identity_spec_name {
            return Err(AppError::FailedPrecondition(format!(
                "identity '{}' cannot be written because identity spec '{}' was replaced by manifest '{}'; retry identity creation",
                record.name, identity_spec_name, manifest.name
            )));
        }
        let observed_fingerprint = identity_spec_fingerprint(&manifest)?;
        if observed_fingerprint == expected_fingerprint {
            return Ok(());
        }
        Err(AppError::FailedPrecondition(format!(
            "identity '{}' cannot be written because identity spec '{}' changed while creating it; retry identity creation",
            record.name, identity_spec_name
        )))
    }
}

#[tonic::async_trait]
impl UserOwnedIdentityStore for FileUserOwnedIdentityStore {
    async fn list_identities(
        &self,
        owner: &IdentityOwnerKey,
    ) -> Result<Vec<UserOwnedIdentityRecord>, AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let _lock = FileLock::shared(store.layout.state_lock())?;
            let root = store.layout.user_owned_identities_root(&owner_key);
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
                match store.load_identity_unlocked(&owner_key, &name) {
                    Ok(record) => records.push(record),
                    Err(AppError::IdentityNotFound(_)) => {}
                    Err(error) => return Err(error),
                }
            }
            records.sort_by(|left, right| left.name.cmp(&right.name));
            Ok(records)
        })
        .await?
    }

    async fn load_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<Option<UserOwnedIdentityRecord>, AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let identity_name = identity_name.to_string();
        tokio::task::spawn_blocking(move || {
            let _lock = FileLock::shared(store.layout.state_lock())?;
            match store.load_identity_unlocked(&owner_key, &identity_name) {
                Ok(record) => Ok(Some(record)),
                Err(AppError::IdentityNotFound(_)) => Ok(None),
                Err(error) => Err(error),
            }
        })
        .await?
    }

    async fn replace_identity(
        &self,
        owner: &IdentityOwnerKey,
        record: &UserOwnedIdentityRecord,
        material: &BTreeMap<String, String>,
    ) -> Result<(), AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let record = record.clone();
        let material = material.clone();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let _material_lock =
                Self::material_lock_for_layout(&store.layout, &owner_key, record.name.as_str())?;
            let _state_lock = FileLock::exclusive(store.layout.state_lock())?;
            Self::validate_identity_spec_write_precondition_unlocked(&store.layout, &record)?;
            let credential_storage =
                match store.load_identity_document_unlocked(&owner_key, record.name.as_str()) {
                    Ok(document) => document.credential_storage,
                    Err(AppError::IdentityNotFound(_)) => store
                        .credential_store
                        .default_write_storage()
                        .map_err(AppError::from)?,
                    Err(error) => return Err(error),
                };
            let credential_set_id =
                CredentialSetId::for_user_owned_identity(&owner_key, record.name.as_str());
            let material_snapshot = store.credential_store.snapshot_material_unlocked(
                &WorkspaceName::default(),
                &credential_set_id,
                credential_storage,
            )?;
            let manifest_path = store
                .layout
                .user_owned_identity_manifest_file(&owner_key, record.name.as_str());
            if let Some(parent) = manifest_path.parent() {
                storage_fs::ensure_private_dir(parent)?;
            }
            let manifest_snapshot = snapshot_file_unlocked(&manifest_path)?;
            let result = (|| {
                let document = UserOwnedIdentityDocument::from_record(&record, credential_storage);
                write_file_unlocked(&manifest_path, serde_yaml::to_string(&document)?.as_bytes())?;
                store.credential_store.replace_material_unlocked(
                    &WorkspaceName::default(),
                    &credential_set_id,
                    credential_storage,
                    &material,
                )?;
                Ok(())
            })();
            if result.is_err() {
                restore_file_unlocked(&manifest_path, manifest_snapshot)?;
                store.credential_store.restore_material_unlocked(
                    &WorkspaceName::default(),
                    &credential_set_id,
                    &material_snapshot,
                )?;
            }
            result
        })
        .await?
    }

    async fn delete_identity(
        &self,
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<bool, AppError> {
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let identity_name = identity_name.to_string();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let identity_name = validate_identity_name(&identity_name)?;
            let _material_lock =
                Self::material_lock_for_layout(&store.layout, &owner_key, &identity_name)?;
            let _state_lock = FileLock::exclusive(store.layout.state_lock())?;
            let document = match store.load_identity_document_unlocked(&owner_key, &identity_name) {
                Ok(document) => document,
                Err(AppError::IdentityNotFound(_)) => return Ok(false),
                Err(error) => return Err(error),
            };
            let credential_set_id =
                CredentialSetId::for_user_owned_identity(&owner_key, &identity_name);
            let manifest_path = store
                .layout
                .user_owned_identity_manifest_file(&owner_key, &identity_name);
            store.credential_store.remove_material_unlocked(
                &WorkspaceName::default(),
                &credential_set_id,
                document.credential_storage,
            )?;
            remove_file_if_exists_unlocked(&manifest_path)?;
            let identity_dir = store
                .layout
                .user_owned_identity_dir(&owner_key, &identity_name);
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
        owner: &IdentityOwnerKey,
        identity_name: &str,
    ) -> Result<Box<dyn UserOwnedIdentityMaterialGuard>, AppError> {
        let layout = self.layout.clone();
        let store = self.clone();
        let owner_key = owner.as_str().to_string();
        let identity_name = identity_name.to_string();
        tokio::task::spawn_blocking(move || {
            let owner_key = validate_identity_owner_key(&owner_key)?;
            let identity_name = validate_identity_name(&identity_name)?;
            let lock = Self::material_lock_for_layout(&layout, &owner_key, &identity_name)?;
            let _state_lock = FileLock::shared(layout.state_lock())?;
            let document = store.load_identity_document_unlocked(&owner_key, &identity_name)?;
            Ok(Box::new(FileUserOwnedIdentityMaterialGuard {
                layout,
                credential_store: store.credential_store.clone(),
                owner_key,
                identity_name,
                credential_storage: document.credential_storage,
                _lock: lock,
            }) as Box<dyn UserOwnedIdentityMaterialGuard>)
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

    async fn snapshot_source_identity_binding(
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

    async fn restore_source_identity_binding(
        &self,
        user_id: &str,
        workspace_name: &str,
        source_name: &str,
        surface_id: &str,
        selection: Option<&SourceIdentitySelection>,
    ) -> Result<(), AppError> {
        let store = self.clone();
        let user_id = user_id.to_string();
        let workspace_name = workspace_name.to_string();
        let source_name = source_name.to_string();
        let surface_id = surface_id.to_string();
        let selection = selection.cloned();
        tokio::task::spawn_blocking(move || {
            let user_id = validate_user_id(&user_id)?;
            let workspace_name = WorkspaceName::parse(&workspace_name)?;
            let source_name = SourceName::parse(&source_name)?;
            let surface_id = validate_source_surface_id(&surface_id)?;
            if let Some(selection) = &selection {
                selection.validate()?;
            }
            let _lock = FileLock::exclusive(store.layout.state_lock())?;
            let path = store.layout.user_owned_source_identity_binding_file(
                &user_id,
                &workspace_name,
                &source_name,
                &surface_id,
            );
            write_files_transactionally(&[&path], || {
                match selection {
                    Some(selection) => {
                        if let Some(parent) = path.parent() {
                            storage_fs::ensure_private_dir(parent)?;
                        }
                        let document =
                            UserSourceIdentityBindingDocument::from_selection(&selection);
                        write_file_unlocked(&path, serde_yaml::to_string(&document)?.as_bytes())?;
                    }
                    None => {
                        remove_file_if_exists_unlocked(&path)?;
                    }
                }
                Ok(())
            })
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
                return Err(AppError::FailedPrecondition(format!(
                    "user '{user_id}' has no selected identity for source '{}' surface '{surface_id}'",
                    source_name.as_str()
                )));
            }
            let raw = fs::read_to_string(&path)?;
            let document: UserSourceIdentityBindingDocument = serde_yaml::from_str(&raw)?;
            document.into_selection()
        })
        .await?
    }
}

struct FileUserOwnedIdentityMaterialGuard {
    layout: AppStateLayout,
    credential_store: CredentialStore,
    owner_key: String,
    identity_name: String,
    credential_storage: CredentialStorageKind,
    _lock: FileLock,
}

#[tonic::async_trait]
impl UserOwnedIdentityMaterialGuard for FileUserOwnedIdentityMaterialGuard {
    async fn read_material(&self) -> Result<BTreeMap<String, String>, AppError> {
        let credential_store = self.credential_store.clone();
        let credential_set_id =
            CredentialSetId::for_user_owned_identity(&self.owner_key, &self.identity_name);
        let credential_storage = self.credential_storage;
        let identity_name = self.identity_name.clone();
        tokio::task::spawn_blocking(move || {
            let material = credential_store.read_material(
                &WorkspaceName::default(),
                &credential_set_id,
                credential_storage,
            )?;
            if material.is_empty() {
                return Err(AppError::FailedPrecondition(format!(
                    "identity '{identity_name}' is missing credential material"
                )));
            }
            Ok(material)
        })
        .await?
    }

    async fn write_material(&self, material: &BTreeMap<String, String>) -> Result<(), AppError> {
        let layout = self.layout.clone();
        let credential_store = self.credential_store.clone();
        let owner_key = self.owner_key.clone();
        let identity_name = self.identity_name.clone();
        let credential_storage = self.credential_storage;
        let material = material.clone();
        tokio::task::spawn_blocking(move || {
            let credential_set_id =
                CredentialSetId::for_user_owned_identity(&owner_key, &identity_name);
            let _state_lock = FileLock::exclusive(layout.state_lock())?;
            let manifest_path =
                layout.user_owned_identity_manifest_file(&owner_key, &identity_name);
            if !manifest_path.exists() {
                return Err(AppError::IdentityNotFound(identity_name));
            }
            credential_store.replace_material_unlocked(
                &WorkspaceName::default(),
                &credential_set_id,
                credential_storage,
                &material,
            )
        })
        .await?
    }
}

#[tonic::async_trait]
impl SourceIdentityProvider for UserOwnedIdentityManager {
    async fn resolve_source_identity_selection(
        &self,
        request: &SourceIdentitySelectionRequest,
    ) -> Result<Option<SourceIdentitySelection>, AppError> {
        let Some(user_id) = request.subject.user_id() else {
            return Ok(None);
        };
        let user_id = validate_user_id(user_id)?;
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
        let Some(user_id) = request.subject.user_id() else {
            return Ok(None);
        };
        let owner = IdentityOwnerKey::new(user_id)?;
        let identity_name = validate_identity_name(&request.selection.identity)?;
        self.resolve_identity(&owner, &identity_name).await
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct UserOwnedIdentityDocument {
    version: u32,
    name: String,
    identity_spec: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity_spec_fingerprint: Option<String>,
    #[serde(default = "default_user_owned_identity_credential_storage")]
    credential_storage: CredentialStorageKind,
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
    #[serde(default)]
    accepted_identity: Option<String>,
}

impl UserSourceIdentityBindingDocument {
    fn from_selection(selection: &SourceIdentitySelection) -> Self {
        Self {
            version: USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION,
            identity: selection.identity.clone(),
            accepted_identity: selection.accepted_identity.clone(),
        }
    }

    fn into_selection(self) -> Result<SourceIdentitySelection, AppError> {
        if self.version != USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION {
            return Err(AppError::FailedPrecondition(format!(
                "source identity binding for identity '{}' has unsupported document version {}; expected {}",
                self.identity, self.version, USER_SOURCE_IDENTITY_BINDING_DOCUMENT_VERSION
            )));
        }
        SourceIdentitySelection::new(self.identity, self.accepted_identity)
    }
}

impl UserOwnedIdentityDocument {
    fn from_record(
        record: &UserOwnedIdentityRecord,
        credential_storage: CredentialStorageKind,
    ) -> Self {
        Self {
            version: USER_OWNED_IDENTITY_DOCUMENT_VERSION,
            name: record.name.clone(),
            identity_spec: record.identity_spec.clone(),
            identity_spec_fingerprint: record.identity_spec_fingerprint.clone(),
            credential_storage,
            issuer: record.issuer.clone(),
            identity_type: record.identity_type.clone(),
            metadata: record.metadata.clone(),
        }
    }

    fn into_record(self, expected_name: &str) -> Result<UserOwnedIdentityRecord, AppError> {
        if self.version != USER_OWNED_IDENTITY_DOCUMENT_VERSION {
            return Err(AppError::FailedPrecondition(format!(
                "identity '{}' has unsupported document version {}; expected {}",
                self.name, self.version, USER_OWNED_IDENTITY_DOCUMENT_VERSION
            )));
        }
        let name = validate_identity_name(&self.name)?;
        if name != expected_name {
            return Err(AppError::FailedPrecondition(format!(
                "identity file for '{expected_name}' contains identity '{name}'"
            )));
        }
        Ok(UserOwnedIdentityRecord {
            name,
            identity_spec: validate_identity_spec_name(&self.identity_spec)?,
            identity_spec_fingerprint: self.identity_spec_fingerprint,
            issuer: self.issuer,
            identity_type: self.identity_type,
            metadata: self.metadata,
        })
    }
}

fn default_user_owned_identity_credential_storage() -> CredentialStorageKind {
    CredentialStorageKind::File
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
    record: &UserOwnedIdentityRecord,
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
    record: &UserOwnedIdentityRecord,
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

fn oauth_client_material_persistence_for_identity(
    oauth: &ManifestOAuthCredentialSpec,
    provided: &BTreeMap<String, String>,
) -> OAuthClientMaterialPersistence {
    let client_secret = oauth
        .client
        .secret
        .as_ref()
        .is_some_and(|secret| provided.contains_key(&secret.input));
    OAuthClientMaterialPersistence::PinnedEndpoint { client_secret }
}

fn validate_user_id(user_id: &str) -> Result<String, AppError> {
    parse_path_segment("user", user_id)
}

fn validate_identity_owner_key(owner: &str) -> Result<String, AppError> {
    parse_path_segment("identity owner", owner)
}

fn validate_identity_name(name: &str) -> Result<String, AppError> {
    parse_path_segment("identity", name)
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
        Some(bytes) => write_file_unlocked(path, &bytes).map_err(Into::into),
        None => remove_file_if_exists_unlocked(path).map_err(Into::into),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credentials::CredentialStoragePreference;
    use crate::features::{Features, dsl_v4_features};
    use crate::identities::runtime::FIXED_TOKEN_MATERIAL_KEY;
    use crate::identity::{
        SourceIdentityBinding, SourceIdentitySelection, SourceIdentitySelectionRequest,
        SourceIdentitySubject,
    };
    use tempfile::TempDir;

    #[test]
    fn owner_key_rejects_storage_unsafe_values() {
        IdentityOwnerKey::new(" ").unwrap_err();
        IdentityOwnerKey::new("a/b").unwrap_err();
        IdentityOwnerKey::new("a\\b").unwrap_err();
        IdentityOwnerKey::new("..").unwrap_err();
    }

    fn test_layout(temp: &TempDir) -> AppStateLayout {
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        layout.ensure().expect("layout directories");
        layout
    }

    fn manager() -> (TempDir, UserOwnedIdentityManager, IdentitySpecManager) {
        manager_with_features(dsl_v4_features())
    }

    fn manager_with_features(
        features: Features,
    ) -> (TempDir, UserOwnedIdentityManager, IdentitySpecManager) {
        let temp = TempDir::new().expect("tempdir");
        let layout = test_layout(&temp);
        let identity_specs =
            IdentitySpecManager::new_with_usage_providers(layout.clone(), features, Vec::new());
        (
            temp,
            UserOwnedIdentityManager::new(
                layout.clone(),
                identity_specs.clone(),
                CredentialStore::new(layout),
            ),
            identity_specs,
        )
    }

    fn manager_with_github_pat_spec() -> (TempDir, UserOwnedIdentityManager, IdentitySpecManager) {
        let (temp, manager, identity_specs) = manager();
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml())
            .expect("add identity spec");
        (temp, manager, identity_specs)
    }

    fn github_local_command(token: &str) -> CreateFixedTokenIdentityCommand {
        CreateFixedTokenIdentityCommand {
            name: "github_local".to_string(),
            identity_spec: "github_pat".to_string(),
            token: token.to_string(),
        }
    }

    fn local_owner() -> IdentityOwnerKey {
        IdentityOwnerKey::new("local").expect("local owner")
    }

    async fn create_github_local(manager: &UserOwnedIdentityManager, token: &str) {
        manager
            .create_user_owned_fixed_token_identity(
                &UserPrincipal::local(),
                github_local_command(token),
            )
            .await
            .expect("create identity");
    }

    fn github_local_record(identity_spec: &str, identity_type: &str) -> UserOwnedIdentityRecord {
        UserOwnedIdentityRecord {
            name: "github_local".to_string(),
            identity_spec: identity_spec.to_string(),
            identity_spec_fingerprint: None,
            issuer: "github".to_string(),
            identity_type: identity_type.to_string(),
            metadata: BTreeMap::new(),
        }
    }

    fn fingerprinted_github_local_record(manifest_yaml: &str) -> UserOwnedIdentityRecord {
        let manifest = parse_identity_manifest_yaml(manifest_yaml).expect("identity spec");
        UserOwnedIdentityRecord {
            identity_spec_fingerprint: Some(
                identity_spec_fingerprint(&manifest).expect("identity spec fingerprint"),
            ),
            ..github_local_record(&manifest.name, manifest.identity_type.label())
        }
    }

    fn material(key: &str, value: &str) -> BTreeMap<String, String> {
        BTreeMap::from([(key.to_string(), value.to_string())])
    }

    fn fixed_identity_spec_yaml() -> String {
        fixed_identity_spec_yaml_for_host("github.com")
    }

    fn fixed_identity_spec_yaml_for_host(host: &str) -> String {
        format!(
            r"
kind: identity
spec_version: 1
name: github_pat
version: 0.1.0
issuer: github
type: fixed_token
audience:
  host: {host}
"
        )
    }

    /// Declares spec-owned inputs (`DEMO_TENANT`, `DEMO_OAUTH_CLIENT_SECRET`);
    /// `client_id` selects between a spec default and a legacy identity-owned input.
    fn demo_oauth_identity_spec(client_id: &str) -> IdentityManifest {
        coral_spec::parse_identity_manifest_yaml(&format!(
            r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
issuer: demo
type: oauth
audience:
  host: example.test
inputs:
  DEMO_TENANT:
    kind: variable
    default: tenant-a
  DEMO_OAUTH_CLIENT_SECRET:
    kind: secret
oauth:
  method:
    flow:
      type: authorization_code
      pkce: required
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    endpoints:
      authorization_url: https://{{{{input.DEMO_TENANT}}}}.example.test/oauth/authorize
      token_url: https://{{{{input.DEMO_TENANT}}}}.example.test/oauth/token
    client:
      id:
        {client_id}
      secret:
        input: DEMO_OAUTH_CLIENT_SECRET
        transport: request_body
"
        ))
        .expect("identity spec")
    }

    fn resolution_request(identity_name: &str) -> SourceIdentityResolutionRequest {
        SourceIdentityResolutionRequest {
            workspace_name: "default".to_string(),
            subject: SourceIdentitySubject::User("local".to_string()),
            source_name: "github".to_string(),
            surface_id: "rest".to_string(),
            binding: SourceIdentityBinding::user_owned(),
            selection: SourceIdentitySelection::new(identity_name, None).expect("selection"),
            identity_requirements: coral_spec::v4::IdentityRequirements {
                accepts: Vec::new(),
            },
        }
    }

    async fn resolve_github_local_err(
        manager: &UserOwnedIdentityManager,
        expect_message: &str,
    ) -> String {
        manager
            .resolve_source_identity(&resolution_request("github_local"))
            .await
            .expect_err(expect_message)
            .to_string()
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
    async fn list_user_owned_identities_requires_dsl_v4_feature() {
        let (_temp, manager, _identity_specs) = manager_with_features(Features::default());

        let error = manager
            .list_user_owned_identities(&UserPrincipal::local())
            .await
            .expect_err("identity listing requires the dsl_v4 feature");

        assert!(
            matches!(&error, AppError::SourceUnservable(message) if message.contains("dsl_v4")),
            "unexpected error: {error:?}"
        );
    }

    #[tokio::test]
    async fn refreshed_material_write_rejects_deleted_identity() {
        let (temp, manager, _identity_specs) = manager_with_github_pat_spec();
        create_github_local(&manager, "old-token").await;
        let layout = test_layout(&temp);
        let refresh_guard = manager
            .store
            .material_guard(&local_owner(), "github_local")
            .await
            .expect("open material guard");
        fs::remove_dir_all(layout.user_owned_identity_dir("local", "github_local"))
            .expect("delete identity");

        let error = refresh_guard
            .write_material(&material(FIXED_TOKEN_MATERIAL_KEY, "new-token"))
            .await
            .expect_err("deleted identity material must not be recreated");

        assert!(matches!(error, AppError::IdentityNotFound(name) if name == "github_local"));
        assert!(
            !layout
                .user_owned_identity_material_file("local", "github_local")
                .exists(),
            "deleted identity material should stay deleted"
        );
    }

    #[tokio::test]
    async fn identity_material_uses_configured_keychain_storage_for_create_refresh_and_delete() {
        let temp = TempDir::new().expect("tempdir");
        let layout = test_layout(&temp);
        let credential_store = CredentialStore::with_available_keychain_for_test(
            layout.clone(),
            CredentialStoragePreference::Keychain,
        );
        let identity_specs = IdentitySpecManager::new_with_usage_providers(
            layout.clone(),
            dsl_v4_features(),
            Vec::new(),
        );
        let manager = UserOwnedIdentityManager::new(
            layout.clone(),
            identity_specs.clone(),
            credential_store.clone(),
        );
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml())
            .expect("add identity spec");
        let credential_set_id = CredentialSetId::for_user_owned_identity("local", "github_local");

        create_github_local(&manager, "identity-token").await;

        assert!(
            !layout
                .user_owned_identity_material_file("local", "github_local")
                .exists(),
            "keychain-backed identity material must not be written as a plaintext file"
        );
        let stored = credential_store
            .read_material(
                &WorkspaceName::default(),
                &credential_set_id,
                CredentialStorageKind::Keychain,
            )
            .expect("read keychain material");
        assert_eq!(
            stored.get(FIXED_TOKEN_MATERIAL_KEY).map(String::as_str),
            Some("identity-token")
        );
        let manifest =
            fs::read_to_string(layout.user_owned_identity_manifest_file("local", "github_local"))
                .expect("read identity manifest");
        assert!(
            manifest.contains("credential_storage: keychain"),
            "identity manifest should record material storage route: {manifest}"
        );

        manager
            .store
            .material_guard(&local_owner(), "github_local")
            .await
            .expect("open keychain material guard")
            .write_material(&material(FIXED_TOKEN_MATERIAL_KEY, "refreshed-token"))
            .await
            .expect("write refreshed keychain material");
        let refreshed = credential_store
            .read_material(
                &WorkspaceName::default(),
                &credential_set_id,
                CredentialStorageKind::Keychain,
            )
            .expect("read refreshed keychain material");
        assert_eq!(
            refreshed.get(FIXED_TOKEN_MATERIAL_KEY).map(String::as_str),
            Some("refreshed-token")
        );
        assert!(
            !layout
                .user_owned_identity_material_file("local", "github_local")
                .exists(),
            "keychain-backed refresh must not create a plaintext material file"
        );

        assert!(
            manager
                .delete_identity(&local_owner(), "github_local")
                .await
                .expect("delete keychain identity"),
            "identity delete should report removal"
        );
        let deleted = credential_store
            .read_material(
                &WorkspaceName::default(),
                &credential_set_id,
                CredentialStorageKind::Keychain,
            )
            .expect("read deleted keychain material");
        assert!(
            deleted.is_empty(),
            "keychain material should be removed with the identity"
        );
    }

    #[test]
    fn create_identity_command_debug_redacts_secret_values() {
        let credential_input = IdentityCredentialInput {
            key: "DEMO_OAUTH_CLIENT_SECRET".to_string(),
            value: "client-secret".to_string(),
        };
        let oauth_command = CreateOAuthIdentityCommand {
            name: "demo_local".to_string(),
            identity_spec: "demo_oauth".to_string(),
            credential_inputs: vec![credential_input.clone()],
        };
        let fixed_command = CreateFixedTokenIdentityCommand {
            name: "github_pat_local".to_string(),
            identity_spec: "github_pat".to_string(),
            token: "pat-token".to_string(),
        };

        let input_debug = format!("{credential_input:?}");
        let oauth_debug = format!("{oauth_command:?}");
        let fixed_debug = format!("{fixed_command:?}");

        assert!(input_debug.contains("DEMO_OAUTH_CLIENT_SECRET"));
        assert!(oauth_debug.contains("DEMO_OAUTH_CLIENT_SECRET"));
        assert!(fixed_debug.contains("github_pat_local"));
        assert!(!input_debug.contains("client-secret"));
        assert!(!oauth_debug.contains("client-secret"));
        assert!(!fixed_debug.contains("pat-token"));
    }

    #[test]
    fn identity_creation_rejects_declared_spec_inputs() {
        let manifest = demo_oauth_identity_spec("default: demo-client");
        let oauth = oauth_method(&manifest.name, &manifest.config).expect("oauth method");
        let provided = material("DEMO_OAUTH_CLIENT_SECRET", "client-secret");

        let error =
            reject_identity_owned_inputs("demo_local", &manifest.name, &manifest, oauth, &provided)
                .expect_err("declared input belongs to the identity spec");

        assert!(
            error
                .to_string()
                .contains("belongs to identity spec 'demo_oauth'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn identity_oauth_persistence_pins_endpoint_without_spec_owned_secret() {
        let manifest = demo_oauth_identity_spec("default: demo-client");
        let oauth = oauth_method(&manifest.name, &manifest.config).expect("oauth method");
        let provided = BTreeMap::new();

        assert_eq!(
            oauth_client_material_persistence_for_identity(oauth, &provided),
            OAuthClientMaterialPersistence::PinnedEndpoint {
                client_secret: false,
            }
        );
    }

    #[test]
    fn identity_material_values_store_only_token_material() {
        let material = OAuthCredentialMaterial {
            input_key: OAUTH_ACCESS_TOKEN_MATERIAL_KEY.to_string(),
            access_token: "access-token".to_string(),
            internal_metadata: BTreeMap::new(),
            safe_metadata: BTreeMap::new(),
        };
        let values = material_values(material);

        assert_eq!(
            values
                .get(OAUTH_ACCESS_TOKEN_MATERIAL_KEY)
                .map(String::as_str),
            Some("access-token")
        );
        assert_eq!(values.len(), 1);
    }

    #[tokio::test]
    async fn identity_replacement_waits_for_in_flight_material_refresh() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        create_github_local(&manager, "old-token").await;

        let refresh_guard = manager
            .store
            .material_guard(&local_owner(), "github_local")
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
            tokio::time::timeout(std::time::Duration::from_millis(200), &mut replacement,)
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
            .material_guard(&local_owner(), "github_local")
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
    async fn lists_user_owned_identity_records() {
        let (temp, manager, _identity_specs) = manager();
        let principal = UserPrincipal::local();
        let record = UserOwnedIdentityRecord {
            metadata: BTreeMap::from([("scope".to_string(), "repo".to_string())]),
            ..github_local_record("github_oauth", "oauth")
        };

        manager
            .store
            .replace_identity(
                &local_owner(),
                &record,
                &material(OAUTH_ACCESS_TOKEN_MATERIAL_KEY, "gho_token"),
            )
            .await
            .expect("write identity");
        let layout = test_layout(&temp);
        fs::create_dir_all(layout.user_owned_identity_dir("local", "partial_identity"))
            .expect("partial identity dir");

        let listed = manager
            .list_user_owned_identities(&principal)
            .await
            .expect("list identities");
        assert_eq!(listed, vec![record]);
    }

    #[tokio::test]
    async fn identity_write_revalidates_fingerprinted_spec_under_state_lock() {
        let (_temp, manager, identity_specs) = manager_with_github_pat_spec();
        let record = fingerprinted_github_local_record(&fixed_identity_spec_yaml());
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("remove identity spec");

        let error = manager
            .store
            .replace_identity(
                &local_owner(),
                &record,
                &material(FIXED_TOKEN_MATERIAL_KEY, "identity-token"),
            )
            .await
            .expect_err("missing identity spec should reject identity write");

        assert!(
            error.to_string().contains("is not installed"),
            "unexpected error: {error}"
        );
        assert!(
            manager
                .store
                .load_identity(&local_owner(), "github_local")
                .await
                .expect("load identity")
                .is_none(),
            "failed write must not leave an orphaned identity"
        );
    }

    #[tokio::test]
    async fn built_in_provider_ignores_workspace_owned_bindings() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        let request = SourceIdentityResolutionRequest {
            subject: SourceIdentitySubject::Workspace,
            binding: SourceIdentityBinding::workspace_owned("github_local", None).expect("binding"),
            ..resolution_request("github_local")
        };

        let resolved = manager
            .resolve_source_identity(&request)
            .await
            .expect("workspace binding ignored");
        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn provider_returns_none_for_missing_user_owned_identity() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();

        let resolved = manager
            .resolve_source_identity(&resolution_request("github_local"))
            .await
            .expect("missing identity should not fail provider resolution");

        assert!(resolved.is_none());
    }

    #[tokio::test]
    async fn resolves_user_owned_source_identity_selection_per_user() {
        let (_temp, manager, _identity_specs) = manager();
        let principal = UserPrincipal::for_user("saul").expect("principal");
        let workspace_name = WorkspaceName::parse("default").expect("workspace");
        let source_name = SourceName::parse("github_v4").expect("source");
        let selection =
            SourceIdentitySelection::new("github_saul", Some("github-rest-read".to_string()))
                .expect("selection");
        manager
            .replace_user_owned_source_identity_binding(
                &principal,
                &workspace_name,
                &source_name,
                "rest",
                &selection,
            )
            .await
            .expect("write source identity binding");

        let request = SourceIdentitySelectionRequest {
            workspace_name: "default".to_string(),
            subject: SourceIdentitySubject::User("saul".to_string()),
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
        let snapshot = manager
            .snapshot_user_owned_source_identity_binding(
                &principal,
                &workspace_name,
                &source_name,
                "rest",
            )
            .await
            .expect("snapshot source identity binding");
        assert_eq!(snapshot, Some(selection.clone()));

        let replacement =
            SourceIdentitySelection::new("github_tina", Some("github-rest-read".to_string()))
                .expect("replacement selection");
        manager
            .replace_user_owned_source_identity_binding(
                &principal,
                &workspace_name,
                &source_name,
                "rest",
                &replacement,
            )
            .await
            .expect("replace source identity binding");
        manager
            .restore_user_owned_source_identity_binding(
                &principal,
                &workspace_name,
                &source_name,
                "rest",
                snapshot.as_ref(),
            )
            .await
            .expect("restore source identity binding");
        let restored = manager
            .resolve_source_identity_selection(&request)
            .await
            .expect("resolve restored source identity binding")
            .expect("selection");
        assert_eq!(restored, selection);

        let missing_user = SourceIdentitySelectionRequest {
            subject: SourceIdentitySubject::User("tina".to_string()),
            ..request.clone()
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
        manager
            .restore_user_owned_source_identity_binding(
                &principal,
                &workspace_name,
                &source_name,
                "rest",
                None,
            )
            .await
            .expect("restore missing source identity binding");
        manager
            .resolve_source_identity_selection(&request)
            .await
            .expect_err("restored missing binding should not resolve");
    }

    #[tokio::test]
    async fn provider_reports_orphaned_user_owned_identity() {
        let (_temp, manager, _identity_specs) = manager();
        manager
            .store
            .replace_identity(
                &local_owner(),
                &github_local_record("missing_oauth", "oauth"),
                &material(OAUTH_ACCESS_TOKEN_MATERIAL_KEY, "gho_token"),
            )
            .await
            .expect("write identity");

        let error = resolve_github_local_err(&manager, "orphaned identity should fail").await;
        assert!(error.contains("is orphaned"));
    }

    #[tokio::test]
    async fn provider_rejects_identity_without_stored_spec_fingerprint() {
        let (_temp, manager, _identity_specs) = manager_with_github_pat_spec();
        manager
            .store
            .replace_identity(
                &local_owner(),
                &github_local_record("github_pat", "fixed_token"),
                &material(FIXED_TOKEN_MATERIAL_KEY, "identity-token"),
            )
            .await
            .expect("write legacy identity");

        let error = resolve_github_local_err(&manager, "legacy identity should fail closed").await;
        assert!(
            error.contains("created before identity spec fingerprinting"),
            "unexpected error: {error}"
        );
    }

    #[tokio::test]
    async fn provider_rejects_identity_when_spec_fingerprint_changes_after_force_readd() {
        let (_temp, manager, identity_specs) = manager_with_github_pat_spec();
        create_github_local(&manager, "identity-token").await;
        identity_specs
            .remove_identity_spec("github_pat", true)
            .expect("force remove spec");
        identity_specs
            .add_identity_spec(&fixed_identity_spec_yaml_for_host("attacker.test"))
            .expect("add different spec with same name");

        let error =
            resolve_github_local_err(&manager, "changed identity spec should fail closed").await;
        assert!(
            error.contains("has changed since the identity was created"),
            "unexpected error: {error}"
        );
    }
}
