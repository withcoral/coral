//! Database-backed identity instance management.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::IdentitySpecType;

use super::crypto::{
    IdentityDocumentBinding, decrypt_identity_document, encrypt_identity_document,
};
use super::model::{IdentityAudience, IdentityName, IdentityOwner, IdentitySpecReference};
use crate::bootstrap::AppError;
use crate::credentials::encryption::CredentialKeyProvider;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::identity::Principal;
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::{
    InstalledIdentitySpec, ResolvedIdentitySpec, record_to_installed, resolve_installed_for_use,
};
use crate::state::db::{
    CoralDb, CoralTx, DbError, DbRepos, IdentityDocumentRecord, IdentityRecord,
    IdentitySpecDocumentRecord, IdentitySpecKey, IdentitySpecRecord, IdentitySpecScope,
    now_unix_nanos_i64,
};
use crate::workspaces::WorkspaceName;

const FIXED_TOKEN_KEY: &str = "TOKEN";
const MAX_MUTATION_ATTEMPTS: usize = 8;

/// Database-backed behavior for owner-scoped identities.
#[derive(Clone)]
pub(crate) struct IdentityManager {
    db: Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    #[cfg(test)]
    before_write_gate: Option<OneShotGate>,
    #[cfg(test)]
    before_retry_gate: Option<OneShotGate>,
    #[cfg(test)]
    before_upsert_gate: Option<BeforeUpsertGate>,
    #[cfg(test)]
    before_use_snapshot_gate: Option<OneShotGate>,
}

#[cfg(test)]
#[derive(Clone)]
struct OneShotGate {
    prepared: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
    used: Arc<AtomicBool>,
}

#[cfg(test)]
impl OneShotGate {
    async fn wait(&self) {
        if !self.used.swap(true, Ordering::SeqCst) {
            self.prepared.wait().await;
            self.resume.wait().await;
        }
    }
}

#[cfg(test)]
#[derive(Clone)]
struct BeforeUpsertGate {
    barrier: Arc<tokio::sync::Barrier>,
    used: Arc<AtomicBool>,
}

struct SelectedFixedTokenSpec {
    requested_key: IdentitySpecKey,
    workspace_created_at_unix_nanos: Option<i64>,
    record: IdentitySpecRecord,
    reference: IdentitySpecReference,
}

/// Coherent decrypted identity data prepared for one runtime use.
pub(crate) struct ResolvedIdentityForUse {
    pub(crate) identity: IdentityRecord,
    pub(crate) identity_spec: ResolvedIdentitySpec,
    material: BTreeMap<String, String>,
}

impl ResolvedIdentityForUse {
    /// Borrow the validated decrypted identity material.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the runtime identity consumer lands in the next stack layer"
        )
    )]
    pub(crate) fn material(&self) -> &BTreeMap<String, String> {
        &self.material
    }
}

impl fmt::Debug for ResolvedIdentityForUse {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ResolvedIdentityForUse")
            .field("owner", &self.identity.owner)
            .field("name", &self.identity.name)
            .field("identity_spec", &self.identity_spec)
            .field("material_value_count", &self.material.len())
            .finish_non_exhaustive()
    }
}

struct IdentityUseSnapshot {
    identity: Option<IdentityRecord>,
    identity_document: Option<IdentityDocumentRecord>,
    identity_spec: Option<IdentitySpecRecord>,
    identity_spec_document: Option<IdentitySpecDocumentRecord>,
}

impl IdentityManager {
    pub(crate) fn new(db: Arc<CoralDb>, key_provider: Arc<dyn CredentialKeyProvider>) -> Self {
        Self {
            db,
            key_provider,
            #[cfg(test)]
            before_write_gate: None,
            #[cfg(test)]
            before_retry_gate: None,
            #[cfg(test)]
            before_upsert_gate: None,
            #[cfg(test)]
            before_use_snapshot_gate: None,
        }
    }

    #[cfg(test)]
    fn with_before_write_gate(
        mut self,
        prepared: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_write_gate = Some(OneShotGate {
            prepared,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    fn with_before_retry_gate(
        mut self,
        prepared: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_retry_gate = Some(OneShotGate {
            prepared,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    pub(crate) fn with_before_upsert_gate(mut self, barrier: Arc<tokio::sync::Barrier>) -> Self {
        self.before_upsert_gate = Some(BeforeUpsertGate {
            barrier,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    fn with_before_use_snapshot_gate(
        mut self,
        prepared: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_use_snapshot_gate = Some(OneShotGate {
            prepared,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    /// Create or replace one user-owned fixed-token identity from an exact global spec.
    pub(crate) async fn create_or_replace_user_fixed_token(
        &self,
        principal: &Principal,
        identity_name: &str,
        identity_spec_name: &str,
        token: String,
    ) -> Result<IdentityRecord, AppError> {
        let owner = IdentityOwner::for_user(principal.clone());
        let name = IdentityName::parse(identity_name)?;
        let spec_key = IdentitySpecKey::global(identity_spec_name)?;
        self.create_or_replace_fixed_token(owner, name, spec_key, token)
            .await
    }

    /// Create or replace one workspace-owned fixed-token identity with global fallback.
    pub(crate) async fn create_or_replace_workspace_fixed_token(
        &self,
        workspace: &WorkspaceName,
        identity_name: &str,
        identity_spec_name: &str,
        token: String,
    ) -> Result<IdentityRecord, AppError> {
        let owner = IdentityOwner::workspace(workspace.clone());
        let name = IdentityName::parse(identity_name)?;
        let spec_key = IdentitySpecKey::workspace(workspace.clone(), identity_spec_name)?;
        self.create_or_replace_fixed_token(owner, name, spec_key, token)
            .await
    }

    async fn create_or_replace_fixed_token(
        &self,
        owner: IdentityOwner,
        name: IdentityName,
        requested_key: IdentitySpecKey,
        token: String,
    ) -> Result<IdentityRecord, AppError> {
        let token = token.trim().to_owned();
        if token.is_empty() {
            return Err(AppError::InvalidInput(
                "fixed-token identity token must not be blank".to_string(),
            ));
        }
        let mut pinned_workspace_created_at_unix_nanos = None;
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let selected = match self
                .load_fixed_token_spec(
                    &owner,
                    &requested_key,
                    pinned_workspace_created_at_unix_nanos,
                )
                .await
            {
                Ok(selected) => selected,
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            if pinned_workspace_created_at_unix_nanos.is_none() {
                pinned_workspace_created_at_unix_nanos = selected.workspace_created_at_unix_nanos;
            }
            let document = self
                .prepare_fixed_token_document(&owner, &name, &selected.reference, token.clone())
                .await?;
            #[cfg(test)]
            if let Some(gate) = &self.before_write_gate {
                gate.wait().await;
            }
            match self.try_write(&owner, &name, &selected, &document).await {
                Ok(Some(record)) => return Ok(record),
                Ok(None) => {
                    #[cfg(test)]
                    if let Some(gate) = &self.before_retry_gate {
                        gate.wait().await;
                    }
                    tokio::task::yield_now().await;
                }
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    /// List safe persisted metadata for one exact owner in identity-name order.
    ///
    /// Management reads remain available when the referenced exact spec is no
    /// longer installed so callers can inspect and delete legacy identities.
    pub(crate) async fn list_for_owner(
        &self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        let result = async {
            owner_workspace_created_at(&mut tx, owner).await?;
            #[cfg(test)]
            if let Some(gate) = &self.before_write_gate {
                gate.wait().await;
            }
            Ok(tx.identities().list(owner).await?)
        }
        .await;
        complete_transaction(tx, result).await
    }

    /// Get safe persisted metadata for one exact owner and identity name.
    ///
    /// This does not resolve the referenced spec or decrypt setup material.
    pub(crate) async fn get(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<IdentityRecord, AppError> {
        let user_name = owner
            .workspace_name()
            .is_none()
            .then(|| IdentityName::parse(identity_name))
            .transpose()?;
        let mut tx = self.db.begin_read_snapshot().await?;
        let result = async {
            owner_workspace_created_at(&mut tx, owner).await?;
            #[cfg(test)]
            if let Some(gate) = &self.before_write_gate {
                gate.wait().await;
            }
            let name = match user_name {
                Some(name) => name,
                None => IdentityName::parse(identity_name)?,
            };
            tx.identities()
                .get(owner, &name)
                .await?
                .ok_or_else(|| identity_not_found(&name))
        }
        .await;
        complete_transaction(tx, result).await
    }

    /// Resolve one identity, its material, and its exact pinned spec from one snapshot.
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "the runtime identity consumer lands in the next stack layer"
        )
    )]
    pub(crate) async fn get_for_use(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<ResolvedIdentityForUse, AppError> {
        let name = IdentityName::parse(identity_name)?;
        let mut tx = self.db.begin_read_snapshot().await?;
        let result = async {
            owner_workspace_created_at(&mut tx, owner).await?;
            let identity = tx.identities().get(owner, &name).await?;
            #[cfg(test)]
            if let Some(gate) = &self.before_use_snapshot_gate {
                gate.wait().await;
            }
            load_identity_use_snapshot(&mut tx, owner, &name, identity).await
        }
        .await;
        let snapshot = complete_transaction(tx, result).await?;
        let key_provider = Arc::clone(&self.key_provider);
        run_blocking_identity_operation(move || {
            prepare_identity_for_use(snapshot, &name, key_provider.as_ref())
        })
        .await
    }

    /// Delete one identity and its cascading encrypted setup document.
    pub(crate) async fn delete(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<(), AppError> {
        let (name, workspace_created_at_unix_nanos) =
            self.prepare_delete(owner, identity_name).await?;
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            match self
                .try_delete(owner, &name, workspace_created_at_unix_nanos)
                .await
            {
                Ok(()) => return Ok(()),
                Err(AppError::RetryableTransactionConflict) => {
                    #[cfg(test)]
                    if let Some(gate) = &self.before_retry_gate {
                        gate.wait().await;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    async fn prepare_delete(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<(IdentityName, Option<i64>), AppError> {
        if owner.workspace_name().is_none() {
            return Ok((IdentityName::parse(identity_name)?, None));
        }
        let mut tx = self.db.begin_read_snapshot().await?;
        let result = async {
            let workspace_created_at_unix_nanos =
                owner_workspace_created_at(&mut tx, owner).await?;
            let name = IdentityName::parse(identity_name)?;
            Ok((name, workspace_created_at_unix_nanos))
        }
        .await;
        complete_transaction(tx, result).await
    }

    async fn try_delete(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        expected_workspace_created_at_unix_nanos: Option<i64>,
    ) -> Result<(), AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let result = async {
            let workspace_created_at_unix_nanos =
                owner_workspace_created_at(&mut tx, owner).await?;
            if workspace_created_at_unix_nanos != expected_workspace_created_at_unix_nanos {
                return Err(owner_workspace_not_found(owner));
            }
            #[cfg(test)]
            if let Some(gate) = &self.before_write_gate {
                gate.wait().await;
            }
            if !tx.identities().delete(owner, name).await? {
                return Err(identity_not_found(name));
            }
            Ok(())
        }
        .await;
        complete_transaction(tx, result).await
    }

    async fn load_fixed_token_spec(
        &self,
        owner: &IdentityOwner,
        requested_key: &IdentitySpecKey,
        expected_workspace_created_at_unix_nanos: Option<i64>,
    ) -> Result<SelectedFixedTokenSpec, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        let workspace_created_at_unix_nanos = owner_workspace_created_at(&mut tx, owner).await?;
        if expected_workspace_created_at_unix_nanos
            .is_some_and(|expected| Some(expected) != workspace_created_at_unix_nanos)
        {
            tx.rollback().await?;
            return Err(owner_workspace_not_found(owner));
        }
        let record = resolve_spec_record(&mut tx, requested_key).await?;
        tx.commit().await?;
        let record = record.ok_or_else(|| spec_not_found(requested_key))?;
        let installed = record_to_installed(record.clone())?;
        if installed.manifest.identity_type != IdentitySpecType::FixedToken {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{}' has type 'oauth', not 'fixed_token'",
                requested_key.name()
            )));
        }
        let audience = IdentityAudience::from_manifest(&installed.manifest.audience)?;
        let reference = IdentitySpecReference::new(
            owner,
            installed.key,
            identity_spec_fingerprint(&installed.manifest)?,
            installed.manifest.issuer,
            "fixed_token",
            audience,
        )?;
        Ok(SelectedFixedTokenSpec {
            requested_key: requested_key.clone(),
            workspace_created_at_unix_nanos,
            record,
            reference,
        })
    }

    async fn prepare_fixed_token_document(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        reference: &IdentitySpecReference,
        token: String,
    ) -> Result<EncryptedEnvelopeDocument, AppError> {
        let owner = owner.clone();
        let name = name.clone();
        let reference = reference.clone();
        let key_provider = Arc::clone(&self.key_provider);
        run_blocking_identity_operation(move || {
            let values = BTreeMap::from([(FIXED_TOKEN_KEY.to_string(), token)]);
            let binding = IdentityDocumentBinding::new(&owner, &name, &reference)?;
            encrypt_identity_document(&binding, &values, key_provider.as_ref()).map_err(Into::into)
        })
        .await
    }

    async fn try_write(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        selected: &SelectedFixedTokenSpec,
        document: &EncryptedEnvelopeDocument,
    ) -> Result<Option<IdentityRecord>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let workspace_created_at_unix_nanos = match owner_workspace_created_at(&mut tx, owner).await
        {
            Ok(created_at) => created_at,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        if workspace_created_at_unix_nanos != selected.workspace_created_at_unix_nanos {
            tx.rollback().await?;
            return Err(owner_workspace_not_found(owner));
        }
        let current = match resolve_spec_record(&mut tx, &selected.requested_key).await {
            Ok(current) => current,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        if current.as_ref() != Some(&selected.record) {
            tx.rollback().await?;
            return Ok(None);
        }
        #[cfg(test)]
        if let Some(gate) = &self.before_upsert_gate
            && !gate.used.swap(true, Ordering::SeqCst)
        {
            gate.barrier.wait().await;
        }
        let now = match now_unix_nanos_i64() {
            Ok(now) => now,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        let result = async {
            let record = tx
                .identities()
                .upsert(owner, name, &selected.reference, now)
                .await?;
            tx.identity_documents()
                .upsert(owner, name, document, now)
                .await?;
            Ok::<_, AppError>(record)
        }
        .await;
        match result {
            Ok(record) => {
                tx.commit().await?;
                Ok(Some(record))
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }
}

async fn load_identity_use_snapshot(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    identity: Option<IdentityRecord>,
) -> Result<IdentityUseSnapshot, AppError> {
    let identity_document = tx.identity_documents().get(owner, name).await?;
    let (identity_spec, identity_spec_document) = match identity.as_ref() {
        Some(identity) => {
            let key = identity.spec_reference.key();
            let identity_spec = tx.identity_specs().get(key).await?;
            let identity_spec_document = match identity_spec.as_ref() {
                Some(record) => tx.identity_spec_documents().get(&record.id).await?,
                None => None,
            };
            (identity_spec, identity_spec_document)
        }
        None => (None, None),
    };
    Ok(IdentityUseSnapshot {
        identity,
        identity_document,
        identity_spec,
        identity_spec_document,
    })
}

fn prepare_identity_for_use(
    snapshot: IdentityUseSnapshot,
    name: &IdentityName,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<ResolvedIdentityForUse, AppError> {
    let identity = snapshot.identity.ok_or_else(|| identity_not_found(name))?;
    let identity_document = snapshot
        .identity_document
        .ok_or_else(|| recreate_identity(name, "has no encrypted setup document"))?;
    validate_identity_document_key(&identity, &identity_document)?;
    let identity_spec_record = snapshot.identity_spec.ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "identity '{name}' is orphaned because its exact identity spec '{}:{}' is not installed; restore that exact spec or recreate the identity",
            scope_label(identity.spec_reference.key().scope()),
            identity.spec_reference.key().name(),
        ))
    })?;
    let identity_spec_id = identity_spec_record.id.clone();
    let installed = record_to_installed(identity_spec_record)?;
    validate_identity_reference(&identity, &installed)?;
    let identity_spec = resolve_installed_for_use(
        installed,
        &identity_spec_id,
        snapshot.identity_spec_document,
        key_provider,
    )?;
    let material = decrypt_fixed_token_material(&identity, identity_document, key_provider)?;
    Ok(ResolvedIdentityForUse {
        identity,
        identity_spec,
        material,
    })
}

fn validate_identity_reference(
    identity: &IdentityRecord,
    installed: &InstalledIdentitySpec,
) -> Result<(), AppError> {
    if identity.spec_reference.audience().is_none() {
        return Err(recreate_identity(
            &identity.name,
            "uses legacy metadata without a pinned audience",
        ));
    }
    if installed.manifest.identity_type != IdentitySpecType::FixedToken {
        return Err(recreate_identity(
            &identity.name,
            "references an unsupported OAuth identity spec",
        ));
    }
    let audience =
        IdentityAudience::from_manifest(&installed.manifest.audience).map_err(|_error| {
            corrupt_identity(
                &identity.owner,
                &identity.name,
                "has invalid audience metadata",
            )
        })?;
    let fingerprint = identity_spec_fingerprint(&installed.manifest).map_err(|_error| {
        corrupt_identity(
            &identity.owner,
            &identity.name,
            "has an invalid exact-spec fingerprint",
        )
    })?;
    let expected = IdentitySpecReference::new(
        &identity.owner,
        installed.key.clone(),
        fingerprint,
        installed.manifest.issuer.clone(),
        "fixed_token",
        audience,
    )
    .map_err(|_error| {
        corrupt_identity(
            &identity.owner,
            &identity.name,
            "has invalid derived exact-spec metadata",
        )
    })?;
    if identity.spec_reference != expected {
        return Err(AppError::FailedPrecondition(format!(
            "identity '{}' no longer matches its exact installed identity spec; restore the pinned spec revision or recreate the identity",
            identity.name,
        )));
    }
    Ok(())
}

fn decrypt_fixed_token_material(
    identity: &IdentityRecord,
    document: IdentityDocumentRecord,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<BTreeMap<String, String>, AppError> {
    let envelope = document.envelope;
    // Resolve the stored key first so an unavailable key provider stays a credential
    // error; everything the envelope layer rejects afterward is stored-material fault.
    let kek = key_provider
        .key(&envelope.key_id)
        .map_err(AppError::Credentials)?;
    let binding =
        IdentityDocumentBinding::new(&identity.owner, &identity.name, &identity.spec_reference)
            .map_err(|_error| {
                corrupt_identity(
                    &identity.owner,
                    &identity.name,
                    "has invalid authenticated metadata",
                )
            })?;
    let material = decrypt_identity_document(&binding, &envelope, &kek).map_err(|_error| {
        AppError::from(corrupt_identity(
            &identity.owner,
            &identity.name,
            "has an encrypted setup document that failed authentication or decoding",
        ))
    })?;
    if material.len() != 1
        || material.get(FIXED_TOKEN_KEY).is_none_or(|token| {
            let trimmed = token.trim();
            trimmed.is_empty() || trimmed != token
        })
    {
        return Err(corrupt_identity(
            &identity.owner,
            &identity.name,
            "has invalid fixed-token material",
        )
        .into());
    }
    Ok(material)
}

fn validate_identity_document_key(
    identity: &IdentityRecord,
    document: &IdentityDocumentRecord,
) -> Result<(), AppError> {
    if document.owner == identity.owner && document.name == identity.name {
        return Ok(());
    }
    Err(corrupt_identity(
        &identity.owner,
        &identity.name,
        "has an encrypted setup document with a different key",
    )
    .into())
}

async fn resolve_spec_record(
    tx: &mut CoralTx<'_>,
    requested_key: &IdentitySpecKey,
) -> Result<Option<IdentitySpecRecord>, AppError> {
    if let some @ Some(_) = tx.identity_specs().get(requested_key).await? {
        return Ok(some);
    }
    let IdentitySpecScope::Workspace(_) = requested_key.scope() else {
        return Ok(None);
    };
    let global_key = IdentitySpecKey::global(requested_key.name())?;
    Ok(tx.identity_specs().get(&global_key).await?)
}

async fn owner_workspace_created_at(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
) -> Result<Option<i64>, AppError> {
    let Some(workspace) = owner.workspace_name() else {
        return Ok(None);
    };
    let record = tx
        .workspaces()
        .get(workspace.as_str())
        .await?
        .ok_or_else(|| AppError::WorkspaceNotFound(workspace.to_string()))?;
    Ok(Some(record.created_at_unix_nanos))
}

fn owner_workspace_not_found(owner: &IdentityOwner) -> AppError {
    let workspace = owner
        .workspace_name()
        .expect("only workspace owners have a persisted workspace generation");
    AppError::WorkspaceNotFound(workspace.to_string())
}

async fn run_blocking_identity_operation<T, F>(operation: F) -> Result<T, AppError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, AppError> + Send + 'static,
{
    let span = tracing::Span::current();
    tokio::task::spawn_blocking(move || span.in_scope(operation)).await?
}

async fn complete_transaction<T>(
    tx: CoralTx<'_>,
    result: Result<T, AppError>,
) -> Result<T, AppError> {
    match result {
        Ok(value) => {
            tx.commit().await?;
            Ok(value)
        }
        Err(error) => {
            tx.rollback().await?;
            Err(error)
        }
    }
}

fn identity_not_found(name: &IdentityName) -> AppError {
    AppError::IdentityNotFound(name.to_string())
}

fn spec_not_found(key: &IdentitySpecKey) -> AppError {
    let scope = scope_label(key.scope());
    AppError::IdentitySpecNotFound {
        name: key.name().to_string(),
        scope,
    }
}

fn scope_label(scope: &IdentitySpecScope) -> String {
    match scope {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    }
}

fn recreate_identity(name: &IdentityName, detail: &str) -> AppError {
    AppError::FailedPrecondition(format!(
        "identity '{name}' {detail}; restore its exact identity spec or recreate the identity"
    ))
}

fn corrupt_identity(owner: &IdentityOwner, name: &IdentityName, detail: &str) -> DbError {
    DbError::CorruptData(format!(
        "identity '{}:{}:{name}' is corrupt: {detail}",
        owner.kind(),
        owner.key(),
    ))
}

#[cfg(test)]
pub(crate) mod tests;
