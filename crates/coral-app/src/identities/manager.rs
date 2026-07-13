//! Database-backed identity instance management.

#![cfg_attr(
    not(test),
    expect(dead_code, reason = "Identity use consumers land in B5.")
)]

mod oauth_create;
mod oauth_refresh;

pub(crate) use oauth_create::IdentityOAuthCommitPhase;
use oauth_refresh::IdentityOAuthRefreshOutcome;

use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::IdentitySpecType;

use crate::bootstrap::AppError;
use crate::credentials::encryption::{CredentialKeyProvider, EncryptedEnvelopeDocument};
use crate::credentials::oauth::{OAuthAuthorization, OAuthCredentialService};
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::{
    IdentityDocumentBinding, UserPrincipal, decrypt_identity_document, encrypt_identity_document,
    is_legacy_identity_document_aad_version, rewrap_identity_document,
    rewrap_identity_spec_document, run_key_operation,
};
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::{
    InstalledIdentitySpec, ResolvedIdentitySpec, record_to_installed, resolve_installed_for_use,
    spec_not_found,
};
use crate::state::db::{
    CoralDb, CoralTx, DbRepos, IdentityDocumentRecord, IdentityDocumentWrite,
    IdentityOAuthRefreshClaim, IdentityRecord, IdentitySpecDocumentRecord,
    IdentitySpecDocumentWrite, IdentitySpecKey, IdentitySpecRecord, now_unix_nanos_i64,
};
use crate::workspaces::WorkspaceName;

pub(super) const FIXED_TOKEN_KEY: &str = "TOKEN";
pub(super) const OAUTH_ACCESS_TOKEN_KEY: &str = "ACCESS_TOKEN";
const MAX_MUTATION_ATTEMPTS: usize = 8;

/// App-private progress emitted before an OAuth identity becomes durable.
pub(crate) enum IdentityOAuthCreationEvent {
    Authorization(OAuthAuthorization),
    Completed(BTreeMap<String, String>),
}

/// Coherent decrypted identity data prepared for one runtime use.
pub(crate) struct ResolvedIdentityForUse {
    pub(crate) identity: IdentityRecord,
    pub(crate) identity_spec: ResolvedIdentitySpec,
    material: BTreeMap<String, String>,
    revision: IdentityForUseRevision,
}

impl ResolvedIdentityForUse {
    pub(crate) fn material(&self) -> &BTreeMap<String, String> {
        &self.material
    }

    pub(crate) fn revision(&self) -> &IdentityForUseRevision {
        &self.revision
    }
}

impl fmt::Debug for ResolvedIdentityForUse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResolvedIdentityForUse")
            .field("owner", &self.identity.owner)
            .field("name", &self.identity.name)
            .field("identity_spec", &self.identity_spec)
            .field("material_value_count", &self.material.len())
            .finish_non_exhaustive()
    }
}

/// Opaque complete database revision paired with resolved identity material.
pub(crate) struct IdentityForUseRevision {
    _snapshot: IdentityUseSnapshot,
}

#[derive(Clone, PartialEq, Eq)]
struct IdentityUseSnapshot {
    workspace_created_at_unix_nanos: Option<i64>,
    identity: Option<IdentityRecord>,
    identity_document: Option<IdentityDocumentRecord>,
    identity_spec: Option<IdentitySpecRecord>,
    identity_spec_document: Option<IdentitySpecDocumentRecord>,
    oauth_refresh_claim: Option<IdentityOAuthRefreshClaim>,
}

struct PreparedIdentityForUse {
    identity: IdentityRecord,
    identity_spec: ResolvedIdentitySpec,
    material: BTreeMap<String, String>,
    identity_rewrap: Option<IdentityDocumentWrite>,
    identity_spec_rewrap: Option<IdentitySpecDocumentWrite>,
}

impl PreparedIdentityForUse {
    fn needs_rewrap(&self) -> bool {
        self.identity_rewrap.is_some() || self.identity_spec_rewrap.is_some()
    }
}

/// Database-backed behavior for owner-scoped identity instances.
#[derive(Clone)]
pub(crate) struct IdentityManager {
    db: Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    oauth: OAuthCredentialService,
    #[cfg(test)]
    before_write_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_upsert_gate: Option<BeforeUpsertGate>,
    #[cfg(test)]
    before_use_cas_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_refresh_claim_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_refresh_wait_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_refresh_finalize_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_retry_gate: Option<BeforeWriteGate>,
}

#[cfg(test)]
#[derive(Clone)]
struct BeforeWriteGate {
    selected: Arc<tokio::sync::Barrier>,
    resume: Arc<tokio::sync::Barrier>,
    used: Arc<AtomicBool>,
}

#[cfg(test)]
impl BeforeWriteGate {
    async fn wait_once(&self) {
        self.wait_once_and_report().await;
    }

    async fn wait_once_and_report(&self) -> bool {
        if self.used.swap(true, Ordering::SeqCst) {
            false
        } else {
            self.selected.wait().await;
            self.resume.wait().await;
            true
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

impl IdentityManager {
    pub(crate) fn new(db: Arc<CoralDb>, key_provider: Arc<dyn CredentialKeyProvider>) -> Self {
        Self {
            db,
            key_provider,
            oauth: OAuthCredentialService::new(),
            #[cfg(test)]
            before_write_gate: None,
            #[cfg(test)]
            before_upsert_gate: None,
            #[cfg(test)]
            before_use_cas_gate: None,
            #[cfg(test)]
            before_refresh_claim_gate: None,
            #[cfg(test)]
            before_refresh_wait_gate: None,
            #[cfg(test)]
            before_refresh_finalize_gate: None,
            #[cfg(test)]
            before_retry_gate: None,
        }
    }

    #[cfg(test)]
    fn with_before_write_gate(
        mut self,
        selected: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_write_gate = Some(BeforeWriteGate {
            selected,
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
    fn with_before_use_cas_gate(
        mut self,
        selected: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_use_cas_gate = Some(BeforeWriteGate {
            selected,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    fn with_before_refresh_wait_gate(
        mut self,
        selected: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_refresh_wait_gate = Some(BeforeWriteGate {
            selected,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    fn with_before_refresh_claim_gate(
        mut self,
        selected: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_refresh_claim_gate = Some(BeforeWriteGate {
            selected,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    fn with_before_refresh_finalize_gate(
        mut self,
        selected: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_refresh_finalize_gate = Some(BeforeWriteGate {
            selected,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    #[cfg(test)]
    fn with_before_retry_gate(
        mut self,
        reached: Arc<tokio::sync::Barrier>,
        resume: Arc<tokio::sync::Barrier>,
    ) -> Self {
        self.before_retry_gate = Some(BeforeWriteGate {
            selected: reached,
            resume,
            used: Arc::new(AtomicBool::new(false)),
        });
        self
    }

    /// Create or replace a user-owned fixed-token identity from one exact global spec.
    pub(crate) async fn create_or_replace_user_fixed_token(
        &self,
        principal: &UserPrincipal,
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

    /// Create or replace a workspace-owned fixed-token identity using workspace-first resolution.
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
        spec_key: IdentitySpecKey,
        token: String,
    ) -> Result<IdentityRecord, AppError> {
        let token = token.trim().to_string();
        if token.is_empty() {
            return Err(AppError::InvalidInput(
                "fixed-token identity token must not be blank".to_string(),
            ));
        }
        let mut document: Option<(IdentitySpecReference, IdentityDocumentWrite)> = None;
        let mut pinned_workspace_created_at_unix_nanos = None;
        #[cfg(test)]
        let mut before_write_gate = self.before_write_gate.clone();
        #[cfg(test)]
        let mut before_retry_gate = self.before_retry_gate.clone();

        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let selected = match self
                .load_fixed_token_spec(&owner, &spec_key, pinned_workspace_created_at_unix_nanos)
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
            if document
                .as_ref()
                .is_none_or(|(reference, _document)| reference != &selected.reference)
            {
                let document_owner = owner.clone();
                let document_name = name.clone();
                let document_reference = selected.reference.clone();
                let document_token = token.clone();
                let key_provider = Arc::clone(&self.key_provider);
                let prepared = run_key_operation(move || {
                    prepare_fixed_token_document(
                        &document_owner,
                        &document_name,
                        &document_reference,
                        document_token,
                        key_provider.as_ref(),
                    )
                })
                .await?;
                document = Some((selected.reference.clone(), prepared));
            }
            #[cfg(test)]
            if let Some(gate) = before_write_gate.take() {
                gate.selected.wait().await;
                gate.resume.wait().await;
            }
            match self
                .try_write(
                    &owner,
                    &name,
                    &selected,
                    &document.as_ref().expect("document prepared above").1,
                )
                .await
            {
                Ok(Some(record)) => return Ok(record),
                Ok(None) | Err(AppError::RetryableTransactionConflict) => {
                    #[cfg(test)]
                    if let Some(gate) = before_retry_gate.take() {
                        gate.selected.wait().await;
                        gate.resume.wait().await;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    /// List safe persisted fields for one exact owner, including orphaned identities.
    pub(crate) async fn list_for_owner(
        &self,
        owner: &IdentityOwner,
    ) -> Result<Vec<IdentityRecord>, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        owner_workspace_created_at(&mut tx, owner).await?;
        #[cfg(test)]
        if let Some(gate) = &self.before_write_gate {
            gate.wait_once().await;
        }
        let records = tx.identities().list_for_owner(owner).await?;
        tx.commit().await?;
        Ok(records)
    }

    /// Get safe persisted fields for one identity, including an orphaned identity.
    pub(crate) async fn get(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<IdentityRecord, AppError> {
        if owner.workspace_name().is_none() {
            IdentityName::parse(identity_name)?;
        }
        let mut tx = self.db.begin_read_snapshot().await?;
        owner_workspace_created_at(&mut tx, owner).await?;
        let name = IdentityName::parse(identity_name)?;
        let record = tx.identities().load_optional(owner, &name).await?;
        tx.commit().await?;
        record.ok_or_else(|| identity_not_found(&name))
    }

    /// Resolve one identity and its exact installed spec from one coherent snapshot.
    pub(crate) fn get_for_use<'a>(
        &'a self,
        owner: &'a IdentityOwner,
        identity_name: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<ResolvedIdentityForUse, AppError>> + Send + 'a>> {
        Box::pin(self.get_for_use_inner(owner, identity_name))
    }

    async fn get_for_use_inner(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<ResolvedIdentityForUse, AppError> {
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            let (name, snapshot) = match self.load_for_use_snapshot(owner, identity_name).await {
                Ok(snapshot) => snapshot,
                Err(AppError::RetryableTransactionConflict) => {
                    tokio::task::yield_now().await;
                    continue;
                }
                Err(error) => return Err(error),
            };
            let crypto_snapshot = snapshot.clone();
            let crypto_name = name.clone();
            let key_provider = Arc::clone(&self.key_provider);
            let prepared = run_key_operation(move || {
                prepare_identity_for_use(crypto_snapshot, &crypto_name, key_provider.as_ref())
            })
            .await?;
            let (snapshot, prepared) = match self
                .refresh_prepared_identity(owner, &name, snapshot, prepared)
                .await?
            {
                IdentityOAuthRefreshOutcome::Unchanged(state) => *state,
                IdentityOAuthRefreshOutcome::Retry => {
                    tokio::task::yield_now().await;
                    continue;
                }
                IdentityOAuthRefreshOutcome::Refreshed(resolved) => return Ok(*resolved),
            };
            #[cfg(test)]
            if prepared.needs_rewrap()
                && let Some(gate) = &self.before_use_cas_gate
            {
                gate.wait_once().await;
            }
            let revision = if prepared.needs_rewrap() && snapshot.oauth_refresh_claim.is_none() {
                match self
                    .try_rewrap_for_use(owner, &name, &snapshot, &prepared)
                    .await
                {
                    Ok(Some(revision)) => revision,
                    Ok(None) | Err(AppError::RetryableTransactionConflict) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            } else {
                snapshot
            };
            return Ok(ResolvedIdentityForUse {
                identity: prepared.identity,
                identity_spec: prepared.identity_spec,
                material: prepared.material,
                revision: IdentityForUseRevision {
                    _snapshot: revision,
                },
            });
        }
        Err(AppError::RetryableTransactionConflict)
    }

    async fn load_for_use_snapshot(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<(IdentityName, IdentityUseSnapshot), AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        let workspace_created_at_unix_nanos = owner_workspace_created_at(&mut tx, owner).await?;
        let name = IdentityName::parse(identity_name)?;
        let snapshot =
            load_identity_use_snapshot(&mut tx, owner, &name, workspace_created_at_unix_nanos)
                .await?;
        tx.commit().await?;
        Ok((name, snapshot))
    }

    async fn try_rewrap_for_use(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        expected: &IdentityUseSnapshot,
        prepared: &PreparedIdentityForUse,
    ) -> Result<Option<IdentityUseSnapshot>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let workspace_created_at_unix_nanos = owner_workspace_created_at(&mut tx, owner).await?;
        let mut current =
            load_identity_use_snapshot(&mut tx, owner, name, workspace_created_at_unix_nanos)
                .await?;
        if current != *expected {
            tx.rollback().await?;
            return Ok(None);
        }
        let spec_rewrap_blocked = if prepared.identity_spec_rewrap.is_some() {
            let key = current
                .identity
                .as_ref()
                .expect("validated identity snapshot")
                .spec_reference
                .key();
            tx.identities()
                .has_oauth_refresh_claimed_dependents(key)
                .await?
        } else {
            false
        };
        if spec_rewrap_blocked && prepared.identity_rewrap.is_none() {
            tx.rollback().await?;
            return Ok(Some(current));
        }
        #[cfg(test)]
        if let Some(gate) = &self.before_upsert_gate
            && !gate.used.swap(true, Ordering::SeqCst)
        {
            gate.barrier.wait().await;
        }
        let now = now_unix_nanos_i64()?;
        let result = async {
            if let Some(write) = &prepared.identity_spec_rewrap
                && !spec_rewrap_blocked
            {
                let key = current
                    .identity
                    .as_ref()
                    .expect("validated identity snapshot")
                    .spec_reference
                    .key();
                current.identity_spec_document =
                    Some(tx.identity_spec_documents().upsert(key, write, now).await?);
            }
            if let Some(write) = &prepared.identity_rewrap {
                current.identity_document = Some(
                    tx.identity_documents()
                        .upsert(owner, name, write, now)
                        .await?,
                );
            }
            Ok::<_, AppError>(())
        }
        .await;
        if let Err(error) = result {
            tx.rollback().await?;
            return Err(error);
        }
        tx.commit().await?;
        Ok(Some(current))
    }

    /// Delete one identity and its cascading encrypted document.
    pub(crate) async fn delete(
        &self,
        owner: &IdentityOwner,
        identity_name: &str,
    ) -> Result<(), AppError> {
        if owner.workspace_name().is_none() {
            IdentityName::parse(identity_name)?;
        }
        for _ in 0..MAX_MUTATION_ATTEMPTS {
            match self.try_delete(owner, identity_name).await {
                Ok(()) => return Ok(()),
                Err(AppError::RetryableTransactionConflict) => {
                    #[cfg(test)]
                    if let Some(gate) = &self.before_retry_gate {
                        gate.wait_once().await;
                    }
                    tokio::task::yield_now().await;
                }
                Err(error) => return Err(error),
            }
        }
        Err(AppError::RetryableTransactionConflict)
    }

    async fn try_delete(&self, owner: &IdentityOwner, identity_name: &str) -> Result<(), AppError> {
        let mut tx = self.db.begin_serializable().await?;
        owner_workspace_created_at(&mut tx, owner).await?;
        #[cfg(test)]
        if let Some(gate) = &self.before_write_gate {
            gate.wait_once().await;
        }
        let name = IdentityName::parse(identity_name)?;
        let deleted = match tx.identities().delete(owner, &name).await {
            Ok(deleted) => deleted,
            Err(error) => {
                tx.rollback().await?;
                return Err(error.into());
            }
        };
        if !deleted {
            tx.rollback().await?;
            return Err(identity_not_found(&name));
        }
        tx.commit().await?;
        Ok(())
    }

    async fn load_fixed_token_spec(
        &self,
        owner: &IdentityOwner,
        key: &IdentitySpecKey,
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
        let record = tx.identity_specs().resolve_optional(key).await?;
        tx.commit().await?;
        let record = record.ok_or_else(|| spec_not_found(key))?;
        let installed = record_to_installed(record.clone())?;
        if installed.manifest.identity_type != IdentitySpecType::FixedToken {
            return Err(AppError::InvalidInput(format!(
                "identity spec '{}' has type '{}', not 'fixed_token'",
                key.name(),
                installed.manifest.identity_type.label(),
            )));
        }
        let reference = IdentitySpecReference::new(
            owner,
            installed.key,
            identity_spec_fingerprint(&installed.manifest)?,
            installed.manifest.issuer,
            installed.manifest.identity_type.label(),
        )?;
        Ok(SelectedFixedTokenSpec {
            requested_key: key.clone(),
            workspace_created_at_unix_nanos,
            record,
            reference,
        })
    }

    async fn try_write(
        &self,
        owner: &IdentityOwner,
        name: &IdentityName,
        selected: &SelectedFixedTokenSpec,
        document: &IdentityDocumentWrite,
    ) -> Result<Option<IdentityRecord>, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let workspace_created_at_unix_nanos = owner_workspace_created_at(&mut tx, owner).await?;
        if workspace_created_at_unix_nanos != selected.workspace_created_at_unix_nanos {
            // A delete/recreate under the same name is a new owner generation. An in-flight
            // credential write must not cross that lifecycle boundary.
            tx.rollback().await?;
            return Err(owner_workspace_not_found(owner));
        }
        let current = tx
            .identity_specs()
            .resolve_optional(&selected.requested_key)
            .await?;
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
        let now = now_unix_nanos_i64()?;
        let result = async {
            let record = tx
                .identities()
                .upsert(owner, name, &selected.reference, &BTreeMap::new(), now)
                .await?;
            tx.identity_documents()
                .upsert(owner, name, document, now)
                .await?;
            Ok::<_, AppError>(record)
        }
        .await;
        let record = match result {
            Ok(record) => record,
            Err(error) => {
                tx.rollback().await?;
                return Err(error);
            }
        };
        tx.commit().await?;
        Ok(Some(record))
    }
}

async fn load_identity_use_snapshot(
    tx: &mut CoralTx<'_>,
    owner: &IdentityOwner,
    name: &IdentityName,
    workspace_created_at_unix_nanos: Option<i64>,
) -> Result<IdentityUseSnapshot, AppError> {
    let identity = tx.identities().load_optional(owner, name).await?;
    let oauth_refresh_claim = tx
        .identities()
        .load_oauth_refresh_claim(owner, name)
        .await?;
    let identity_document = tx.identity_documents().load_optional(owner, name).await?;
    let (identity_spec, identity_spec_document) = match identity.as_ref() {
        Some(identity) => {
            let key = identity.spec_reference.key();
            (
                tx.identity_specs().load_optional(key).await?,
                tx.identity_spec_documents().load_optional(key).await?,
            )
        }
        None => (None, None),
    };
    Ok(IdentityUseSnapshot {
        workspace_created_at_unix_nanos,
        identity,
        identity_document,
        identity_spec,
        identity_spec_document,
        oauth_refresh_claim,
    })
}

fn prepare_identity_for_use(
    snapshot: IdentityUseSnapshot,
    name: &IdentityName,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<PreparedIdentityForUse, AppError> {
    let identity = snapshot.identity.ok_or_else(|| identity_not_found(name))?;
    let identity_document = snapshot
        .identity_document
        .ok_or_else(|| recreate_identity(name, "has no encrypted material"))?;
    if is_legacy_identity_document_aad_version(identity_document.aad_version) {
        return Err(recreate_identity(
            name,
            "uses legacy encrypted material that is not bound to its identity spec",
        ));
    }
    let identity_spec_record = snapshot.identity_spec.ok_or_else(|| {
        AppError::FailedPrecondition(format!(
            "identity '{name}' is orphaned because identity spec '{}' is not installed; restore the exact spec or recreate the identity",
            identity.spec_reference.key().name(),
        ))
    })?;
    let installed = record_to_installed(identity_spec_record)?;
    validate_identity_reference(&identity, &installed)?;
    let identity_spec = resolve_installed_for_use(
        installed,
        snapshot.identity_spec_document.clone(),
        key_provider,
    )?;
    let binding =
        identity_document_binding(&identity.owner, &identity.name, &identity.spec_reference);
    let envelope = identity_envelope(&identity_document);
    let material = decrypt_identity_document(&binding, &envelope, key_provider)?;
    let material_is_valid = match identity_spec.spec.manifest.identity_type {
        IdentitySpecType::FixedToken => {
            material.len() == 1
                && material
                    .get(FIXED_TOKEN_KEY)
                    .is_some_and(|token| !token.trim().is_empty())
        }
        IdentitySpecType::OAuth => material
            .get(OAUTH_ACCESS_TOKEN_KEY)
            .is_some_and(|token| !token.trim().is_empty()),
    };
    if !material_is_valid {
        return Err(corrupt_identity_material(&identity));
    }
    let identity_rewrap = rewrap_identity_document(&binding, &envelope, key_provider)?
        .map(identity_document_write)
        .transpose()?;
    let identity_spec_rewrap = match snapshot.identity_spec_document.as_ref() {
        Some(document) => {
            let envelope = identity_spec_envelope(document);
            let (scope_kind, scope_id, spec_name) = document.key.document_aad_parts();
            rewrap_identity_spec_document(scope_kind, scope_id, spec_name, &envelope, key_provider)?
                .map(identity_spec_document_write)
                .transpose()?
        }
        None => None,
    };
    Ok(PreparedIdentityForUse {
        identity,
        identity_spec,
        material,
        identity_rewrap,
        identity_spec_rewrap,
    })
}

fn validate_identity_reference(
    identity: &IdentityRecord,
    installed: &InstalledIdentitySpec,
) -> Result<(), AppError> {
    let expected = IdentitySpecReference::new(
        &identity.owner,
        installed.key.clone(),
        identity_spec_fingerprint(&installed.manifest)?,
        installed.manifest.issuer.clone(),
        installed.manifest.identity_type.label(),
    )?;
    if identity.spec_reference != expected {
        return Err(recreate_identity(
            &identity.name,
            "no longer matches its exact installed identity spec",
        ));
    }
    Ok(())
}

fn identity_document_binding<'a>(
    owner: &'a IdentityOwner,
    name: &'a IdentityName,
    reference: &'a IdentitySpecReference,
) -> IdentityDocumentBinding<'a> {
    let (spec_scope_kind, spec_scope_id, spec_name) = reference.key().document_aad_parts();
    IdentityDocumentBinding::new(
        owner.kind(),
        owner.key(),
        name.as_str(),
        spec_scope_kind,
        spec_scope_id,
        spec_name,
        reference.fingerprint(),
    )
}

fn identity_envelope(document: &IdentityDocumentRecord) -> EncryptedEnvelopeDocument {
    EncryptedEnvelopeDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: document.wrapped_dek.clone(),
        wrapped_dek_nonce: document.wrapped_dek_nonce.clone(),
        key_id: document.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    }
}

fn identity_spec_envelope(document: &IdentitySpecDocumentRecord) -> EncryptedEnvelopeDocument {
    EncryptedEnvelopeDocument {
        ciphertext: document.ciphertext.clone(),
        nonce: document.nonce.clone(),
        wrapped_dek: document.wrapped_dek.clone(),
        wrapped_dek_nonce: document.wrapped_dek_nonce.clone(),
        key_id: document.key_id.clone(),
        algorithm: document.algorithm.clone(),
        aad_version: document.aad_version,
    }
}

fn identity_document_write(
    document: EncryptedEnvelopeDocument,
) -> Result<IdentityDocumentWrite, AppError> {
    IdentityDocumentWrite::new(
        document.ciphertext,
        document.nonce,
        document.wrapped_dek,
        document.wrapped_dek_nonce,
        document.key_id,
        document.algorithm,
        document.aad_version,
    )
}

fn identity_spec_document_write(
    document: EncryptedEnvelopeDocument,
) -> Result<IdentitySpecDocumentWrite, AppError> {
    IdentitySpecDocumentWrite::new(
        document.ciphertext,
        document.nonce,
        document.wrapped_dek,
        document.wrapped_dek_nonce,
        document.key_id,
        document.algorithm,
        document.aad_version,
    )
}

fn recreate_identity(name: &IdentityName, detail: &str) -> AppError {
    AppError::FailedPrecondition(format!("identity '{name}' {detail}; recreate the identity"))
}

fn corrupt_identity_material(identity: &IdentityRecord) -> AppError {
    AppError::Database(format!(
        "identity '{}:{}:{}' has invalid encrypted material",
        identity.owner.kind(),
        identity.owner.key(),
        identity.name,
    ))
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

fn prepare_fixed_token_document(
    owner: &IdentityOwner,
    name: &IdentityName,
    reference: &IdentitySpecReference,
    token: String,
    key_provider: &dyn CredentialKeyProvider,
) -> Result<IdentityDocumentWrite, AppError> {
    let values = BTreeMap::from([(FIXED_TOKEN_KEY.to_string(), token)]);
    let binding = identity_document_binding(owner, name, reference);
    let EncryptedEnvelopeDocument {
        ciphertext,
        nonce,
        wrapped_dek,
        wrapped_dek_nonce,
        key_id,
        algorithm,
        aad_version,
    } = encrypt_identity_document(&binding, &values, key_provider)?;
    IdentityDocumentWrite::new(
        ciphertext,
        nonce,
        wrapped_dek,
        wrapped_dek_nonce,
        key_id,
        algorithm,
        aad_version,
    )
}

fn identity_not_found(name: &IdentityName) -> AppError {
    AppError::IdentityNotFound(name.to_string())
}

#[cfg(test)]
pub(crate) mod tests;
