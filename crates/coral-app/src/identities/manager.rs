//! Database-backed identity instance management.

use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::IdentitySpecType;

use crate::bootstrap::AppError;
use crate::credentials::encryption::{CredentialKeyProvider, EncryptedEnvelopeDocument};
use crate::identities::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::identity::{
    IdentityDocumentBinding, UserPrincipal, encrypt_identity_document, run_key_operation,
};
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::{record_to_installed, spec_not_found};
use crate::state::db::{
    CoralDb, CoralTx, DbRepos, IdentityDocumentWrite, IdentityRecord, IdentitySpecKey,
    IdentitySpecRecord, now_unix_nanos_i64,
};
use crate::workspaces::WorkspaceName;

const FIXED_TOKEN_KEY: &str = "TOKEN";
const MAX_MUTATION_ATTEMPTS: usize = 8;

/// Database-backed behavior for owner-scoped identity instances.
#[derive(Clone)]
pub(crate) struct IdentityManager {
    db: Arc<CoralDb>,
    key_provider: Arc<dyn CredentialKeyProvider>,
    #[cfg(test)]
    before_write_gate: Option<BeforeWriteGate>,
    #[cfg(test)]
    before_upsert_gate: Option<BeforeUpsertGate>,
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
        if !self.used.swap(true, Ordering::SeqCst) {
            self.selected.wait().await;
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

impl IdentityManager {
    pub(crate) fn new(db: Arc<CoralDb>, key_provider: Arc<dyn CredentialKeyProvider>) -> Self {
        Self {
            db,
            key_provider,
            #[cfg(test)]
            before_write_gate: None,
            #[cfg(test)]
            before_upsert_gate: None,
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
                .upsert(owner, name, &selected.reference, now)
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
    let (spec_scope_kind, spec_scope_id, spec_name) = reference.key().document_aad_parts();
    let binding = IdentityDocumentBinding::new(
        owner.kind(),
        owner.key(),
        name.as_str(),
        spec_scope_kind,
        spec_scope_id,
        spec_name,
        reference.fingerprint(),
    );
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
