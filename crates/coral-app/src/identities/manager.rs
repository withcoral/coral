//! Database-backed identity instance creation.

#![cfg_attr(
    not(test),
    expect(
        dead_code,
        reason = "identity service consumers land in a later stack layer"
    )
)]

use std::collections::BTreeMap;
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::IdentitySpecType;

use super::crypto::{IdentityDocumentBinding, encrypt_identity_document};
use super::model::{IdentityName, IdentityOwner, IdentitySpecReference};
use crate::bootstrap::AppError;
use crate::credentials::encryption::CredentialKeyProvider;
use crate::encrypted_document::EncryptedEnvelopeDocument;
use crate::identity::Principal;
use crate::identity_specs::identity_spec_fingerprint;
use crate::identity_specs::manager::record_to_installed;
use crate::state::db::{
    CoralDb, CoralTx, DbRepos, IdentityRecord, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecScope, now_unix_nanos_i64,
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
        let reference = IdentitySpecReference::new(
            owner,
            installed.key,
            identity_spec_fingerprint(&installed.manifest)?,
            installed.manifest.issuer,
            "fixed_token",
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

fn spec_not_found(key: &IdentitySpecKey) -> AppError {
    let scope = match key.scope() {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    };
    AppError::IdentitySpecNotFound {
        name: key.name().to_string(),
        scope,
    }
}

#[cfg(test)]
pub(crate) mod tests;
