use std::future::Future;
#[cfg(test)]
use std::sync::Arc;
#[cfg(test)]
use std::sync::atomic::{AtomicBool, Ordering};

use coral_spec::IdentityManifest;

use super::{
    CoralDb, CoralTx, DbRepos, IdentitySpecDocumentRecord, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecScope, now_unix_nanos_i64,
};
use crate::bootstrap::AppError;
use crate::encrypted_document::EncryptedEnvelopeDocument;

pub(crate) struct IdentitySpecState<'a> {
    db: &'a CoralDb,
    #[cfg(test)]
    before_lifecycle_write: Option<BeforeLifecycleWriteGate>,
}

#[cfg(test)]
#[derive(Clone)]
pub(crate) struct BeforeLifecycleWriteGate {
    barrier: Arc<tokio::sync::Barrier>,
    used: Arc<AtomicBool>,
}

#[cfg(test)]
impl BeforeLifecycleWriteGate {
    pub(crate) fn new(barrier: Arc<tokio::sync::Barrier>) -> Self {
        Self {
            barrier,
            used: Arc::new(AtomicBool::new(false)),
        }
    }
}

#[derive(PartialEq, Eq)]
pub(crate) struct IdentitySpecMutationSnapshot {
    pub(crate) record: Option<IdentitySpecRecord>,
    pub(crate) document: Option<IdentitySpecDocumentRecord>,
}

impl CoralDb {
    pub(crate) fn identity_spec_state(&self) -> IdentitySpecState<'_> {
        IdentitySpecState {
            db: self,
            #[cfg(test)]
            before_lifecycle_write: None,
        }
    }
}

impl IdentitySpecState<'_> {
    #[cfg(test)]
    pub(crate) fn with_before_lifecycle_write(
        mut self,
        gate: Option<BeforeLifecycleWriteGate>,
    ) -> Self {
        self.before_lifecycle_write = gate;
        self
    }

    pub(crate) async fn add_or_replace_exact<F, Fut>(
        &self,
        key: &IdentitySpecKey,
        manifest: &IdentityManifest,
        manifest_yaml: &str,
        prepare_mutation: F,
    ) -> Result<(IdentitySpecRecord, bool), AppError>
    where
        F: FnOnce(IdentitySpecMutationSnapshot) -> Fut,
        Fut: Future<Output = Result<(Option<EncryptedEnvelopeDocument>, String), AppError>>,
    {
        let mut tx = self.db.begin_serializable().await?;
        let result = async {
            require_scope_workspace(&mut tx, key.scope()).await?;
            let snapshot = load_mutation_snapshot(&mut tx, key).await?;
            let replaced = snapshot.record.is_some();
            let (document, fingerprint) = prepare_mutation(snapshot).await?;
            require_equivalent_dependents(&mut tx, key, &fingerprint).await?;
            #[cfg(test)]
            self.wait_before_lifecycle_write().await;
            let now = now_unix_nanos_i64()?;
            let record = tx
                .identity_specs()
                .upsert(key, manifest, manifest_yaml, now)
                .await?;
            match document.as_ref() {
                Some(document) => {
                    tx.identity_spec_documents()
                        .upsert(&record.id, document, now)
                        .await?;
                }
                None => {
                    tx.identity_spec_documents().delete(&record.id).await?;
                }
            }
            Ok((record, replaced))
        }
        .await;
        match result {
            Ok(output) => {
                tx.commit().await?;
                Ok(output)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    pub(crate) async fn delete_exact(&self, key: &IdentitySpecKey) -> Result<bool, AppError> {
        let mut tx = self.db.begin_serializable().await?;
        let result = async {
            require_scope_workspace(&mut tx, key.scope()).await?;
            if tx.identity_specs().get(key).await?.is_none() {
                return Ok(false);
            }
            let dependent_count = tx.identities().count_dependents(key).await?;
            if dependent_count > 0 {
                return Err(AppError::FailedPrecondition(format!(
                    "identity spec '{}' in scope '{}' has {dependent_count} stored identity references and cannot be deleted",
                    key.name(),
                    scope_label(key.scope()),
                )));
            }
            #[cfg(test)]
            self.wait_before_lifecycle_write().await;
            tx.identity_specs().delete(key).await.map_err(Into::into)
        }
        .await;
        match result {
            Ok(true) => {
                tx.commit().await?;
                Ok(true)
            }
            Ok(false) => {
                tx.rollback().await?;
                Ok(false)
            }
            Err(error) => {
                tx.rollback().await?;
                Err(error)
            }
        }
    }

    #[cfg(test)]
    pub(crate) async fn load_exact(
        &self,
        key: &IdentitySpecKey,
    ) -> Result<IdentitySpecMutationSnapshot, AppError> {
        let mut tx = self.db.begin_read_snapshot().await?;
        require_scope_workspace(&mut tx, key.scope()).await?;
        let snapshot = load_mutation_snapshot(&mut tx, key).await?;
        tx.commit().await?;
        Ok(snapshot)
    }

    #[cfg(test)]
    async fn wait_before_lifecycle_write(&self) {
        if let Some(gate) = &self.before_lifecycle_write
            && !gate.used.swap(true, Ordering::SeqCst)
        {
            gate.barrier.wait().await;
        }
    }
}

fn scope_label(scope: &IdentitySpecScope) -> String {
    match scope {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    }
}

async fn require_equivalent_dependents(
    tx: &mut CoralTx<'_>,
    key: &IdentitySpecKey,
    fingerprint: &str,
) -> Result<(), AppError> {
    let dependent_count = tx.identities().count_dependents(key).await?;
    if dependent_count == 0 {
        return Ok(());
    }
    let equivalent_count = tx
        .identities()
        .count_exact_dependents(key, fingerprint)
        .await?;
    if equivalent_count == dependent_count {
        return Ok(());
    }
    Err(AppError::FailedPrecondition(format!(
        "identity spec '{}' in scope '{}' has {dependent_count} stored identity references and may only be installed or replaced with a semantically equivalent manifest",
        key.name(),
        scope_label(key.scope()),
    )))
}

async fn load_mutation_snapshot(
    tx: &mut CoralTx<'_>,
    key: &IdentitySpecKey,
) -> Result<IdentitySpecMutationSnapshot, AppError> {
    let record = tx.identity_specs().get(key).await?;
    let document = match record.as_ref() {
        Some(record) => tx.identity_spec_documents().get(&record.id).await?,
        None => None,
    };
    Ok(IdentitySpecMutationSnapshot { record, document })
}

async fn require_scope_workspace(
    tx: &mut CoralTx<'_>,
    scope: &IdentitySpecScope,
) -> Result<(), AppError> {
    if let IdentitySpecScope::Workspace(workspace) = scope
        && tx.workspaces().get(workspace.as_str()).await?.is_none()
    {
        return Err(AppError::WorkspaceNotFound(workspace.to_string()));
    }
    Ok(())
}
