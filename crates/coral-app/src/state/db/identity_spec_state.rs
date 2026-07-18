use std::future::Future;

use coral_spec::IdentityManifest;

use super::{
    CoralDb, CoralTx, DbRepos, IdentitySpecDocumentRecord, IdentitySpecKey, IdentitySpecRecord,
    IdentitySpecScope, now_unix_nanos_i64,
};
use crate::bootstrap::AppError;
use crate::encrypted_document::EncryptedEnvelopeDocument;

pub(crate) struct IdentitySpecState<'a> {
    db: &'a CoralDb,
}

#[derive(PartialEq, Eq)]
pub(crate) struct IdentitySpecMutationSnapshot {
    pub(crate) record: Option<IdentitySpecRecord>,
    pub(crate) document: Option<IdentitySpecDocumentRecord>,
}

impl CoralDb {
    pub(crate) fn identity_spec_state(&self) -> IdentitySpecState<'_> {
        IdentitySpecState { db: self }
    }
}

impl IdentitySpecState<'_> {
    pub(crate) async fn add_or_replace_exact<F, Fut>(
        &self,
        key: &IdentitySpecKey,
        manifest: &IdentityManifest,
        manifest_yaml: &str,
        prepare_document: F,
    ) -> Result<(IdentitySpecRecord, bool), AppError>
    where
        F: FnOnce(IdentitySpecMutationSnapshot) -> Fut,
        Fut: Future<Output = Result<Option<EncryptedEnvelopeDocument>, AppError>>,
    {
        let mut tx = self.db.begin_serializable().await?;
        let result = async {
            require_scope_workspace(&mut tx, key.scope()).await?;
            let snapshot = load_mutation_snapshot(&mut tx, key).await?;
            let replaced = snapshot.record.is_some();
            let document = prepare_document(snapshot).await?;
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
}

fn scope_label(scope: &IdentitySpecScope) -> String {
    match scope {
        IdentitySpecScope::Global => "global".to_string(),
        IdentitySpecScope::Workspace(workspace) => format!("workspace:{workspace}"),
    }
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
