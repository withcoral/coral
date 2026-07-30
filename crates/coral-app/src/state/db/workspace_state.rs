//! Transactional persistence for workspace deletion.

use super::{CoralDb, CoralTx, DbError, DbRepos};

pub(crate) struct WorkspaceDeletion<'a> {
    tx: CoralTx<'a>,
}

impl CoralDb {
    pub(crate) async fn begin_workspace_deletion(
        &self,
        workspace_id: &str,
    ) -> Result<Option<WorkspaceDeletion<'_>>, DbError> {
        let mut tx = self.begin().await?;
        if tx.workspaces().delete(workspace_id).await? {
            Ok(Some(WorkspaceDeletion { tx }))
        } else {
            tx.rollback().await?;
            Ok(None)
        }
    }
}

impl WorkspaceDeletion<'_> {
    pub(crate) async fn commit(self) -> Result<(), DbError> {
        self.tx.commit().await
    }

    pub(crate) async fn rollback(self) -> Result<(), DbError> {
        self.tx.rollback().await
    }
}
