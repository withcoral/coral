//! Transactional persistence for workspace creation and deletion.

#![cfg_attr(not(test), expect(dead_code, reason = "used higher in the PR stack"))]

use sea_query::{Expr, ExprTrait, OnConflict, Query};

use super::schema::{Users, Workspaces};
use super::{CoralDb, CoralTx, DbError, DbRepos, DbSession};
use crate::workspaces::MemberRole;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum WorkspaceCreationOutcome {
    Created,
    AlreadyExists,
    UserNotFound,
}

pub(crate) struct WorkspaceDeletion<'a> {
    tx: CoralTx<'a>,
}

impl CoralDb {
    pub(crate) async fn create_workspace_with_owner(
        &self,
        workspace_id: &str,
        creator_user_id: &str,
        created_at_unix_nanos: i64,
    ) -> Result<WorkspaceCreationOutcome, DbError> {
        let mut tx = self.begin().await?;
        if !hold_user_for_workspace_creation(&mut tx, creator_user_id).await? {
            tx.rollback().await?;
            return Ok(WorkspaceCreationOutcome::UserNotFound);
        }
        if !try_create_workspace_with_owner(
            &mut tx,
            workspace_id,
            creator_user_id,
            created_at_unix_nanos,
        )
        .await?
        {
            tx.rollback().await?;
            return Ok(WorkspaceCreationOutcome::AlreadyExists);
        }
        tx.commit().await?;
        Ok(WorkspaceCreationOutcome::Created)
    }

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

pub(super) async fn hold_user_for_workspace_creation(
    tx: &mut CoralTx<'_>,
    user_id: &str,
) -> Result<bool, DbError> {
    let statement = Query::update()
        .table(Users::Table)
        .value(
            Users::LastLoginAtUnixNanos,
            Expr::col(Users::LastLoginAtUnixNanos),
        )
        .and_where(Expr::col(Users::UserId).eq(user_id))
        .to_owned();
    Ok(DbSession::execute_rows_affected(tx, statement).await? == 1)
}

pub(super) async fn try_create_workspace_with_owner(
    tx: &mut CoralTx<'_>,
    workspace_id: &str,
    creator_user_id: &str,
    created_at_unix_nanos: i64,
) -> Result<bool, DbError> {
    let statement = Query::insert()
        .into_table(Workspaces::Table)
        .columns([Workspaces::Id, Workspaces::CreatedAtUnixNanos])
        .values_panic([
            Expr::val(workspace_id.to_string()),
            Expr::val(created_at_unix_nanos),
        ])
        .on_conflict(OnConflict::column(Workspaces::Id).do_nothing().to_owned())
        .to_owned();
    if DbSession::execute_rows_affected(tx, statement).await? == 0 {
        return Ok(false);
    }
    tx.workspace_members()
        .insert(
            workspace_id,
            creator_user_id,
            MemberRole::Owner,
            created_at_unix_nanos,
        )
        .await?;
    Ok(true)
}

impl WorkspaceDeletion<'_> {
    pub(crate) async fn commit(self) -> Result<(), DbError> {
        self.tx.commit().await
    }

    pub(crate) async fn rollback(self) -> Result<(), DbError> {
        self.tx.rollback().await
    }
}
