//! Persistence for Secret Store grants. Callers supply the owning domain
//! transaction, which holds the Workspace for a write.
use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Order, Query};

use crate::{Error, WorkspaceId};

#[derive(Iden)]
enum WorkspaceSecretStoreGrants {
    Table,
    WorkspaceId,
    StoreName,
}

/// Whether this write created the grant.
pub(super) async fn insert(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    store: &str,
) -> Result<bool, Error> {
    let rows = tx
        .execute_change(
            Query::insert()
                .into_table(WorkspaceSecretStoreGrants::Table)
                .columns([
                    WorkspaceSecretStoreGrants::WorkspaceId,
                    WorkspaceSecretStoreGrants::StoreName,
                ])
                .values_panic([workspace.to_string().into(), store.into()])
                .on_conflict(
                    OnConflict::columns([
                        WorkspaceSecretStoreGrants::WorkspaceId,
                        WorkspaceSecretStoreGrants::StoreName,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(rows == 1)
}

/// Whether this write removed a grant.
pub(super) async fn delete(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    store: &str,
) -> Result<bool, Error> {
    let rows = tx
        .execute_change(
            Query::delete()
                .from_table(WorkspaceSecretStoreGrants::Table)
                .and_where(
                    Expr::col(WorkspaceSecretStoreGrants::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(WorkspaceSecretStoreGrants::StoreName).eq(store))
                .to_owned(),
        )
        .await?;
    Ok(rows == 1)
}

/// The stores granted to one Workspace, ordered by name.
pub(super) async fn list_for_workspace(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
) -> Result<Vec<String>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(WorkspaceSecretStoreGrants::StoreName)
                .from(WorkspaceSecretStoreGrants::Table)
                .and_where(
                    Expr::col(WorkspaceSecretStoreGrants::WorkspaceId).eq(workspace.to_string()),
                )
                .order_by(WorkspaceSecretStoreGrants::StoreName, Order::Asc)
                .to_owned(),
        )
        .await?;
    Ok(rows.into_iter().map(|(store,)| store).collect())
}

/// Every grant as a Workspace identity and a store, ordered by store.
pub(super) async fn list_all(
    tx: &mut Transaction<'_>,
) -> Result<Vec<(WorkspaceId, String)>, Error> {
    let rows: Vec<(String, String)> = tx
        .fetch_all(
            Query::select()
                .columns([
                    WorkspaceSecretStoreGrants::WorkspaceId,
                    WorkspaceSecretStoreGrants::StoreName,
                ])
                .from(WorkspaceSecretStoreGrants::Table)
                .order_by(WorkspaceSecretStoreGrants::StoreName, Order::Asc)
                .order_by(WorkspaceSecretStoreGrants::WorkspaceId, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(workspace, store)| Ok((WorkspaceId::parse(&workspace)?, store)))
        .collect()
}
