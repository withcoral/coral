//! Persistence for management concurrency metadata and accepted request identities.
//! Callers supply the owning domain transaction.
use std::collections::HashMap;

use coral_db::{Transaction, UnixNanos};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

use crate::list_repository::{ResourceScope, Resources};
use crate::{Error, LifecycleTargetKind, ResourceMetadata, UserId, WorkspaceId};

#[derive(Iden)]
enum WorkspaceResourceMetadata {
    Table,
    WorkspaceId,
    Kind,
    TargetId,
    Etag,
    CreatedAt,
    UpdatedAt,
}

#[derive(Iden)]
enum WorkspaceManagementRequests {
    Table,
    UserId,
    RequestId,
    OperationDigest,
    GrantResult,
}

/// Records what the claimed `request` of `user` granted, on the claim's row.
pub(super) async fn record_email_grant(
    tx: &mut Transaction<'_>,
    user: &UserId,
    request: &crate::RequestId,
    workspace: &WorkspaceId,
    email: &str,
    target: &UserId,
    role: crate::WorkspaceRole,
) -> Result<(), Error> {
    use base64::Engine as _;
    let operation = serde_json::json!([
        "grant_by_email",
        workspace.to_string(),
        email.trim().to_ascii_lowercase(),
        role.as_storage_str()
    ])
    .to_string();
    let digest = ring::digest::digest(&ring::digest::SHA256, operation.as_bytes());
    let result = serde_json::json!([
        workspace.to_string(),
        target.as_str(),
        role.as_storage_str()
    ])
    .to_string();
    tx.execute(
        Query::update()
            .table(WorkspaceManagementRequests::Table)
            .values([
                (
                    WorkspaceManagementRequests::OperationDigest,
                    base64::engine::general_purpose::STANDARD
                        .encode(digest.as_ref())
                        .into(),
                ),
                (WorkspaceManagementRequests::GrantResult, result.into()),
            ])
            .and_where(Expr::col(WorkspaceManagementRequests::UserId).eq(user.as_str()))
            .and_where(Expr::col(WorkspaceManagementRequests::RequestId).eq(request.to_string()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn metadata(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    kind: LifecycleTargetKind,
    id: &str,
) -> Result<ResourceMetadata, Error> {
    let row: Option<(String, UnixNanos, UnixNanos)> = tx
        .fetch_optional(
            Query::select()
                .columns([
                    WorkspaceResourceMetadata::Etag,
                    WorkspaceResourceMetadata::CreatedAt,
                    WorkspaceResourceMetadata::UpdatedAt,
                ])
                .from(WorkspaceResourceMetadata::Table)
                .and_where(
                    Expr::col(WorkspaceResourceMetadata::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(WorkspaceResourceMetadata::Kind).eq(kind.as_storage_str()))
                .and_where(Expr::col(WorkspaceResourceMetadata::TargetId).eq(id))
                .to_owned(),
        )
        .await?;
    let (etag, created_at, updated_at) = row.ok_or_else(missing_metadata)?;
    Ok(ResourceMetadata {
        etag,
        created_at,
        updated_at,
    })
}

/// The etag that the metadata of one resource holds. Coral writes only UUIDs
/// there, so other text is corrupt.
pub(super) async fn metadata_etag(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    kind: LifecycleTargetKind,
    id: &str,
) -> Result<uuid::Uuid, Error> {
    let stored = metadata(tx, workspace, kind, id).await?.etag;
    uuid::Uuid::parse_str(&stored)
        .map_err(|_error| Error::CorruptData("resource metadata has an invalid etag".into()))
}

fn missing_metadata() -> Error {
    Error::CorruptData("resource metadata missing".into())
}

/// The metadata of the resources of one list, by resource id.
pub(super) struct ListedMetadata(HashMap<String, ResourceMetadata>);

impl ListedMetadata {
    /// The metadata of one resource of the list, as [`metadata`] reads it.
    pub(super) fn of(&self, id: &str) -> Result<ResourceMetadata, Error> {
        self.0.get(id).cloned().ok_or_else(missing_metadata)
    }
}

/// Reads the metadata of every resource of one list in one statement.
pub(super) async fn listed_metadata(
    tx: &mut Transaction<'_>,
    resources: Resources<'_>,
) -> Result<ListedMetadata, Error> {
    let mut statement = Query::select();
    statement
        .columns([
            WorkspaceResourceMetadata::TargetId,
            WorkspaceResourceMetadata::Etag,
            WorkspaceResourceMetadata::CreatedAt,
            WorkspaceResourceMetadata::UpdatedAt,
        ])
        .from(WorkspaceResourceMetadata::Table)
        .and_where(
            Expr::col(WorkspaceResourceMetadata::Kind).eq(resources.kind().as_storage_str()),
        );
    match resources.scope() {
        ResourceScope::Within(workspace, ids) => {
            statement
                .and_where(
                    Expr::col(WorkspaceResourceMetadata::WorkspaceId).eq(workspace.to_string()),
                )
                .and_where(Expr::col(WorkspaceResourceMetadata::TargetId).in_subquery(ids));
        }
        // A Workspace's own metadata has the Workspace as its target.
        ResourceScope::Workspaces(ids) => {
            statement
                .and_where(Expr::col(WorkspaceResourceMetadata::WorkspaceId).in_subquery(ids))
                .and_where(
                    Expr::col(WorkspaceResourceMetadata::TargetId)
                        .equals(WorkspaceResourceMetadata::WorkspaceId),
                );
        }
    }
    let rows: Vec<(String, String, UnixNanos, UnixNanos)> = tx.fetch_all(statement).await?;
    Ok(ListedMetadata(
        rows.into_iter()
            .map(|(id, etag, created_at, updated_at)| {
                (
                    id,
                    ResourceMetadata {
                        etag,
                        created_at,
                        updated_at,
                    },
                )
            })
            .collect(),
    ))
}

pub(super) async fn upsert_metadata(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    kind: LifecycleTargetKind,
    id: &str,
    etag: &str,
    occurred_at: UnixNanos,
) -> Result<(), Error> {
    tx.execute(
        Query::insert()
            .into_table(WorkspaceResourceMetadata::Table)
            .columns([
                WorkspaceResourceMetadata::WorkspaceId,
                WorkspaceResourceMetadata::Kind,
                WorkspaceResourceMetadata::TargetId,
                WorkspaceResourceMetadata::Etag,
                WorkspaceResourceMetadata::CreatedAt,
                WorkspaceResourceMetadata::UpdatedAt,
            ])
            .values_panic([
                workspace.to_string().into(),
                kind.as_storage_str().into(),
                id.into(),
                etag.into(),
                occurred_at.into(),
                occurred_at.into(),
            ])
            .on_conflict(
                OnConflict::columns([
                    WorkspaceResourceMetadata::WorkspaceId,
                    WorkspaceResourceMetadata::Kind,
                    WorkspaceResourceMetadata::TargetId,
                ])
                .update_columns([
                    WorkspaceResourceMetadata::Etag,
                    WorkspaceResourceMetadata::UpdatedAt,
                ])
                .to_owned(),
            )
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn insert_request(
    tx: &mut Transaction<'_>,
    user: &UserId,
    request: &crate::RequestId,
) -> Result<bool, Error> {
    let inserted = tx
        .execute(
            Query::insert()
                .into_table(WorkspaceManagementRequests::Table)
                .columns([
                    WorkspaceManagementRequests::UserId,
                    WorkspaceManagementRequests::RequestId,
                ])
                .values_panic([user.as_str().into(), request.to_string().into()])
                .on_conflict(
                    OnConflict::columns([
                        WorkspaceManagementRequests::UserId,
                        WorkspaceManagementRequests::RequestId,
                    ])
                    .do_nothing()
                    .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(inserted != 0)
}
