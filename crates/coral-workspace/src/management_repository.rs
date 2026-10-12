//! Persistence for management concurrency metadata and accepted request identities.
//! Callers supply the owning domain transaction.
use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

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

pub(super) async fn record_email_grant(
    tx: &mut Transaction<'_>,
    context: &crate::ChangeContext,
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
            .and_where(
                Expr::col(WorkspaceManagementRequests::UserId)
                    .eq(context.actor().user_id().as_str()),
            )
            .and_where(
                Expr::col(WorkspaceManagementRequests::RequestId).eq(context
                    .request_id()
                    .ok_or_else(|| Error::InvalidInput("request identity required".into()))?),
            )
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
    let row: Option<(String, i64, i64)> = tx
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
    let (etag, created_at, updated_at) =
        row.ok_or_else(|| Error::CorruptData("resource metadata missing".into()))?;
    Ok(ResourceMetadata {
        etag,
        created_at,
        updated_at,
    })
}

pub(super) async fn upsert_metadata(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    kind: LifecycleTargetKind,
    id: &str,
    etag: &str,
    occurred_at_unix_nanos: i64,
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
                occurred_at_unix_nanos.into(),
                occurred_at_unix_nanos.into(),
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
    request_id: &str,
) -> Result<bool, Error> {
    let inserted = tx
        .execute(
            Query::insert()
                .into_table(WorkspaceManagementRequests::Table)
                .columns([
                    WorkspaceManagementRequests::UserId,
                    WorkspaceManagementRequests::RequestId,
                ])
                .values_panic([user.as_str().into(), request_id.into()])
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
