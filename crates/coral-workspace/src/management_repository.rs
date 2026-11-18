//! Persistence for management concurrency metadata and accepted request identities.
//! Callers supply the owning domain transaction.
//!
//! A resource's own row holds its version and its times, and the repository
//! of that row reads them. This Module only decodes them, and still writes
//! the table that held them before, for a Coral that reads it.
use coral_db::{Transaction, UnixNanos, Version};
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

use crate::{Error, LifecycleTargetKind, ResourceMetadata, UserId, WorkspaceId};

/// The table that held each resource's etag and times before its own row
/// did. Coral writes it and does not read it.
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

/// The concurrency metadata that a resource's own row holds. A version that
/// is not positive is corrupt. A time is absent on the row of an Agent or a
/// Registration that a Coral from before the row kept its times created.
pub(crate) fn row_metadata(
    version: i64,
    created_at: Option<UnixNanos>,
    updated_at: Option<UnixNanos>,
) -> Result<ResourceMetadata, Error> {
    let version = Version::new(version)
        .ok_or_else(|| Error::CorruptData("resource row has nonpositive version".to_string()))?;
    Ok(ResourceMetadata {
        version,
        created_at,
        updated_at,
    })
}

/// The outcome of a write that names the version its caller read of the row:
/// it changes that one row, or the row is no longer at that version.
pub(crate) fn changed_at_version(rows: u64) -> Result<(), Error> {
    if rows == 1 {
        Ok(())
    } else {
        Err(Error::EtagMismatch)
    }
}

/// Writes the row that a Coral which still reads this table shows and checks
/// a resource by: a new etag and the time of the change. This Coral reads
/// neither.
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
