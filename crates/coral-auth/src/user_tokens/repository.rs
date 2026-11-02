//! Refresh Grant persistence. Grants are deployment-level OAuth state: a
//! presented refresh token finds its grant by the token's digest, because the
//! token is a secret rather than an ID. Removal facets match the Workspace and
//! User they retire.
//!
//! Nothing observes these rows, so writes use `execute`, not `execute_change`:
//! a grant changing must not wake activity observers or Run waits.

use coral_db::{Database, Transaction};
use coral_workspace::{UserId, WorkspaceId};
use sea_query::{Expr, ExprTrait as _, Iden, Order, Query};

use super::{Scope, TokenRefusal};

#[derive(Iden)]
enum Grants {
    #[iden = "oauth_refresh_grants"]
    Table,
    GrantId,
    UserId,
    ClientId,
    Resource,
    PrivateAudience,
    WorkspaceId,
    CreatedAtUnixNanos,
    ExpiresAtUnixNanos,
    Rotations,
}

#[derive(Iden)]
enum Tokens {
    #[iden = "oauth_refresh_tokens"]
    Table,
    TokenDigest,
    GrantId,
    ConsumedAtUnixNanos,
}

/// A grant as stored, before the domain checks what it binds.
pub(super) struct StoredGrant {
    pub(super) grant_id: String,
    pub(super) user_id: String,
    pub(super) client_id: String,
    pub(super) resource: String,
    pub(super) private_audience: Option<String>,
    pub(super) workspace_id: Option<String>,
    pub(super) expires_at_unix_nanos: i64,
}

/// A presented refresh token's grant, and whether the token was spent.
pub(super) struct PresentedToken {
    pub(super) grant: StoredGrant,
    pub(super) consumed: bool,
}

#[derive(sqlx::FromRow)]
struct PresentedRow {
    grant_id: String,
    user_id: String,
    client_id: String,
    resource: String,
    private_audience: Option<String>,
    workspace_id: Option<String>,
    expires_at_unix_nanos: i64,
    consumed_at_unix_nanos: Option<i64>,
}

const fn unavailable(_error: &coral_db::Error) -> TokenRefusal {
    TokenRefusal::Unavailable
}

/// Reads the grant a token digest belongs to, without holding a writer.
pub(super) async fn find(
    database: &Database,
    token_digest: &str,
) -> Result<Option<PresentedToken>, TokenRefusal> {
    let row: Option<PresentedRow> = database
        .fetch_optional(
            Query::select()
                .columns([
                    (Grants::Table, Grants::GrantId),
                    (Grants::Table, Grants::UserId),
                    (Grants::Table, Grants::ClientId),
                    (Grants::Table, Grants::Resource),
                    (Grants::Table, Grants::PrivateAudience),
                    (Grants::Table, Grants::WorkspaceId),
                    (Grants::Table, Grants::ExpiresAtUnixNanos),
                ])
                .column((Tokens::Table, Tokens::ConsumedAtUnixNanos))
                .from(Tokens::Table)
                .inner_join(
                    Grants::Table,
                    Expr::col((Tokens::Table, Tokens::GrantId))
                        .equals((Grants::Table, Grants::GrantId)),
                )
                .and_where(Expr::col((Tokens::Table, Tokens::TokenDigest)).eq(token_digest))
                .to_owned(),
        )
        .await
        .map_err(|error| unavailable(&error))?;
    Ok(row.map(|row| PresentedToken {
        consumed: row.consumed_at_unix_nanos.is_some(),
        grant: StoredGrant {
            grant_id: row.grant_id,
            user_id: row.user_id,
            client_id: row.client_id,
            resource: row.resource,
            private_audience: row.private_audience,
            workspace_id: row.workspace_id,
            expires_at_unix_nanos: row.expires_at_unix_nanos,
        },
    }))
}

pub(super) async fn insert_grant(
    tx: &mut Transaction<'_>,
    grant_id: &str,
    scope: &Scope,
    created_at_unix_nanos: i64,
    expires_at_unix_nanos: i64,
) -> Result<(), TokenRefusal> {
    tx.execute(
        Query::insert()
            .into_table(Grants::Table)
            .columns([
                Grants::GrantId,
                Grants::UserId,
                Grants::ClientId,
                Grants::Resource,
                Grants::PrivateAudience,
                Grants::WorkspaceId,
                Grants::CreatedAtUnixNanos,
                Grants::ExpiresAtUnixNanos,
                Grants::Rotations,
            ])
            .values_panic([
                grant_id.into(),
                scope.user_id.as_str().into(),
                scope.client_id.clone().into(),
                scope.resource.clone().into(),
                scope
                    .private_audience
                    .map(|audience| audience.as_str().to_owned())
                    .into(),
                scope.workspace_id.as_ref().map(ToString::to_string).into(),
                created_at_unix_nanos.into(),
                expires_at_unix_nanos.into(),
                0_i64.into(),
            ])
            .to_owned(),
    )
    .await
    .map_err(|error| unavailable(&error))?;
    Ok(())
}

pub(super) async fn insert_token(
    tx: &mut Transaction<'_>,
    token_digest: &str,
    grant_id: &str,
) -> Result<(), TokenRefusal> {
    tx.execute(
        Query::insert()
            .into_table(Tokens::Table)
            .columns([Tokens::TokenDigest, Tokens::GrantId])
            .values_panic([token_digest.into(), grant_id.into()])
            .to_owned(),
    )
    .await
    .map_err(|error| unavailable(&error))?;
    Ok(())
}

/// Holds an unexpired grant's row until the commit; reports whether it exists.
pub(super) async fn hold_live_grant(
    tx: &mut Transaction<'_>,
    grant_id: &str,
    now_unix_nanos: i64,
) -> Result<bool, TokenRefusal> {
    let rows = tx
        .execute(
            Query::update()
                .table(Grants::Table)
                .value(Grants::Rotations, Expr::col(Grants::Rotations))
                .and_where(Expr::col(Grants::GrantId).eq(grant_id))
                .and_where(Expr::col(Grants::ExpiresAtUnixNanos).gt(now_unix_nanos))
                .to_owned(),
        )
        .await
        .map_err(|error| unavailable(&error))?;
    Ok(rows == 1)
}

/// Spends a current token; reports whether this call spent it.
pub(super) async fn consume(
    tx: &mut Transaction<'_>,
    token_digest: &str,
    now_unix_nanos: i64,
) -> Result<bool, TokenRefusal> {
    let rows = tx
        .execute(
            Query::update()
                .table(Tokens::Table)
                .value(Tokens::ConsumedAtUnixNanos, now_unix_nanos)
                .and_where(Expr::col(Tokens::TokenDigest).eq(token_digest))
                .and_where(Expr::col(Tokens::ConsumedAtUnixNanos).is_null())
                .to_owned(),
        )
        .await
        .map_err(|error| unavailable(&error))?;
    Ok(rows == 1)
}

/// Counts one rotation unless the grant already rotated `limit` times.
pub(super) async fn record_rotation(
    tx: &mut Transaction<'_>,
    grant_id: &str,
    limit: i64,
) -> Result<bool, TokenRefusal> {
    let rows = tx
        .execute(
            Query::update()
                .table(Grants::Table)
                .value(Grants::Rotations, Expr::col(Grants::Rotations).add(1))
                .and_where(Expr::col(Grants::GrantId).eq(grant_id))
                .and_where(Expr::col(Grants::Rotations).lt(limit))
                .to_owned(),
        )
        .await
        .map_err(|error| unavailable(&error))?;
    Ok(rows == 1)
}

/// Deletes a grant with every token it issued.
pub(super) async fn delete_grant(
    tx: &mut Transaction<'_>,
    grant_id: &str,
) -> Result<(), TokenRefusal> {
    tx.execute(
        Query::delete()
            .from_table(Grants::Table)
            .and_where(Expr::col(Grants::GrantId).eq(grant_id))
            .to_owned(),
    )
    .await
    .map_err(|error| unavailable(&error))?;
    Ok(())
}

/// Deletes every grant that can no longer refresh, with its tokens.
pub(super) async fn purge_expired(
    tx: &mut Transaction<'_>,
    now_unix_nanos: i64,
) -> Result<(), TokenRefusal> {
    tx.execute(
        Query::delete()
            .from_table(Grants::Table)
            .and_where(Expr::col(Grants::ExpiresAtUnixNanos).lte(now_unix_nanos))
            .to_owned(),
    )
    .await
    .map_err(|error| unavailable(&error))?;
    Ok(())
}

/// Deletes a User's oldest grants until at most `keep` remain.
pub(super) async fn evict_oldest(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
    keep: u64,
) -> Result<(), TokenRefusal> {
    let grants: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(Grants::GrantId)
                .from(Grants::Table)
                .and_where(Expr::col(Grants::UserId).eq(user_id.as_str()))
                .order_by(Grants::CreatedAtUnixNanos, Order::Desc)
                .order_by(Grants::GrantId, Order::Desc)
                .to_owned(),
        )
        .await
        .map_err(|error| unavailable(&error))?;
    let keep = usize::try_from(keep).unwrap_or(usize::MAX);
    let evicted: Vec<String> = grants.into_iter().skip(keep).map(|(id,)| id).collect();
    if evicted.is_empty() {
        return Ok(());
    }
    tx.execute(
        Query::delete()
            .from_table(Grants::Table)
            .and_where(Expr::col(Grants::GrantId).is_in(evicted))
            .to_owned(),
    )
    .await
    .map_err(|error| unavailable(&error))?;
    Ok(())
}

/// Deletes every grant a User holds.
pub(super) async fn delete_user_grants(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<(), coral_db::Error> {
    tx.execute(
        Query::delete()
            .from_table(Grants::Table)
            .and_where(Expr::col(Grants::UserId).eq(user_id.as_str()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

/// Deletes the grants a User holds that are scoped to one Workspace.
pub(super) async fn delete_workspace_grants(
    tx: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
) -> Result<(), coral_db::Error> {
    tx.execute(
        Query::delete()
            .from_table(Grants::Table)
            .and_where(Expr::col(Grants::WorkspaceId).eq(workspace_id.to_string()))
            .and_where(Expr::col(Grants::UserId).eq(user_id.as_str()))
            .to_owned(),
    )
    .await?;
    Ok(())
}
