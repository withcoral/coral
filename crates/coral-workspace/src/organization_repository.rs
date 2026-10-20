//! Organization persistence mechanics. All operations join the caller's transaction;
//! authority policy, sequencing, and commit/rollback belong to `OrganizationModule`.

use coral_db::Transaction;
use sea_query::{Expr, ExprTrait as _, Iden, OnConflict, Query};

use super::{OrganizationLifecycleAction, OrganizationLifecycleEvent, OrganizationMembership};
use crate::{ChangeContext, Error, OrganizationRole, UserId};

#[derive(Iden)]
enum OrganizationAuthorityRecords {
    Table,
    UserId,
    Revision,
    Role,
    Enabled,
    Removed,
}
#[derive(Iden)]
enum OrganizationAuthorityGuard {
    Table,
    Id,
}
#[derive(Iden)]
enum OrganizationLifecycleEvents {
    Table,
    Id,
    ActorUserId,
    TargetUserId,
    Action,
    OccurredAtUnixNanos,
    TraceId,
}

pub(super) async fn insert_membership_if_absent(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
    role: OrganizationRole,
) -> Result<bool, Error> {
    let inserted = tx
        .execute(
            Query::insert()
                .into_table(OrganizationAuthorityRecords::Table)
                .columns([
                    OrganizationAuthorityRecords::UserId,
                    OrganizationAuthorityRecords::Revision,
                    OrganizationAuthorityRecords::Role,
                    OrganizationAuthorityRecords::Enabled,
                ])
                .values_panic([
                    user_id.as_str().into(),
                    1_i64.into(),
                    role_str(role).into(),
                    1_i64.into(),
                ])
                .on_conflict(
                    OnConflict::column(OrganizationAuthorityRecords::UserId)
                        .do_nothing()
                        .to_owned(),
                )
                .to_owned(),
        )
        .await?;
    Ok(inserted == 1)
}

pub(super) async fn removed_revision(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<Option<u64>, Error> {
    let row: Option<(i64,)> = tx
        .fetch_optional(
            Query::select()
                .column(OrganizationAuthorityRecords::Revision)
                .from(OrganizationAuthorityRecords::Table)
                .and_where(Expr::col(OrganizationAuthorityRecords::UserId).eq(user_id.as_str()))
                .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(1_i64))
                .to_owned(),
        )
        .await?;
    row.map(|(revision,)| decode_revision(revision)).transpose()
}

pub(super) async fn rejoin_membership(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
    role: OrganizationRole,
    revision: i64,
) -> Result<(), Error> {
    let updated = tx
        .execute(
            Query::update()
                .table(OrganizationAuthorityRecords::Table)
                .value(OrganizationAuthorityRecords::Role, role_str(role))
                .value(OrganizationAuthorityRecords::Enabled, 1_i64)
                .value(OrganizationAuthorityRecords::Removed, 0_i64)
                .value(OrganizationAuthorityRecords::Revision, revision)
                .and_where(Expr::col(OrganizationAuthorityRecords::UserId).eq(user_id.as_str()))
                .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(1_i64))
                .and_where(Expr::col(OrganizationAuthorityRecords::Revision).eq(revision - 1))
                .to_owned(),
        )
        .await?;
    if updated != 1 {
        return Err(Error::OrganizationAuthorityConflict);
    }
    Ok(())
}

pub(super) async fn hold_administration(tx: &mut Transaction<'_>) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(OrganizationAuthorityGuard::Table)
            .value(OrganizationAuthorityGuard::Id, 1_i64)
            .and_where(Expr::col(OrganizationAuthorityGuard::Id).eq(1_i64))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn hold_user(tx: &mut Transaction<'_>, user_id: &UserId) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(OrganizationAuthorityRecords::Table)
            .value(
                OrganizationAuthorityRecords::Revision,
                Expr::col(OrganizationAuthorityRecords::Revision),
            )
            .and_where(Expr::col(OrganizationAuthorityRecords::UserId).eq(user_id.as_str()))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn find_membership(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<Option<OrganizationMembership>, Error> {
    find_membership_locked(tx, user_id, coral_db::RowLock::None).await
}

/// Reads the User's Organization authority row holding it as `lock` says: a
/// shared hold lets decisions read it side by side while role or disablement
/// administration, which writes it, waits for them.
pub(super) async fn find_membership_locked(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
    lock: coral_db::RowLock,
) -> Result<Option<OrganizationMembership>, Error> {
    let row: Option<(i64, String, i64)> = tx
        .fetch_optional_locked(
            Query::select()
                .columns([
                    OrganizationAuthorityRecords::Revision,
                    OrganizationAuthorityRecords::Role,
                    OrganizationAuthorityRecords::Enabled,
                ])
                .from(OrganizationAuthorityRecords::Table)
                .and_where(Expr::col(OrganizationAuthorityRecords::UserId).eq(user_id.as_str()))
                .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(0_i64))
                .to_owned(),
            lock,
        )
        .await?;
    let Some((revision, role, enabled)) = row else {
        return Ok(None);
    };
    let role = match role.as_str() {
        "owner" => OrganizationRole::Owner,
        "member" => OrganizationRole::Member,
        _ => return Err(Error::CorruptData("invalid Organization role".into())),
    };
    Ok(Some(OrganizationMembership {
        user_id: user_id.clone(),
        role,
        enabled: enabled == 1,
        revision: decode_revision(revision)?,
    }))
}

fn decode_revision(revision: i64) -> Result<u64, Error> {
    u64::try_from(revision)
        .ok()
        .filter(|revision| *revision > 0)
        .ok_or_else(|| Error::CorruptData("invalid Organization authority revision".into()))
}

pub(super) async fn current_members(tx: &mut Transaction<'_>) -> Result<Vec<UserId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(OrganizationAuthorityRecords::UserId)
                .from(OrganizationAuthorityRecords::Table)
                .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(0_i64))
                .order_by(OrganizationAuthorityRecords::UserId, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(|(id,)| UserId::parse(&id)).collect()
}

pub(super) async fn enabled_owner_count(tx: &mut Transaction<'_>) -> Result<i64, Error> {
    let (count,): (i64,) = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(OrganizationAuthorityRecords::UserId).count())
                .from(OrganizationAuthorityRecords::Table)
                .and_where(Expr::col(OrganizationAuthorityRecords::Role).eq("owner"))
                .and_where(Expr::col(OrganizationAuthorityRecords::Enabled).eq(1_i64))
                .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(0_i64))
                .to_owned(),
        )
        .await?
        .ok_or_else(|| Error::CorruptData("Organization Owner count is missing".into()))?;
    Ok(count)
}

pub(super) async fn update_membership(
    tx: &mut Transaction<'_>,
    user_id: &UserId,
    role: OrganizationRole,
    enabled: bool,
    revision: i64,
) -> Result<(), Error> {
    tx.execute(
        Query::update()
            .table(OrganizationAuthorityRecords::Table)
            .value(OrganizationAuthorityRecords::Role, role_str(role))
            .value(OrganizationAuthorityRecords::Enabled, i64::from(enabled))
            .value(OrganizationAuthorityRecords::Revision, revision)
            .and_where(Expr::col(OrganizationAuthorityRecords::UserId).eq(user_id.as_str()))
            .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(0_i64))
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn record_event(
    tx: &mut Transaction<'_>,
    context: &ChangeContext,
    target: &UserId,
    action: OrganizationLifecycleAction,
) -> Result<(), Error> {
    tx.execute_change(
        Query::insert()
            .into_table(OrganizationLifecycleEvents::Table)
            .columns([
                OrganizationLifecycleEvents::Id,
                OrganizationLifecycleEvents::ActorUserId,
                OrganizationLifecycleEvents::TargetUserId,
                OrganizationLifecycleEvents::Action,
                OrganizationLifecycleEvents::OccurredAtUnixNanos,
                OrganizationLifecycleEvents::TraceId,
            ])
            .values_panic([
                uuid::Uuid::new_v4().to_string().into(),
                context.actor().user_id().as_str().into(),
                target.as_str().into(),
                action.as_str().into(),
                context.occurred_at_unix_nanos().into(),
                context.trace_id().as_str().into(),
            ])
            .to_owned(),
    )
    .await?;
    Ok(())
}

pub(super) async fn list_lifecycle_events(
    tx: &mut Transaction<'_>,
) -> Result<Vec<OrganizationLifecycleEvent>, Error> {
    let events: Vec<EventRow> = tx
        .fetch_all(
            Query::select()
                .columns([
                    OrganizationLifecycleEvents::Id,
                    OrganizationLifecycleEvents::ActorUserId,
                    OrganizationLifecycleEvents::TargetUserId,
                    OrganizationLifecycleEvents::Action,
                    OrganizationLifecycleEvents::OccurredAtUnixNanos,
                    OrganizationLifecycleEvents::TraceId,
                ])
                .from(OrganizationLifecycleEvents::Table)
                .order_by(
                    OrganizationLifecycleEvents::OccurredAtUnixNanos,
                    sea_query::Order::Asc,
                )
                .order_by(OrganizationLifecycleEvents::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    events.into_iter().map(EventRow::into_event).collect()
}

const fn role_str(role: OrganizationRole) -> &'static str {
    match role {
        OrganizationRole::Owner => "owner",
        OrganizationRole::Member => "member",
    }
}

#[derive(sqlx::FromRow)]
struct EventRow {
    id: String,
    actor_user_id: String,
    target_user_id: String,
    action: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}
impl EventRow {
    fn into_event(self) -> Result<OrganizationLifecycleEvent, Error> {
        let corrupt = || Error::CorruptData("invalid Organization lifecycle evidence".into());
        let action = match self.action.as_str() {
            "organization_membership.created" => OrganizationLifecycleAction::Created,
            "organization_membership.role_updated" => OrganizationLifecycleAction::RoleUpdated,
            "organization_membership.removed" => OrganizationLifecycleAction::Removed,
            "organization_membership.state_updated" => OrganizationLifecycleAction::StateUpdated,
            _ => return Err(corrupt()),
        };
        Ok(OrganizationLifecycleEvent {
            id: uuid::Uuid::parse_str(&self.id).map_err(|_error| corrupt())?,
            actor_user_id: UserId::parse(&self.actor_user_id).map_err(|_error| corrupt())?,
            target_user_id: UserId::parse(&self.target_user_id).map_err(|_error| corrupt())?,
            action,
            outcome: crate::LifecycleOutcome::Accepted,
            occurred_at_unix_nanos: self.occurred_at_unix_nanos,
            trace_id: crate::TraceId::parse(&self.trace_id).map_err(|_error| corrupt())?,
        })
    }
}

pub(super) async fn remove_membership(
    tx: &mut Transaction<'_>,
    user: &UserId,
    revision: i64,
) -> Result<(), Error> {
    let updated = tx
        .execute(
            Query::update()
                .table(OrganizationAuthorityRecords::Table)
                .value(OrganizationAuthorityRecords::Removed, 1_i64)
                .value(OrganizationAuthorityRecords::Revision, revision)
                .and_where(Expr::col(OrganizationAuthorityRecords::UserId).eq(user.as_str()))
                .and_where(Expr::col(OrganizationAuthorityRecords::Removed).eq(0_i64))
                .and_where(Expr::col(OrganizationAuthorityRecords::Revision).eq(revision - 1))
                .to_owned(),
        )
        .await?;
    if updated != 1 {
        return Err(Error::OrganizationAuthorityConflict);
    }
    Ok(())
}
