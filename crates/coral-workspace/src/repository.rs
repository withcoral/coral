use sea_query::{Alias, Condition, Expr, ExprTrait as _, Iden, OnConflict, Order, Query};
use sqlx::FromRow;

use coral_db::{Database, QueryExecutor, Transaction};

use crate::{
    AdministrativeLifecycleEvent, AuthoritySource, Error, LifecycleAction, LifecycleOutcome,
    LifecycleTargetKind, TraceId, UserId, Workspace, WorkspaceDisplayName, WorkspaceId,
    WorkspaceMembership, WorkspaceRole, WorkspaceState,
};

#[derive(Iden)]
enum WorkspaceRecords {
    Table,
    Id,
    DisplayName,
    State,
    LifecycleVersion,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
enum WorkspaceMemberships {
    Table,
    WorkspaceId,
    UserId,
    Role,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
enum WorkspaceLifecycleEvents {
    Table,
    Id,
    WorkspaceId,
    ActorUserId,
    AuthoritySource,
    Action,
    TargetKind,
    TargetId,
    Outcome,
    ChangedFieldPaths,
    OccurredAtUnixNanos,
    TraceId,
}

#[derive(Debug, FromRow)]
struct AuthorityRow {
    id: String,
    display_name: String,
    state: String,
    lifecycle_version: i64,
    membership_role: Option<String>,
}

impl AuthorityRow {
    fn decode(self) -> Result<(Workspace, Option<WorkspaceRole>), Error> {
        let workspace_id = WorkspaceId::parse(&self.id).map_err(|_parse_error| {
            Error::CorruptData(format!("Workspace row has invalid identity '{}'", self.id))
        })?;
        let display_name =
            WorkspaceDisplayName::parse(&self.display_name).map_err(|_parse_error| {
                Error::CorruptData(format!(
                    "Workspace '{workspace_id}' has an invalid display name"
                ))
            })?;
        let state = WorkspaceState::from_storage_str(&self.state)?;
        if self.lifecycle_version <= 0 {
            return Err(Error::CorruptData(format!(
                "Workspace '{workspace_id}' has nonpositive lifecycle version"
            )));
        }
        let role = self
            .membership_role
            .as_deref()
            .map(WorkspaceRole::from_storage_str)
            .transpose()?;
        Ok((
            Workspace::new(workspace_id, display_name, state, self.lifecycle_version),
            role,
        ))
    }
}

#[derive(Debug, FromRow)]
struct WorkspaceRow {
    id: String,
    display_name: String,
    state: String,
    lifecycle_version: i64,
}

impl WorkspaceRow {
    fn decode(self) -> Result<Workspace, Error> {
        AuthorityRow {
            id: self.id,
            display_name: self.display_name,
            state: self.state,
            lifecycle_version: self.lifecycle_version,
            membership_role: None,
        }
        .decode()
        .map(|(workspace, _)| workspace)
    }
}

#[derive(Debug, FromRow)]
struct MembershipRow {
    workspace_id: String,
    user_id: String,
    role: String,
}

impl MembershipRow {
    fn decode(self) -> Result<WorkspaceMembership, Error> {
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_parse_error| {
            Error::CorruptData(format!(
                "Workspace Membership row has invalid Workspace identity '{}'",
                self.workspace_id
            ))
        })?;
        let user_id = UserId::parse(&self.user_id).map_err(|_parse_error| {
            Error::CorruptData("Workspace Membership row has invalid User identity".to_string())
        })?;
        let role = WorkspaceRole::from_storage_str(&self.role)?;
        Ok(WorkspaceMembership::new(workspace_id, user_id, role))
    }
}

#[derive(Debug, FromRow)]
struct LifecycleEventRow {
    workspace_id: String,
    actor_user_id: String,
    authority_source: String,
    action: String,
    target_kind: String,
    target_id: String,
    outcome: String,
    changed_field_paths: String,
    occurred_at_unix_nanos: i64,
    trace_id: String,
}

impl LifecycleEventRow {
    fn decode(self) -> Result<AdministrativeLifecycleEvent, Error> {
        let workspace_id = WorkspaceId::parse(&self.workspace_id).map_err(|_parse_error| {
            Error::CorruptData("Lifecycle Event has invalid Workspace identity".to_string())
        })?;
        let actor_user_id = UserId::parse(&self.actor_user_id).map_err(|_parse_error| {
            Error::CorruptData("Lifecycle Event has invalid actor User identity".to_string())
        })?;
        let authority_source = AuthoritySource::from_storage_str(&self.authority_source)?;
        let action = LifecycleAction::from_storage_str(&self.action)?;
        let target_kind = LifecycleTargetKind::from_storage_str(&self.target_kind)?;
        let outcome = LifecycleOutcome::from_storage_str(&self.outcome)?;
        let changed_field_paths: Vec<String> = serde_json::from_str(&self.changed_field_paths)?;
        let trace_id = TraceId::parse(&self.trace_id).map_err(|_parse_error| {
            Error::CorruptData("Lifecycle Event has invalid trace id".to_string())
        })?;
        Ok(AdministrativeLifecycleEvent::new(
            workspace_id,
            actor_user_id,
            authority_source,
            action,
            target_kind,
            self.target_id,
            outcome,
            changed_field_paths,
            self.occurred_at_unix_nanos,
            trace_id,
        ))
    }
}

pub(super) async fn find_workspace(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Option<Workspace>, Error> {
    let row: Option<WorkspaceRow> = executor
        .fetch_optional(workspace_statement(workspace_id))
        .await?;
    row.map(WorkspaceRow::decode).transpose()
}

fn workspace_statement(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .columns([
            WorkspaceRecords::Id,
            WorkspaceRecords::DisplayName,
            WorkspaceRecords::State,
            WorkspaceRecords::LifecycleVersion,
        ])
        .from(WorkspaceRecords::Table)
        .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
        .to_owned()
}

pub(super) async fn authority(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
) -> Result<Option<(Workspace, Option<WorkspaceRole>)>, Error> {
    let row: Option<AuthorityRow> = executor
        .fetch_optional(authority_statement(workspace_id, user_id))
        .await?;
    row.map(AuthorityRow::decode).transpose()
}

fn authority_statement(workspace_id: &WorkspaceId, user_id: &UserId) -> sea_query::SelectStatement {
    Query::select()
        .columns([
            (WorkspaceRecords::Table, WorkspaceRecords::Id),
            (WorkspaceRecords::Table, WorkspaceRecords::DisplayName),
            (WorkspaceRecords::Table, WorkspaceRecords::State),
            (WorkspaceRecords::Table, WorkspaceRecords::LifecycleVersion),
        ])
        .expr_as(
            Expr::col((WorkspaceMemberships::Table, WorkspaceMemberships::Role)),
            Alias::new("membership_role"),
        )
        .from(WorkspaceRecords::Table)
        .join(
            sea_query::JoinType::LeftJoin,
            WorkspaceMemberships::Table,
            Condition::all()
                .add(
                    Expr::col((
                        WorkspaceMemberships::Table,
                        WorkspaceMemberships::WorkspaceId,
                    ))
                    .equals((WorkspaceRecords::Table, WorkspaceRecords::Id)),
                )
                .add(
                    Expr::col((WorkspaceMemberships::Table, WorkspaceMemberships::UserId))
                        .eq(user_id.as_str()),
                ),
        )
        .and_where(
            Expr::col((WorkspaceRecords::Table, WorkspaceRecords::Id)).eq(workspace_id.to_string()),
        )
        .to_owned()
}

/// Holds the Workspace row shared for the rest of the transaction: decisions
/// that read it proceed side by side, and the Workspace, Membership and
/// Agent lifecycle writers, which hold it exclusively, wait for them.
pub(super) async fn share_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<bool, Error> {
    let held: Option<(i64,)> = transaction
        .fetch_optional_locked(
            Query::select()
                .column(WorkspaceRecords::LifecycleVersion)
                .from(WorkspaceRecords::Table)
                .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
                .to_owned(),
            coral_db::RowLock::Shared,
        )
        .await?;
    Ok(held.is_some())
}

pub(super) async fn hold_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<bool, Error> {
    let statement = Query::update()
        .table(WorkspaceRecords::Table)
        .value(
            WorkspaceRecords::LifecycleVersion,
            Expr::col(WorkspaceRecords::LifecycleVersion),
        )
        .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(super) async fn insert_workspace(
    transaction: &mut Transaction<'_>,
    workspace: &Workspace,
    occurred_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(WorkspaceRecords::Table)
        .columns([
            WorkspaceRecords::Id,
            WorkspaceRecords::DisplayName,
            WorkspaceRecords::State,
            WorkspaceRecords::LifecycleVersion,
            WorkspaceRecords::CreatedAtUnixNanos,
            WorkspaceRecords::UpdatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(workspace.id().to_string()),
            Expr::val(workspace.display_name().as_str()),
            Expr::val(workspace.state().as_storage_str()),
            Expr::val(workspace.lifecycle_version()),
            Expr::val(occurred_at_unix_nanos),
            Expr::val(occurred_at_unix_nanos),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn update_display_name(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    display_name: &WorkspaceDisplayName,
    updated_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(WorkspaceRecords::Table)
        .values([
            (
                WorkspaceRecords::DisplayName,
                Expr::val(display_name.as_str()),
            ),
            (
                WorkspaceRecords::UpdatedAtUnixNanos,
                Expr::val(updated_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn update_lifecycle(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    state: WorkspaceState,
    lifecycle_version: i64,
    updated_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(WorkspaceRecords::Table)
        .values([
            (WorkspaceRecords::State, Expr::val(state.as_storage_str())),
            (
                WorkspaceRecords::LifecycleVersion,
                Expr::val(lifecycle_version),
            ),
            (
                WorkspaceRecords::UpdatedAtUnixNanos,
                Expr::val(updated_at_unix_nanos),
            ),
        ])
        .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn membership_role_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
) -> Result<Option<WorkspaceRole>, Error> {
    let statement = Query::select()
        .column(WorkspaceMemberships::Role)
        .from(WorkspaceMemberships::Table)
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
        .to_owned();
    let stored: Option<(String,)> = transaction.fetch_optional(statement).await?;
    stored
        .map(|(role,)| WorkspaceRole::from_storage_str(&role))
        .transpose()
}

pub(super) async fn upsert_membership(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
    role: WorkspaceRole,
    occurred_at_unix_nanos: i64,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(WorkspaceMemberships::Table)
        .columns([
            WorkspaceMemberships::WorkspaceId,
            WorkspaceMemberships::UserId,
            WorkspaceMemberships::Role,
            WorkspaceMemberships::CreatedAtUnixNanos,
            WorkspaceMemberships::UpdatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(workspace_id.to_string()),
            Expr::val(user_id.as_str()),
            Expr::val(role.as_storage_str()),
            Expr::val(occurred_at_unix_nanos),
            Expr::val(occurred_at_unix_nanos),
        ])
        .on_conflict(
            OnConflict::columns([
                WorkspaceMemberships::WorkspaceId,
                WorkspaceMemberships::UserId,
            ])
            .update_columns([
                WorkspaceMemberships::Role,
                WorkspaceMemberships::UpdatedAtUnixNanos,
            ])
            .to_owned(),
        )
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

pub(super) async fn delete_membership(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
) -> Result<bool, Error> {
    let statement = Query::delete()
        .from_table(WorkspaceMemberships::Table)
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
        .to_owned();
    Ok(transaction.execute(statement).await? == 1)
}

pub(super) async fn list_memberships(
    database: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Vec<WorkspaceMembership>, Error> {
    let statement = Query::select()
        .columns([
            WorkspaceMemberships::WorkspaceId,
            WorkspaceMemberships::UserId,
            WorkspaceMemberships::Role,
        ])
        .from(WorkspaceMemberships::Table)
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned();
    let rows: Vec<MembershipRow> = database.fetch_all(statement).await?;
    let mut memberships = rows
        .into_iter()
        .map(MembershipRow::decode)
        .collect::<Result<Vec<_>, Error>>()?;
    memberships.sort_by(|left, right| left.user_id().cmp(right.user_id()));
    Ok(memberships)
}

/// Each Workspace where the User holds a Membership, with that Membership's role.
pub(super) async fn list_workspaces(
    database: &Database,
    actor_user_id: &UserId,
) -> Result<Vec<(Workspace, WorkspaceRole)>, Error> {
    let mut statement = Query::select();
    statement
        .columns([
            (WorkspaceRecords::Table, WorkspaceRecords::Id),
            (WorkspaceRecords::Table, WorkspaceRecords::DisplayName),
            (WorkspaceRecords::Table, WorkspaceRecords::State),
            (WorkspaceRecords::Table, WorkspaceRecords::LifecycleVersion),
        ])
        .expr_as(
            Expr::col((WorkspaceMemberships::Table, WorkspaceMemberships::Role)),
            Alias::new("membership_role"),
        )
        .from(WorkspaceRecords::Table)
        .inner_join(
            WorkspaceMemberships::Table,
            Expr::col((
                WorkspaceMemberships::Table,
                WorkspaceMemberships::WorkspaceId,
            ))
            .equals((WorkspaceRecords::Table, WorkspaceRecords::Id)),
        )
        .and_where(
            Expr::col((WorkspaceMemberships::Table, WorkspaceMemberships::UserId))
                .eq(actor_user_id.as_str()),
        );
    let rows: Vec<AuthorityRow> = database.fetch_all(statement).await?;
    let mut workspaces = rows
        .into_iter()
        .map(|row| {
            let (workspace, role) = row.decode()?;
            let role = role.ok_or_else(|| {
                Error::CorruptData("Workspace Membership row has no role".to_string())
            })?;
            Ok((workspace, role))
        })
        .collect::<Result<Vec<_>, Error>>()?;
    workspaces.sort_by_cached_key(|(workspace, _)| workspace.listing_key());
    Ok(workspaces)
}

/// Designated Owners of one Workspace, whatever their Organization Membership state.
/// The caller holds the Workspace, so the count cannot change before its write.
pub(super) async fn owner_count(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<usize, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(WorkspaceMemberships::UserId)
                .from(WorkspaceMemberships::Table)
                .and_where(
                    Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()),
                )
                .and_where(
                    Expr::col(WorkspaceMemberships::Role).eq(WorkspaceRole::Owner.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(rows.len())
}

/// Workspaces where this User is the only designated Owner, in stable order.
pub(super) async fn sole_owner_workspace_ids(
    transaction: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<Vec<WorkspaceId>, Error> {
    let mut sole = Vec::new();
    for workspace in membership_workspace_ids(transaction, user_id).await? {
        if membership_role_in_transaction(transaction, &workspace, user_id).await?
            == Some(WorkspaceRole::Owner)
            && owner_count(transaction, &workspace).await? == 1
        {
            sole.push(workspace);
        }
    }
    Ok(sole)
}

/// Every Workspace in the Organization, in stable order.
pub(super) async fn all_workspaces(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<Workspace>, Error> {
    let rows: Vec<WorkspaceRow> = transaction
        .fetch_all(
            Query::select()
                .columns([
                    WorkspaceRecords::Id,
                    WorkspaceRecords::DisplayName,
                    WorkspaceRecords::State,
                    WorkspaceRecords::LifecycleVersion,
                ])
                .from(WorkspaceRecords::Table)
                .order_by(WorkspaceRecords::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter().map(WorkspaceRow::decode).collect()
}

/// Every Workspace identity, in any state, in stable order.
pub(super) async fn workspace_ids(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<WorkspaceId>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(WorkspaceRecords::Id)
                .from(WorkspaceRecords::Table)
                .order_by(WorkspaceRecords::Id, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(value,)| {
            WorkspaceId::parse(&value)
                .map_err(|_error| Error::CorruptData("invalid Workspace id".into()))
        })
        .collect()
}

/// Workspace identities where one User holds a designated Membership, in stable order.
pub(super) async fn membership_workspace_ids(
    transaction: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<Vec<WorkspaceId>, Error> {
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(WorkspaceMemberships::WorkspaceId)
                .from(WorkspaceMemberships::Table)
                .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
                .order_by(WorkspaceMemberships::WorkspaceId, Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(value,)| {
            WorkspaceId::parse(&value)
                .map_err(|_error| Error::CorruptData("invalid Workspace Membership id".into()))
        })
        .collect()
}

pub(super) struct NewLifecycleEvent<'a> {
    pub(super) workspace_id: &'a WorkspaceId,
    pub(super) actor_user_id: &'a UserId,
    pub(super) authority_source: AuthoritySource,
    pub(super) action: LifecycleAction,
    pub(super) target_kind: LifecycleTargetKind,
    pub(super) target_id: &'a str,
    pub(super) changed_field_paths: &'a [&'a str],
    pub(super) occurred_at_unix_nanos: i64,
    pub(super) trace_id: &'a TraceId,
}

pub(super) async fn insert_lifecycle_event(
    transaction: &mut Transaction<'_>,
    event: NewLifecycleEvent<'_>,
) -> Result<(), Error> {
    let fields = serde_json::to_string(event.changed_field_paths)?;
    let statement = Query::insert()
        .into_table(WorkspaceLifecycleEvents::Table)
        .columns([
            WorkspaceLifecycleEvents::Id,
            WorkspaceLifecycleEvents::WorkspaceId,
            WorkspaceLifecycleEvents::ActorUserId,
            WorkspaceLifecycleEvents::AuthoritySource,
            WorkspaceLifecycleEvents::Action,
            WorkspaceLifecycleEvents::TargetKind,
            WorkspaceLifecycleEvents::TargetId,
            WorkspaceLifecycleEvents::Outcome,
            WorkspaceLifecycleEvents::ChangedFieldPaths,
            WorkspaceLifecycleEvents::OccurredAtUnixNanos,
            WorkspaceLifecycleEvents::TraceId,
        ])
        .values_panic([
            Expr::val(uuid::Uuid::new_v4().hyphenated().to_string()),
            Expr::val(event.workspace_id.to_string()),
            Expr::val(event.actor_user_id.as_str()),
            Expr::val(event.authority_source.as_storage_str()),
            Expr::val(event.action.as_storage_str()),
            Expr::val(event.target_kind.as_storage_str()),
            Expr::val(event.target_id),
            Expr::val(LifecycleOutcome::Accepted.as_storage_str()),
            Expr::val(fields),
            Expr::val(event.occurred_at_unix_nanos),
            Expr::val(event.trace_id.as_str()),
        ])
        .to_owned();
    transaction.execute_change(statement).await?;
    Ok(())
}

pub(super) async fn list_lifecycle_events(
    database: &Database,
    workspace_id: &WorkspaceId,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    let statement = Query::select()
        .columns([
            WorkspaceLifecycleEvents::WorkspaceId,
            WorkspaceLifecycleEvents::ActorUserId,
            WorkspaceLifecycleEvents::AuthoritySource,
            WorkspaceLifecycleEvents::Action,
            WorkspaceLifecycleEvents::TargetKind,
            WorkspaceLifecycleEvents::TargetId,
            WorkspaceLifecycleEvents::Outcome,
            WorkspaceLifecycleEvents::ChangedFieldPaths,
            WorkspaceLifecycleEvents::OccurredAtUnixNanos,
            WorkspaceLifecycleEvents::TraceId,
        ])
        .from(WorkspaceLifecycleEvents::Table)
        .and_where(Expr::col(WorkspaceLifecycleEvents::WorkspaceId).eq(workspace_id.to_string()))
        .order_by(WorkspaceLifecycleEvents::OccurredAtUnixNanos, Order::Asc)
        .order_by(WorkspaceLifecycleEvents::Id, Order::Asc)
        .to_owned();
    let rows: Vec<LifecycleEventRow> = database.fetch_all(statement).await?;
    rows.into_iter().map(LifecycleEventRow::decode).collect()
}

/// Events that Organization Owner governance authorized, in every Workspace.
/// Databases from before Membership-only authority also hold that source on
/// what an Organization Owner did inside a Workspace, so the actions are named.
pub(super) async fn list_governance_events(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<AdministrativeLifecycleEvent>, Error> {
    let statement = Query::select()
        .columns([
            WorkspaceLifecycleEvents::WorkspaceId,
            WorkspaceLifecycleEvents::ActorUserId,
            WorkspaceLifecycleEvents::AuthoritySource,
            WorkspaceLifecycleEvents::Action,
            WorkspaceLifecycleEvents::TargetKind,
            WorkspaceLifecycleEvents::TargetId,
            WorkspaceLifecycleEvents::Outcome,
            WorkspaceLifecycleEvents::ChangedFieldPaths,
            WorkspaceLifecycleEvents::OccurredAtUnixNanos,
            WorkspaceLifecycleEvents::TraceId,
        ])
        .from(WorkspaceLifecycleEvents::Table)
        .and_where(
            Expr::col(WorkspaceLifecycleEvents::AuthoritySource)
                .eq(AuthoritySource::OrganizationOwner.as_storage_str()),
        )
        .and_where(
            Expr::col(WorkspaceLifecycleEvents::Action).is_in(
                [
                    LifecycleAction::WorkspaceArchived,
                    LifecycleAction::WorkspaceRestored,
                    LifecycleAction::MembershipCreated,
                    LifecycleAction::MembershipRoleUpdated,
                    LifecycleAction::MembershipRemoved,
                ]
                .map(LifecycleAction::as_storage_str),
            ),
        )
        .order_by(WorkspaceLifecycleEvents::OccurredAtUnixNanos, Order::Asc)
        .order_by(WorkspaceLifecycleEvents::Id, Order::Asc)
        .to_owned();
    let rows: Vec<LifecycleEventRow> = transaction.fetch_all(statement).await?;
    rows.into_iter().map(LifecycleEventRow::decode).collect()
}

pub(super) async fn all_workspace_ids(
    tx: &mut coral_db::Transaction<'_>,
) -> Result<Vec<WorkspaceId>, Error> {
    let rows: Vec<(String,)> = tx
        .fetch_all(
            Query::select()
                .column(WorkspaceRecords::Id)
                .from(WorkspaceRecords::Table)
                .order_by(WorkspaceRecords::Id, sea_query::Order::Asc)
                .to_owned(),
        )
        .await?;
    rows.into_iter()
        .map(|(id,)| WorkspaceId::parse(&id))
        .collect()
}
