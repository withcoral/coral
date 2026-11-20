use sea_query::{Alias, Condition, Expr, ExprTrait as _, Iden, Order, Query};
use sqlx::FromRow;

use coral_db::{Database, QueryExecutor, Transaction, UnixNanos, Version};

use crate::management_repository::{changed_at_version, row_metadata};
use crate::{
    AdministrativeLifecycleEvent, AuthoritySource, Error, LifecycleAction, LifecycleOutcome,
    LifecycleTargetKind, ResourceMetadata, TraceId, UserId, Workspace, WorkspaceDisplayName,
    WorkspaceId, WorkspaceMembership, WorkspaceRole, WorkspaceState,
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
    Version,
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
    created_at_unix_nanos: UnixNanos,
    updated_at_unix_nanos: UnixNanos,
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
        let version = Version::new(self.lifecycle_version).ok_or_else(|| {
            Error::CorruptData(format!(
                "Workspace '{workspace_id}' has nonpositive lifecycle version"
            ))
        })?;
        let role = self
            .membership_role
            .as_deref()
            .map(WorkspaceRole::from_storage_str)
            .transpose()?;
        Ok((
            Workspace::new(
                workspace_id,
                display_name,
                state,
                version,
                self.created_at_unix_nanos,
                self.updated_at_unix_nanos,
            ),
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
    created_at_unix_nanos: UnixNanos,
    updated_at_unix_nanos: UnixNanos,
}

impl WorkspaceRow {
    fn decode(self) -> Result<Workspace, Error> {
        AuthorityRow {
            id: self.id,
            display_name: self.display_name,
            state: self.state,
            lifecycle_version: self.lifecycle_version,
            created_at_unix_nanos: self.created_at_unix_nanos,
            updated_at_unix_nanos: self.updated_at_unix_nanos,
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
    version: i64,
    created_at_unix_nanos: UnixNanos,
    updated_at_unix_nanos: UnixNanos,
}

impl MembershipRow {
    fn decode(self) -> Result<(WorkspaceMembership, ResourceMetadata), Error> {
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
        let metadata = row_metadata(
            self.version,
            Some(self.created_at_unix_nanos),
            Some(self.updated_at_unix_nanos),
        )?;
        Ok((
            WorkspaceMembership::new(workspace_id, user_id, role),
            metadata,
        ))
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
    occurred_at_unix_nanos: UnixNanos,
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

/// The columns a [`WorkspaceRow`] reads.
const WORKSPACE_COLUMNS: [WorkspaceRecords; 6] = [
    WorkspaceRecords::Id,
    WorkspaceRecords::DisplayName,
    WorkspaceRecords::State,
    WorkspaceRecords::LifecycleVersion,
    WorkspaceRecords::CreatedAtUnixNanos,
    WorkspaceRecords::UpdatedAtUnixNanos,
];

fn workspace_statement(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .columns(WORKSPACE_COLUMNS)
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
        .columns(WORKSPACE_COLUMNS.map(|column| (WorkspaceRecords::Table, column)))
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
    workspace_held(transaction, workspace_id, coral_db::RowLock::Shared).await
}

/// Holds the Workspace row exclusively for the rest of the transaction, as
/// the Workspace, Membership and Agent lifecycle writers do.
pub(super) async fn hold_workspace(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
) -> Result<bool, Error> {
    workspace_held(transaction, workspace_id, coral_db::RowLock::Exclusive).await
}

/// Whether the Workspace exists, holding its row as `lock` says.
async fn workspace_held(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    lock: coral_db::RowLock,
) -> Result<bool, Error> {
    let held: Option<(i64,)> = transaction
        .fetch_optional_locked(
            Query::select()
                .column(WorkspaceRecords::LifecycleVersion)
                .from(WorkspaceRecords::Table)
                .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
                .to_owned(),
            lock,
        )
        .await?;
    Ok(held.is_some())
}

pub(super) async fn insert_workspace(
    transaction: &mut Transaction<'_>,
    workspace: &Workspace,
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
            Expr::val(workspace.version().get()),
            Expr::val(workspace.created_at()),
            Expr::val(workspace.updated_at()),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// Renames the Workspace that is at version `expected`, and raises its
/// version by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Workspace that is not at `expected`.
pub(super) async fn update_display_name(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    display_name: &WorkspaceDisplayName,
    expected: Version,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(WorkspaceRecords::Table)
        .values([
            (
                WorkspaceRecords::DisplayName,
                Expr::val(display_name.as_str()),
            ),
            (
                WorkspaceRecords::LifecycleVersion,
                Expr::col(WorkspaceRecords::LifecycleVersion).add(1_i64),
            ),
            (WorkspaceRecords::UpdatedAtUnixNanos, Expr::val(updated_at)),
        ])
        .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceRecords::LifecycleVersion).eq(expected.get()))
        .to_owned();
    changed_at_version(transaction.execute(statement).await?)
}

/// Moves the Workspace that is at version `expected` to `state`, and raises
/// its version by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Workspace that is not at `expected`.
pub(super) async fn update_lifecycle(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    state: WorkspaceState,
    expected: Version,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(WorkspaceRecords::Table)
        .values([
            (WorkspaceRecords::State, Expr::val(state.as_storage_str())),
            (
                WorkspaceRecords::LifecycleVersion,
                Expr::col(WorkspaceRecords::LifecycleVersion).add(1_i64),
            ),
            (WorkspaceRecords::UpdatedAtUnixNanos, Expr::val(updated_at)),
        ])
        .and_where(Expr::col(WorkspaceRecords::Id).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceRecords::LifecycleVersion).eq(expected.get()))
        .to_owned();
    changed_at_version(transaction.execute(statement).await?)
}

/// The role one User holds in one Workspace, with the concurrency metadata of
/// that Membership, or `None` when the User holds none there.
pub(super) async fn membership_in_transaction(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
) -> Result<Option<(WorkspaceRole, ResourceMetadata)>, Error> {
    let statement = memberships()
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
        .to_owned();
    let row: Option<MembershipRow> = transaction.fetch_optional(statement).await?;
    row.map(|row| {
        let (membership, metadata) = row.decode()?;
        Ok((membership.role(), metadata))
    })
    .transpose()
}

/// Writes the role a User takes in a Workspace. The caller holds the
/// Workspace and passes the version of the Membership it read there, or
/// `None` where it found none. A Membership that was absent is created at the
/// first version. One that was read has its role changed and its version
/// raised by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Membership that is not at the
/// version read, or that is gone.
pub(super) async fn write_membership_role(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
    role: WorkspaceRole,
    read: Option<Version>,
    occurred_at: UnixNanos,
) -> Result<(), Error> {
    match read {
        None => insert_membership(transaction, workspace_id, user_id, role, occurred_at).await,
        Some(expected) => {
            update_membership_role(
                transaction,
                workspace_id,
                user_id,
                role,
                expected,
                occurred_at,
            )
            .await
        }
    }
}

/// Creates the Membership at the first version. The caller holds the
/// Workspace and has found that the User holds no Membership there.
pub(super) async fn insert_membership(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
    role: WorkspaceRole,
    occurred_at: UnixNanos,
) -> Result<(), Error> {
    let statement = Query::insert()
        .into_table(WorkspaceMemberships::Table)
        .columns([
            WorkspaceMemberships::WorkspaceId,
            WorkspaceMemberships::UserId,
            WorkspaceMemberships::Role,
            WorkspaceMemberships::Version,
            WorkspaceMemberships::CreatedAtUnixNanos,
            WorkspaceMemberships::UpdatedAtUnixNanos,
        ])
        .values_panic([
            Expr::val(workspace_id.to_string()),
            Expr::val(user_id.as_str()),
            Expr::val(role.as_storage_str()),
            Expr::val(Version::FIRST.get()),
            Expr::val(occurred_at),
            Expr::val(occurred_at),
        ])
        .to_owned();
    transaction.execute(statement).await?;
    Ok(())
}

/// Changes the role of the Membership that is at version `expected`, and
/// raises its version by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Membership that is not at
/// `expected`, or that is gone.
async fn update_membership_role(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
    role: WorkspaceRole,
    expected: Version,
    occurred_at: UnixNanos,
) -> Result<(), Error> {
    let statement = Query::update()
        .table(WorkspaceMemberships::Table)
        .values([
            (WorkspaceMemberships::Role, Expr::val(role.as_storage_str())),
            (
                WorkspaceMemberships::Version,
                Expr::col(WorkspaceMemberships::Version).add(1_i64),
            ),
            (
                WorkspaceMemberships::UpdatedAtUnixNanos,
                Expr::val(occurred_at),
            ),
        ])
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
        .and_where(Expr::col(WorkspaceMemberships::Version).eq(expected.get()))
        .to_owned();
    changed_at_version(transaction.execute(statement).await?)
}

/// Removes the Membership the User holds in the Workspace, at whatever
/// version it is. Returns whether the User held one.
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

/// Removes the Membership that is at version `expected`.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Membership that is not at
/// `expected`, or that is gone.
pub(super) async fn delete_membership_at_version(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    user_id: &UserId,
    expected: Version,
) -> Result<(), Error> {
    let statement = Query::delete()
        .from_table(WorkspaceMemberships::Table)
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
        .and_where(Expr::col(WorkspaceMemberships::Version).eq(expected.get()))
        .to_owned();
    changed_at_version(transaction.execute(statement).await?)
}

/// The Memberships of one Workspace, each with its concurrency metadata,
/// ordered by User.
pub(super) async fn list_memberships(
    database: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Vec<(WorkspaceMembership, ResourceMetadata)>, Error> {
    let statement = memberships()
        .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned();
    fetch_memberships(database, statement).await
}

/// The designated Owner Memberships of one Workspace, or of every Workspace,
/// ordered by User.
pub(super) async fn list_owner_memberships(
    database: impl QueryExecutor,
    workspace_id: Option<&WorkspaceId>,
) -> Result<Vec<WorkspaceMembership>, Error> {
    let mut statement = memberships();
    statement
        .and_where(Expr::col(WorkspaceMemberships::Role).eq(WorkspaceRole::Owner.as_storage_str()));
    if let Some(workspace_id) = workspace_id {
        statement
            .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()));
    }
    Ok(fetch_memberships(database, statement)
        .await?
        .into_iter()
        .map(|(membership, _metadata)| membership)
        .collect())
}

fn memberships() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            WorkspaceMemberships::WorkspaceId,
            WorkspaceMemberships::UserId,
            WorkspaceMemberships::Role,
            WorkspaceMemberships::Version,
            WorkspaceMemberships::CreatedAtUnixNanos,
            WorkspaceMemberships::UpdatedAtUnixNanos,
        ])
        .from(WorkspaceMemberships::Table)
        .to_owned()
}

async fn fetch_memberships(
    database: impl QueryExecutor,
    statement: sea_query::SelectStatement,
) -> Result<Vec<(WorkspaceMembership, ResourceMetadata)>, Error> {
    let rows: Vec<MembershipRow> = database.fetch_all(statement).await?;
    let mut memberships = rows
        .into_iter()
        .map(MembershipRow::decode)
        .collect::<Result<Vec<_>, Error>>()?;
    memberships.sort_by(|(left, _), (right, _)| left.user_id().cmp(right.user_id()));
    Ok(memberships)
}

/// Selects the User ids of Memberships, in one column: of one Workspace or of
/// every Workspace, and of one role or of every role.
pub(super) fn membership_user_ids(
    workspace_id: Option<&WorkspaceId>,
    role: Option<WorkspaceRole>,
) -> sea_query::SelectStatement {
    let mut statement = Query::select();
    statement
        .column(WorkspaceMemberships::UserId)
        .from(WorkspaceMemberships::Table);
    if let Some(workspace_id) = workspace_id {
        statement
            .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).eq(workspace_id.to_string()));
    }
    if let Some(role) = role {
        statement.and_where(Expr::col(WorkspaceMemberships::Role).eq(role.as_storage_str()));
    }
    statement
}

/// Each Workspace where the User holds a Membership, with that Membership's role.
pub(super) async fn list_workspaces(
    database: impl QueryExecutor,
    actor_user_id: &UserId,
) -> Result<Vec<(Workspace, WorkspaceRole)>, Error> {
    let mut statement = Query::select();
    statement
        .columns(WORKSPACE_COLUMNS.map(|column| (WorkspaceRecords::Table, column)))
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
    let count: Option<(i64,)> = transaction
        .fetch_optional(
            Query::select()
                .expr(Expr::col(WorkspaceMemberships::UserId).count())
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
    Ok(count.map_or(0, |(count,)| usize::try_from(count).unwrap_or_default()))
}

/// Workspaces where this User is the only designated Owner, in stable order.
pub(super) async fn sole_owner_workspace_ids(
    transaction: &mut Transaction<'_>,
    user_id: &UserId,
) -> Result<Vec<WorkspaceId>, Error> {
    // Of the Workspaces the User is a designated Owner of, those with one
    // designated Owner: that Owner is the User.
    let owned = Query::select()
        .column(WorkspaceMemberships::WorkspaceId)
        .from(WorkspaceMemberships::Table)
        .and_where(Expr::col(WorkspaceMemberships::UserId).eq(user_id.as_str()))
        .and_where(Expr::col(WorkspaceMemberships::Role).eq(WorkspaceRole::Owner.as_storage_str()))
        .to_owned();
    let rows: Vec<(String,)> = transaction
        .fetch_all(
            Query::select()
                .column(WorkspaceMemberships::WorkspaceId)
                .from(WorkspaceMemberships::Table)
                .and_where(
                    Expr::col(WorkspaceMemberships::Role).eq(WorkspaceRole::Owner.as_storage_str()),
                )
                .and_where(Expr::col(WorkspaceMemberships::WorkspaceId).in_subquery(owned))
                .group_by_col(WorkspaceMemberships::WorkspaceId)
                .and_having(Expr::col(WorkspaceMemberships::UserId).count().eq(1))
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

/// Every Workspace in the Organization, in stable order.
pub(super) async fn all_workspaces(
    transaction: &mut Transaction<'_>,
) -> Result<Vec<Workspace>, Error> {
    let rows: Vec<WorkspaceRow> = transaction
        .fetch_all(
            Query::select()
                .columns(WORKSPACE_COLUMNS)
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
    pub(super) occurred_at: UnixNanos,
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
            Expr::val(event.occurred_at),
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
                    LifecycleAction::SecretStoreGranted,
                    LifecycleAction::SecretStoreRevoked,
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

/// Counts the active Workspaces.
pub(crate) async fn active_workspace_total(tx: &mut Transaction<'_>) -> Result<u64, Error> {
    let total: Option<(i64,)> = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(WorkspaceRecords::Id).count())
                .from(WorkspaceRecords::Table)
                .and_where(
                    Expr::col(WorkspaceRecords::State).eq(WorkspaceState::Active.as_storage_str()),
                )
                .to_owned(),
        )
        .await?;
    Ok(total.map_or(0, |(total,)| u64::try_from(total).unwrap_or_default()))
}
