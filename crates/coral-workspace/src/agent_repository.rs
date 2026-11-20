use coral_db::{QueryExecutor, Transaction, UnixNanos, Version};
use sea_query::{Expr, ExprTrait as _, Iden, Query};
use sqlx::FromRow;

use crate::management_repository::{changed_at_version, row_metadata};
use crate::{
    Agent, AgentDisplayName, AgentRegistration, AgentRegistrationId, AgentState, Error, Principal,
    PrincipalId, PrincipalKind, ResourceMetadata, UserId, WorkspaceId,
};

#[derive(Iden)]
enum WorkspaceAgents {
    Table,
    Id,
    WorkspaceId,
    DisplayName,
    CreatedBy,
    Deleted,
    State,
    Version,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

#[derive(Iden)]
enum AgentRegistrations {
    Table,
    Id,
    WorkspaceId,
    AgentId,
    Enabled,
    ExpiresAtUnixNanos,
    CredentialVerifier,
    Version,
    CreatedAtUnixNanos,
    UpdatedAtUnixNanos,
}

pub(super) async fn has_creator(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    user: &UserId,
) -> Result<bool, Error> {
    let row: Option<(String,)> = tx
        .fetch_optional(
            Query::select()
                .column(WorkspaceAgents::Id)
                .from(WorkspaceAgents::Table)
                .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(WorkspaceAgents::CreatedBy).eq(user.as_str()))
                .limit(1)
                .to_owned(),
        )
        .await?;
    Ok(row.is_some())
}

#[derive(FromRow)]
struct AgentRow {
    id: String,
    workspace_id: String,
    display_name: String,
    created_by: String,
    state: String,
    version: i64,
    created_at_unix_nanos: Option<UnixNanos>,
    updated_at_unix_nanos: Option<UnixNanos>,
}

impl AgentRow {
    /// The Agent with the concurrency metadata its row holds. The row of an
    /// Agent that a Coral from before the row kept its times created has
    /// none.
    fn decode(self) -> Result<(Agent, ResourceMetadata), Error> {
        let metadata = row_metadata(
            self.version,
            self.created_at_unix_nanos,
            self.updated_at_unix_nanos,
        )?;
        let agent = Agent {
            principal: Principal::from_parts(PrincipalId::parse(&self.id)?, PrincipalKind::Agent),
            workspace_id: WorkspaceId::parse(&self.workspace_id)?,
            display_name: AgentDisplayName::parse(&self.display_name)?,
            created_by: UserId::parse(&self.created_by)?,
            state: AgentState::from_storage_str(&self.state)?,
        };
        Ok((agent, metadata))
    }
}

#[derive(FromRow)]
struct RegistrationRow {
    id: String,
    workspace_id: String,
    agent_id: String,
    enabled: i64,
    expires_at_unix_nanos: Option<UnixNanos>,
    version: i64,
    created_at_unix_nanos: Option<UnixNanos>,
    updated_at_unix_nanos: Option<UnixNanos>,
}

impl RegistrationRow {
    /// The Registration with the concurrency metadata its row holds, which
    /// has no times where a Coral from before the row kept them created it.
    fn decode(self) -> Result<(AgentRegistration, ResourceMetadata), Error> {
        let metadata = row_metadata(
            self.version,
            self.created_at_unix_nanos,
            self.updated_at_unix_nanos,
        )?;
        let registration = AgentRegistration {
            id: AgentRegistrationId::parse(&self.id)?,
            workspace_id: WorkspaceId::parse(&self.workspace_id)?,
            agent_id: PrincipalId::parse(&self.agent_id)?,
            enabled: match self.enabled {
                0 => false,
                1 => true,
                _ => return Err(Error::CorruptData("invalid Registration state".to_string())),
            },
            expires_at: self.expires_at_unix_nanos,
        };
        Ok((registration, metadata))
    }

    /// The Registration alone, for a reader that takes no part in a change.
    fn registration(self) -> Result<AgentRegistration, Error> {
        self.decode().map(|(registration, _metadata)| registration)
    }
}

/// Creates the Agent and its Registration, each at the first version, with
/// `created_at` as the time of creation and of the last change.
pub(super) async fn insert(
    transaction: &mut Transaction<'_>,
    agent: &Agent,
    registration: &AgentRegistration,
    verifier: &str,
    created_at: UnixNanos,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::insert()
                .into_table(WorkspaceAgents::Table)
                .columns([
                    WorkspaceAgents::Id,
                    WorkspaceAgents::WorkspaceId,
                    WorkspaceAgents::DisplayName,
                    WorkspaceAgents::CreatedBy,
                    WorkspaceAgents::State,
                    WorkspaceAgents::CreatedAtUnixNanos,
                    WorkspaceAgents::UpdatedAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(agent.principal().id().as_str()),
                    Expr::val(agent.workspace_id().to_string()),
                    Expr::val(agent.display_name().as_str()),
                    Expr::val(agent.created_by().as_str()),
                    Expr::val(agent.state().as_storage_str()),
                    Expr::val(created_at),
                    Expr::val(created_at),
                ])
                .to_owned(),
        )
        .await?;
    transaction
        .execute(
            Query::insert()
                .into_table(AgentRegistrations::Table)
                .columns([
                    AgentRegistrations::Id,
                    AgentRegistrations::WorkspaceId,
                    AgentRegistrations::AgentId,
                    AgentRegistrations::Enabled,
                    AgentRegistrations::ExpiresAtUnixNanos,
                    AgentRegistrations::CredentialVerifier,
                    AgentRegistrations::CreatedAtUnixNanos,
                    AgentRegistrations::UpdatedAtUnixNanos,
                ])
                .values_panic([
                    Expr::val(registration.id().to_string()),
                    Expr::val(registration.workspace_id().to_string()),
                    Expr::val(registration.agent_id().as_str()),
                    Expr::val(i64::from(registration.is_enabled())),
                    Expr::val(registration.expires_at()),
                    Expr::val(verifier),
                    Expr::val(created_at),
                    Expr::val(created_at),
                ])
                .to_owned(),
        )
        .await?;
    Ok(())
}

fn agents(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .columns([
            WorkspaceAgents::Id,
            WorkspaceAgents::WorkspaceId,
            WorkspaceAgents::DisplayName,
            WorkspaceAgents::CreatedBy,
            WorkspaceAgents::State,
            WorkspaceAgents::Version,
            WorkspaceAgents::CreatedAtUnixNanos,
            WorkspaceAgents::UpdatedAtUnixNanos,
        ])
        .from(WorkspaceAgents::Table)
        .and_where(Expr::col(WorkspaceAgents::Deleted).eq(0))
        .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned()
}

/// The Agents of one Workspace that are not deleted, each with its
/// concurrency metadata.
pub(super) async fn list(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Vec<(Agent, ResourceMetadata)>, Error> {
    let rows: Vec<AgentRow> = executor.fetch_all(agents(workspace_id)).await?;
    rows.into_iter().map(AgentRow::decode).collect()
}

/// Selects the ids of the Users who created the Agents [`list`] returns, in
/// one column.
pub(super) fn creator_ids(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .column(WorkspaceAgents::CreatedBy)
        .from(WorkspaceAgents::Table)
        .and_where(Expr::col(WorkspaceAgents::Deleted).eq(0))
        .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned()
}

pub(super) async fn find(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    agent_id: &PrincipalId,
) -> Result<Agent, Error> {
    find_with_metadata(executor, workspace_id, agent_id)
        .await
        .map(|(agent, _metadata)| agent)
}

/// One Agent that is not deleted, with its concurrency metadata.
pub(super) async fn find_with_metadata(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    agent_id: &PrincipalId,
) -> Result<(Agent, ResourceMetadata), Error> {
    let row: Option<AgentRow> = executor
        .fetch_optional(
            agents(workspace_id)
                .and_where(Expr::col(WorkspaceAgents::Id).eq(agent_id.as_str()))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::AgentNotFound)?.decode()
}

pub(super) async fn history(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    agent: &PrincipalId,
) -> Result<crate::AgentHistory, Error> {
    let row: Option<(String, i64)> = tx
        .fetch_optional(
            Query::select()
                .columns([WorkspaceAgents::DisplayName, WorkspaceAgents::Deleted])
                .from(WorkspaceAgents::Table)
                .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(WorkspaceAgents::Id).eq(agent.as_str()))
                .to_owned(),
        )
        .await?;
    let (display_name, deleted) = row.ok_or(Error::AgentNotFound)?;
    Ok(crate::AgentHistory {
        id: agent.clone(),
        workspace: workspace.clone(),
        display_name: AgentDisplayName::parse(&display_name)?,
        deleted: match deleted {
            0 => false,
            1 => true,
            _ => return Err(Error::CorruptData("invalid Agent deletion marker".into())),
        },
    })
}

fn registrations(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    any_workspace_registrations()
        .and_where(Expr::col(AgentRegistrations::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned()
}

/// Every Workspace's Registrations. Only [`registration_by_verifier_alone`] reads
/// this unscoped: authentication there finds out the Workspace.
fn any_workspace_registrations() -> sea_query::SelectStatement {
    Query::select()
        .columns([
            AgentRegistrations::Id,
            AgentRegistrations::WorkspaceId,
            AgentRegistrations::AgentId,
            AgentRegistrations::Enabled,
            AgentRegistrations::ExpiresAtUnixNanos,
            AgentRegistrations::Version,
            AgentRegistrations::CreatedAtUnixNanos,
            AgentRegistrations::UpdatedAtUnixNanos,
        ])
        .from(AgentRegistrations::Table)
        .to_owned()
}

/// The Registration of every Agent of one Workspace, each with its
/// concurrency metadata. A deleted Agent has none.
pub(super) async fn list_registrations(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Vec<(AgentRegistration, ResourceMetadata)>, Error> {
    let rows: Vec<RegistrationRow> = executor.fetch_all(registrations(workspace_id)).await?;
    rows.into_iter().map(RegistrationRow::decode).collect()
}

pub(super) async fn registration(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    agent_id: &PrincipalId,
) -> Result<AgentRegistration, Error> {
    registration_with_metadata(executor, workspace_id, agent_id)
        .await
        .map(|(registration, _metadata)| registration)
}

/// The Registration of one Agent, with its concurrency metadata.
pub(super) async fn registration_with_metadata(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    agent_id: &PrincipalId,
) -> Result<(AgentRegistration, ResourceMetadata), Error> {
    let row: Option<RegistrationRow> = executor
        .fetch_optional(
            registrations(workspace_id)
                .and_where(Expr::col(AgentRegistrations::AgentId).eq(agent_id.as_str()))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::AgentNotFound)?.decode()
}

pub(super) async fn registration_by_id(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    id: &AgentRegistrationId,
) -> Result<AgentRegistration, Error> {
    registration_by_id_with_metadata(executor, workspace_id, id)
        .await
        .map(|(registration, _metadata)| registration)
}

/// One Registration, with its concurrency metadata.
pub(super) async fn registration_by_id_with_metadata(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    id: &AgentRegistrationId,
) -> Result<(AgentRegistration, ResourceMetadata), Error> {
    let row: Option<RegistrationRow> = executor
        .fetch_optional(
            registrations(workspace_id)
                .and_where(Expr::col(AgentRegistrations::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::AgentNotFound)?.decode()
}

/// Marks the Agent that is at version `expected` deleted, which raises its
/// version by one, and deletes its Registration.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for an Agent that is not at `expected`.
pub(super) async fn delete_current(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    agent: &PrincipalId,
    expected: Version,
    deleted_at: UnixNanos,
) -> Result<(), Error> {
    // The Agent's row stays, so the Registration that refers to it can go
    // after the Agent is checked and marked. The retained row is attribution
    // only; no current Agent interface can restore it.
    let marked = transaction
        .execute(
            Query::update()
                .table(WorkspaceAgents::Table)
                .value(WorkspaceAgents::Deleted, 1)
                .value(
                    WorkspaceAgents::State,
                    AgentState::Disabled.as_storage_str(),
                )
                .value(
                    WorkspaceAgents::Version,
                    Expr::col(WorkspaceAgents::Version).add(1_i64),
                )
                .value(WorkspaceAgents::UpdatedAtUnixNanos, deleted_at)
                .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(WorkspaceAgents::Id).eq(agent.as_str()))
                .and_where(Expr::col(WorkspaceAgents::Version).eq(expected.get()))
                .to_owned(),
        )
        .await?;
    changed_at_version(marked)?;
    transaction
        .execute(
            Query::delete()
                .from_table(AgentRegistrations::Table)
                .and_where(Expr::col(AgentRegistrations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(AgentRegistrations::AgentId).eq(agent.as_str()))
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Replaces the verifier of the Registration that is at version `expected`,
/// and raises its version by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Registration that is not at
/// `expected`.
pub(super) async fn update_verifier(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AgentRegistrationId,
    verifier: &str,
    expected: Version,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    let changed = transaction
        .execute(
            Query::update()
                .table(AgentRegistrations::Table)
                .value(AgentRegistrations::CredentialVerifier, verifier)
                .value(
                    AgentRegistrations::Version,
                    Expr::col(AgentRegistrations::Version).add(1_i64),
                )
                .value(AgentRegistrations::UpdatedAtUnixNanos, updated_at)
                .and_where(Expr::col(AgentRegistrations::WorkspaceId).eq(workspace_id.to_string()))
                .and_where(Expr::col(AgentRegistrations::Id).eq(id.to_string()))
                .and_where(Expr::col(AgentRegistrations::Version).eq(expected.get()))
                .to_owned(),
        )
        .await?;
    changed_at_version(changed)
}

/// Replaces the enabled state and expiration of the Registration that is at
/// version `expected`, and raises its version by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for a Registration that is not at
/// `expected`.
pub(super) async fn update_registration(
    transaction: &mut Transaction<'_>,
    registration: &AgentRegistration,
    expected: Version,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    let changed = transaction
        .execute(
            Query::update()
                .table(AgentRegistrations::Table)
                .value(
                    AgentRegistrations::Enabled,
                    i64::from(registration.is_enabled()),
                )
                .value(
                    AgentRegistrations::ExpiresAtUnixNanos,
                    registration.expires_at(),
                )
                .value(
                    AgentRegistrations::Version,
                    Expr::col(AgentRegistrations::Version).add(1_i64),
                )
                .value(AgentRegistrations::UpdatedAtUnixNanos, updated_at)
                .and_where(
                    Expr::col(AgentRegistrations::WorkspaceId)
                        .eq(registration.workspace_id().to_string()),
                )
                .and_where(Expr::col(AgentRegistrations::Id).eq(registration.id().to_string()))
                .and_where(Expr::col(AgentRegistrations::Version).eq(expected.get()))
                .to_owned(),
        )
        .await?;
    changed_at_version(changed)
}

/// Replaces the state of the Agent that is at version `expected`, and raises
/// its version by one.
///
/// # Errors
///
/// Returns [`Error::EtagMismatch`] for an Agent that is not at `expected`.
pub(super) async fn update_state(
    transaction: &mut Transaction<'_>,
    agent: &Agent,
    expected: Version,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    let changed = transaction
        .execute(
            Query::update()
                .table(WorkspaceAgents::Table)
                .value(WorkspaceAgents::State, agent.state().as_storage_str())
                .value(
                    WorkspaceAgents::Version,
                    Expr::col(WorkspaceAgents::Version).add(1_i64),
                )
                .value(WorkspaceAgents::UpdatedAtUnixNanos, updated_at)
                .and_where(
                    Expr::col(WorkspaceAgents::WorkspaceId).eq(agent.workspace_id().to_string()),
                )
                .and_where(Expr::col(WorkspaceAgents::Id).eq(agent.principal().id().as_str()))
                .and_where(Expr::col(WorkspaceAgents::Version).eq(expected.get()))
                .to_owned(),
        )
        .await?;
    changed_at_version(changed)
}

pub(super) async fn registration_by_verifier(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    verifier: &str,
) -> Result<AgentRegistration, Error> {
    let row: Option<RegistrationRow> = executor
        .fetch_optional(
            registrations(workspace_id)
                .and_where(Expr::col(AgentRegistrations::CredentialVerifier).eq(verifier))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::InvalidAgentCredential)?.registration()
}

/// Finds a Registration by its credential verifier in any Workspace. The
/// credential is a secret, not an ID, so this one lookup is not scoped by
/// Workspace; the Registration's own Workspace scopes every later read.
pub(super) async fn registration_by_verifier_alone(
    executor: impl QueryExecutor,
    verifier: &str,
) -> Result<AgentRegistration, Error> {
    let row: Option<RegistrationRow> = executor
        .fetch_optional(
            any_workspace_registrations()
                .and_where(Expr::col(AgentRegistrations::CredentialVerifier).eq(verifier))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::InvalidAgentCredential)?.registration()
}

/// Counts the Agents not deleted, in every Workspace.
pub(crate) async fn agent_total(tx: &mut Transaction<'_>) -> Result<u64, Error> {
    let total: Option<(i64,)> = tx
        .fetch_optional(
            Query::select()
                .expr(Expr::col(WorkspaceAgents::Id).count())
                .from(WorkspaceAgents::Table)
                .and_where(Expr::col(WorkspaceAgents::Deleted).eq(0))
                .to_owned(),
        )
        .await?;
    Ok(total.map_or(0, |(total,)| u64::try_from(total).unwrap_or_default()))
}
