use coral_db::{QueryExecutor, Transaction, UnixNanos, Version};
use sea_query::{Expr, ExprTrait as _, Iden, Query};
use sqlx::FromRow;

use crate::{
    Agent, AgentDisplayName, AgentRegistration, AgentRegistrationId, AgentState, Error, Principal,
    PrincipalId, PrincipalKind, UserId, WorkspaceId,
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
}

impl AgentRow {
    fn decode(self) -> Result<Agent, Error> {
        Ok(Agent {
            principal: Principal::from_parts(PrincipalId::parse(&self.id)?, PrincipalKind::Agent),
            workspace_id: WorkspaceId::parse(&self.workspace_id)?,
            display_name: AgentDisplayName::parse(&self.display_name)?,
            created_by: UserId::parse(&self.created_by)?,
            state: AgentState::from_storage_str(&self.state)?,
        })
    }
}

#[derive(FromRow)]
struct RegistrationRow {
    id: String,
    workspace_id: String,
    agent_id: String,
    enabled: i64,
    expires_at_unix_nanos: Option<UnixNanos>,
}

impl RegistrationRow {
    fn decode(self) -> Result<AgentRegistration, Error> {
        Ok(AgentRegistration {
            id: AgentRegistrationId::parse(&self.id)?,
            workspace_id: WorkspaceId::parse(&self.workspace_id)?,
            agent_id: PrincipalId::parse(&self.agent_id)?,
            enabled: match self.enabled {
                0 => false,
                1 => true,
                _ => return Err(Error::CorruptData("invalid Registration state".to_string())),
            },
            expires_at: self.expires_at_unix_nanos,
        })
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
        ])
        .from(WorkspaceAgents::Table)
        .and_where(Expr::col(WorkspaceAgents::Deleted).eq(0))
        .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned()
}

pub(super) async fn list(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Vec<Agent>, Error> {
    let rows: Vec<AgentRow> = executor.fetch_all(agents(workspace_id)).await?;
    rows.into_iter().map(AgentRow::decode).collect()
}

/// Selects one column of the Agents of one Workspace that are not deleted.
fn agent_column(workspace_id: &WorkspaceId, column: WorkspaceAgents) -> sea_query::SelectStatement {
    Query::select()
        .column(column)
        .from(WorkspaceAgents::Table)
        .and_where(Expr::col(WorkspaceAgents::Deleted).eq(0))
        .and_where(Expr::col(WorkspaceAgents::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned()
}

/// Selects the ids of the Agents [`list`] returns, in one column.
pub(super) fn agent_ids(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    agent_column(workspace_id, WorkspaceAgents::Id)
}

/// Selects the ids of the Users who created the Agents [`list`] returns, in
/// one column.
pub(super) fn creator_ids(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    agent_column(workspace_id, WorkspaceAgents::CreatedBy)
}

pub(super) async fn find(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    agent_id: &PrincipalId,
) -> Result<Agent, Error> {
    let row: Option<AgentRow> = executor
        .fetch_optional(
            agents(workspace_id)
                .and_where(Expr::col(WorkspaceAgents::Id).eq(agent_id.as_str()))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::AgentNotFound)?.decode()
}

/// The current version of one Agent that is not deleted, or `None` when the
/// Workspace has no such Agent.
pub(super) async fn agent_version(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    agent: &str,
) -> Result<Option<Version>, Error> {
    let stored: Option<(i64,)> = tx
        .fetch_optional(
            agent_column(workspace, WorkspaceAgents::Version)
                .and_where(Expr::col(WorkspaceAgents::Id).eq(agent))
                .to_owned(),
        )
        .await?;
    stored.map(|(version,)| row_version(version)).transpose()
}

/// The current version of one Registration, or `None` when the Workspace has
/// no such Registration.
pub(super) async fn registration_version(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    registration: &str,
) -> Result<Option<Version>, Error> {
    let stored: Option<(i64,)> = tx
        .fetch_optional(
            Query::select()
                .column(AgentRegistrations::Version)
                .from(AgentRegistrations::Table)
                .and_where(Expr::col(AgentRegistrations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(AgentRegistrations::Id).eq(registration))
                .to_owned(),
        )
        .await?;
    stored.map(|(version,)| row_version(version)).transpose()
}

/// The version a row holds. A version that is not positive is corrupt.
fn row_version(stored: i64) -> Result<Version, Error> {
    Version::new(stored)
        .ok_or_else(|| Error::CorruptData("resource row has nonpositive version".to_string()))
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
        ])
        .from(AgentRegistrations::Table)
        .to_owned()
}

/// The Registration of every Agent of one Workspace. A deleted Agent has none.
pub(super) async fn list_registrations(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
) -> Result<Vec<AgentRegistration>, Error> {
    let rows: Vec<RegistrationRow> = executor.fetch_all(registrations(workspace_id)).await?;
    rows.into_iter().map(RegistrationRow::decode).collect()
}

/// Selects the ids of the Registrations [`list_registrations`] returns, in
/// one column.
pub(super) fn registration_ids(workspace_id: &WorkspaceId) -> sea_query::SelectStatement {
    Query::select()
        .column(AgentRegistrations::Id)
        .from(AgentRegistrations::Table)
        .and_where(Expr::col(AgentRegistrations::WorkspaceId).eq(workspace_id.to_string()))
        .to_owned()
}

pub(super) async fn registration(
    executor: impl QueryExecutor,
    workspace_id: &WorkspaceId,
    agent_id: &PrincipalId,
) -> Result<AgentRegistration, Error> {
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
    let row: Option<RegistrationRow> = executor
        .fetch_optional(
            registrations(workspace_id)
                .and_where(Expr::col(AgentRegistrations::Id).eq(id.to_string()))
                .to_owned(),
        )
        .await?;
    row.ok_or(Error::AgentNotFound)?.decode()
}

/// Deletes the Agent's Registration and marks the Agent deleted, which raises
/// the Agent's version by one.
pub(super) async fn delete_current(
    transaction: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    agent: &PrincipalId,
    deleted_at: UnixNanos,
) -> Result<(), Error> {
    transaction
        .execute(
            Query::delete()
                .from_table(AgentRegistrations::Table)
                .and_where(Expr::col(AgentRegistrations::WorkspaceId).eq(workspace.to_string()))
                .and_where(Expr::col(AgentRegistrations::AgentId).eq(agent.as_str()))
                .to_owned(),
        )
        .await?;
    // The retained row is attribution only; no current Agent interface can restore it.
    transaction
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
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Replaces the Registration's verifier and raises its version by one.
pub(super) async fn update_verifier(
    transaction: &mut Transaction<'_>,
    workspace_id: &WorkspaceId,
    id: &AgentRegistrationId,
    verifier: &str,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    transaction
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
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Replaces the Registration's enabled state and expiration, and raises its
/// version by one.
pub(super) async fn update_registration(
    transaction: &mut Transaction<'_>,
    registration: &AgentRegistration,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    transaction
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
                .to_owned(),
        )
        .await?;
    Ok(())
}

/// Replaces the Agent's state and raises its version by one.
pub(super) async fn update_state(
    transaction: &mut Transaction<'_>,
    agent: &Agent,
    updated_at: UnixNanos,
) -> Result<(), Error> {
    transaction
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
                .to_owned(),
        )
        .await?;
    Ok(())
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
    row.ok_or(Error::InvalidAgentCredential)?.decode()
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
    row.ok_or(Error::InvalidAgentCredential)?.decode()
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
