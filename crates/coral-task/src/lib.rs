//! Durable Task lifecycle and Principal-specific visibility for Coral.
//!
//! This private domain Module is the transport-neutral Interface for starting,
//! ending, and inspecting Tasks and orchestrating attributed commands above the
//! Task-free Functions, Search, and Run modules. It generates every Task identity, preserves
//! Workspace and Principal attribution, bounds declared intent, and owns the
//! one-way transition to a terminal success or failure outcome.
//!
//! # Interface
//!
//! - [`TaskModule::start`] generates and durably starts one attributed Task.
//! - [`TaskModule::end`] records the first terminal outcome and returns the
//!   durable winner of a concurrent terminal race.
//! - [`TaskModule::get`] and [`TaskModule::list`] apply Principal-specific
//!   confidentiality through the same Interface used by callers.
//! - [`TaskCommands`] begins an attempt before work and commits owning mutations,
//!   typed associations, and completion in one transaction. Search responses are
//!   retained completely before returning success; other owning content is linked.
//! - [`TaskModule::activity_roots`] and [`TaskModule::activity_interactions`] read bounded
//!   summary-only hierarchy pages. Exact scope checks do not browse or count neighbors.
//!   Context impact sums all retained discovery response bytes and accepted Run totals,
//!   independent of child paging. Authoring attempts are not applicable
//!   to this metric and do not suppress the parent total. No participating attempts
//!   measures zero; missing discovery or execution measurements remain unavailable.
//! - [`TaskModule::inspect_run`], [`TaskModule::inspect_run_event`],
//!   [`TaskModule::inspect_invocation`] and [`TaskModule::activity_run_entries`] compose
//!   Run-owned evidence with independently authorized Task submission metadata in one
//!   read snapshot. Standalone Runs have no synthetic association.
//! - [`TaskModule::interactions`] and [`TaskModule::interaction`] separate bounded
//!   summaries from exact private evidence.
//! - [`TaskModule::expire_group`] and the standalone retention facets recheck one
//!   exact unit. Deployment scheduling and activity browsing remain separate.
//!
//! # Invariants
//!
//! Callers cannot supply Task identifiers. Every externally influenced read or
//! write matches both Workspace and Task identity. A User sees that User's
//! Tasks; Workspace Owners additionally see Workspace-owned Agent
//! Tasks but never another User's Tasks. Agents can start and end their own
//! identified Tasks but cannot inspect Task history. Ending a Task cannot
//! change its Workspace, Principal, intent, or start evidence. Closure rejects new
//! attempts, but begun attempts may complete and accepted Runs continue.
//! Incomplete attempts never authorize replay. Task-group inactivity expires open
//! goals too; associated nonterminal Runs protect the whole group.
//!
//! The Module reads no process environment. Adapters supply current
//! authenticated Principal and time evidence explicitly. Persistence rows and
//! transport envelopes do not cross the Interface.

mod activity;
pub use activity::{
    ActivityIdentity, ActivityInteraction, ActivityInteractions, ActivityRoot, ActivityRoots,
    ActivityWindow, InteractionWindow, TaskIdentity,
};
mod execution_history;
pub use execution_history::{
    ActionRun, ActivityRunEntries, ExecutionHistory, RequiredActions, TaskSubmission,
};
mod commands;
mod error;
mod interaction;
mod interaction_repository;
mod repository;
mod retention;
mod types;
pub use commands::TaskCommands;
pub use interaction::{
    AttemptEvidence, AttemptFailure, AttemptKind, AttemptOutcome, AttributedCommand,
    DiscoveryContext, InteractionDetail, InteractionId, InteractionSummary,
};
pub use retention::HISTORY_RETENTION_NANOS;

use std::sync::Arc;

use coral_db::Database;
use coral_workspace::{
    AuthenticatedPrincipal, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};

pub use error::Error;
pub use types::{
    FoundTaskId, INTENT_QUERY_MAX_CHARS, INTENT_QUERY_MIN_CHARS, IntentQuery,
    TASK_FEEDBACK_MAX_CHARS, TASK_INTENT_MAX_CHARS, Task, TaskId, TaskIntent, TaskOutcome,
};

/// Durable Task lifecycle and visibility Module.
#[derive(Clone)]
pub struct TaskModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    #[cfg(test)]
    pub(crate) retention_control: Arc<crate::command_tests::Control>,
}

impl TaskModule {
    /// Builds the Module from initialized persistence and Workspace authority.
    #[must_use]
    pub fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
            #[cfg(test)]
            retention_control: Arc::default(),
        }
    }

    /// Generates an identity and durably starts one Task.
    ///
    /// # Errors
    ///
    /// Returns an authority or persistence error. Failure creates no Task.
    pub async fn start(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        intent: TaskIntent,
        started_at_unix_nanos: i64,
    ) -> Result<Task, Error> {
        let mut transaction = self.database.begin().await?;
        self.workspaces
            .check_principal_inspection_in_transaction(
                &mut transaction,
                actor,
                workspace_id,
                started_at_unix_nanos,
            )
            .await?
            .require_current_at(commands::now()?)?;
        // The primary key proves the identifier unused: a conflict writes
        // nothing, and the next attempt draws fresh words.
        let mut attempts = 0;
        let task = loop {
            let task = Task::new(
                TaskId::generate()?,
                workspace_id.clone(),
                actor.principal().clone(),
                intent.clone(),
                None,
                None,
                started_at_unix_nanos,
                None,
            );
            if repository::insert(&mut transaction, &task).await? {
                break task;
            }
            attempts += 1;
            if attempts == coral_identifier::ISSUE_ATTEMPTS {
                return Err(Error::IdentifierIssuance(format!(
                    "{attempts} identifiers in a row were already in use"
                )));
            }
        };
        transaction.commit().await?;
        Ok(task)
    }

    /// Returns one Task when it is visible to the current Principal.
    ///
    /// # Errors
    ///
    /// An absent Task, inaccessible Workspace, and concealed Task all return
    /// [`Error::TaskNotFound`].
    pub async fn get(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        task_id: &TaskId,
    ) -> Result<Task, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let task = self
            .visible_in_transaction(&mut transaction, actor, workspace_id, task_id)
            .await?;
        transaction.commit().await?;
        Ok(task)
    }

    /// Lists current Tasks visible to one Principal in one Workspace.
    ///
    /// # Errors
    ///
    /// Returns an authority or persistence error. Agents cannot list Task
    /// history.
    pub async fn list(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<Vec<Task>, Error> {
        let include_workspace_agents = self.inspection_scope(actor, workspace_id, false).await?;
        repository::list_visible(
            &self.database,
            workspace_id,
            actor.principal(),
            include_workspace_agents,
        )
        .await
    }

    async fn inspection_scope(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        conceal_resource: bool,
    ) -> Result<bool, Error> {
        let Some(user) = actor.authenticated_user() else {
            return if conceal_resource {
                Err(Error::TaskNotFound)
            } else {
                Err(Error::PermissionDenied(
                    "Agents cannot inspect Task history",
                ))
            };
        };
        match self
            .workspaces
            .check(user, workspace_id, WorkspaceAction::Inspect)
            .await
        {
            Ok(evidence) => Ok(evidence.effective_role() == WorkspaceRole::Owner),
            Err(
                coral_workspace::Error::WorkspaceNotFound
                | coral_workspace::Error::PermissionDenied(_),
            ) if conceal_resource => Err(Error::TaskNotFound),
            Err(error) => Err(error.into()),
        }
    }

    /// Records outcome, optional feedback, and terminal time atomically.
    ///
    /// Concurrent callers receive the first durable completion, including absent feedback.
    /// Repeated ending cannot change feedback or renew retention.
    ///
    /// # Errors
    ///
    /// An absent or concealed Task returns [`Error::TaskNotFound`].
    pub async fn end(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        task_id: &TaskId,
        outcome: TaskOutcome,
        feedback: Option<&str>,
        ended_at_unix_nanos: i64,
    ) -> Result<Task, Error> {
        if let Some(value) = feedback {
            types::validate_feedback(value)?;
        }
        let mut transaction = self.database.begin().await?;
        if !repository::hold(&mut transaction, workspace_id, task_id).await? {
            return Err(Error::TaskNotFound);
        }
        self.workspaces
            .check_principal_inspection_in_transaction(
                &mut transaction,
                actor,
                workspace_id,
                ended_at_unix_nanos,
            )
            .await
            .map_err(|error| match error {
                coral_workspace::Error::WorkspaceNotFound
                | coral_workspace::Error::PermissionDenied(_) => Error::TaskNotFound,
                other => other.into(),
            })?
            .require_current_at(commands::now()?)?;
        let task = repository::complete_for_principal(
            &mut transaction,
            workspace_id,
            task_id,
            actor.principal(),
            outcome,
            feedback,
            ended_at_unix_nanos,
        )
        .await?
        .ok_or(Error::TaskNotFound)?;
        transaction.commit().await?;
        Ok(task)
    }

    async fn visible_in_transaction(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        task: &TaskId,
    ) -> Result<Task, Error> {
        let user = actor.authenticated_user().ok_or(Error::TaskNotFound)?;
        let authority = self
            .workspaces
            .check_in_transaction(transaction, user, workspace, WorkspaceAction::Inspect)
            .await
            .map_err(|error| match error {
                coral_workspace::Error::WorkspaceNotFound
                | coral_workspace::Error::PermissionDenied(_) => Error::TaskNotFound,
                other => other.into(),
            })?;
        repository::find_visible(
            transaction,
            workspace,
            task,
            actor.principal(),
            authority.effective_role() == WorkspaceRole::Owner,
        )
        .await?
        .ok_or(Error::TaskNotFound)
    }

    /// Lists a bounded chronological page without loading private exact evidence.
    /// # Errors
    /// Reauthorizes Task visibility; rejects invalid bounds and concealed cursors.
    pub async fn interactions(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        task: &TaskId,
        after: Option<&InteractionId>,
        limit: u16,
    ) -> Result<Vec<InteractionSummary>, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidInput(
                "interaction limit must be between 1 and 100".into(),
            ));
        }
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.visible_in_transaction(&mut transaction, actor, workspace, task)
            .await?;
        let page =
            interaction_repository::list(&mut transaction, workspace, task, after, limit).await?;
        transaction.commit().await?;
        Ok(page)
    }

    /// Returns the intent the agent declared when it submitted a Run.
    /// A Run without a Task, or whose Task the caller cannot see, has none.
    /// # Errors
    /// Returns persistence failures.
    pub async fn run_intent(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        run: &coral_run::RunId,
    ) -> Result<Option<TaskIntent>, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        let Some((task, intent)) =
            interaction_repository::run_intent(&mut transaction, workspace, run).await?
        else {
            return Ok(None);
        };
        match self
            .visible_in_transaction(&mut transaction, actor, workspace, &task)
            .await
        {
            Ok(_task) => {}
            Err(Error::TaskNotFound) => return Ok(None),
            Err(error) => return Err(error),
        }
        transaction.commit().await?;
        Ok(Some(intent))
    }

    /// Reads exact retained evidence without consulting today's catalog.
    /// # Errors
    /// Conceals inaccessible goals and unavailable interactions.
    pub async fn interaction(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        task: &TaskId,
        id: &InteractionId,
    ) -> Result<InteractionDetail, Error> {
        let mut transaction = self.database.begin_read_snapshot().await?;
        self.visible_in_transaction(&mut transaction, actor, workspace, task)
            .await?;
        let detail = interaction_repository::detail(&mut transaction, workspace, task, id).await?;
        transaction.commit().await?;
        Ok(detail)
    }
}

#[cfg(test)]
mod command_tests;
#[cfg(test)]
mod tests;
