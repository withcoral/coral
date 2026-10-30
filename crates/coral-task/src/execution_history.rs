//! Task-owned optional execution associations. Each read and its association share a snapshot.
//! Run remains Task-free; adapters project these results without coordinating domain reads.
use crate::{Error, InteractionSummary, TaskIdentity, TaskModule};
use coral_db::Transaction;
use coral_run::{
    InvocationDetail, InvocationId, RunDetail, RunEntries, RunEntryWindow, RunEventDetail, RunId,
    RunModule,
};
use coral_workspace::{AuthenticatedPrincipal, HistoryScope, WorkspaceId};

/// Independently authorized original submission, absent for standalone or concealed Task history.
pub struct TaskSubmission {
    /// Non-authorizing goal navigation.
    pub task: TaskIdentity,
    /// Original tool intent and outcome, without exact discovery or source.
    pub interaction: InteractionSummary,
}
/// Run-owned evidence with an independently authorized optional Task association.
/// Used by exact Run/Event/Invocation reads.
pub struct ExecutionHistory<T> {
    /// Independently authorized Run-owned evidence.
    pub execution: T,
    /// Original Task submission, never fabricated for standalone execution.
    pub submission: Option<TaskSubmission>,
}
/// Source-free execution children with independently authorized Task navigation and location.
pub struct ActivityRunEntries {
    /// Independently authorized Run-owned child page.
    pub execution: RunEntries,
    /// Original Task submission, absent for standalone or concealed Task history.
    pub submission: Option<TaskSubmission>,
    /// Current chronological ordinal of the submission in its Task, when visible.
    pub submission_position: Option<u64>,
}
/// The Run context of one queued action, with Task-owned optional exec intent.
pub struct ActionRun {
    /// Independently authorized Run summary.
    pub run: coral_run::RunSummary,
    /// Independently authorized Task navigation, absent for standalone work.
    pub task: Option<TaskIdentity>,
    /// Original exec intent, never inferred from source or the Task goal.
    pub intent: Option<crate::TaskIntent>,
}
/// Bounded queue of current requests, most urgent first, each with its Run context.
pub type RequiredActions = coral_run::RequiredActions<ActionRun>;
impl TaskModule {
    /// Composes queued requests and Task-owned exec intent in one authorized snapshot.
    /// # Errors
    /// Rejects inaccessible scopes, Agent callers and invalid bounded windows.
    pub async fn required_actions(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: Option<&WorkspaceId>,
        after: Option<&coral_run::QueueCursor>,
        limit: u16,
    ) -> Result<RequiredActions, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .action_history_authority_in_transaction(&mut tx, actor, workspace)
            .await?;
        let page = runs
            .required_actions_in_transaction(&mut tx, &authority, after, limit)
            .await?;
        let contexts = page
            .actions
            .iter()
            .map(|queued| &queued.run)
            .collect::<Vec<_>>();
        let submissions =
            crate::interaction_repository::action_run_submissions(&mut tx, &authority, &contexts)
                .await?;
        tx.commit().await?;
        Ok(RequiredActions {
            action_count: page.action_count,
            actions: page
                .actions
                .into_iter()
                .map(|queued| {
                    let submission =
                        submissions.get(&(queued.run.workspace.clone(), queued.run.id.clone()));
                    coral_run::QueuedAction {
                        action: queued.action,
                        run: ActionRun {
                            task: submission.map(|(task, _)| task.clone()),
                            intent: submission.map(|(_, intent)| intent.clone()),
                            run: queued.run,
                        },
                    }
                })
                .collect(),
            next: page.next,
        })
    }
    /// Reads exact Run evidence and its original submission in one snapshot.
    /// # Errors
    /// Conceals inaccessible Runs; absent or concealed Task navigation stays absent.
    pub async fn inspect_run(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        inspection: &coral_run::RunInspection,
    ) -> Result<ExecutionHistory<RunDetail>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let execution = runs
            .inspect_run_in_transaction(&mut tx, actor, workspace, id, inspection)
            .await?;
        let submission = self
            .submission_in_transaction(&mut tx, actor, workspace, id)
            .await?;
        tx.commit().await?;
        Ok(ExecutionHistory {
            execution,
            submission,
        })
    }
    /// Reads source-free execution children and their original submission in one snapshot.
    /// # Errors
    /// Conceals inaccessible or out-of-scope Runs and rejects invalid windows.
    #[expect(
        clippy::too_many_arguments,
        reason = "Task composes Run's scoped child read with optional Task attribution"
    )]
    pub async fn activity_run_entries(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        window: &RunEntryWindow,
        limit: u16,
        selection: Option<&HistoryScope>,
    ) -> Result<ActivityRunEntries, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let execution = runs
            .activity_entries_in_transaction(
                &mut tx, actor, workspace, id, window, limit, selection,
            )
            .await?;
        let submission = self
            .submission_in_transaction(&mut tx, actor, workspace, id)
            .await?;
        let submission_position = if let Some(submission) = &submission {
            Some(
                crate::interaction_repository::position(
                    &mut tx,
                    &submission.task.workspace,
                    &submission.task.task,
                    &submission.interaction.id,
                    None,
                )
                .await?
                .ok_or(Error::InteractionNotFound)?,
            )
        } else {
            None
        };
        tx.commit().await?;
        Ok(ActivityRunEntries {
            execution,
            submission,
            submission_position,
        })
    }
    /// Reads one immutable Event, parent metadata and optional Task navigation together.
    /// # Errors
    /// Conceals inaccessible evidence; no list scope grants exact-read authority.
    pub async fn inspect_run_event(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        sequence: u64,
    ) -> Result<ExecutionHistory<RunEventDetail>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let execution = runs
            .inspect_event_in_transaction(&mut tx, actor, workspace, id, sequence)
            .await?;
        let submission = self
            .submission_in_transaction(&mut tx, actor, workspace, id)
            .await?;
        tx.commit().await?;
        Ok(ExecutionHistory {
            execution,
            submission,
        })
    }
    /// Reads one Invocation's exact evidence and optional Task navigation together.
    /// # Errors
    /// Conceals inaccessible evidence; another User's private Task stays concealed.
    pub async fn inspect_invocation(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &InvocationId,
    ) -> Result<ExecutionHistory<InvocationDetail>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let execution = runs
            .inspect_invocation_in_transaction(&mut tx, actor, workspace, id)
            .await?;
        let submission = self
            .submission_in_transaction(&mut tx, actor, workspace, &execution.summary.run)
            .await?;
        tx.commit().await?;
        Ok(ExecutionHistory {
            execution,
            submission,
        })
    }
    async fn submission_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        run: &RunId,
    ) -> Result<Option<TaskSubmission>, Error> {
        let Some((task, interaction)) =
            crate::interaction_repository::run_interaction(tx, workspace, run).await?
        else {
            return Ok(None);
        };
        match self
            .visible_in_transaction(tx, actor, workspace, &task)
            .await
        {
            Ok(_) => {}
            Err(Error::TaskNotFound) => return Ok(None),
            Err(error) => return Err(error),
        }
        let summary =
            crate::interaction_repository::summary(tx, workspace, &task, &interaction).await?;
        Ok(summary.map(|interaction| TaskSubmission {
            task: TaskIdentity {
                workspace: workspace.clone(),
                task,
            },
            interaction,
        }))
    }
}
