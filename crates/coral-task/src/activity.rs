//! Activity hierarchy: Task owns root grouping and accepted-execution associations.
use crate::{AttemptKind, Error, IntentQuery, InteractionSummary, Task, TaskId, TaskModule};
use coral_run::{RunId, RunModule, RunSummary};
use coral_workspace::{AuthenticatedPrincipal, HistoryScope, WorkspaceId};

// This metric covers discovery and execution, not authoring or completion feedback.
// Missing evidence within this scope is unknown; other attempts are not applicable.
pub(crate) const CONTEXT_IMPACT_ATTEMPTS: [AttemptKind; 3] = [
    AttemptKind::Search,
    AttemptKind::Describe,
    AttemptKind::Submit,
];

/// The measured children's sum always stands as the total. A running child defers
/// its bytes, making the total partial. An unmeasurable child contributes nothing.
fn context_impact(
    totals: &crate::repository::activity::ContextTotals,
) -> crate::types::ContextImpact {
    crate::types::ContextImpact {
        bytes: totals.bytes,
        partial: totals.running > 0,
    }
}

/// Qualified, non-authorizing Task identity.
#[derive(Clone, Debug)]
pub struct TaskIdentity {
    /// Owning Workspace.
    pub workspace: WorkspaceId,
    /// Goal identity, never intent text.
    pub task: TaskId,
}

/// Qualified root reference. References never grant access.
#[derive(Clone, Debug)]
pub enum ActivityIdentity {
    /// A real goal.
    Task(WorkspaceId, TaskId),
    /// A standalone execution.
    Run(WorkspaceId, RunId),
}
/// Bounded root window, ordered by start time, newest first.
#[derive(Clone, Debug, Default)]
pub enum ActivityWindow {
    /// First page.
    #[default]
    Edge,
    /// Strict predecessors.
    Before(ActivityIdentity),
    /// Strict successors.
    After(ActivityIdentity),
    /// Include this exact root and its successors without filtering other roots.
    At(ActivityIdentity),
    /// Refresh only these bounded identities under current authority. Never fill gaps.
    Records(Vec<ActivityIdentity>),
}
/// One summary-only root. Attributed Runs occur under their Task, never twice.
pub enum ActivityRoot {
    /// Task lifecycle and declared intent.
    Task(Task),
    /// Standalone accepted execution, without invented intent.
    Run(RunSummary),
}
impl ActivityRoot {
    /// Stable non-authorizing window anchor.
    #[must_use]
    pub fn identity(&self) -> ActivityIdentity {
        match self {
            Self::Task(task) => {
                ActivityIdentity::Task(task.workspace_id().clone(), task.id().clone())
            }
            Self::Run(run) => ActivityIdentity::Run(run.workspace.clone(), run.id.clone()),
        }
    }
}
/// One authorized root window.
pub struct ActivityRoots {
    /// Bounded ordered summaries.
    pub rows: Vec<ActivityRoot>,
    /// Earlier pages exist.
    pub has_before: bool,
    /// Later pages exist.
    pub has_after: bool,
    /// Requested anchor or all nonempty retained identities are no longer available.
    pub anchor_unavailable: bool,
}
/// Bounded chronological tool window, including direct-link seeking.
#[derive(Clone, Debug, Default)]
pub enum InteractionWindow {
    /// First children.
    #[default]
    Start,
    /// Strict successors.
    After(crate::InteractionId),
    /// Include this exact recorded call.
    At(crate::InteractionId),
    /// Include the submission that accepted this Run.
    Run(RunId),
    /// A bounded scroll viewport. Resolve the anchor's current position in the same snapshot.
    Viewport {
        /// Requested first ordinal when there is no surviving anchor.
        offset: u64,
        /// A currently visible, non-authorizing child reference.
        anchor: Option<crate::InteractionId>,
        /// Maximum number of predecessors before the anchor, less than the page size.
        preceding: u16,
    },
}
/// One bounded chronological page with an authoritative continuation.
pub struct ActivityInteractions {
    /// Independently authorized goal metadata.
    pub task: Task,
    /// At most the requested number of source-free children.
    pub rows: Vec<ActivityInteraction>,
    /// Last returned interaction only when another child exists in the snapshot.
    pub next_after: Option<crate::InteractionId>,
    /// Current number of authorized children, independent of the reading window.
    pub total: i64,
    /// Current ordinal of the first returned child.
    pub offset: u64,
    /// The viewport anchor disappeared; the returned window uses the requested ordinal instead.
    pub anchor_unavailable: bool,
}
/// One recorded tool call, optionally represented by its accepted execution.
pub struct ActivityInteraction {
    /// Submission outcome stays separate from execution state.
    pub interaction: InteractionSummary,
    /// Accepted execution summary only, never source or output.
    pub run: Option<RunSummary>,
}
impl TaskModule {
    /// Sums the caller's own work and their Agents' work in one Workspace over the
    /// last `days`, in one read snapshot. Context counts what Activity counts:
    /// each discovery response and each Run, by when it ended.
    /// # Errors
    /// Rejects windows outside 1–30 days, Agent callers and inaccessible Workspaces.
    pub async fn activity_summary(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        days: u32,
    ) -> Result<coral_run::ActivitySummary, Error> {
        let since = runs.summary_window_start(days)?;
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .action_history_authority_in_transaction(&mut tx, actor, Some(workspace))
            .await?;
        let discovery =
            crate::repository::activity::discovery_context(&mut tx, &authority, since).await?;
        let summary = runs
            .activity_summary_in_transaction(&mut tx, &authority, since, discovery)
            .await?;
        tx.commit().await?;
        Ok(summary)
    }

    /// Reads a single bounded mixed root window in one authorization snapshot.
    /// With a query, the window holds only the Tasks whose intent or Run
    /// submission intent contains it, and each Task records which one matched.
    /// Standalone Runs have no intent, so a query never keeps them.
    /// # Errors
    /// Rejects invalid windows, inaccessible scopes and persistence failures.
    pub async fn activity_roots(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        scope: &HistoryScope,
        window: &ActivityWindow,
        limit: u16,
        query: Option<&IntentQuery>,
    ) -> Result<ActivityRoots, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .history_authority_in_transaction(&mut tx, actor, scope)
            .await?;
        let page =
            crate::repository::activity::roots(&mut tx, &authority, window, limit, query).await?;
        let anchor_unavailable = page.anchor_unavailable
            || (page.identities.is_empty()
                && matches!(window, ActivityWindow::Records(ids) if !ids.is_empty()));
        let mut rows = Vec::with_capacity(page.identities.len());
        for identity in page.identities {
            rows.push(match identity {
                ActivityIdentity::Task(workspace, id) => {
                    let mut task = self
                        .visible_in_transaction(&mut tx, actor, &workspace, &id)
                        .await?;
                    let totals = crate::repository::activity::context_totals(
                        &mut tx, &authority, &workspace, &id,
                    )
                    .await?;
                    task.context_impact = Some(context_impact(&totals));
                    task.intent_match = query.map(|query| query.matches(task.intent()));
                    ActivityRoot::Task(task)
                }
                ActivityIdentity::Run(workspace, id) => ActivityRoot::Run(
                    runs.summary_in_transaction(&mut tx, &authority, &workspace, &id)
                        .await?,
                ),
            });
        }
        tx.commit().await?;
        Ok(ActivityRoots {
            rows,
            has_before: page.has_before,
            has_after: page.has_after,
            anchor_unavailable,
        })
    }
    /// Reads chronological tool summaries and accepted Run state without exact evidence.
    /// With a query the Task's intent does not contain, the page holds only the
    /// submissions whose Run intent contains it.
    /// # Errors
    /// Conceals inaccessible goals and rejects invalid page bounds.
    #[expect(
        clippy::too_many_arguments,
        reason = "one Task read takes its window, an optional list scope and the list's query"
    )]
    pub async fn activity_interactions(
        &self,
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        identity: &crate::TaskIdentity,
        window: &InteractionWindow,
        limit: u16,
        selection: Option<&HistoryScope>,
        query: Option<&IntentQuery>,
    ) -> Result<ActivityInteractions, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidInput("invalid activity page size".into()));
        }
        let workspace = &identity.workspace;
        let task = &identity.task;
        let mut tx = self.database.begin_read_snapshot().await?;
        let (mut goal, authority) = self
            .activity_authority(&mut tx, actor, identity, selection)
            .await?;
        let totals =
            crate::repository::activity::context_totals(&mut tx, &authority, workspace, task)
                .await?;
        goal.context_impact = Some(context_impact(&totals));
        goal.intent_match = query.map(|query| query.matches(goal.intent()));
        let runs_matching = query.filter(|_| goal.intent_match == Some(false));
        if let InteractionWindow::Viewport { preceding, .. } = window
            && *preceding >= limit
        {
            return Err(Error::InvalidInput(
                "invalid viewport predecessor budget".into(),
            ));
        }
        let resolved;
        let window = match window {
            InteractionWindow::Run(run) => {
                let (parent, id) =
                    crate::interaction_repository::run_interaction(&mut tx, workspace, run)
                        .await?
                        .ok_or(Error::InteractionNotFound)?;
                if parent != *task {
                    return Err(Error::InteractionNotFound);
                }
                resolved = InteractionWindow::At(id);
                &resolved
            }
            other => other,
        };
        let page = crate::interaction_repository::activity_page(
            &mut tx,
            workspace,
            task,
            window,
            limit,
            runs_matching,
        )
        .await?;
        let interactions = page.rows;
        let has_more = interactions.len() > usize::from(limit);
        let mut rows = Vec::with_capacity(usize::from(limit));
        for interaction in interactions.into_iter().take(usize::from(limit)) {
            let run = if let Some(id) = &interaction.run {
                match runs
                    .summary_in_transaction(&mut tx, &authority, workspace, id)
                    .await
                {
                    Ok(run) => Some(run),
                    Err(coral_run::Error::RunNotFound) => None,
                    Err(error) => return Err(error.into()),
                }
            } else {
                None
            };
            rows.push(ActivityInteraction { interaction, run });
        }
        tx.commit().await?;
        let next_after = rows
            .last()
            .filter(|_| has_more)
            .map(|row| row.interaction.id.clone());
        Ok(ActivityInteractions {
            total: page.total,
            offset: page.offset,
            anchor_unavailable: page.anchor_unavailable,
            task: goal,
            rows,
            next_after,
        })
    }

    async fn activity_authority(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        identity: &TaskIdentity,
        selection: Option<&HistoryScope>,
    ) -> Result<(Task, coral_workspace::HistoryAuthority), Error> {
        if let Some(selection) = selection {
            let authority = self
                .workspaces
                .history_authority_in_transaction(tx, actor, selection)
                .await?;
            let goal = crate::repository::activity::task_in_scope(tx, &authority, identity).await?;
            return Ok((goal, authority));
        }
        let goal = self
            .visible_in_transaction(tx, actor, &identity.workspace, &identity.task)
            .await?;
        let scope = match goal.principal().kind() {
            coral_workspace::PrincipalKind::User => {
                HistoryScope::Personal(Some(identity.workspace.clone()))
            }
            coral_workspace::PrincipalKind::Agent => HistoryScope::Agent {
                workspace: identity.workspace.clone(),
                agent: goal.principal().id().clone(),
            },
        };
        let authority = self
            .workspaces
            .history_authority_in_transaction(tx, actor, &scope)
            .await?;
        Ok((goal, authority))
    }
}
