//! Exact human-only retained evidence. Upper owners can join the same authorized snapshot.
use crate::{
    Error, InvocationId, InvocationSummary, RunEvent, RunId, RunModule, RunSummary,
    RunTerminalOutcome,
};
use coral_db::Transaction;
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};

/// Independent bounded windows and optional original request selection for exact Run inspection.
#[derive(Clone, Debug)]
pub struct RunInspection {
    /// Exclusive Event sequence, zero for the first page.
    pub event_after: u64,
    /// Event page size, from 1 through 100.
    pub event_limit: u16,
    /// Stable current-action continuation, independent of exact request selection.
    pub action_after: Option<crate::ActionCursor>,
    /// Original Account Connection Request to verify against retained Run attribution.
    pub account_connection: Option<coral_provider::account::ConnectionRequestId>,
}
impl Default for RunInspection {
    fn default() -> Self {
        Self {
            event_after: 0,
            event_limit: 50,
            action_after: None,
            account_connection: None,
        }
    }
}

/// Exact accepted linked source; never today's Saved Function definition.
#[derive(Debug)]
pub struct SourceSnapshot {
    /// Retained Saved Function identity.
    pub saved_function_id: String,
    /// Retained callable path.
    pub path: String,
    /// Exact accepted TypeScript source.
    pub source: String,
}
/// Exact Run evidence and one bounded Event page.
pub struct RunDetail {
    /// Independently verified original User-private request; absent when not selected or unavailable.
    pub account_connection: Option<coral_provider::account::ConnectionRequestId>,
    /// Source-free state projection.
    pub summary: RunSummary,
    /// Exact submitted TypeScript body.
    pub submitted_source: String,
    /// Admission-bounded linked snapshots.
    pub linked_functions: Vec<SourceSnapshot>,
    /// One bounded chronological Event page.
    pub events: Vec<RunEvent>,
    /// More Events exist after this page.
    pub has_more_events: bool,
    /// Retained terminal result, independent of Event pagination.
    pub terminal: Option<RunTerminalOutcome>,
    /// The Run Trace identifier of a Run that failed, timed out or was
    /// interrupted, for operator correlation.
    pub trace_id: Option<coral_telemetry::TraceId>,
}
/// One exact Event and its source-free authorized parent, read in one snapshot.
pub struct RunEventDetail {
    /// Retained returned payload bytes, including a Final Value on run.ended.
    pub context_impact_bytes: i64,
    /// Parent metadata, without accepted source or other Event payloads.
    pub run: RunSummary,
    /// The requested immutable Event.
    pub event: RunEvent,
}
/// Retained Invocation outcome, preserving upstream facts without success inference.
#[derive(Debug)]
pub enum InvocationOutcome {
    /// Complete upstream-native Tool Result.
    UpstreamOutcome(serde_json::Value),
    /// Safe coded Coral inability to produce a complete upstream result.
    CoralFailure {
        /// Retained safe code.
        code: String,
        /// Retained safe message.
        message: String,
    },
    /// Run termination won before dispatch.
    RunTerminated,
    /// Dispatch crossed its fence without a durable complete result.
    Indeterminate,
}
/// Exact Invocation evidence. Outcome kind remains in its summary.
pub struct InvocationDetail {
    /// Arguments/result-free summary.
    pub summary: InvocationSummary,
    /// Exact logical arguments.
    pub arguments: serde_json::Value,
    /// None while nonterminal; otherwise the exact retained outcome variant.
    pub outcome: Option<InvocationOutcome>,
    /// Retained request references, not a current action list.
    pub requests: Vec<crate::RequiredActionRequest>,
}
impl RunModule {
    /// Independently authorizes a retained approval; pending is not decision authority.
    /// # Errors
    /// Conceals absent or inaccessible approvals, including all Agent callers.
    pub async fn inspect_approval(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &crate::approval::ApprovalId,
    ) -> Result<crate::approval::ApprovalRequest, Error> {
        self.approvals().get(actor, workspace, id).await
    }
    /// The Approval Module that decides through this module's Run Inbox and
    /// on its clock, whichever Coral Instance owns the Run.
    #[must_use]
    pub fn approvals(&self) -> crate::approval::ApprovalModule {
        crate::approval::ApprovalModule::new(
            self.database.clone(),
            self.workspaces.clone(),
            self.inbox.clone(),
        )
        .with_clock(self.clock.clone())
    }

    /// Resolves one exact human approval through its owning module.
    /// # Errors
    /// Conceals inaccessible requests and rejects invalid comments.
    pub async fn decide_approval(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &crate::approval::ApprovalId,
        decision: crate::approval::ApprovalDecision,
        comment: Option<&str>,
        trace: &coral_workspace::TraceId,
    ) -> Result<crate::approval::ApprovalRequest, Error> {
        self.approvals()
            .decide(actor, workspace, id, decision, comment, trace)
            .await
    }

    pub(crate) async fn human_run(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        run: &RunId,
    ) -> Result<crate::lifecycle::RunRecord, Error> {
        if actor.authenticated_user().is_none() {
            return Err(Error::RunNotFound);
        }
        let agents = self
            .visibility_scope_in_transaction(tx, actor, workspace)
            .await?;
        crate::run_repository::find_visible_in_transaction(
            tx,
            workspace,
            run,
            actor.principal(),
            agents,
        )
        .await?
        .ok_or(Error::RunNotFound)
    }
    /// Reads exact retained source and bounded Events with current human visibility.
    /// # Errors
    /// Conceals inaccessible Runs; rejects invalid page bounds.
    pub async fn inspect_run(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        inspection: &RunInspection,
    ) -> Result<RunDetail, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let detail = self
            .inspect_run_in_transaction(&mut tx, actor, workspace, id, inspection)
            .await?;
        tx.commit().await?;
        Ok(detail)
    }
    /// Joins an upper owner's read snapshot; Run independently authorizes exact evidence.
    /// # Errors
    /// Conceals inaccessible Runs; rejects invalid page bounds.
    pub async fn inspect_run_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        inspection: &RunInspection,
    ) -> Result<RunDetail, Error> {
        if !(1..=100).contains(&inspection.event_limit) {
            return Err(Error::InvalidInput("invalid Event page size".into()));
        }
        let run = self.human_run(tx, actor, workspace, id).await?;
        let now = self.now()?;
        let mut summary = human_summary(tx, actor, &run, now).await?;
        crate::required_actions::populate_summaries(
            tx,
            &mut summary,
            inspection.action_after.as_ref(),
            now,
        )
        .await?;
        let account_connection = if let (Some(request), Some(user)) =
            (inspection.account_connection, actor.authenticated_user())
        {
            if run.principal == *actor.principal()
                && crate::inspection_repository::has_account_connection(tx, workspace, id, request)
                    .await?
                && coral_provider::account::retained_connection_available_in_transaction(
                    tx,
                    workspace,
                    user.user_id(),
                    request,
                )
                .await?
            {
                Some(request)
            } else {
                None
            }
        } else {
            None
        };
        let (submitted_source, linked_functions) =
            crate::inspection_repository::sources(tx, workspace, id).await?;
        let (events, has_more_events) = crate::run_repository::event_page_in_transaction(
            tx,
            workspace,
            id,
            inspection.event_after,
            inspection.event_limit,
            run.value_limits,
        )
        .await?;
        Ok(RunDetail {
            account_connection,
            summary,
            submitted_source,
            linked_functions,
            events,
            has_more_events,
            trace_id: run.diagnostic_trace_id(),
            terminal: run.terminal_outcome,
        })
    }
    /// Reads one immutable Event and parent metadata, without source or sibling Events.
    /// # Errors
    /// Conceals inaccessible evidence and rejects invalid Event identities.
    pub async fn inspect_event(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        sequence: u64,
    ) -> Result<RunEventDetail, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let detail = self
            .inspect_event_in_transaction(&mut tx, actor, workspace, id, sequence)
            .await?;
        tx.commit().await?;
        Ok(detail)
    }
    /// Joins an upper owner's snapshot for an independently authorized Event and its parent.
    /// # Errors
    /// Conceals inaccessible evidence and rejects invalid Event identities.
    pub async fn inspect_event_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
        sequence: u64,
    ) -> Result<RunEventDetail, Error> {
        let after = sequence
            .checked_sub(1)
            .ok_or_else(|| Error::InvalidInput("invalid Event identity".into()))?;
        let run = self.human_run(tx, actor, workspace, id).await?;
        let (events, _) = crate::run_repository::event_page_in_transaction(
            tx,
            workspace,
            id,
            after,
            1,
            run.value_limits,
        )
        .await?;
        let event = events
            .into_iter()
            .next()
            .filter(|event| event.sequence() == sequence)
            .ok_or(Error::RunNotFound)?;
        Ok(RunEventDetail {
            context_impact_bytes: crate::inspection_repository::event_context_bytes(
                tx, workspace, id, sequence,
            )
            .await?,
            run: human_summary(tx, actor, &run, self.now()?).await?,
            event,
        })
    }
    /// Reads logical arguments and native outcome for one authorized Invocation.
    /// # Errors
    /// Conceals absent or inaccessible evidence from humans and all Agent callers.
    pub async fn inspect_invocation(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &InvocationId,
    ) -> Result<InvocationDetail, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let detail = self
            .inspect_invocation_in_transaction(&mut tx, actor, workspace, id)
            .await?;
        tx.commit().await?;
        Ok(detail)
    }
    /// Joins an upper owner's snapshot for independently authorized Invocation evidence.
    /// # Errors
    /// Conceals absent or inaccessible evidence from humans and all Agent callers.
    pub async fn inspect_invocation_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &InvocationId,
    ) -> Result<InvocationDetail, Error> {
        let run_id = crate::inspection_repository::invocation_run(tx, workspace, id)
            .await?
            .ok_or(Error::RunNotFound)?;
        let run = self.human_run(tx, actor, workspace, &run_id).await?;
        crate::inspection_repository::invocation(tx, &run, id, self.now()?).await
    }
}
async fn human_summary(
    tx: &mut Transaction<'_>,
    actor: &AuthenticatedPrincipal,
    run: &crate::lifecycle::RunRecord,
    now: i64,
) -> Result<RunSummary, Error> {
    // human_run already authorized this exact record in the same snapshot.
    let (summary, _) = crate::history_repository::visible_run_summary(
        tx,
        actor.principal(),
        true,
        &run.workspace_id,
        &run.id,
        now,
    )
    .await?;
    Ok(summary)
}
