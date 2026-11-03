//! Source/arguments/result-free execution summaries for the activity hierarchy.
use crate::{Error, InvocationId, InvocationResultKind, RunId, RunModule, RunState};
use coral_workspace::{HistoryAuthority, WorkspaceId};

/// Typed Invocation lifecycle, independent of its ended outcome.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum InvocationStatus {
    /// Admitted but not dispatched.
    Pending,
    /// Durable Policy Enforcement Point crossed.
    Dispatching,
    /// Durable outcome recorded.
    Ended,
}
/// Source-free Run summary.
#[derive(Clone, Debug)]
pub struct RunSummary {
    /// Current Workspace display label, independent of its stable identity.
    pub workspace_label: String,
    /// Safe retained Agent label, absent for User work.
    pub agent_label: Option<String>,
    /// Current authorized action window, absent when this history query did not request it.
    /// A present empty page means no actions in that window, not unavailable evidence.
    pub action_page: Option<crate::ActionPage>,
    /// Net retained UTF-8 payload bytes: returned output minus Tool Results.
    /// Absent while running or when an Invocation result is indeterminate.
    pub context_impact_bytes: Option<i64>,
    /// Accountable invoking Principal, also selecting exact child history attribution.
    pub principal: coral_workspace::Principal,
    /// Owning Workspace.
    pub workspace: WorkspaceId,
    /// Stable Run identity.
    pub id: RunId,
    /// Current state.
    pub state: RunState,
    /// Durable admission time.
    pub started_at: i64,
    /// Terminal time if ended.
    pub ended_at: Option<i64>,
    /// Elapsed nanoseconds at the page sample time.
    pub duration: i64,
    /// Current human work exists.
    pub requires_action: bool,
}
/// Arguments/result-free Invocation summary.
#[derive(Clone, Debug)]
pub struct InvocationSummary {
    /// Negative retained Tool Result bytes; absent before completion or on uncertainty.
    pub context_impact_bytes: Option<i64>,
    /// Accountable caller, independent of a reviewer or credential owner.
    pub principal: coral_workspace::Principal,
    /// Durable dispatch time, absent before dispatch.
    pub dispatched_at: Option<i64>,
    /// Owning Workspace.
    pub workspace: WorkspaceId,
    /// Stable Invocation identity.
    pub id: InvocationId,
    /// Enclosing Run.
    pub run: RunId,
    /// Retained Provider identity.
    pub provider: coral_provider::ProviderId,
    /// Retained Tool key.
    pub tool: String,
    /// Retained Program call path.
    pub call_path: String,
    /// Native lifecycle state.
    pub state: InvocationStatus,
    /// Native ended outcome kind.
    pub outcome: Option<InvocationResultKind>,
    /// Admission time, before gates.
    pub started_at: i64,
    /// Terminal time if ended.
    pub ended_at: Option<i64>,
    /// Elapsed nanoseconds at the page sample time.
    pub duration: i64,
    /// Current human work exists.
    pub requires_action: bool,
    /// Where authored source made the call, when recorded.
    pub source_location: Option<crate::SourceLocation>,
}
impl RunModule {
    /// Joins an upper owner's authorized snapshot for one source-free Run summary.
    /// # Errors
    /// Conceals absent or unauthorized executions.
    pub async fn summary_in_transaction(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        authority: &HistoryAuthority,
        workspace: &WorkspaceId,
        id: &RunId,
    ) -> Result<RunSummary, Error> {
        crate::history_repository::run_summary(tx, authority, workspace, id, self.now()?).await
    }
}
