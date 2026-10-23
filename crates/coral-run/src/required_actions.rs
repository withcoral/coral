//! Current human-work projection. Request owners supply only safe pending
//! evidence; Run visibility is checked before composing it in one read snapshot.
//! Revisions are content-addressed, not authority or a history of human actions.

use crate::lifecycle::RunRecord;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalKind};
use sha2::{Digest as _, Sha256};

use crate::{Error, approval};

/// Who must act outside the Program runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredActor {
    /// Only the User who invoked the Run.
    InvokingUser,
    /// A designated Owner of the Agent's Workspace.
    WorkspaceOwner,
}

/// An opaque request address, granting no inspection or decision authority.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RequiredActionRequest {
    /// A User-private Account connection.
    AccountConnection(coral_provider::account::ConnectionRequestId),
    /// Approval of one exact Invocation, with no arguments or Account details.
    InvocationApproval(approval::ApprovalId),
}

impl RequiredActionRequest {
    pub(crate) const APPROVAL: &str = "approval";
    pub(crate) const CONNECTION: &str = "connection";

    /// Stable kind label shared by cursors and repository rows.
    pub(crate) const fn kind(&self) -> &'static str {
        match self {
            Self::InvocationApproval(_) => Self::APPROVAL,
            Self::AccountConnection(_) => Self::CONNECTION,
        }
    }

    pub(crate) fn id(&self) -> String {
        match self {
            Self::InvocationApproval(id) => id.to_string(),
            Self::AccountConnection(id) => id.to_string(),
        }
    }

    /// Inverse of [`Self::kind`] and [`Self::id`]. `None` for an unknown kind or identity.
    pub(crate) fn from_parts(kind: &str, id: &str) -> Option<Self> {
        match kind {
            Self::APPROVAL => approval::ApprovalId::parse(id)
                .ok()
                .map(Self::InvocationApproval),
            Self::CONNECTION => coral_provider::account::ConnectionRequestId::parse(id)
                .ok()
                .map(Self::AccountConnection),
            _ => None,
        }
    }

    /// Parses the stable `kind/id` text form, which grants no authority.
    /// # Errors
    /// Rejects malformed request kinds and noncanonical identities.
    pub fn parse(value: &str) -> Result<Self, Error> {
        value
            .split_once('/')
            .and_then(|(kind, id)| Self::from_parts(kind, id))
            .ok_or_else(|| Error::InvalidInput("invalid required action request".into()))
    }
}

impl std::fmt::Display for RequiredActionRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.kind(), self.id())
    }
}

/// One visible Request, even if several Invocations wait on it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequiredAction {
    /// Request kind and non-authorizing identifier.
    pub request: RequiredActionRequest,
    /// Actor needed to resolve the request, not authority granted to the observer.
    pub required_actor: RequiredActor,
    /// Request deadline in Unix nanoseconds.
    pub deadline_at_unix_nanos: i64,
    /// Stable, non-secret root-relative Reef path. Adapters resolve the Reef origin.
    pub run_status_path: String,
}

/// Non-authorizing keyset position in one Run's current action summaries.
/// The addressed request need not remain pending or retained to continue after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionCursor(pub(crate) RequiredActionRequest);

impl ActionCursor {
    /// Parses the opaque continuation returned with an action summary window.
    /// # Errors
    /// Rejects malformed request kinds and noncanonical identities.
    pub fn parse(value: &str) -> Result<Self, Error> {
        RequiredActionRequest::parse(value)
            .map(Self)
            .map_err(|_error| Error::InvalidInput("invalid action cursor".into()))
    }
}
impl std::fmt::Display for ActionCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// Human-only summary of current work, without arguments, source, comments or Accounts.
#[derive(Debug, Clone)]
pub struct ActionSummary {
    /// Opaque, non-authorizing request identity.
    pub request: RequiredActionRequest,
    /// Retained Operation path; empty when a shared connection spans different Operations.
    pub call_path: String,
    /// Current Provider display label, absent after the Provider is removed.
    pub provider_label: Option<String>,
    /// Effective wait deadline for this Run, not the shared connection's lifetime.
    pub deadline_at_unix_nanos: i64,
}

/// One explicitly observed current action window, distinct from unrequested history metadata.
#[derive(Debug, Clone)]
pub struct ActionPage {
    /// At most 100 safe current requests, with shared connections deduplicated.
    pub actions: Vec<ActionSummary>,
    /// Whole-Run current distinct-request count, independent of this window.
    pub total: i64,
    /// Stable request-kind/identity continuation, absent at the end.
    pub next_action: Option<ActionCursor>,
}

/// One current request in the human action queue, shown with a Run that waits on it.
/// A connection request shared by several Runs appears once, with the Run whose
/// deadline comes first.
#[derive(Debug)]
pub struct QueuedAction<R = crate::RunSummary> {
    /// The request and its current summary.
    pub action: ActionSummary,
    /// Context for the request. Callers that attach their own evidence keep this shape.
    pub run: R,
}

/// Non-authorizing position in the deadline-ordered queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueCursor {
    deadline_at_unix_nanos: i64,
    request: RequiredActionRequest,
}

impl QueueCursor {
    /// Parses the opaque continuation returned with a queue page.
    /// # Errors
    /// Rejects malformed positions.
    pub fn parse(value: &str) -> Result<Self, Error> {
        value
            .split_once('/')
            .and_then(|(deadline, request)| {
                Some(Self {
                    deadline_at_unix_nanos: deadline.parse().ok()?,
                    request: RequiredActionRequest::parse(request).ok()?,
                })
            })
            .ok_or_else(|| Error::InvalidInput("invalid action queue cursor".into()))
    }
}

impl std::fmt::Display for QueueCursor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.deadline_at_unix_nanos, self.request)
    }
}

/// Bounded human action queue: one entry per current request, most urgent first.
#[derive(Debug)]
pub struct RequiredActions<R = crate::RunSummary> {
    /// Distinct current requests across the complete authorized scope, not this page.
    pub action_count: i64,
    /// This page, ordered by deadline.
    pub actions: Vec<QueuedAction<R>>,
    /// Continuation after this page, absent at the end.
    pub next: Option<QueueCursor>,
}

impl crate::RunModule {
    /// Counts distinct requests without loading Runs or exact evidence.
    /// # Errors
    /// Rejects Agent callers and inaccessible scopes.
    pub async fn required_action_count(
        &self,
        actor: &coral_workspace::AuthenticatedPrincipal,
        workspace: Option<&coral_workspace::WorkspaceId>,
    ) -> Result<i64, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .action_history_authority_in_transaction(&mut tx, actor, workspace)
            .await?;
        let count =
            crate::required_actions_repository::count(&mut tx, &authority, self.now()?).await?;
        tx.commit().await?;
        Ok(count)
    }

    /// Lists own actions and Agent approvals across currently authorized Workspaces,
    /// one entry per request, most urgent first. Selection and count share the
    /// caller's snapshot, where `authority` came from
    /// `action_history_authority_in_transaction`. No arguments are loaded.
    /// # Errors
    /// Rejects page sizes outside 1–100.
    pub async fn required_actions_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        authority: &coral_workspace::HistoryAuthority,
        after: Option<&QueueCursor>,
        limit: u16,
    ) -> Result<RequiredActions, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidInput("invalid action page size".into()));
        }
        let now = self.now()?;
        let action_count = crate::required_actions_repository::count(tx, authority, now).await?;
        let (rows, has_more) = crate::required_actions_repository::queue(
            tx,
            authority,
            after.map(|cursor| (cursor.deadline_at_unix_nanos, &cursor.request)),
            now,
            limit,
        )
        .await?;
        let mut actions = Vec::with_capacity(rows.len());
        for row in rows {
            actions.push(QueuedAction {
                run: crate::history_repository::run_summary(
                    tx,
                    authority,
                    &row.workspace,
                    &row.run,
                    now,
                )
                .await?,
                action: summary(row.action),
            });
        }
        let next = actions.last().filter(|_| has_more).map(|last| QueueCursor {
            deadline_at_unix_nanos: last.action.deadline_at_unix_nanos,
            request: last.action.request.clone(),
        });
        Ok(RequiredActions {
            action_count,
            actions,
            next,
        })
    }
}

fn summary(row: crate::required_actions_repository::ActionEvidence) -> ActionSummary {
    ActionSummary {
        call_path: row.shared_call_path().to_owned(),
        request: row.request,
        provider_label: row.provider_label,
        deadline_at_unix_nanos: row.request_deadline.min(row.run_deadline),
    }
}

/// Populate bounded cards only after exact Run authorization in this snapshot.
pub(crate) async fn populate_summaries(
    tx: &mut Transaction<'_>,
    run: &mut crate::RunSummary,
    after: Option<&ActionCursor>,
    now: i64,
) -> Result<(), Error> {
    let (evidence, total, has_more) =
        crate::required_actions_repository::page(tx, &run.workspace, &run.id, after, now, 100)
            .await?;
    run.requires_action = total > 0;
    let actions: Vec<_> = evidence.into_iter().map(summary).collect();
    let next_action = actions
        .last()
        .filter(|_| has_more)
        .map(|action| ActionCursor(action.request.clone()));
    run.action_page = Some(ActionPage {
        actions,
        total,
        next_action,
    });
    Ok(())
}

/// Whether the Run still takes human actions, as recorded: running, with no
/// termination intent. A Run past its deadline keeps its actions until its
/// owner records the timeout; readers never derive it from the clock.
pub(crate) fn run_accepts_actions(run: &RunRecord) -> bool {
    !run.state.is_terminal() && run.termination.is_none()
}

/// The Run's current actions as recorded, each with its deadline. `now`
/// dates only the invoking authority, whose expiry is the one time-dependent
/// visibility here.
pub(crate) async fn snapshot(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    observer: &Principal,
    now: i64,
) -> Result<Vec<RequiredAction>, Error> {
    if !run_accepts_actions(run)
        || !crate::history_repository::invoking_authority_current(tx, run, now).await?
    {
        return Ok(Vec::new());
    }
    let path = format!("/workspaces/{}/runs/{}", run.workspace_id, run.id);
    let required_actor = match run.principal.kind() {
        PrincipalKind::User => RequiredActor::InvokingUser,
        PrincipalKind::Agent => RequiredActor::WorkspaceOwner,
    };
    let mut actions = Vec::new();
    for (id, deadline) in approval::pending_for_run(tx, run).await? {
        actions.push(RequiredAction {
            request: RequiredActionRequest::InvocationApproval(id),
            required_actor,
            deadline_at_unix_nanos: deadline,
            run_status_path: path.clone(),
        });
    }
    if observer.kind() == PrincipalKind::User && observer == &run.principal {
        for id in crate::invocation_repository::connection_requests_for_run(tx, run).await? {
            if let Some(deadline) =
                coral_provider::account::pending_connection_deadline_in_transaction(
                    tx,
                    &run.workspace_id,
                    observer,
                    id,
                )
                .await?
            {
                actions.push(RequiredAction {
                    request: RequiredActionRequest::AccountConnection(id),
                    required_actor: RequiredActor::InvokingUser,
                    deadline_at_unix_nanos: deadline,
                    run_status_path: path.clone(),
                });
            }
        }
    }
    Ok(actions)
}

pub(crate) fn revision(actions: &[RequiredAction]) -> [u8; 32] {
    if actions.is_empty() {
        return [0; 32];
    }
    let mut digest = Sha256::new();
    for action in actions {
        let (kind, id) = match &action.request {
            RequiredActionRequest::AccountConnection(id) => (0_u8, id.to_string()),
            RequiredActionRequest::InvocationApproval(id) => (1_u8, id.to_string()),
        };
        digest.update([kind]);
        digest.update(id.as_bytes());
        digest.update(action.deadline_at_unix_nanos.to_be_bytes());
        digest.update([match action.required_actor {
            RequiredActor::InvokingUser => 0,
            RequiredActor::WorkspaceOwner => 1,
        }]);
    }
    digest.finalize().into()
}
