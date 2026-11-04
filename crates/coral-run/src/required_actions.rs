//! Current human-work projection. Request owners supply only safe pending
//! evidence; Run visibility is checked before composing it in one read snapshot.
//! Revisions are content-addressed, not authority or a history of human actions.

use crate::lifecycle::RunRecord;
use coral_db::Transaction;
use coral_workspace::{Principal, PrincipalKind, WorkspaceId};
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

/// One kind of current request, which a queue read can select.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RequiredActionKind {
    /// Approval of one exact Invocation.
    Approval,
    /// A User-private Account connection.
    AccountConnection,
}

/// Opaque fingerprint of one caller's current requests of one kind. Equal
/// revisions mean the same requests, whatever their count. It covers identities
/// the caller can already list, so it reveals nothing else and grants no authority.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequiredActionSetRevision([u8; 32]);

impl std::fmt::Display for RequiredActionSetRevision {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.iter().try_for_each(|byte| write!(f, "{byte:02x}"))
    }
}

/// Distinct current requests by kind. A connection request shared by several
/// Runs counts once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequiredActionCounts {
    /// Current Approval Requests.
    pub approvals: i64,
    /// Current Account Connection Requests.
    pub connections: i64,
    /// Changes whenever the set of current Approval Requests changes.
    pub approval_revision: RequiredActionSetRevision,
    /// Changes whenever the set of current Account Connection Requests changes.
    pub connection_revision: RequiredActionSetRevision,
}

impl RequiredActionCounts {
    /// Folds current requests, each once and in a stable order.
    pub(crate) fn of(requests: &[(WorkspaceId, RequiredActionRequest)]) -> Self {
        let tally = |kind: &str| {
            let mut digest = Sha256::new();
            let mut count = 0;
            for (workspace, request) in requests.iter().filter(|(_, r)| r.kind() == kind) {
                digest.update(format!("{workspace}/{request}\n"));
                count += 1;
            }
            (count, RequiredActionSetRevision(digest.finalize().into()))
        };
        let (approvals, approval_revision) = tally(RequiredActionRequest::APPROVAL);
        let (connections, connection_revision) = tally(RequiredActionRequest::CONNECTION);
        Self {
            approvals,
            connections,
            approval_revision,
            connection_revision,
        }
    }

    /// Current requests of every kind.
    #[must_use]
    pub const fn total(&self) -> i64 {
        self.approvals + self.connections
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
    /// Stable, non-secret root-relative Reef path of the page where the person
    /// decides this request: the Approvals Page with the request open, or the
    /// Run's page with the connection request open. Adapters resolve the Reef origin.
    pub action_path: String,
    /// The waiting Tool's Call Path; absent when one shared connection request
    /// spans Invocations of different Tools.
    pub call_path: Option<String>,
    /// The waiting Tool's Provider display label; absent once the Provider is removed.
    pub provider_label: Option<String>,
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
    /// Retained Tool path; empty when a shared connection spans different Tools.
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
    /// Distinct current requests of the selected kind across the complete
    /// authorized scope, not this page.
    pub action_count: i64,
    /// This page, ordered by deadline.
    pub actions: Vec<QueuedAction<R>>,
    /// Continuation after this page, absent at the end.
    pub next: Option<QueueCursor>,
}

impl crate::RunModule {
    /// Counts distinct requests by kind, with each kind's revision, without
    /// loading Runs or exact evidence.
    /// # Errors
    /// Rejects Agent callers and inaccessible scopes.
    pub async fn required_action_counts(
        &self,
        actor: &coral_workspace::AuthenticatedPrincipal,
        workspace: Option<&coral_workspace::WorkspaceId>,
    ) -> Result<RequiredActionCounts, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .action_history_authority_in_transaction(&mut tx, actor, workspace)
            .await?;
        let requests =
            crate::required_actions_repository::current_requests(&mut tx, &authority, self.now()?)
                .await?;
        let counts = RequiredActionCounts::of(&requests);
        tx.commit().await?;
        Ok(counts)
    }

    /// Lists own actions and Agent approvals across currently authorized Workspaces,
    /// one entry per request, most urgent first. Selection and count share the
    /// caller's snapshot, where `authority` came from
    /// `action_history_authority_in_transaction`. A `kind` selects that kind alone,
    /// for the list and its count, and `after` continues only a read of the same
    /// kind. No arguments are loaded.
    /// # Errors
    /// Rejects page sizes outside 1–100.
    pub async fn required_actions_in_transaction(
        &self,
        tx: &mut Transaction<'_>,
        authority: &coral_workspace::HistoryAuthority,
        kind: Option<RequiredActionKind>,
        after: Option<&QueueCursor>,
        limit: u16,
    ) -> Result<RequiredActions, Error> {
        if !(1..=100).contains(&limit) {
            return Err(Error::InvalidInput("invalid action page size".into()));
        }
        let now = self.now()?;
        let action_count =
            crate::required_actions_repository::count(tx, authority, kind, now).await?;
        let (rows, has_more) = crate::required_actions_repository::queue(
            tx,
            authority,
            kind,
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
    let required_actor = match run.principal.kind() {
        PrincipalKind::User => RequiredActor::InvokingUser,
        PrincipalKind::Agent => RequiredActor::WorkspaceOwner,
    };
    let mut actions = Vec::new();
    for (id, deadline) in approval::pending_for_run(tx, run).await? {
        let action_path = format!(
            "/workspaces/{ws}/approvals?request=workspaces/{ws}/approvals/{id}",
            ws = run.workspace_id
        );
        actions.push(RequiredAction {
            request: RequiredActionRequest::InvocationApproval(id),
            required_actor,
            deadline_at_unix_nanos: deadline,
            action_path,
            call_path: None,
            provider_label: None,
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
                let action_path = format!(
                    "/workspaces/{ws}/runs/{run}?request=workspaces/{ws}/accountConnections/{id}",
                    ws = run.workspace_id,
                    run = run.id
                );
                actions.push(RequiredAction {
                    request: RequiredActionRequest::AccountConnection(id),
                    required_actor: RequiredActor::InvokingUser,
                    deadline_at_unix_nanos: deadline,
                    action_path,
                    call_path: None,
                    provider_label: None,
                });
            }
        }
    }
    name_waiting_tools(tx, run, now, &mut actions).await?;
    Ok(actions)
}

/// Names each action's waiting Tool and Provider from the Run's Invocation
/// evidence, so the assistant can say what is asked without the arguments.
async fn name_waiting_tools(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    now: i64,
    actions: &mut [RequiredAction],
) -> Result<(), Error> {
    if actions.is_empty() {
        return Ok(());
    }
    let mut after = None;
    loop {
        let (evidence, _total, has_more) = crate::required_actions_repository::page(
            tx,
            &run.workspace_id,
            &run.id,
            after.as_ref(),
            now,
            100,
        )
        .await?;
        let last = evidence.last().map(|row| ActionCursor(row.request.clone()));
        for row in evidence {
            if let Some(action) = actions
                .iter_mut()
                .find(|action| action.request == row.request)
            {
                let call_path = row.shared_call_path();
                action.call_path = (!call_path.is_empty()).then(|| call_path.to_owned());
                action.provider_label = row.provider_label;
            }
        }
        if !has_more {
            return Ok(());
        }
        after = last;
    }
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
