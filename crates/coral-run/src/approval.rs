//! Exact Invocation approval and User-only decision authority.
//!
//! [`ApprovalModule`] lists, inspects and decides requests with canonical Workspace
//! evidence. A User sees only their own Invocations; Workspace Owners also see
//! Workspace-owned Agent Invocations. Agents cannot inspect or decide approvals.
//! The Invocation creates a request only after Account selection. Its immutable
//! target and once-selected credentials are the binding, not a second argument
//! snapshot or a caller-supplied approval target. Private Generation provenance
//! stays private. The Dispatch Fence checks the approval again, at its own
//! clock sample, from the decision the held gate recorded: an approval never
//! changes once made.
//!
//! A decision is authorized and concealed here, then waits in the Run's Run
//! Inbox until the Run's owner, in whichever Coral Instance holds the Run,
//! records it in its next chunk; the first decision wins, and a repeat
//! receives the recorded answer. The decision reads no policy: the Dispatch
//! Fence judges the approval when the Invocation dispatches. A request's
//! deadline is the owner's timer: it expires the request in a chunk of its
//! own, in mailbox order with the decisions that race it, and only the owner
//! records an expiry. The first terminal transition and its safe lifecycle
//! event commit together; a lost response can be recovered by
//! reading or deciding again. Invocation owns live gate ordering and Promise
//! settlement. Approval owns only request validity, decisions and evidence,
//! not required-action projections or UI.

use crate::lifecycle::RunRecord;
use std::sync::Arc;
use std::time::Duration;

use coral_db::{Database, Transaction};
use coral_provider::ProviderId;
use coral_provider::access::CredentialRoute;
use coral_provider::account::ProviderAccountId;
use coral_workspace::TraceId;
use coral_workspace::{
    AuthenticatedPrincipal, Principal, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};
use tokio::time::Instant;
use uuid::Uuid;

use crate::approval_repository::{self, StoredApproval};
use crate::engine::{Clock, SystemClock};
use crate::fence::ApprovalFact;
use crate::invocation::AdmittedInvocation;
use crate::owner::{ChunkWork, DecisionRequest};
use crate::run_inbox::{InboxRequest, OutsideRequest, RunInbox};
use crate::run_repository::OwnedEvent;
use crate::{CoralFailure, Error, InvocationId, RunId, invocation_repository, run_repository};

/// How long a decision waits for its Run Owner to record it (ADR 0025).
const DECISION_WAIT: Duration = Duration::from_secs(5);

/// Opaque, non-authorizing Approval Request identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ApprovalId(Uuid);
impl ApprovalId {
    /// Parses a canonical lowercase UUID.
    /// # Errors
    /// Rejects malformed or noncanonical identifiers.
    pub fn parse(value: &str) -> Result<Self, Error> {
        let id = Uuid::parse_str(value)
            .map_err(|_error| Error::InvalidInput("invalid Approval Request id".into()))?;
        if id.to_string() != value {
            return Err(Error::InvalidInput("invalid Approval Request id".into()));
        }
        Ok(Self(id))
    }
}
impl std::fmt::Display for ApprovalId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

/// First durable request outcome; terminal outcomes never change.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalState {
    /// Waiting for an authorized User.
    Pending,
    /// Approved for this exact Invocation only.
    Approved,
    /// Declined by an authorized User.
    Declined,
    /// The approval deadline elapsed.
    Expired,
    /// No longer needed or the Invocation/Run stopped before a decision.
    Closed,
}
impl ApprovalState {
    pub(super) const fn storage(self) -> &'static str {
        match self {
            Self::Pending => "pending",
            Self::Approved => "approved",
            Self::Declined => "declined",
            Self::Expired => "expired",
            Self::Closed => "closed",
        }
    }
}
impl TryFrom<String> for ApprovalState {
    type Error = Error;
    fn try_from(value: String) -> Result<Self, Error> {
        match value.as_str() {
            "pending" => Ok(Self::Pending),
            "approved" => Ok(Self::Approved),
            "declined" => Ok(Self::Declined),
            "expired" => Ok(Self::Expired),
            "closed" => Ok(Self::Closed),
            _ => Err(Error::CorruptData("invalid approval state".into())),
        }
    }
}

/// One human decision, never a policy change or reusable permit.
#[derive(Debug, Clone, Copy)]
pub enum ApprovalDecision {
    /// Permit this exact Invocation to continue through its remaining checks.
    Approve,
    /// Reject this Invocation with `approval_declined`.
    Decline,
}

impl ApprovalDecision {
    /// The state this decision records.
    pub(crate) const fn state(self) -> ApprovalState {
        match self {
            Self::Approve => ApprovalState::Approved,
            Self::Decline => ApprovalState::Declined,
        }
    }
}

/// Authorized human detail. Never includes credentials or private Generations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRequest {
    /// Current safe Provider label, absent after the Provider is removed.
    pub provider_label: Option<String>,
    /// Safe retained invoking Agent label, absent for User work.
    pub agent_label: Option<String>,
    /// Non-authorizing identity.
    pub id: ApprovalId,
    /// Exact child Invocation.
    pub invocation_id: InvocationId,
    /// Exact containing Run.
    pub run_id: RunId,
    /// Accountable caller, not the credential owner or approver.
    pub invoking_principal: Principal,
    /// Exact Provider.
    pub provider_id: ProviderId,
    /// Retained Provider account-setting version.
    pub provider_account_setting_version: i64,
    /// Retained Credential Route.
    pub credential_route: CredentialRoute,
    /// Selected Provider Account, absent only for a credential-free route.
    pub provider_account_id: Option<ProviderAccountId>,
    /// Retained Program Call Path.
    pub call_path: String,
    /// Exact logical arguments, available only through authorized detail.
    pub arguments: serde_json::Value,
    /// Current durable decision.
    pub state: ApprovalState,
    /// Current decision availability, independent of the retained durable state.
    pub actionable: bool,
    /// Run-bound deadline in Unix nanoseconds.
    pub deadline_at_unix_nanos: i64,
    /// Winning human approver; absent for automatic closure/expiry.
    pub approver: Option<Principal>,
    /// Current approver name only when Workspace profile rules authorize it.
    pub approver_name: Option<String>,
    /// Winning decision timestamp.
    pub decided_at_unix_nanos: Option<i64>,
    /// Optional human comment, excluded from lifecycle evidence and telemetry.
    pub comment: Option<String>,
}

/// Safe immutable evidence of a first terminal approval transition.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalLifecycleEvent {
    /// Stable event identity.
    pub id: String,
    /// Exact non-authorizing request target.
    pub request_id: ApprovalId,
    /// Accountable User, or Coral when absent.
    pub actor: Option<Principal>,
    /// Accepted request outcome. The action is `invocation_approval.decided`.
    pub state: ApprovalState,
    /// Names only, never argument or comment values.
    pub changed_field_paths: Vec<String>,
    /// Durable decision time.
    pub occurred_at_unix_nanos: i64,
    /// Safe W3C correlation.
    pub trace_id: TraceId,
}

/// Transport-neutral Approval decision owner.
#[derive(Clone)]
pub struct ApprovalModule {
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    clock: Arc<dyn Clock>,
    /// Where a decision waits for its Run's owner.
    inbox: RunInbox,
}
impl ApprovalModule {
    /// Builds the Approval module against the Run domain's migrated database,
    /// deciding through `inbox`.
    #[must_use]
    pub(crate) fn new(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        inbox: RunInbox,
    ) -> Self {
        Self {
            database,
            workspaces,
            clock: Arc::new(SystemClock),
            inbox,
        }
    }

    /// Decides deadlines with `clock`, such as the owning Run Module's clock.
    #[must_use]
    pub(crate) fn with_clock(self, clock: Arc<dyn Clock>) -> Self {
        Self { clock, ..self }
    }

    /// Lists exact approvals for an authorized Run, including terminal decisions.
    /// # Errors
    /// Conceals other Users' work, non-Owner Agent work, and all Agent access.
    pub async fn list_for_run(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        run: &RunId,
    ) -> Result<Vec<ApprovalRequest>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let run = self.visible_run(&mut tx, actor, workspace, run).await?;
        let rows = approval_repository::list_for_run(&mut tx, workspace, &run.id).await?;
        let mut requests = Vec::new();
        for row in rows {
            requests.push(
                view(
                    &mut tx,
                    &self.workspaces,
                    actor,
                    workspace,
                    &run,
                    row,
                    self.now()?,
                )
                .await?,
            );
        }
        tx.commit().await?;
        Ok(requests)
    }

    /// Reads exact authorized approval detail without changing its outcome.
    /// # Errors
    /// Absent and concealed requests return the same error.
    pub async fn get(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &ApprovalId,
    ) -> Result<ApprovalRequest, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let row = approval_repository::load(&mut tx, workspace, id).await?;
        let run = self
            .visible_run(&mut tx, actor, workspace, &row.run_id)
            .await?;
        let request = view(
            &mut tx,
            &self.workspaces,
            actor,
            workspace,
            &run,
            row,
            self.now()?,
        )
        .await?;
        tx.commit().await?;
        Ok(request)
    }

    /// Decides one Approval Request through its Run's Run Inbox, whichever
    /// Coral Instance owns the Run, and returns the request as recorded.
    ///
    /// Authentication supplies current Organization Membership; Workspace supplies
    /// the User's Workspace Membership authority. Comments are limited to 1024 UTF-8 bytes.
    /// Approval does not attest to any other current dispatch-authority fact.
    /// The first decision on a request wins; a repeat, and a loser, receive
    /// the recorded answer: the decision, who made it and when. A request
    /// decided or closed already, or one of a terminal Run, is answered from
    /// the record and puts nothing in the Run Inbox. The wait for the owner
    /// to record the decision ends early when the Run ends: the record
    /// answers.
    /// # Errors
    /// Conceals unauthorized targets; rejects invalid comments and persistence faults.
    /// [`Error::RunRequestNotYetRecorded`] when the owner has not recorded
    /// the decision within the wait: retry.
    pub async fn decide(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &ApprovalId,
        decision: ApprovalDecision,
        comment: Option<&str>,
        trace: &TraceId,
    ) -> Result<ApprovalRequest, Error> {
        let arrived = Instant::now();
        if comment.is_some_and(|text| text.len() > 1024 || text.contains('\0')) {
            return Err(Error::InvalidInput(
                "approval comment must be at most 1024 bytes without null characters".into(),
            ));
        }
        // One snapshot authorizes the request and reads the request as it
        // stands.
        let (request, run) = {
            let mut tx = self.database.begin_read_snapshot().await?;
            let row = approval_repository::load(&mut tx, workspace, id).await?;
            let run = self
                .visible_run(&mut tx, actor, workspace, &row.run_id)
                .await?;
            let now = self.now()?;
            if row.state == ApprovalState::Pending {
                let authority = self
                    .workspaces
                    .check_principal_inspection_in_snapshot(&mut tx, actor, workspace, now)
                    .await
                    .map_err(conceal)?;
                // Agent approval requires the live Membership role, not the role in
                // the request's earlier authentication context.
                if run.principal.kind() == coral_workspace::PrincipalKind::Agent
                    && authority.effective_user_role() != Some(WorkspaceRole::Owner)
                {
                    return Err(Error::ApprovalNotFound);
                }
            }
            let request = view(&mut tx, &self.workspaces, actor, workspace, &run, row, now).await?;
            tx.commit().await?;
            (request, run)
        };
        if request.state != ApprovalState::Pending || run.state.is_terminal() {
            return Ok(request);
        }
        let inbox_request = InboxRequest {
            workspace_id: workspace.clone(),
            run_id: run.id.clone(),
            request: OutsideRequest::Decide(DecisionRequest {
                approval: id.clone(),
                deadline_at_unix_nanos: request.deadline_at_unix_nanos,
                decision,
                actor: actor.principal().clone(),
                comment: comment.map(str::to_owned),
                trace: trace.clone(),
            }),
        };
        // The Run's visibility to the caller is read again in the transaction
        // that inserts the row, which holds no Run row. The snapshot's
        // inspection and Agent-owner checks are not repeated.
        let mut tx = self.database.begin().await?;
        self.visible_run(&mut tx, actor, workspace, &run.id).await?;
        self.inbox
            .submit(&mut tx, &inbox_request, self.now()?)
            .await?;
        tx.commit().await?;
        self.inbox.wake_for(&run.id);
        if !self
            .inbox
            .recorded(&inbox_request, arrived + DECISION_WAIT)
            .await?
        {
            return Err(Error::RunRequestNotYetRecorded);
        }
        self.get(actor, workspace, id).await
    }

    /// Lists safe evidence through the same human-only visibility as request detail.
    /// # Errors
    /// Conceals inaccessible requests and propagates persistence corruption.
    pub async fn list_lifecycle_events(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &ApprovalId,
    ) -> Result<Vec<ApprovalLifecycleEvent>, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let row = approval_repository::load(&mut tx, workspace, id).await?;
        self.visible_run(&mut tx, actor, workspace, &row.run_id)
            .await?;
        let events = approval_repository::list_lifecycle_events(&mut tx, workspace, id)
            .await?
            .into_iter()
            .map(|event| ApprovalLifecycleEvent {
                id: event.id,
                request_id: id.clone(),
                actor: event.actor,
                state: event.state,
                changed_field_paths: vec!["state".into()],
                occurred_at_unix_nanos: event.occurred_at_unix_nanos,
                trace_id: event.trace_id,
            })
            .collect();
        tx.commit().await?;
        Ok(events)
    }

    async fn visible_run(
        &self,
        tx: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &RunId,
    ) -> Result<RunRecord, Error> {
        let user = actor.authenticated_user().ok_or(Error::ApprovalNotFound)?;
        let evidence = self
            .workspaces
            .check_in_transaction(tx, user, workspace, WorkspaceAction::Inspect)
            .await
            .map_err(conceal)?;
        run_repository::find_visible_in_transaction(
            tx,
            workspace,
            id,
            actor.principal(),
            evidence.effective_role() == WorkspaceRole::Owner,
        )
        .await?
        .ok_or(Error::ApprovalNotFound)
    }
    fn now(&self) -> Result<i64, Error> {
        self.clock.now_unix_nanos().map_err(Error::Clock)
    }
}

fn conceal(error: coral_workspace::Error) -> Error {
    match error {
        coral_workspace::Error::WorkspaceNotFound
        | coral_workspace::Error::PermissionDenied(_)
        | coral_workspace::Error::WorkspaceArchived => Error::ApprovalNotFound,
        other => other.into(),
    }
}
/// Whether the Tool's current policy asks for a decision, as a read
/// snapshot shows it: the request's `actionable` projection. A decision
/// itself reads no policy; the Dispatch Fence judges the approval against
/// the policy current when the Invocation dispatches.
async fn decision_required(
    tx: &mut Transaction<'_>,
    workspaces: &WorkspaceModule,
    run: &RunRecord,
    target: &invocation_repository::ApprovalTarget,
    now: i64,
) -> Result<bool, Error> {
    if let Err(error) = workspaces
        .check_invoking_principal_in_snapshot(tx, &run.workspace_id, &run.principal, now)
        .await
    {
        // A lost authority makes the request moot; any other failure is the
        // caller's to see.
        crate::fence::authority_failure(error)?;
        return Ok(false);
    }
    let tool = coral_catalog::tool_in_transaction(tx, &target.generation, &target.tool)
        .await
        .map_err(coral_provider::Error::from)?
        .ok_or(Error::ApprovalNotFound)?;
    let policy_target = coral_policy::ToolPolicyTarget::new(
        run.workspace_id.clone(),
        target.provider_id.clone(),
        target.tool.clone(),
    );
    let policy = coral_policy::discovery_decision_in_transaction(
        tx,
        &policy_target,
        tool.hints(),
        tool.http_method(),
    )
    .await?;
    Ok(policy.access_state() == coral_policy::ToolAccessState::ApprovalRequired)
}

/// The approver's current name, when Workspace profile rules let `actor`
/// read it.
async fn approver_name(
    tx: &mut Transaction<'_>,
    workspaces: &WorkspaceModule,
    actor: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
    approver: &Principal,
) -> Result<Option<String>, Error> {
    let user = actor.authenticated_user().ok_or(Error::ApprovalNotFound)?;
    let id = coral_workspace::UserId::parse(approver.id().as_str())?;
    match workspaces
        .inspect_user_profile_in_transaction(tx, user, workspace, &id)
        .await
    {
        Ok(profile) => Ok(profile.name.filter(|name| !name.trim().is_empty())),
        Err(coral_workspace::Error::MembershipNotFound) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

async fn view(
    tx: &mut Transaction<'_>,
    workspaces: &WorkspaceModule,
    actor: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
    run: &RunRecord,
    row: StoredApproval,
    now: i64,
) -> Result<ApprovalRequest, Error> {
    let target =
        invocation_repository::approval_target(tx, workspace, &run.id, &row.invocation_id).await?;
    let actionable = row.state == ApprovalState::Pending
        && crate::required_actions::run_accepts_actions(run)
        && decision_required(tx, workspaces, run, &target, now).await?;
    let (provider_label, agent_label) =
        crate::inspection_repository::approval_labels(tx, workspace, &row.invocation_id).await?;
    let approver_name = match &row.approver {
        Some(approver) => approver_name(tx, workspaces, actor, workspace, approver).await?,
        None => None,
    };
    Ok(ApprovalRequest {
        approver_name,
        provider_label,
        agent_label,
        id: row.id,
        invocation_id: row.invocation_id,
        run_id: run.id.clone(),
        invoking_principal: run.principal.clone(),
        provider_id: target.provider_id,
        provider_account_setting_version: target.provider_account_setting_version,
        credential_route: target.credential_route,
        provider_account_id: target.provider_account_id,
        call_path: target.call_path,
        arguments: target.arguments,
        state: row.state,
        actionable,
        deadline_at_unix_nanos: row.deadline_at_unix_nanos,
        approver: row.approver,
        decided_at_unix_nanos: row.decided_at_unix_nanos,
        comment: row.comment,
    })
}

/// The Run's Approval Requests still undecided whose deadline passed by
/// `by_unix_nanos`, each with its deadline: the expired-lease sweep queues
/// their timers for the Run's recovered owner.
pub(crate) async fn undecided_by(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    by_unix_nanos: i64,
) -> Result<Vec<(ApprovalId, i64)>, Error> {
    approval_repository::undecided_by(tx, &run.workspace_id, &run.id, by_unix_nanos).await
}

// Safe summary only: Agents observing their own Runs must not load approval detail.
pub(super) async fn pending_for_run(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
) -> Result<Vec<(ApprovalId, i64)>, Error> {
    approval_repository::pending_for_run(tx, &run.workspace_id, &run.id).await
}

/// What the held approval gate found for an Invocation.
pub(crate) enum Gate {
    /// The approval the fence decides on once the Invocation is ready.
    Ready(ApprovalFact),
    /// A decision is still pending. The request expires at this instant
    /// unless decided first: the owner's timer records the expiry.
    Pending {
        id: ApprovalId,
        deadline_at_unix_nanos: i64,
    },
}

// All private facets join the Run Owner's chunk transaction; an Invocation
// this settles appends its `invocation.ended` to `events`. A request this
// creates waits at most `approval_wait_nanos`, bounded by the Run deadline.
// The gate never judges a deadline: the owner's timer expires a pending
// request, and the fence judges an approved one at dispatch.
pub(super) async fn gate(
    tx: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    run: &RunRecord,
    required: bool,
    approval_wait_nanos: i64,
    now: i64,
    events: &mut Vec<OwnedEvent>,
) -> Result<Gate, Error> {
    let row =
        approval_repository::find_for_invocation(tx, &admitted.workspace_id, &admitted.id).await?;
    let mut row = if let Some(row) = row {
        row
    } else {
        if !required {
            return Ok(Gate::Ready(ApprovalFact::NotRequested));
        }
        let deadline = std::cmp::min(
            run.deadline_at_unix_nanos,
            now.saturating_add(approval_wait_nanos),
        );
        let id = ApprovalId(Uuid::new_v4());
        approval_repository::insert_pending(
            tx,
            &admitted.workspace_id,
            &id,
            &admitted.id,
            &admitted.run_id,
            deadline,
            now,
        )
        .await?;
        approval_repository::load(tx, &admitted.workspace_id, &id).await?
    };
    if row.state == ApprovalState::Pending && !required {
        transition(
            tx,
            &admitted.workspace_id,
            &mut row,
            ApprovalState::Closed,
            now,
            None,
            &TraceId::current_or_new(),
        )
        .await?;
        events.extend(settle_rejection(tx, run, &row, now).await?);
    }
    match row.state {
        ApprovalState::Pending => Ok(Gate::Pending {
            id: row.id,
            deadline_at_unix_nanos: row.deadline_at_unix_nanos,
        }),
        ApprovalState::Approved => Ok(Gate::Ready(approval_fact(&row))),
        ApprovalState::Closed if !required => Ok(Gate::Ready(ApprovalFact::NotRequested)),
        ApprovalState::Declined => Err(Error::InvocationRejected(
            CoralFailure::ApprovalDeclined.into(),
        )),
        ApprovalState::Expired => Err(Error::InvocationRejected(
            CoralFailure::ApprovalExpired.into(),
        )),
        ApprovalState::Closed => Err(Error::InvocationRejected(
            CoralFailure::ApprovalRequired.into(),
        )),
    }
}

/// The request's recorded decision as the fence decides on it.
fn approval_fact(row: &StoredApproval) -> ApprovalFact {
    match row.state {
        ApprovalState::Pending => ApprovalFact::Pending {
            deadline_at_unix_nanos: row.deadline_at_unix_nanos,
        },
        ApprovalState::Approved => ApprovalFact::Approved {
            deadline_at_unix_nanos: row.deadline_at_unix_nanos,
        },
        ApprovalState::Declined => ApprovalFact::Declined,
        ApprovalState::Expired => ApprovalFact::Expired,
        ApprovalState::Closed => ApprovalFact::Closed,
    }
}

/// A failure an outside writer recorded on the Invocation that prevents its
/// continuation: a declined or expired decision, a disabled Tool, lost
/// authority.
pub(super) async fn committed_failure(
    tx: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
) -> Result<Option<CoralFailure>, Error> {
    let failure =
        invocation_repository::completed_failure(tx, &admitted.workspace_id, &admitted.id).await?;
    Ok(failure.filter(|failure| prevents_continuation(*failure)))
}

pub(super) fn prevents_continuation(failure: CoralFailure) -> bool {
    matches!(
        failure,
        CoralFailure::ApprovalDeclined
            | CoralFailure::ApprovalExpired
            | CoralFailure::ToolDisabled
            | CoralFailure::AuthorityLost
    )
}

pub(super) async fn close_for_invocation(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    invocation: &InvocationId,
    now: i64,
) -> Result<(), Error> {
    let row = approval_repository::find_for_invocation(tx, workspace, invocation).await?;
    if let Some(mut row) = row
        && row.state == ApprovalState::Pending
    {
        transition(
            tx,
            workspace,
            &mut row,
            ApprovalState::Closed,
            now,
            None,
            &TraceId::current_or_new(),
        )
        .await?;
    }
    Ok(())
}

/// What the Run Owner decided for one Approval Request, as the pure `apply`
/// decided it in mailbox order.
enum Decided {
    /// A human's decision, recorded as made.
    Decision(DecisionRequest),
    /// The request is closed: the Run no longer accepted actions when the
    /// decision was applied.
    Close(DecisionRequest),
    /// The request's deadline passed (its timer fired): it expires, and its
    /// Invocation ends.
    Expire,
}

/// One decision as the Run Owner records it in a chunk: the Approval
/// Request's transition and its lifecycle event, and the end of a declined
/// or expired Invocation. A request no longer pending records nothing: the
/// first decision won, and the record answers. The owner runs its decisions
/// one at a time, so the read before the write decides every outcome the
/// request can have; a write that still fails with a definite answer fails
/// the chunk, as any row work's does. Nothing here compares the clock with
/// the request's deadline: the owner applied the expiry when its timer
/// fired, or a decision came first.
pub(crate) struct DecideWork {
    approval: ApprovalId,
    decided: Decided,
}

impl DecideWork {
    /// A human's decision; `decide: false` closes the request instead, the
    /// Run no longer accepting actions when the owner applied it.
    pub(crate) fn decision(request: DecisionRequest, decide: bool) -> Self {
        Self {
            approval: request.approval.clone(),
            decided: if decide {
                Decided::Decision(request)
            } else {
                Decided::Close(request)
            },
        }
    }

    /// The owner's timer for the request fired: the request expires.
    pub(crate) fn expiry(approval: ApprovalId) -> Self {
        Self {
            approval,
            decided: Decided::Expire,
        }
    }

    async fn record(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let row = approval_repository::load(tx, &run.workspace_id, &self.approval).await;
        let mut row = match row {
            Ok(row) => row,
            Err(Error::ApprovalNotFound) => return Ok(Vec::new()),
            Err(error) => return Err(error),
        };
        if row.state != ApprovalState::Pending {
            return Ok(Vec::new());
        }
        let (state, decided_at, human, trace) = match &self.decided {
            Decided::Decision(request) => (
                request.decision.state(),
                now,
                Some((&request.actor, request.comment.as_deref())),
                request.trace.clone(),
            ),
            Decided::Close(request) => (ApprovalState::Closed, now, None, request.trace.clone()),
            // The expiry is dated at the deadline, not when the owner
            // recorded it.
            Decided::Expire => (
                ApprovalState::Expired,
                row.deadline_at_unix_nanos,
                None,
                TraceId::current_or_new(),
            ),
        };
        transition(
            tx,
            &run.workspace_id,
            &mut row,
            state,
            decided_at,
            human,
            &trace,
        )
        .await?;
        Ok(settle_rejection(tx, run, &row, now)
            .await?
            .into_iter()
            .collect())
    }
}

impl std::fmt::Debug for DecideWork {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut work = formatter.debug_struct("DecideWork");
        work.field("approval", &self.approval);
        // The request's comment is never printed.
        match &self.decided {
            Decided::Decision(request) => work.field("decision", &request.decision),
            Decided::Close(_) => work.field("decided", &"close"),
            Decided::Expire => work.field("decided", &"expire"),
        };
        work.finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ChunkWork for DecideWork {
    async fn run(
        &mut self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        self.record(tx, run, now_unix_nanos.max(run.accepted_at_unix_nanos))
            .await
    }

    /// The requester reads the record once the chunk deleted its row.
    fn settle(self: Box<Self>, _recorded: Result<(), Error>) {}
}

/// Ends the Invocation of a declined or expired request and returns its
/// `invocation.ended`; a request in any other state ends nothing.
async fn settle_rejection(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    row: &StoredApproval,
    now: i64,
) -> Result<Option<OwnedEvent>, Error> {
    let failure = match row.state {
        ApprovalState::Declined => CoralFailure::ApprovalDeclined,
        ApprovalState::Expired => CoralFailure::ApprovalExpired,
        _ => return Ok(None),
    };
    crate::invocation_lifecycle::finish_approval_failure(tx, run, &row.invocation_id, failure, now)
        .await
}

async fn transition(
    tx: &mut Transaction<'_>,
    workspace: &WorkspaceId,
    row: &mut StoredApproval,
    state: ApprovalState,
    now: i64,
    human: Option<(&Principal, Option<&str>)>,
    trace: &TraceId,
) -> Result<(), Error> {
    let actor = human.map(|(actor, _)| actor);
    let comment = human.and_then(|(_, comment)| comment);
    if !approval_repository::update_pending(tx, workspace, &row.id, state, now, human).await? {
        return Err(Error::InvocationPersistence(
            "Approval decision found its request decided".into(),
        ));
    }
    approval_repository::insert_lifecycle_event(tx, workspace, &row.id, state, now, actor, trace)
        .await?;
    row.state = state;
    row.decided_at_unix_nanos = Some(now);
    row.approver = actor.cloned();
    row.comment = comment.map(str::to_string);
    Ok(())
}
