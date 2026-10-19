//! Exact Invocation approval and User-only decision authority.
//!
//! [`ApprovalModule`] lists, inspects and decides requests with canonical Workspace
//! evidence. A User sees only their own Invocations; Workspace Owners also see
//! Workspace-owned Agent Invocations. Agents cannot inspect or decide approvals.
//! The Invocation creates a request only after Account selection. Its immutable
//! target and once-selected credentials are the binding, not a second argument
//! snapshot or a caller-supplied approval target. Private Generation provenance
//! stays private. Approval validity is checked again inside the dispatch fence.
//!
//! Every transition holds the Run before Workspace authority, just like Invocation
//! and cancellation. The first terminal transition and its safe lifecycle event
//! commit together; a lost response can be recovered by reading or deciding again.
//! Invocation owns live gate ordering and Promise settlement. Approval owns only
//! request validity, decisions and evidence, not required-action projections or UI.

use crate::lifecycle::RunRecord;
use std::sync::Arc;

use coral_db::{Database, Transaction};
use coral_provider::ProviderId;
use coral_provider::access::CredentialRoute;
use coral_provider::account::ProviderAccountId;
use coral_workspace::TraceId;
use coral_workspace::{
    AuthenticatedPrincipal, Principal, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};
use uuid::Uuid;

use crate::approval_repository::{self, StoredApproval};
use crate::engine::{Clock, SystemClock};
use crate::invocation::AdmittedInvocation;
use crate::run_repository::OwnedEvent;
use crate::{
    CoralFailure, Error, InvocationId, RunId, RunState, invocation_repository, run_repository,
};

/// Host approval wait limit, further bounded by the accepted Run deadline.
const APPROVAL_WAIT_NANOS: i64 = 900_000_000_000;

/// Opaque, non-authorizing Approval Request identity.
#[derive(Debug, Clone, PartialEq, Eq)]
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
}
impl ApprovalModule {
    /// Builds the Approval module against the Run domain's migrated database.
    #[must_use]
    pub fn new(database: Arc<Database>, workspaces: WorkspaceModule) -> Self {
        Self {
            database,
            workspaces,
            clock: Arc::new(SystemClock),
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

    /// Commits the first authorized decision and safe lifecycle evidence together.
    ///
    /// Authentication supplies current Organization Membership; Workspace supplies
    /// the User's Workspace Membership authority. Comments are limited to 1024 UTF-8 bytes.
    /// Approval does not attest to any other current dispatch-authority fact.
    /// # Errors
    /// Conceals unauthorized targets; rejects invalid comments and persistence faults.
    pub async fn decide(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: &WorkspaceId,
        id: &ApprovalId,
        decision: ApprovalDecision,
        comment: Option<&str>,
        trace: &TraceId,
    ) -> Result<ApprovalRequest, Error> {
        if comment.is_some_and(|text| text.len() > 1024 || text.contains('\0')) {
            return Err(Error::InvalidInput(
                "approval comment must be at most 1024 bytes without null characters".into(),
            ));
        }
        let mut tx = self.database.begin().await?;
        // An approval never changes Run. Conceal it before holding that Run.
        let run_id = approval_repository::load(&mut tx, workspace, id)
            .await?
            .run_id;
        self.visible_run(&mut tx, actor, workspace, &run_id).await?;
        let Some(_) = run_repository::hold_run(&mut tx, workspace, &run_id).await? else {
            return Err(Error::ApprovalNotFound);
        };
        let run = self.visible_run(&mut tx, actor, workspace, &run_id).await?;
        let mut row = approval_repository::load(&mut tx, workspace, id).await?;
        if row.state == ApprovalState::Pending {
            let now = self.now()?;
            let authority = self
                .workspaces
                .check_principal_inspection_in_transaction(&mut tx, actor, workspace, now)
                .await
                .map_err(conceal)?;
            // Agent approval requires the live Membership role, not the role in
            // the request's earlier authentication context.
            if run.principal.kind() == coral_workspace::PrincipalKind::Agent
                && authority.effective_user_role() != Some(WorkspaceRole::Owner)
            {
                return Err(Error::ApprovalNotFound);
            }
            let target = invocation_repository::approval_target(
                &mut tx,
                workspace,
                &run.id,
                &row.invocation_id,
            )
            .await?;
            let gate = decision_gate(
                &mut tx,
                &self.workspaces,
                &run,
                &target,
                now,
                PolicyRead::Hold,
            )
            .await?;
            let state = if run.state != RunState::Running
                || run.termination.is_some()
                || now >= run.deadline_at_unix_nanos
            {
                ApprovalState::Closed
            } else if now >= row.deadline_at_unix_nanos {
                ApprovalState::Expired
            } else if !matches!(gate, DecisionGate::Required) {
                ApprovalState::Closed
            } else {
                match decision {
                    ApprovalDecision::Approve => ApprovalState::Approved,
                    ApprovalDecision::Decline => ApprovalState::Declined,
                }
            };
            let human = matches!(state, ApprovalState::Approved | ApprovalState::Declined);
            transition(
                &mut tx,
                workspace,
                &mut row,
                state,
                now,
                human.then_some((actor.principal(), comment)),
                trace,
            )
            .await?;
            settle_decision(&mut tx, &run, &row, state, gate, now).await?;
        }
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
enum DecisionGate {
    Required,
    NotRequired,
    Rejected(CoralFailure),
}

/// How a decision gate reads Operation policy.
#[derive(Clone, Copy)]
enum PolicyRead {
    /// Holds the policy until commit, as the worker and a human decision compete
    /// under the same Run/Workspace holds.
    Hold,
    /// Makes the identical decision without acquiring mutation locks.
    Inspect,
}

async fn decision_gate(
    tx: &mut Transaction<'_>,
    workspaces: &WorkspaceModule,
    run: &RunRecord,
    target: &invocation_repository::ApprovalTarget,
    now: i64,
    policy_read: PolicyRead,
) -> Result<DecisionGate, Error> {
    match workspaces
        .check_invoking_principal_in_snapshot(tx, &run.workspace_id, &run.principal, now)
        .await
    {
        Ok(()) => {}
        Err(
            coral_workspace::Error::WorkspaceNotFound
            | coral_workspace::Error::PermissionDenied(_)
            | coral_workspace::Error::WorkspaceArchived
            | coral_workspace::Error::AgentNotFound
            | coral_workspace::Error::AgentDisabled
            | coral_workspace::Error::AgentRegistrationExpired,
        ) => return Ok(DecisionGate::Rejected(CoralFailure::AuthorityLost)),
        Err(error) => return Err(error.into()),
    }
    let operation =
        coral_catalog::operation_in_transaction(tx, &target.generation, &target.operation)
            .await
            .map_err(coral_provider::Error::from)?
            .ok_or(Error::ApprovalNotFound)?;
    let policy_target = coral_policy::OperationPolicyTarget::new(
        run.workspace_id.clone(),
        target.provider_id.clone(),
        target.operation.clone(),
    );
    let policy = match policy_read {
        PolicyRead::Hold => {
            coral_policy::invocation_decision_in_transaction(
                tx,
                &policy_target,
                operation.hints(),
                operation.http_method(),
            )
            .await?
        }
        PolicyRead::Inspect => {
            coral_policy::discovery_decision_in_transaction(
                tx,
                &policy_target,
                operation.hints(),
                operation.http_method(),
            )
            .await?
        }
    };
    Ok(match policy.access_state() {
        coral_policy::OperationAccessState::ApprovalRequired => DecisionGate::Required,
        coral_policy::OperationAccessState::Enabled => DecisionGate::NotRequired,
        coral_policy::OperationAccessState::Disabled => {
            DecisionGate::Rejected(CoralFailure::OperationDisabled)
        }
    })
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
        && now < row.deadline_at_unix_nanos
        && crate::required_actions::run_accepts_actions(run, now)
        && matches!(
            decision_gate(tx, workspaces, run, &target, now, PolicyRead::Inspect).await?,
            DecisionGate::Required
        );
    let (provider_label, agent_label) =
        crate::inspection_repository::approval_labels(tx, workspace, &row.invocation_id).await?;
    let approver_name = if let Some(approver) = &row.approver {
        let user = actor.authenticated_user().ok_or(Error::ApprovalNotFound)?;
        let id = coral_workspace::UserId::parse(approver.id().as_str())?;
        match workspaces
            .inspect_user_profile_in_transaction(tx, user, workspace, &id)
            .await
        {
            Ok(profile) => profile.name.filter(|name| !name.trim().is_empty()),
            Err(coral_workspace::Error::MembershipNotFound) => None,
            Err(error) => return Err(error.into()),
        }
    } else {
        None
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

// Safe summary only: Agents observing their own Runs must not load approval detail.
pub(super) async fn pending_for_run(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    now: i64,
) -> Result<Vec<(ApprovalId, i64)>, Error> {
    approval_repository::pending_for_run(tx, &run.workspace_id, &run.id, now).await
}

// All private facets join the Run Owner's chunk transaction with its Run hold;
// an Invocation this settles appends its `invocation.ended` to `events`.
pub(super) async fn gate(
    tx: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    run: &RunRecord,
    required: bool,
    now: i64,
    events: &mut Vec<OwnedEvent>,
) -> Result<bool, Error> {
    let row =
        approval_repository::find_for_invocation(tx, &admitted.workspace_id, &admitted.id).await?;
    let mut row = if let Some(row) = row {
        row
    } else {
        if !required {
            return Ok(true);
        }
        let deadline = std::cmp::min(
            run.deadline_at_unix_nanos,
            now.saturating_add(APPROVAL_WAIT_NANOS),
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
    if row.state == ApprovalState::Pending {
        let state = if now >= row.deadline_at_unix_nanos {
            Some(ApprovalState::Expired)
        } else if !required {
            Some(ApprovalState::Closed)
        } else {
            None
        };
        if let Some(state) = state {
            transition(
                tx,
                &admitted.workspace_id,
                &mut row,
                state,
                now,
                None,
                &TraceId::current_or_new(),
            )
            .await?;
            events.extend(settle_rejection(tx, run, &row, now).await?);
        }
    }
    match row.state {
        ApprovalState::Pending => Ok(false),
        ApprovalState::Approved => {
            if required && now >= row.deadline_at_unix_nanos {
                Err(Error::InvocationRejected(
                    CoralFailure::ApprovalExpired.into(),
                ))
            } else {
                Ok(true)
            }
        }
        ApprovalState::Closed if !required => Ok(true),
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

/// Returns when this Invocation's approval expires while it is still pending
/// and unexpired, so a held [`gate`] would write nothing and keep waiting until then.
pub(super) async fn pending_until_in_snapshot(
    tx: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
    now: i64,
) -> Result<Option<i64>, Error> {
    let row =
        approval_repository::find_for_invocation(tx, &admitted.workspace_id, &admitted.id).await?;
    Ok(row
        .filter(|row| row.state == ApprovalState::Pending && now < row.deadline_at_unix_nanos)
        .map(|row| row.deadline_at_unix_nanos))
}

pub(super) async fn committed_rejection(
    tx: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
) -> Result<(), Error> {
    let failure =
        invocation_repository::completed_failure(tx, &admitted.workspace_id, &admitted.id).await?;
    match failure.filter(|failure| prevents_continuation(*failure)) {
        Some(failure) => Err(Error::InvocationRejected(failure.into())),
        None => Ok(()),
    }
}

pub(super) fn prevents_continuation(failure: CoralFailure) -> bool {
    matches!(
        failure,
        CoralFailure::ApprovalDeclined
            | CoralFailure::ApprovalExpired
            | CoralFailure::OperationDisabled
            | CoralFailure::AuthorityLost
    )
}

// The Run hold serializes every Approval decision. Load before sampling the
// fence time so a database wait cannot turn a once-valid approval into a permit.
pub(super) struct DispatchApproval(Option<StoredApproval>);

impl DispatchApproval {
    pub(super) fn require_current_at(&self, now: i64) -> Result<(), CoralFailure> {
        match &self.0 {
            Some(row)
                if row.state == ApprovalState::Approved && now < row.deadline_at_unix_nanos =>
            {
                Ok(())
            }
            Some(row)
                if row.state == ApprovalState::Expired || now >= row.deadline_at_unix_nanos =>
            {
                Err(CoralFailure::ApprovalExpired)
            }
            Some(row) if row.state == ApprovalState::Declined => {
                Err(CoralFailure::ApprovalDeclined)
            }
            _ => Err(CoralFailure::ApprovalRequired),
        }
    }
}

pub(super) async fn dispatch_evidence(
    tx: &mut Transaction<'_>,
    admitted: &AdmittedInvocation,
) -> Result<DispatchApproval, Error> {
    approval_repository::find_for_run_invocation(
        tx,
        &admitted.workspace_id,
        &admitted.run_id,
        &admitted.id,
    )
    .await
    .map(DispatchApproval)
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

/// Ends the Invocation a human decision rejected, or a closed request's
/// Invocation that the gate had already rejected. This decision still writes
/// outside the Run Owner (until slice 6): the Invocation's end is one chunk
/// of its own under the Run's claim.
async fn settle_decision(
    tx: &mut Transaction<'_>,
    run: &RunRecord,
    row: &StoredApproval,
    state: ApprovalState,
    gate: DecisionGate,
    now: i64,
) -> Result<(), Error> {
    let mut events = Vec::new();
    if state == ApprovalState::Closed
        && crate::required_actions::run_accepts_actions(run, now)
        && let DecisionGate::Rejected(failure) = gate
    {
        events.push(
            crate::invocation_lifecycle::finish_approval_failure(
                tx,
                run,
                &row.invocation_id,
                failure,
                now,
            )
            .await?,
        );
    } else {
        events.extend(settle_rejection(tx, run, row, now).await?);
    }
    crate::invocation_lifecycle::commit_outside_owner(tx, run, &events).await
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
        .map(Some)
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
            "Approval decision lost its Run hold".into(),
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
