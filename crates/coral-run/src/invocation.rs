//! Run-bound Operation Invocation admission, dispatch fence, and completion,
//! as Run Requests to the Run Owner.
//!
//! The [`OperationInvoker`] is private to one accepted Program Run. It resolves
//! a Call Path outside the Run, admits it as one owner request (answered at
//! Applied: the Invocation row and `invocation.started` ride the owner's next
//! chunk), plans away from persistence, runs its held rechecks and the one
//! short dispatch fence as row work inside the owner's chunk commits, decrypts
//! and sends outside every lock, and reports the terminal Invocation through
//! the owner: an upstream result at Applied, a known failure once it is
//! recorded. Operational failure is host-fatal rather than catchable. Known
//! failures persist the same closed [`CoralFailure`] code and message
//! projected to the Program, even during child shutdown. Only shutdown and
//! recovery record lost dispatching work as indeterminate; a stop that
//! reaches the owner before a fenced Invocation sends ends it as
//! `run_terminated` instead.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_policy::{OperationAccessState, OperationPolicyTarget};
use coral_provider::access::ProviderNamespace;
use coral_provider::{InvocationOperationVerification, ResolvedInvocationOperation};
use coral_upstream::{McpRejection, McpResources, ReadyDispatch, UpstreamOutcome};
use coral_workspace::{AuthenticatedPrincipal, Principal, PrincipalWorkspaceEvidence};
use coral_workspace::{WorkspaceId, WorkspaceModule};
use tokio::sync::oneshot;

use crate::engine::Clock;
use crate::execution::ExecutionStop;
use crate::invocation_lifecycle::{FinishOutcome, InvocationState, UnfinishedInvocation};
use crate::lifecycle::RunRecord;
use crate::owner::{ChunkWork, OwnerHandle};
use crate::run_repository::{OwnedEvent, OwnedEventKind};
use crate::types::AcceptedRun;
use crate::{
    CoralFailure, Error, InvocationId, InvocationResultKind, RunId, invocation_repository,
};

pub(crate) enum UpstreamSendError {
    DestinationRejected,
    InputTooLarge,
    TooLarge,
    /// No complete result arrived: transport, protocol, or an unusable result.
    Incomplete,
    /// The peer answered the request with a JSON-RPC error.
    Rejected(McpRejection),
    Canceled,
    Fatal(coral_upstream::Error),
}

#[async_trait]
pub(crate) trait UpstreamSender: Send + Sync {
    async fn send(
        &self,
        dispatch: ReadyDispatch,
        stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, UpstreamSendError>;
}

pub(crate) struct ProductionUpstream {
    http: coral_upstream::HttpUpstream,
    mcp: coral_upstream::McpUpstream,
}

impl ProductionUpstream {
    pub(crate) fn new(policy: coral_outbound::DestinationPolicy) -> Result<Self, Error> {
        Ok(Self {
            http: coral_upstream::HttpUpstream::new(policy)?,
            mcp: coral_upstream::McpUpstream::new(policy)?,
        })
    }
}

#[async_trait]
impl UpstreamSender for ProductionUpstream {
    async fn send(
        &self,
        dispatch: ReadyDispatch,
        stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, UpstreamSendError> {
        // A stop that is already set never launches the upstream side effect;
        // one that arrives during the send loses to a complete outcome.
        if stop.is_requested() {
            return Err(UpstreamSendError::Canceled);
        }
        let send = async {
            match dispatch {
                ReadyDispatch::Http(ready) => {
                    self.http.send(*ready).await.map(UpstreamOutcome::Http)
                }
                ReadyDispatch::Mcp(ready) => self.mcp.send(*ready).await.map(UpstreamOutcome::Mcp),
            }
        };
        tokio::select! {
            biased;
            outcome = send => outcome.map_err(|error| match error {
                coral_upstream::Error::DestinationRejected => UpstreamSendError::DestinationRejected,
                coral_upstream::Error::InputTooLarge => UpstreamSendError::InputTooLarge,
                coral_upstream::Error::ResultTooLarge => UpstreamSendError::TooLarge,
                coral_upstream::Error::McpRejected(rejection) => {
                    UpstreamSendError::Rejected(rejection)
                }
                coral_upstream::Error::Send(_)
                | coral_upstream::Error::Body(_)
                | coral_upstream::Error::McpRequest
                | coral_upstream::Error::McpProtocolChanged
                | coral_upstream::Error::McpResult => UpstreamSendError::Incomplete,
                private_defect => UpstreamSendError::Fatal(private_defect),
            }),
            () = stop.requested() => Err(UpstreamSendError::Canceled),
        }
    }
}

#[cfg(test)]
#[async_trait]
pub(crate) trait InvocationObserver: Send + Sync {
    fn approval_authority_checked(&self) {}

    fn fence_checks_loaded(&self) {}

    fn fence_attempt_finished(&self, _permit_returned: bool, _durable_dispatch_start: bool) {}

    async fn approval_waiting(&self) -> Result<(), Error> {
        Ok(())
    }

    fn change_wait_started(&self) {}

    async fn before_fence(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn before_failure_commit(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn before_terminal_commit(&self) -> Result<(), String> {
        Ok(())
    }

    fn invocation_failure_committed(&self, _code: &'static str) {}

    fn run_completion_checked(&self, _has_nonterminal_invocation: bool) {}
}

#[cfg(test)]
pub(crate) struct NoopInvocationObserver;

#[cfg(test)]
#[async_trait]
impl InvocationObserver for NoopInvocationObserver {}

pub(crate) enum InvocationHostError {
    Rejected(CoralFailure),
    /// A complete, durably retained upstream outcome that is not a declared success.
    Upstream {
        rejection: crate::error::UpstreamRejection,
        outcome_json: String,
    },
    Fatal(Error),
}

impl From<Error> for InvocationHostError {
    fn from(error: Error) -> Self {
        match error {
            Error::InvocationRejected(message) => Self::Rejected(message),
            fatal => Self::Fatal(fatal),
        }
    }
}

/// Exact immutable Run context privately retained by one Operation invoker.
#[derive(Clone)]
pub(crate) struct OperationInvoker {
    accounts: Option<coral_provider::account::AccountModule>,
    database: Arc<coral_db::Database>,
    workspaces: WorkspaceModule,
    actor: AuthenticatedPrincipal,
    run_id: RunId,
    workspace_id: WorkspaceId,
    principal: Principal,
    accepted_at_unix_nanos: i64,
    deadline_at_unix_nanos: i64,
    limits: coral_value::ValueLimits,
    owner: OwnerHandle,
    stop: Arc<ExecutionStop>,
    mcp_resources: Arc<McpResources>,
    upstream: Arc<dyn UpstreamSender>,
    clock: Arc<dyn Clock>,
    #[cfg(test)]
    observer: Arc<dyn InvocationObserver>,
}

/// Longest a waiting Invocation goes without a snapshot recheck.
const WAIT_FALLBACK_POLL: std::time::Duration = std::time::Duration::from_secs(5);
/// Shortest gap between rechecks of one waiting Invocation.
const WAIT_MIN_RECHECK_GAP: std::time::Duration = std::time::Duration::from_millis(500);
/// Longest a waiting Invocation goes without a held recheck, for anything a
/// snapshot cannot see.
const WAIT_HELD_RECHECK: std::time::Duration = std::time::Duration::from_secs(30);

/// What a waiting Invocation waits for between held rechecks.
#[derive(Clone, Copy)]
enum PendingWait {
    Connection,
    Approval,
}

async fn next_change(
    changes: &mut Option<coral_db::ChangeSubscription>,
) -> Result<(), coral_db::Error> {
    match changes {
        Some(subscription) => subscription.changed().await,
        None => std::future::pending().await,
    }
}

impl OperationInvoker {
    #[expect(
        clippy::too_many_arguments,
        reason = "the private Run-bound invoker retains every dispatch-authority fact explicitly"
    )]
    pub(crate) fn new(
        accounts: Option<coral_provider::account::AccountModule>,
        database: Arc<coral_db::Database>,
        workspaces: WorkspaceModule,
        actor: AuthenticatedPrincipal,
        accepted: &AcceptedRun,
        owner: OwnerHandle,
        upstream: Arc<dyn UpstreamSender>,
        clock: Arc<dyn Clock>,
        #[cfg(test)] observer: Arc<dyn InvocationObserver>,
    ) -> Self {
        Self {
            accounts,
            database,
            workspaces,
            actor,
            run_id: accepted.id.clone(),
            workspace_id: accepted.workspace_id.clone(),
            principal: accepted.principal.clone(),
            accepted_at_unix_nanos: accepted.accepted_at_unix_nanos,
            deadline_at_unix_nanos: accepted.deadline_at_unix_nanos,
            limits: accepted.value_limits,
            stop: owner.stop(),
            owner,
            mcp_resources: Arc::new(McpResources::default()),
            upstream,
            clock,
            #[cfg(test)]
            observer,
        }
    }

    pub(crate) async fn invoke(
        &self,
        call_path: &str,
        arguments: serde_json::Value,
    ) -> Result<String, InvocationHostError> {
        let arguments_json = serde_json::to_string(&arguments).map_err(Error::ProgramValueJson)?;
        self.limits
            .validate_json(&arguments_json)
            .map_err(|error| {
                InvocationHostError::Rejected(match error {
                    coral_value::ValueError::Invalid => CoralFailure::InvalidInput,
                    coral_value::ValueError::TooLarge => CoralFailure::InputTooLarge,
                })
            })?;
        let address = CallPath::parse(call_path)?;
        let (target, policy) = self.resolve(&address).await?;
        let admitted = Arc::new(AdmittedInvocation {
            id: InvocationId::generate(),
            workspace_id: self.workspace_id.clone(),
            run_id: self.run_id.clone(),
            target,
            call_path: call_path.to_string(),
            arguments_json,
            admitted_at_unix_nanos: self.now()?.max(self.accepted_at_unix_nanos),
        });
        self.owner
            .admit(Box::new(AdmissionWork {
                admitted: admitted.clone(),
            }))
            .await?;
        let mut progress = InvocationProgress::default();
        if policy == OperationAccessState::Disabled {
            return Err(self
                .finish_failure(&admitted, &progress, CoralFailure::OperationDisabled)
                .await?);
        }
        let mut ready = match self.plan(&admitted.target, &arguments) {
            Ok(ready) => ready,
            Err(error) => {
                let failure = match error {
                    coral_upstream::Error::DestinationRejected => {
                        CoralFailure::OutboundDestinationRejected
                    }
                    coral_upstream::Error::InputTooLarge => CoralFailure::InputTooLarge,
                    coral_upstream::Error::InvalidArguments(_)
                    | coral_upstream::Error::UnsupportedRequestMedia(_) => {
                        CoralFailure::InvalidInput
                    }
                    coral_upstream::Error::OperationUnavailable => {
                        CoralFailure::OperationUnavailable
                    }
                    private_error => return Err(Error::Upstream(private_error).into()),
                };
                return Err(self.finish_failure(&admitted, &progress, failure).await?);
            }
        };
        match self.resolve_credentials(&admitted, &mut progress).await {
            Ok(()) => {}
            Err(Error::InvocationRejected(failure)) => {
                return Err(self.finish_failure(&admitted, &progress, failure).await?);
            }
            Err(error) => return Err(error.into()),
        }
        #[cfg(test)]
        self.observer.before_fence().await?;
        loop {
            let outcome = self.fence(&admitted, &mut progress).await?;
            #[cfg(test)]
            self.observe_fence_attempt(&admitted, &outcome).await?;
            match outcome {
                FenceOutcome::Permitted => break,
                FenceOutcome::Rejected(rejection) => {
                    return Err(self.finish_failure(&admitted, &progress, rejection).await?);
                }
                FenceOutcome::RunTerminated => {
                    return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
                }
                FenceOutcome::RefreshRequired => {
                    // No dispatch start exists. Prepare the same authorization again,
                    // outside every lock; never reselect an Account or replay JS.
                    if let Err(error) = self.prepare_credentials(&admitted, &progress).await {
                        return match error {
                            Error::InvocationRejected(failure) => {
                                Err(self.finish_failure(&admitted, &progress, failure).await?)
                            }
                            error => Err(error.into()),
                        };
                    }
                    ready = self
                        .plan(&admitted.target, &arguments)
                        .map_err(Error::from)?;
                }
            }
        }
        self.dispatch_and_finish(&admitted, &progress, ready).await
    }

    /// Resolves the Call Path's current Operation and policy, outside the Run:
    /// the fence rechecks both before any dispatch.
    async fn resolve(
        &self,
        address: &CallPath,
    ) -> Result<(ResolvedInvocationOperation, OperationAccessState), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(target) = coral_provider::resolve_invocation_operation_in_transaction(
            &mut transaction,
            &self.workspace_id,
            &address.namespace,
            &address.relative_path,
        )
        .await?
        else {
            transaction.rollback().await?;
            return Err(Error::InvocationRejected(CoralFailure::OperationNotFound));
        };
        let policy = coral_policy::invocation_decision_in_transaction(
            &mut transaction,
            &policy_target(&self.workspace_id, &target),
            target.operation().hints(),
            target.operation().http_method(),
        )
        .await?
        .access_state();
        transaction.commit().await?;
        Ok((target, policy))
    }

    #[cfg(test)]
    async fn observe_fence_attempt(
        &self,
        admitted: &AdmittedInvocation,
        outcome: &FenceOutcome,
    ) -> Result<(), Error> {
        // Observe the committed fact outside the fence transaction, not an
        // in-memory flag or an assertion against a persistence table layout.
        let started = invocation_repository::has_dispatch_start(
            &self.database,
            &self.workspace_id,
            &self.run_id,
            &admitted.id,
        )
        .await?;
        self.observer
            .fence_attempt_finished(matches!(outcome, FenceOutcome::Permitted), started);
        Ok(())
    }

    async fn dispatch_and_finish(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        mut ready: ReadyDispatch,
    ) -> Result<String, InvocationHostError> {
        if let Err(error) = self.authorize(admitted, progress, &mut ready).await {
            return match error {
                Error::InvocationRejected(failure) => {
                    Err(self.finish_failure(admitted, progress, failure).await?)
                }
                error => Err(error.into()),
            };
        }
        // The owner's own terminal state, before anything leaves the process:
        // a stop that reached the owner ends this fenced Invocation as
        // `run_terminated` instead of sending.
        if self.owner.is_stopping() {
            self.end_before_send(admitted).await?;
            return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
        }
        let outcome = match self.upstream.send(ready, self.stop.clone()).await {
            Ok(outcome) => outcome,
            Err(UpstreamSendError::Canceled) => {
                return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
            }
            Err(UpstreamSendError::DestinationRejected) => {
                return Err(self
                    .finish_failure(
                        admitted,
                        progress,
                        CoralFailure::OutboundDestinationRejected,
                    )
                    .await?);
            }
            Err(UpstreamSendError::InputTooLarge) => {
                return Err(self
                    .finish_failure(admitted, progress, CoralFailure::InputTooLarge)
                    .await?);
            }
            Err(UpstreamSendError::TooLarge) => {
                return Err(self
                    .finish_failure(admitted, progress, CoralFailure::ResultTooLarge)
                    .await?);
            }
            Err(UpstreamSendError::Fatal(private_defect)) => {
                return Err(Error::Upstream(private_defect).into());
            }
            Err(UpstreamSendError::Incomplete) => {
                return Err(self
                    .finish_failure(admitted, progress, CoralFailure::UpstreamIncomplete)
                    .await?);
            }
            Err(UpstreamSendError::Rejected(rejection)) => {
                return Err(self
                    .finish_failure(
                        admitted,
                        progress,
                        CoralFailure::from_mcp_rejection(rejection),
                    )
                    .await?);
            }
        };
        let result_json =
            serde_json::to_string(&outcome.operation_result()).map_err(Error::ProgramValueJson)?;
        if result_json.len() > self.limits.upstream_bytes()
            || self.limits.validate_json(&result_json).is_err()
        {
            return Err(self
                .finish_failure(admitted, progress, CoralFailure::ResultTooLarge)
                .await?);
        }
        match self.finish_upstream(admitted, progress, &result_json).await {
            Ok(()) => {}
            Err(Error::InvocationRejected(CoralFailure::RunOutputTooLarge)) => {
                return Err(self
                    .finish_failure(admitted, progress, CoralFailure::RunOutputTooLarge)
                    .await?);
            }
            Err(error) => return Err(error.into()),
        }
        match crate::error::UpstreamRejection::of(outcome.class()) {
            None => Ok(result_json),
            Some(rejection) => Err(InvocationHostError::Upstream {
                rejection,
                outcome_json: result_json,
            }),
        }
    }

    /// Decrypts the selected credentials outside every lock, after the fence,
    /// and authorizes the dispatch with them. A token renewed since the fence
    /// is the one dispatched.
    async fn authorize(
        &self,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        ready: &mut ReadyDispatch,
    ) -> Result<(), Error> {
        let Some(evidence) = &progress.credential_evidence else {
            return Ok(());
        };
        let accounts = self
            .accounts
            .as_ref()
            .ok_or(Error::InvocationRejected(CoralFailure::AccountRequired))?;
        let secret = tokio::select! {
            biased;
            () = self.stop.requested() => return Err(Error::ProgramHostTerminated),
            result = accounts.credentials(
                &self.workspace_id,
                admitted.target.provider_access().id(),
                evidence,
                self.now()?,
            ) => result.map_err(account_error)?,
        };
        let authorization = match ready {
            ReadyDispatch::Http(http) => http.authorize(&secret),
            ReadyDispatch::Mcp(mcp) => mcp.authorize(&secret),
        };
        authorization.map_err(|error| {
            Error::InvocationRejected(if matches!(error, coral_upstream::Error::InputTooLarge) {
                CoralFailure::InputTooLarge
            } else {
                CoralFailure::AccountRequired
            })
        })
    }

    fn plan(
        &self,
        target: &ResolvedInvocationOperation,
        arguments: &serde_json::Value,
    ) -> Result<ReadyDispatch, coral_upstream::Error> {
        ReadyDispatch::plan(
            target.operation(),
            target.schema_graph(),
            arguments,
            &target.provider_access().id().to_string(),
            self.mcp_resources.clone(),
            self.limits,
        )
    }

    async fn resolve_credentials(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &mut InvocationProgress,
    ) -> Result<(), Error> {
        // Subscribe before the first check so a commit during it still wakes this wait.
        // A wake-up is only a hint: the held recheck reads everything again.
        let mut changes = self.database.subscribe_changes().ok();
        loop {
            match self.select_credentials(admitted, progress.clone()).await? {
                SelectionOutcome::Ready(selected) => {
                    *progress = selected;
                    break;
                }
                SelectionOutcome::Waiting(selected) => {
                    *progress = selected;
                    self.wait_until_changed(
                        &mut changes,
                        admitted,
                        progress,
                        PendingWait::Connection,
                    )
                    .await?;
                }
                SelectionOutcome::Rejected(failure) => {
                    return Err(Error::InvocationRejected(failure));
                }
                SelectionOutcome::RunNotCurrent => return Err(Error::ProgramHostTerminated),
            }
        }
        loop {
            match self.approval_ready(admitted, progress).await? {
                GateOutcome::Ready => break,
                GateOutcome::Waiting => {
                    #[cfg(test)]
                    self.observer.approval_waiting().await?;
                    // No transaction or credential material is held while a human decides.
                    self.wait_until_changed(
                        &mut changes,
                        admitted,
                        progress,
                        PendingWait::Approval,
                    )
                    .await?;
                }
                GateOutcome::Rejected(failure) => return Err(Error::InvocationRejected(failure)),
                GateOutcome::RunNotCurrent => return Err(Error::ProgramHostTerminated),
            }
        }
        self.prepare_credentials(admitted, progress).await
    }

    /// Waits until a read snapshot shows that a held recheck could change this wait.
    ///
    /// A held recheck runs inside the owner's chunk commit and locks the
    /// Workspace, User, Provider, policy and Account rows, so waiting
    /// Invocations must not repeat it while nothing changed. Snapshots take no
    /// locks. A held recheck still runs at least every [`WAIT_HELD_RECHECK`]
    /// for anything a snapshot cannot see; the dispatch fence rechecks
    /// authority and approval regardless.
    async fn wait_until_changed(
        &self,
        changes: &mut Option<coral_db::ChangeSubscription>,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        wait: PendingWait,
    ) -> Result<(), Error> {
        let held_at = tokio::time::Instant::now();
        // The held recheck just ran, so this snapshot only finds the next expiry.
        // A snapshot that disagrees with it still waits before the next held recheck.
        let mut until = self.pending_until(admitted, progress, wait).await;
        loop {
            self.wait_for_recheck(changes, until).await?;
            if held_at.elapsed() >= WAIT_HELD_RECHECK {
                return Ok(());
            }
            until = self.pending_until(admitted, progress, wait).await;
            if until.is_none() {
                return Ok(());
            }
        }
    }

    /// Returns when a read snapshot shows this wait unchanged: the earliest time
    /// it expires without a commit. Any other answer, including a failure, leaves
    /// the outcome to the held recheck.
    async fn pending_until(
        &self,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        wait: PendingWait,
    ) -> Option<i64> {
        self.pending_until_in_snapshot(admitted, progress, wait)
            .await
            .ok()
            .flatten()
    }

    async fn pending_until_in_snapshot(
        &self,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        wait: PendingWait,
    ) -> Result<Option<i64>, Error> {
        let now = self.now()?;
        let mut tx = self.database.begin_read_snapshot().await?;
        let Some((policy, authority_until)) = self.snapshot_policy(&mut tx, admitted, now).await?
        else {
            return Ok(None);
        };
        let wait_until = match wait {
            PendingWait::Connection => match progress.connection_request {
                Some(request) if policy != OperationAccessState::Disabled => {
                    coral_provider::account::connection_pending_until_in_snapshot(
                        &mut tx,
                        &admitted.target,
                        &self.principal,
                        request,
                        now,
                    )
                    .await
                    .ok()
                    .flatten()
                }
                _ => None,
            },
            PendingWait::Approval => {
                if policy == OperationAccessState::ApprovalRequired
                    && coral_provider::account::verify_in_snapshot(
                        &mut tx,
                        &admitted.target,
                        &self.principal,
                        progress.credential_evidence.as_ref(),
                    )
                    .await
                    .is_ok()
                {
                    crate::approval::pending_until_in_snapshot(&mut tx, admitted, now).await?
                } else {
                    None
                }
            }
        };
        tx.commit().await?;
        Ok(wait_until.map(|until| until.min(authority_until)))
    }

    /// Reads, in a snapshot, what the held rechecks check before their own
    /// gate: the Run, a committed rejection, Principal authority, the Operation
    /// and its policy. While all of them are unchanged, returns the policy and
    /// the earliest time the Run or the authority expires.
    async fn snapshot_policy(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        admitted: &AdmittedInvocation,
        now: i64,
    ) -> Result<Option<(OperationAccessState, i64)>, Error> {
        let Some(run) =
            crate::run_repository::find_exact_in_transaction(tx, &self.workspace_id, &self.run_id)
                .await?
        else {
            return Ok(None);
        };
        if !self.run_is_current(&run, now)
            || crate::approval::committed_rejection(tx, admitted)
                .await
                .is_err()
        {
            return Ok(None);
        }
        let Ok(authority) = self
            .workspaces
            .check_dispatch_in_snapshot(tx, &self.actor, &self.workspace_id, now)
            .await
        else {
            return Ok(None);
        };
        if !matches!(
            coral_provider::verify_invocation_operation_in_snapshot(
                tx,
                &self.workspace_id,
                &admitted.target,
            )
            .await,
            Ok(InvocationOperationVerification::Current)
        ) {
            return Ok(None);
        }
        let until = authority
            .expires_at_unix_nanos()
            .map_or(self.deadline_at_unix_nanos, |expiry| {
                expiry.min(self.deadline_at_unix_nanos)
            });
        Ok(coral_policy::invocation_decision_in_snapshot(
            tx,
            &policy_target(&self.workspace_id, &admitted.target),
            admitted.target.operation().hints(),
            admitted.target.operation().http_method(),
        )
        .await
        .ok()
        .map(|decision| (decision.access_state(), until)))
    }

    /// Waits for a committed change, the expiry `until`, the fallback poll, or a
    /// stop request, at most once per [`WAIT_MIN_RECHECK_GAP`].
    async fn wait_for_recheck(
        &self,
        changes: &mut Option<coral_db::ChangeSubscription>,
        until: Option<i64>,
    ) -> Result<(), Error> {
        #[cfg(test)]
        self.observer.change_wait_started();
        let checked_at = tokio::time::Instant::now();
        // Expiry changes the wait without a commit.
        let now = self.now()?;
        let wake = checked_at
            + until.map_or(WAIT_FALLBACK_POLL, |until| {
                crate::duration_until(until, now).min(WAIT_FALLBACK_POLL)
            });
        tokio::select! {
            () = self.stop.requested() => return Err(Error::ProgramHostTerminated),
            result = next_change(changes) => if result.is_err() { *changes = None },
            () = tokio::time::sleep_until(wake) => {}
        }
        // Commits elsewhere wake every waiter. Coalesce bursts into bounded rechecks.
        tokio::select! {
            () = self.stop.requested() => Err(Error::ProgramHostTerminated),
            () = tokio::time::sleep_until(checked_at + WAIT_MIN_RECHECK_GAP) => Ok(()),
        }
    }

    async fn prepare_credentials(
        &self,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
    ) -> Result<(), Error> {
        let Some(evidence) = &progress.credential_evidence else {
            return Ok(());
        };
        let accounts = self
            .accounts
            .as_ref()
            .ok_or(Error::InvocationRejected(CoralFailure::AccountRequired))?;
        tokio::select! {
            biased;
            () = self.stop.requested() => Err(Error::ProgramHostTerminated),
            result = accounts.credentials(
                &self.workspace_id,
                admitted.target.provider_access().id(),
                evidence,
                self.now()?,
            ) => result.map(|_| ()).map_err(account_error),
        }
    }

    /// Queues `work` on the owner and returns its outcome once its chunk is
    /// recorded.
    async fn ask<O>(
        &self,
        build: impl FnOnce(Replier<O>) -> Box<dyn ChunkWork>,
    ) -> Result<O, Error> {
        let (sender, receiver) = oneshot::channel();
        self.owner.work(build(Replier::new(sender)))?;
        receiver.await.map_err(|_lost| Error::OwnerLost)?
    }

    async fn select_credentials(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: InvocationProgress,
    ) -> Result<SelectionOutcome, Error> {
        self.ask(|reply| {
            Box::new(SelectCredentialsWork {
                invoker: self.clone(),
                admitted: admitted.clone(),
                progress,
                reply,
            })
        })
        .await
    }

    async fn select_credentials_in(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
        admitted: &AdmittedInvocation,
        progress: InvocationProgress,
    ) -> Result<SelectionOutcome, Error> {
        catch_rejection(
            self.select_credentials_checked(tx, run, now, admitted, progress)
                .await,
            SelectionOutcome::Rejected,
        )
    }

    async fn select_credentials_checked(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
        admitted: &AdmittedInvocation,
        mut progress: InvocationProgress,
    ) -> Result<SelectionOutcome, Error> {
        use coral_provider::account::ConnectionResolution;
        if !self.run_is_current(run, now) {
            return Ok(SelectionOutcome::RunNotCurrent);
        }
        let (_, policy) = self.current_target_authority(tx, admitted).await?;
        if policy == OperationAccessState::Disabled {
            return Err(Error::InvocationRejected(CoralFailure::OperationDisabled));
        }
        let selection = if self.accounts.is_some() {
            coral_provider::account::resolve_connection_in_transaction(
                tx,
                &admitted.target,
                &self.principal,
                progress.connection_request,
                self.now()?,
            )
            .await
        } else {
            coral_provider::account::select_in_transaction(tx, &admitted.target, &self.principal)
                .await
                .map(ConnectionResolution::Ready)
        };
        // Expiry and request invalidation stay durable with the chunk even
        // though this Invocation ends with a catchable gate failure.
        let selection = selection.map_err(account_error)?;
        match selection {
            ConnectionResolution::Waiting(request) => {
                if progress.connection_request.is_none() {
                    invocation_repository::attach_connection(
                        tx,
                        &self.workspace_id,
                        &admitted.id,
                        request,
                        self.now()?,
                    )
                    .await?;
                }
                progress.connection_request = Some(request);
                Ok(SelectionOutcome::Waiting(progress))
            }
            ConnectionResolution::Ready(evidence) => {
                if let Some(evidence) = &evidence {
                    invocation_repository::retain_credentials(
                        tx,
                        &self.workspace_id,
                        &admitted.id,
                        evidence,
                    )
                    .await?;
                }
                progress.credential_evidence = evidence;
                Ok(SelectionOutcome::Ready(progress))
            }
        }
    }

    async fn approval_ready(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
    ) -> Result<GateOutcome, Error> {
        self.ask(|reply| {
            Box::new(ApprovalReadyWork {
                invoker: self.clone(),
                admitted: admitted.clone(),
                progress: progress.clone(),
                reply,
            })
        })
        .await
    }

    async fn approval_ready_in(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        events: &mut Vec<OwnedEvent>,
    ) -> Result<GateOutcome, Error> {
        catch_rejection(
            self.approval_ready_checked(tx, run, now, admitted, progress, events)
                .await,
            GateOutcome::Rejected,
        )
    }

    async fn approval_ready_checked(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        events: &mut Vec<OwnedEvent>,
    ) -> Result<GateOutcome, Error> {
        if !self.run_is_current(run, now) {
            return Ok(GateOutcome::RunNotCurrent);
        }
        crate::approval::committed_rejection(tx, admitted).await?;
        let (authority, policy) = self.current_target_authority(tx, admitted).await?;
        if policy == OperationAccessState::Disabled {
            return Err(Error::InvocationRejected(CoralFailure::OperationDisabled));
        }
        coral_provider::account::verify_in_transaction(
            tx,
            &admitted.target,
            &self.principal,
            progress.credential_evidence.as_ref(),
        )
        .await
        .map_err(account_error)?;
        #[cfg(test)]
        self.observer.approval_authority_checked();
        let now = self.now()?;
        if !self.run_is_current(run, now) {
            return Ok(GateOutcome::RunNotCurrent);
        }
        authority
            .require_current_at(now)
            .map_err(|_expired| Error::InvocationRejected(CoralFailure::AuthorityLost))?;
        let ready = crate::approval::gate(
            tx,
            admitted,
            run,
            policy == OperationAccessState::ApprovalRequired,
            now,
            events,
        )
        .await?;
        Ok(if ready {
            GateOutcome::Ready
        } else {
            GateOutcome::Waiting
        })
    }

    async fn fence(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &mut InvocationProgress,
    ) -> Result<FenceOutcome, Error> {
        let outcome = self
            .ask(|reply| {
                Box::new(FenceWork {
                    invoker: self.clone(),
                    admitted: admitted.clone(),
                    progress: progress.clone(),
                    reply,
                })
            })
            .await?;
        if matches!(outcome, FenceOutcome::Permitted) {
            progress.dispatch_started = true;
        }
        Ok(outcome)
    }

    /// The Dispatch Fence, inside the owner's chunk commit: one short held
    /// check of Run, authority, Operation, policy, Account and Approval
    /// evidence, then the durable dispatch start. It verifies the credentials
    /// are current without keeping their material; the dispatch decrypts
    /// outside every lock.
    async fn fence_in(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
    ) -> Result<FenceOutcome, Error> {
        catch_rejection(
            self.fence_checked(tx, run, now, admitted, progress).await,
            FenceOutcome::Rejected,
        )
    }

    async fn fence_checked(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        now: i64,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
    ) -> Result<FenceOutcome, Error> {
        let now = now.max(run.accepted_at_unix_nanos);
        if !self.run_is_current(run, now) {
            return Ok(FenceOutcome::RunTerminated);
        }
        let (authority, policy) = self.current_target_authority(tx, admitted).await?;
        if policy == OperationAccessState::Disabled {
            return Err(Error::InvocationRejected(CoralFailure::OperationDisabled));
        }
        coral_provider::account::verify_in_transaction(
            tx,
            &admitted.target,
            &self.principal,
            progress.credential_evidence.as_ref(),
        )
        .await
        .map_err(account_error)?;
        let approval = if policy == OperationAccessState::ApprovalRequired {
            Some(crate::approval::dispatch_evidence(tx, admitted).await?)
        } else {
            None
        };
        if let Some(evidence) = &progress.credential_evidence {
            let accounts = self
                .accounts
                .as_ref()
                .ok_or(Error::InvocationRejected(CoralFailure::AccountRequired))?;
            let current = accounts
                .dispatch_credentials_in_transaction(
                    tx,
                    &self.workspace_id,
                    admitted.target.provider_access().id(),
                    evidence,
                    self.now()?,
                )
                .await
                .map_err(account_error)?;
            // The material itself stays here: the dispatch decrypts outside
            // every lock.
            if current.is_none() {
                return Ok(FenceOutcome::RefreshRequired);
            }
        }
        #[cfg(test)]
        self.observer.fence_checks_loaded();
        // All mutable evidence is held now. Recheck every time-bound fact after
        // the final awaited read; the remaining write shares the held Run.
        let dispatch_started_at = self.now()?.max(run.accepted_at_unix_nanos);
        if !self.run_is_current(run, dispatch_started_at) {
            return Ok(FenceOutcome::RunTerminated);
        }
        authority
            .require_current_at(dispatch_started_at)
            .map_err(|_expired| Error::InvocationRejected(CoralFailure::AuthorityLost))?;
        if let Some(approval) = approval {
            approval
                .require_current_at(dispatch_started_at)
                .map_err(Error::InvocationRejected)?;
        }
        if !invocation_repository::mark_dispatch_started(
            tx,
            &self.workspace_id,
            &admitted.id,
            dispatch_started_at,
        )
        .await?
        {
            return Err(Error::InvocationRejected(CoralFailure::AuthorityLost));
        }
        Ok(FenceOutcome::Permitted)
    }

    async fn current_target_authority(
        &self,
        tx: &mut coral_db::Transaction<'_>,
        admitted: &AdmittedInvocation,
    ) -> Result<(PrincipalWorkspaceEvidence, OperationAccessState), Error> {
        let authority = self
            .workspace_dispatch_authority(tx)
            .await?
            .ok_or(Error::InvocationRejected(CoralFailure::AuthorityLost))?;
        match coral_provider::verify_invocation_operation_in_transaction(
            tx,
            &self.workspace_id,
            &admitted.target,
        )
        .await?
        {
            InvocationOperationVerification::Current => {}
            InvocationOperationVerification::CredentialRouteChanged => {
                return Err(Error::InvocationRejected(
                    CoralFailure::CredentialRouteChanged,
                ));
            }
            InvocationOperationVerification::ProviderUnavailable => {
                return Err(Error::InvocationRejected(CoralFailure::ProviderUnavailable));
            }
            InvocationOperationVerification::AuthorityLost => {
                return Err(Error::InvocationRejected(CoralFailure::AuthorityLost));
            }
        }
        let policy = coral_policy::invocation_decision_in_transaction(
            tx,
            &policy_target(&self.workspace_id, &admitted.target),
            admitted.target.operation().hints(),
            admitted.target.operation().http_method(),
        )
        .await
        .map_err(|error| match error {
            coral_policy::Error::OperationNotFound => {
                Error::InvocationRejected(CoralFailure::AuthorityLost)
            }
            error => error.into(),
        })?;
        Ok((authority, policy.access_state()))
    }

    async fn workspace_dispatch_authority(
        &self,
        transaction: &mut coral_db::Transaction<'_>,
    ) -> Result<Option<PrincipalWorkspaceEvidence>, Error> {
        match self
            .workspaces
            .check_dispatch_in_transaction(
                transaction,
                &self.actor,
                &self.workspace_id,
                self.now()?,
            )
            .await
        {
            Ok(evidence) => Ok(Some(evidence)),
            Err(
                coral_workspace::Error::WorkspaceNotFound
                | coral_workspace::Error::PermissionDenied(_)
                | coral_workspace::Error::WorkspaceArchived
                | coral_workspace::Error::AgentNotFound
                | coral_workspace::Error::AgentDisabled
                | coral_workspace::Error::AgentRegistrationExpired,
            ) => Ok(None),
            Err(error) => Err(error.into()),
        }
    }

    /// Reports an upstream result: the owner decides it against the Run
    /// Output Budget and answers at Applied; the row and `invocation.ended`
    /// ride its next chunk.
    async fn finish_upstream(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        result_json: &str,
    ) -> Result<(), Error> {
        let work = FinishWork {
            #[cfg(test)]
            observer: self.observer.clone(),
            admitted: admitted.clone(),
            dispatch_started: progress.dispatch_started,
            completion: InvocationCompletion::Upstream {
                result_json: result_json.to_string(),
            },
            reply: None,
        };
        self.owner
            .invocation_result(result_json.len(), Box::new(work))
            .await
    }

    /// Ends the Invocation with a known failure, recorded before the Program
    /// observes it.
    async fn finish_failure(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        failure: CoralFailure,
    ) -> Result<InvocationHostError, Error> {
        #[cfg(test)]
        self.observer.before_failure_commit().await?;
        let outcome = self
            .ask(|reply| {
                Box::new(FinishWork {
                    #[cfg(test)]
                    observer: self.observer.clone(),
                    admitted: admitted.clone(),
                    dispatch_started: progress.dispatch_started,
                    completion: InvocationCompletion::Failure(failure),
                    reply: Some(reply),
                })
            })
            .await?;
        match outcome {
            FinishOutcome::Finished(_) | FinishOutcome::SameFailure => {}
            // Deliver the durable human decision, not a worker's stale gate failure.
            FinishOutcome::Superseded(existing) => {
                return Err(Error::InvocationRejected(existing));
            }
            FinishOutcome::Lost => {
                return Err(Error::InvocationPersistence(
                    "Operation Invocation was already completed".to_string(),
                ));
            }
        }
        #[cfg(test)]
        self.observer.invocation_failure_committed(failure.code());
        Ok(InvocationHostError::Rejected(failure))
    }

    /// Ends a fenced Invocation that never sent as `run_terminated`.
    async fn end_before_send(&self, admitted: &Arc<AdmittedInvocation>) -> Result<(), Error> {
        self.ask(|reply| {
            Box::new(EndBeforeSendWork {
                admitted: admitted.clone(),
                reply,
            })
        })
        .await
    }

    fn run_is_current(&self, run: &RunRecord, now: i64) -> bool {
        run.program_work_is_current(self.owner.claim_id(), now)
            && run.principal == self.principal
            && run.deadline_at_unix_nanos == self.deadline_at_unix_nanos
    }

    fn now(&self) -> Result<i64, Error> {
        self.clock.now_unix_nanos().map_err(Error::Clock)
    }
}

/// Keeps a known failure as the step's outcome: inside the owner's chunk
/// transaction a rejection is a value the chunk records, not an error that
/// fails it.
fn catch_rejection<O>(
    result: Result<O, Error>,
    rejected: impl FnOnce(CoralFailure) -> O,
) -> Result<O, Error> {
    match result {
        Err(Error::InvocationRejected(failure)) => Ok(rejected(failure)),
        other => other,
    }
}

fn account_error(error: coral_provider::Error) -> Error {
    let failure = match error {
        coral_provider::Error::AccountRequired => CoralFailure::AccountRequired,
        coral_provider::Error::OutboundDestinationRejected => {
            CoralFailure::OutboundDestinationRejected
        }
        coral_provider::Error::OAuthClientConfiguration => CoralFailure::OAuthClientConfiguration,
        coral_provider::Error::CredentialRefreshFailed => CoralFailure::CredentialRefreshFailed,
        coral_provider::Error::AccountAuthorizationChanged => {
            CoralFailure::AccountAuthorizationChanged
        }
        coral_provider::Error::AccountConnectionDeclined => CoralFailure::AccountConnectionDeclined,
        coral_provider::Error::AccountConnectionExpired => CoralFailure::AccountConnectionExpired,
        coral_provider::Error::CredentialRouteChanged => CoralFailure::CredentialRouteChanged,
        other => return other.into(),
    };
    Error::InvocationRejected(failure)
}

fn policy_target(
    workspace_id: &WorkspaceId,
    target: &ResolvedInvocationOperation,
) -> OperationPolicyTarget {
    OperationPolicyTarget::new(
        workspace_id.clone(),
        target.provider_access().id().clone(),
        target.operation().key().clone(),
    )
}

struct CallPath {
    namespace: ProviderNamespace,
    relative_path: String,
}

impl CallPath {
    fn parse(value: &str) -> Result<Self, Error> {
        let Some(provider_path) = value.strip_prefix("coral.providers.") else {
            return Err(Error::InvocationRejected(CoralFailure::OperationNotFound));
        };
        let Some((namespace, relative_path)) = provider_path.split_once('.') else {
            return Err(Error::InvocationRejected(CoralFailure::OperationNotFound));
        };
        if relative_path.is_empty()
            || relative_path.chars().any(char::is_control)
            || relative_path.split('.').any(str::is_empty)
        {
            return Err(Error::InvocationRejected(CoralFailure::OperationNotFound));
        }
        let namespace = ProviderNamespace::parse(namespace)
            .map_err(|_error| Error::InvocationRejected(CoralFailure::OperationNotFound))?;
        Ok(Self {
            namespace,
            relative_path: relative_path.to_string(),
        })
    }
}

/// One admitted Invocation's immutable evidence.
#[derive(Debug)]
pub(crate) struct AdmittedInvocation {
    pub(crate) id: InvocationId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) run_id: RunId,
    pub(crate) target: ResolvedInvocationOperation,
    pub(crate) call_path: String,
    pub(crate) arguments_json: String,
    pub(crate) admitted_at_unix_nanos: i64,
}

/// What one Invocation's lifecycle has settled so far: its account wait, its
/// selected credentials, and whether its dispatch start is durable.
#[derive(Debug, Clone, Default)]
pub(crate) struct InvocationProgress {
    pub(crate) connection_request: Option<coral_provider::account::ConnectionRequestId>,
    pub(crate) credential_evidence: Option<coral_provider::account::CredentialEvidence>,
    pub(crate) dispatch_started: bool,
}

pub(crate) enum InvocationCompletion {
    Upstream { result_json: String },
    Failure(CoralFailure),
}

impl InvocationCompletion {
    pub(crate) const fn result_kind(&self) -> InvocationResultKind {
        match self {
            Self::Upstream { .. } => InvocationResultKind::UpstreamOutcome,
            Self::Failure(_) => InvocationResultKind::CoralFailure,
        }
    }
}

enum SelectionOutcome {
    Waiting(InvocationProgress),
    Ready(InvocationProgress),
    Rejected(CoralFailure),
    RunNotCurrent,
}

enum GateOutcome {
    Waiting,
    Ready,
    Rejected(CoralFailure),
    RunNotCurrent,
}

enum FenceOutcome {
    RefreshRequired,
    Permitted,
    Rejected(CoralFailure),
    RunTerminated,
}

/// Carries one step's outcome from the owner's commit transaction back to the
/// Invocation task once the chunk is recorded.
struct Replier<O> {
    outcome: Option<O>,
    sender: oneshot::Sender<Result<O, Error>>,
}

impl<O> Replier<O> {
    const fn new(sender: oneshot::Sender<Result<O, Error>>) -> Self {
        Self {
            outcome: None,
            sender,
        }
    }

    fn record(&mut self, outcome: O) {
        self.outcome = Some(outcome);
    }

    fn settle(self, recorded: Result<(), Error>) {
        // The Invocation task may have stopped waiting; the record stands.
        let _delivered = self
            .sender
            .send(recorded.and_then(|()| self.outcome.ok_or(Error::OwnerLost)));
    }
}

impl<O> fmt::Debug for Replier<O> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("Replier")
    }
}

/// The Invocation row and `invocation.started`, riding the owner's next chunk.
struct AdmissionWork {
    admitted: Arc<AdmittedInvocation>,
}

impl fmt::Debug for AdmissionWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("AdmissionWork")
            .field("invocation", &self.admitted.id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChunkWork for AdmissionWork {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        _run: &RunRecord,
        _now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        invocation_repository::insert_admission(transaction, &self.admitted).await?;
        Ok(vec![OwnedEvent::new(
            OwnedEventKind::InvocationStarted(self.admitted.id.clone()),
            self.admitted.admitted_at_unix_nanos,
        )])
    }

    fn settle(self: Box<Self>, _recorded: Result<(), Error>) {}
}

struct SelectCredentialsWork {
    invoker: OperationInvoker,
    admitted: Arc<AdmittedInvocation>,
    progress: InvocationProgress,
    reply: Replier<SelectionOutcome>,
}

impl fmt::Debug for SelectCredentialsWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SelectCredentialsWork")
            .field("invocation", &self.admitted.id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChunkWork for SelectCredentialsWork {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let outcome = self
            .invoker
            .select_credentials_in(
                transaction,
                run,
                now_unix_nanos,
                &self.admitted,
                self.progress.clone(),
            )
            .await?;
        self.reply.record(outcome);
        Ok(Vec::new())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        self.reply.settle(recorded);
    }
}

struct ApprovalReadyWork {
    invoker: OperationInvoker,
    admitted: Arc<AdmittedInvocation>,
    progress: InvocationProgress,
    reply: Replier<GateOutcome>,
}

impl fmt::Debug for ApprovalReadyWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ApprovalReadyWork")
            .field("invocation", &self.admitted.id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChunkWork for ApprovalReadyWork {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let mut events = Vec::new();
        let outcome = self
            .invoker
            .approval_ready_in(
                transaction,
                run,
                now_unix_nanos,
                &self.admitted,
                &self.progress,
                &mut events,
            )
            .await?;
        self.reply.record(outcome);
        Ok(events)
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        self.reply.settle(recorded);
    }
}

struct FenceWork {
    invoker: OperationInvoker,
    admitted: Arc<AdmittedInvocation>,
    progress: InvocationProgress,
    reply: Replier<FenceOutcome>,
}

impl fmt::Debug for FenceWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FenceWork")
            .field("invocation", &self.admitted.id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChunkWork for FenceWork {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let outcome = self
            .invoker
            .fence_in(
                transaction,
                run,
                now_unix_nanos,
                &self.admitted,
                &self.progress,
            )
            .await?;
        self.reply.record(outcome);
        Ok(Vec::new())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        self.reply.settle(recorded);
    }
}

/// The Invocation's completion: its row, its closed Approval Request, and
/// `invocation.ended`. An upstream result is answered at Applied, so it
/// carries no reply; a failure is answered once recorded.
struct FinishWork {
    #[cfg(test)]
    observer: Arc<dyn InvocationObserver>,
    admitted: Arc<AdmittedInvocation>,
    dispatch_started: bool,
    completion: InvocationCompletion,
    reply: Option<Replier<FinishOutcome>>,
}

impl fmt::Debug for FinishWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FinishWork")
            .field("invocation", &self.admitted.id)
            .field("result_kind", &self.completion.result_kind())
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChunkWork for FinishWork {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let ended_at = now_unix_nanos
            .max(run.accepted_at_unix_nanos)
            .max(self.admitted.admitted_at_unix_nanos);
        let outcome = crate::invocation_lifecycle::finish(
            transaction,
            &self.admitted,
            self.dispatch_started,
            &self.completion,
            ended_at,
        )
        .await?;
        #[cfg(test)]
        self.observer
            .before_terminal_commit()
            .await
            .map_err(Error::InvocationPersistence)?;
        let events = match (&outcome, &self.completion) {
            (FinishOutcome::Finished(event), _) => vec![event.clone()],
            (FinishOutcome::Lost, InvocationCompletion::Upstream { .. }) => {
                // The Program already continued on the applied result; a row
                // that is no longer dispatching means termination settled it.
                return Err(Error::InvocationPersistence(
                    "Program Run lost Invocation completion".to_string(),
                ));
            }
            (
                FinishOutcome::SameFailure | FinishOutcome::Superseded(_) | FinishOutcome::Lost,
                _,
            ) => Vec::new(),
        };
        if let Some(reply) = &mut self.reply {
            reply.record(outcome);
        }
        Ok(events)
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        if let Some(reply) = self.reply {
            reply.settle(recorded);
        }
    }
}

/// Ends a fenced Invocation the owner's stop reached before it sent.
struct EndBeforeSendWork {
    admitted: Arc<AdmittedInvocation>,
    reply: Replier<()>,
}

impl fmt::Debug for EndBeforeSendWork {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("EndBeforeSendWork")
            .field("invocation", &self.admitted.id)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl ChunkWork for EndBeforeSendWork {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let ended_at = now_unix_nanos.max(run.accepted_at_unix_nanos);
        let unfinished = UnfinishedInvocation {
            id: self.admitted.id.clone(),
            state: InvocationState::Dispatching,
        };
        // A row that is no longer dispatching was settled by the termination
        // itself; nothing is left to end.
        let ended = crate::invocation_lifecycle::end_for_termination(
            transaction,
            run,
            &unfinished,
            InvocationResultKind::RunTerminated,
            ended_at,
        )
        .await?;
        self.reply.record(());
        Ok(ended.into_iter().collect())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        self.reply.settle(recorded);
    }
}
