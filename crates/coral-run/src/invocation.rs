//! Run-bound Operation Invocation admission, dispatch fence, and completion.
//!
//! The [`OperationInvoker`] is private to one accepted, claimed Program Run. It
//! atomically admits a resolved Call Path and `invocation.started`, plans away
//! from persistence, linearizes current authority at one short dispatch fence,
//! sends once, and commits the complete terminal Invocation plus
//! `invocation.ended` before returning JSON to the Program host. Operational
//! failure before that terminal commit is host-fatal rather than catchable.
//! Known failures persist the same closed [`CoralFailure`] code and message
//! projected to the Program, even during child shutdown. Only shutdown/recovery
//! records lost dispatching work as indeterminate; adapter cancellation supplies
//! no catchable failure.

use crate::lifecycle::RunRecord;
use std::sync::Arc;

use async_trait::async_trait;
use coral_policy::{OperationAccessState, OperationPolicyTarget};
use coral_provider::access::ProviderNamespace;
use coral_provider::{InvocationOperationVerification, ResolvedInvocationOperation};
use coral_upstream::{McpResources, ReadyDispatch, UpstreamOutcome};
use coral_workspace::{AuthenticatedPrincipal, Principal, PrincipalWorkspaceEvidence};
use coral_workspace::{WorkspaceId, WorkspaceModule};

use crate::engine::Clock;
use crate::execution::ExecutionStop;
use crate::{
    CoralFailure, Error, InvocationId, RunId, RunState, invocation_repository, run_repository,
};

pub(crate) enum UpstreamSendError {
    DestinationRejected,
    InputTooLarge,
    TooLarge,
    Incomplete,
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
    claim_id: String,
    deadline_at_unix_nanos: i64,
    limits: coral_value::ValueLimits,
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
        run_id: RunId,
        workspace_id: WorkspaceId,
        principal: Principal,
        claim_id: String,
        deadline_at_unix_nanos: i64,
        limits: coral_value::ValueLimits,
        stop: Arc<ExecutionStop>,
        upstream: Arc<dyn UpstreamSender>,
        clock: Arc<dyn Clock>,
        #[cfg(test)] observer: Arc<dyn InvocationObserver>,
    ) -> Self {
        Self {
            accounts,
            database,
            workspaces,
            actor,
            run_id,
            workspace_id,
            principal,
            claim_id,
            deadline_at_unix_nanos,
            limits,
            stop,
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
        let (mut admitted, policy) = self.admit(call_path, &address, arguments_json).await?;
        if policy == OperationAccessState::Disabled {
            return Err(self
                .finish_failure(&admitted, CoralFailure::OperationDisabled)
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
                return Err(self.finish_failure(&admitted, failure).await?);
            }
        };
        match self.resolve_credentials(&mut admitted).await {
            Ok(()) => {}
            Err(Error::InvocationRejected(failure)) => {
                return Err(self.finish_failure(&admitted, failure).await?);
            }
            Err(error) => return Err(error.into()),
        }
        #[cfg(test)]
        self.observer.before_fence().await?;
        let fenced = loop {
            let outcome = self.fence(&mut admitted, ready).await?;
            #[cfg(test)]
            self.observe_fence_attempt(&admitted, &outcome).await?;
            match outcome {
                FenceOutcome::Permitted(fenced) => break fenced,
                FenceOutcome::Rejected(rejection) => {
                    return Err(self.finish_failure(&admitted, rejection).await?);
                }
                FenceOutcome::RunTerminated => {
                    return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
                }
                FenceOutcome::RefreshRequired => {
                    // No dispatch start exists. Prepare the same authorization again,
                    // outside the transaction; never reselect an Account or replay JS.
                    if let Err(error) = self.prepare_credentials(&admitted).await {
                        return match error {
                            Error::InvocationRejected(failure) => {
                                Err(self.finish_failure(&admitted, failure).await?)
                            }
                            error => Err(error.into()),
                        };
                    }
                    ready = self
                        .plan(&admitted.target, &arguments)
                        .map_err(Error::from)?;
                }
            }
        };
        self.dispatch_and_finish(&admitted, fenced).await
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
            .fence_attempt_finished(matches!(outcome, FenceOutcome::Permitted(_)), started);
        Ok(())
    }

    async fn dispatch_and_finish(
        &self,
        admitted: &AdmittedInvocation,
        fenced: FencedDispatch,
    ) -> Result<String, InvocationHostError> {
        let outcome = match self
            .upstream
            .send(fenced.into_ready(), self.stop.clone())
            .await
        {
            Ok(outcome) => outcome,
            Err(UpstreamSendError::Canceled) => {
                return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
            }
            Err(UpstreamSendError::DestinationRejected) => {
                return Err(self
                    .finish_failure(admitted, CoralFailure::OutboundDestinationRejected)
                    .await?);
            }
            Err(UpstreamSendError::InputTooLarge) => {
                return Err(self
                    .finish_failure(admitted, CoralFailure::InputTooLarge)
                    .await?);
            }
            Err(UpstreamSendError::TooLarge) => {
                return Err(self
                    .finish_failure(admitted, CoralFailure::ResultTooLarge)
                    .await?);
            }
            Err(UpstreamSendError::Fatal(private_defect)) => {
                return Err(Error::Upstream(private_defect).into());
            }
            Err(UpstreamSendError::Incomplete) => {
                return Err(self
                    .finish_failure(admitted, CoralFailure::UpstreamIncomplete)
                    .await?);
            }
        };
        let result_json =
            serde_json::to_string(&outcome.operation_result()).map_err(Error::ProgramValueJson)?;
        if result_json.len() > self.limits.upstream_bytes()
            || self.limits.validate_json(&result_json).is_err()
        {
            return Err(self
                .finish_failure(admitted, CoralFailure::ResultTooLarge)
                .await?);
        }
        self.finish_upstream(admitted, &result_json).await?;
        Ok(result_json)
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

    async fn admit(
        &self,
        call_path: &str,
        address: &CallPath,
        arguments_json: String,
    ) -> Result<(AdmittedInvocation, OperationAccessState), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &self.workspace_id, &self.run_id).await?
        else {
            return Err(Error::InvocationPersistence(
                "Program Run no longer accepts Operations".to_string(),
            ));
        };
        let admitted_at = self.now()?.max(run.accepted_at_unix_nanos);
        if !self.run_is_current(&run, admitted_at) {
            transaction.rollback().await?;
            return Err(Error::InvocationPersistence(
                "Program Run no longer accepts Operations".to_string(),
            ));
        }
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
        let policy_target = policy_target(&self.workspace_id, &target);
        let policy = coral_policy::invocation_decision_in_transaction(
            &mut transaction,
            &policy_target,
            target.operation().hints(),
        )
        .await?
        .access_state();
        let admitted_at = self.now()?.max(run.accepted_at_unix_nanos);
        if !self.run_is_current(&run, admitted_at) {
            transaction.rollback().await?;
            return Err(Error::InvocationPersistence(
                "Program Run no longer accepts Operations".to_string(),
            ));
        }
        let admitted = AdmittedInvocation {
            id: InvocationId::generate(),
            workspace_id: self.workspace_id.clone(),
            run_id: self.run_id.clone(),
            claim_id: self.claim_id.clone(),
            target,
            call_path: call_path.to_string(),
            arguments_json,
            admitted_at_unix_nanos: admitted_at,
            dispatch_started: false,
            credential_evidence: None,
            connection_request: None,
        };
        invocation_repository::insert_admission(&mut transaction, &admitted, &run).await?;
        transaction.commit().await?;
        Ok((admitted, policy))
    }

    async fn resolve_credentials(&self, admitted: &mut AdmittedInvocation) -> Result<(), Error> {
        // Subscribe before the first check so a commit during it still wakes this wait.
        // A wake-up is only a hint: the held recheck reads everything again.
        let mut changes = self.database.subscribe_changes().ok();
        while !self.select_credentials(admitted).await? {
            self.wait_until_changed(&mut changes, admitted, PendingWait::Connection)
                .await?;
        }
        while !self.approval_ready(admitted).await? {
            #[cfg(test)]
            self.observer.approval_waiting().await?;
            // No transaction or credential material is held while a human decides.
            self.wait_until_changed(&mut changes, admitted, PendingWait::Approval)
                .await?;
        }
        self.prepare_credentials(admitted).await
    }

    /// Waits until a read snapshot shows that a held recheck could change this wait.
    ///
    /// A held recheck locks the Run, Workspace, User, Provider, policy and Account
    /// rows, so waiting Invocations must not repeat it while nothing changed.
    /// Snapshots take no locks. A held recheck still runs at least every
    /// [`WAIT_HELD_RECHECK`] for anything a snapshot cannot see; the dispatch
    /// fence rechecks authority and approval regardless.
    async fn wait_until_changed(
        &self,
        changes: &mut Option<coral_db::ChangeSubscription>,
        admitted: &AdmittedInvocation,
        wait: PendingWait,
    ) -> Result<(), Error> {
        let held_at = tokio::time::Instant::now();
        // The held recheck just ran, so this snapshot only finds the next expiry.
        // A snapshot that disagrees with it still waits before the next held recheck.
        let mut until = self.pending_until(admitted, wait).await;
        loop {
            self.wait_for_recheck(changes, until).await?;
            if held_at.elapsed() >= WAIT_HELD_RECHECK {
                return Ok(());
            }
            until = self.pending_until(admitted, wait).await;
            if until.is_none() {
                return Ok(());
            }
        }
    }

    /// Returns when a read snapshot shows this wait unchanged: the earliest time
    /// it expires without a commit. Any other answer, including a failure, leaves
    /// the outcome to the held recheck.
    async fn pending_until(&self, admitted: &AdmittedInvocation, wait: PendingWait) -> Option<i64> {
        self.pending_until_in_snapshot(admitted, wait)
            .await
            .ok()
            .flatten()
    }

    async fn pending_until_in_snapshot(
        &self,
        admitted: &AdmittedInvocation,
        wait: PendingWait,
    ) -> Result<Option<i64>, Error> {
        let now = self.now()?;
        let mut tx = self.database.begin_read_snapshot().await?;
        let Some((policy, authority_until)) = self.snapshot_policy(&mut tx, admitted, now).await?
        else {
            return Ok(None);
        };
        let wait_until = match wait {
            PendingWait::Connection => match admitted.connection_request {
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
                        admitted.credential_evidence.as_ref(),
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
            run_repository::find_exact_in_transaction(tx, &self.workspace_id, &self.run_id).await?
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

    async fn prepare_credentials(&self, admitted: &AdmittedInvocation) -> Result<(), Error> {
        let Some(evidence) = &admitted.credential_evidence else {
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

    async fn select_credentials(&self, admitted: &mut AdmittedInvocation) -> Result<bool, Error> {
        use coral_provider::account::ConnectionResolution;
        let mut tx = self.database.begin().await?;
        let Some(run) = run_repository::hold_run(&mut tx, &self.workspace_id, &self.run_id).await?
        else {
            return Err(Error::ProgramHostTerminated);
        };
        if !self.run_is_current(&run, self.now()?) {
            return Err(Error::ProgramHostTerminated);
        }
        let (_, policy) = self.current_target_authority(&mut tx, admitted).await?;
        if policy == OperationAccessState::Disabled {
            return Err(Error::InvocationRejected(CoralFailure::OperationDisabled));
        }
        let selection = if self.accounts.is_some() {
            coral_provider::account::resolve_connection_in_transaction(
                &mut tx,
                &admitted.target,
                &self.principal,
                admitted.connection_request,
                self.now()?,
            )
            .await
        } else {
            coral_provider::account::select_in_transaction(
                &mut tx,
                &admitted.target,
                &self.principal,
            )
            .await
            .map(ConnectionResolution::Ready)
        };
        let selection = match selection {
            Ok(selection) => selection,
            Err(error) => {
                // Expiry and request invalidation remain durable even though this
                // Invocation ends with a catchable gate failure.
                let error = account_error(error);
                if matches!(error, Error::InvocationRejected(_)) {
                    tx.commit().await?;
                }
                return Err(error);
            }
        };
        match selection {
            ConnectionResolution::Waiting(request) => {
                if admitted.connection_request.is_none() {
                    invocation_repository::attach_connection(
                        &mut tx,
                        &self.workspace_id,
                        &admitted.id,
                        request,
                        self.now()?,
                    )
                    .await?;
                }
                tx.commit().await?;
                admitted.connection_request = Some(request);
                Ok(false)
            }
            ConnectionResolution::Ready(evidence) => {
                if let Some(evidence) = &evidence {
                    invocation_repository::retain_credentials(
                        &mut tx,
                        &self.workspace_id,
                        &admitted.id,
                        evidence,
                    )
                    .await?;
                }
                tx.commit().await?;
                admitted.credential_evidence = evidence;
                Ok(true)
            }
        }
    }

    async fn approval_ready(&self, admitted: &AdmittedInvocation) -> Result<bool, Error> {
        let mut tx = self.database.begin().await?;
        let Some(run) = run_repository::hold_run(&mut tx, &self.workspace_id, &self.run_id).await?
        else {
            return Err(Error::ProgramHostTerminated);
        };
        if !self.run_is_current(&run, self.now()?) {
            return Err(Error::ProgramHostTerminated);
        }
        crate::approval::committed_rejection(&mut tx, admitted).await?;
        let (authority, policy) = self.current_target_authority(&mut tx, admitted).await?;
        if policy == OperationAccessState::Disabled {
            return Err(Error::InvocationRejected(CoralFailure::OperationDisabled));
        }
        coral_provider::account::verify_in_transaction(
            &mut tx,
            &admitted.target,
            &self.principal,
            admitted.credential_evidence.as_ref(),
        )
        .await
        .map_err(account_error)?;
        #[cfg(test)]
        self.observer.approval_authority_checked();
        let now = self.now()?;
        if !self.run_is_current(&run, now) {
            return Err(Error::ProgramHostTerminated);
        }
        authority
            .require_current_at(now)
            .map_err(|_expired| Error::InvocationRejected(CoralFailure::AuthorityLost))?;
        let result = crate::approval::gate(
            &mut tx,
            admitted,
            &run,
            policy == OperationAccessState::ApprovalRequired,
            now,
        )
        .await;
        if result.is_ok() || matches!(result, Err(Error::InvocationRejected(_))) {
            tx.commit().await?;
        }
        result
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one short transaction holds authority, approval and current material through the durable dispatch start"
    )]
    async fn fence(
        &self,
        admitted: &mut AdmittedInvocation,
        mut ready: ReadyDispatch,
    ) -> Result<FenceOutcome, Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &self.workspace_id, &self.run_id).await?
        else {
            return Ok(FenceOutcome::Rejected(CoralFailure::AuthorityLost));
        };
        let now = self.now()?.max(run.accepted_at_unix_nanos);
        if !self.run_is_current(&run, now) {
            transaction.rollback().await?;
            return Ok(FenceOutcome::RunTerminated);
        }
        let (authority, policy) = match self
            .current_target_authority(&mut transaction, admitted)
            .await
        {
            Ok(evidence) => evidence,
            Err(Error::InvocationRejected(failure)) => return Ok(FenceOutcome::Rejected(failure)),
            Err(error) => return Err(error),
        };
        if policy == OperationAccessState::Disabled {
            transaction.rollback().await?;
            return Ok(FenceOutcome::Rejected(CoralFailure::OperationDisabled));
        }
        if let Err(error) = coral_provider::account::verify_in_transaction(
            &mut transaction,
            &admitted.target,
            &self.principal,
            admitted.credential_evidence.as_ref(),
        )
        .await
        {
            return match account_error(error) {
                Error::InvocationRejected(failure) => Ok(FenceOutcome::Rejected(failure)),
                error => Err(error),
            };
        }
        let approval = if policy == OperationAccessState::ApprovalRequired {
            Some(crate::approval::dispatch_evidence(&mut transaction, admitted).await?)
        } else {
            None
        };
        if let Some(evidence) = &admitted.credential_evidence {
            let accounts = self
                .accounts
                .as_ref()
                .ok_or(Error::InvocationRejected(CoralFailure::AccountRequired))?;
            let secret = match accounts
                .dispatch_credentials_in_transaction(
                    &mut transaction,
                    &self.workspace_id,
                    admitted.target.provider_access().id(),
                    evidence,
                    self.now()?,
                )
                .await
            {
                Ok(Some(secret)) => secret,
                Ok(None) => return Ok(FenceOutcome::RefreshRequired),
                Err(error) => {
                    return match account_error(error) {
                        Error::InvocationRejected(failure) => Ok(FenceOutcome::Rejected(failure)),
                        error => Err(error),
                    };
                }
            };
            let authorization = match &mut ready {
                ReadyDispatch::Http(http) => http.authorize(&secret),
                ReadyDispatch::Mcp(mcp) => mcp.authorize(&secret),
            };
            if let Err(error) = authorization {
                return Ok(FenceOutcome::Rejected(
                    if matches!(error, coral_upstream::Error::InputTooLarge) {
                        CoralFailure::InputTooLarge
                    } else {
                        CoralFailure::AccountRequired
                    },
                ));
            }
        }
        #[cfg(test)]
        self.observer.fence_checks_loaded();
        // All mutable evidence is held now. Recheck every time-bound fact after
        // the final awaited read; the remaining write shares the already-held Run.
        let dispatch_started_at = self.now()?.max(run.accepted_at_unix_nanos);
        if !self.run_is_current(&run, dispatch_started_at) {
            transaction.rollback().await?;
            return Ok(FenceOutcome::RunTerminated);
        }
        if authority.require_current_at(dispatch_started_at).is_err() {
            transaction.rollback().await?;
            return Ok(FenceOutcome::Rejected(CoralFailure::AuthorityLost));
        }
        if let Some(approval) = approval
            && let Err(failure) = approval.require_current_at(dispatch_started_at)
        {
            transaction.rollback().await?;
            return Ok(FenceOutcome::Rejected(failure));
        }
        if !invocation_repository::mark_dispatch_started(
            &mut transaction,
            &self.workspace_id,
            &admitted.id,
            dispatch_started_at,
        )
        .await?
        {
            transaction.rollback().await?;
            return Ok(FenceOutcome::Rejected(CoralFailure::AuthorityLost));
        }
        transaction.commit().await?;
        admitted.dispatch_started = true;
        Ok(FenceOutcome::Permitted(FencedDispatch { ready }))
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

    async fn finish_upstream(
        &self,
        admitted: &AdmittedInvocation,
        result_json: &str,
    ) -> Result<(), Error> {
        self.finish(
            admitted,
            InvocationCompletion::Upstream {
                result_json: result_json.to_string(),
            },
        )
        .await
    }

    async fn finish_failure(
        &self,
        admitted: &AdmittedInvocation,
        failure: CoralFailure,
    ) -> Result<InvocationHostError, Error> {
        #[cfg(test)]
        self.observer.before_failure_commit().await?;
        self.finish(admitted, InvocationCompletion::Failure(failure))
            .await?;
        #[cfg(test)]
        self.observer.invocation_failure_committed(failure.code());
        Ok(InvocationHostError::Rejected(failure))
    }

    async fn finish(
        &self,
        admitted: &AdmittedInvocation,
        completion: InvocationCompletion,
    ) -> Result<(), Error> {
        let mut transaction = self.database.begin().await?;
        let Some(run) =
            run_repository::hold_run(&mut transaction, &self.workspace_id, &self.run_id).await?
        else {
            return Err(Error::InvocationPersistence(
                "Program Run lost Invocation completion".to_string(),
            ));
        };
        let ended_at = self
            .now()?
            .max(run.accepted_at_unix_nanos)
            .max(admitted.admitted_at_unix_nanos);
        let completion_is_current = if admitted.dispatch_started {
            run.state == RunState::Running
                && run.execution_claim_id.as_deref() == Some(self.claim_id.as_str())
        } else {
            self.run_is_current(&run, ended_at)
        };
        if !completion_is_current {
            transaction.rollback().await?;
            return Err(Error::InvocationPersistence(
                "Program Run lost Invocation completion".to_string(),
            ));
        }
        let exceeded = if let InvocationCompletion::Upstream { result_json } = &completion {
            !run.output_fits(result_json.len())
        } else {
            false
        };
        let completion = if exceeded {
            InvocationCompletion::Failure(CoralFailure::RunOutputTooLarge)
        } else {
            completion
        };
        if !crate::invocation_lifecycle::finish(
            &mut transaction,
            admitted,
            &run,
            &completion,
            ended_at,
        )
        .await?
        {
            transaction.rollback().await?;
            return Err(Error::InvocationPersistence(
                "Operation Invocation was already completed".to_string(),
            ));
        }
        #[cfg(test)]
        self.observer
            .before_terminal_commit()
            .await
            .map_err(Error::InvocationPersistence)?;
        transaction.commit().await?;
        if exceeded {
            return Err(Error::InvocationRejected(CoralFailure::RunOutputTooLarge));
        }
        Ok(())
    }

    fn run_is_current(&self, run: &RunRecord, now: i64) -> bool {
        run.program_work_is_current(&self.claim_id, now)
            && run.principal == self.principal
            && run.deadline_at_unix_nanos == self.deadline_at_unix_nanos
    }

    fn now(&self) -> Result<i64, Error> {
        self.clock.now_unix_nanos().map_err(Error::Clock)
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

pub(super) struct AdmittedInvocation {
    pub(super) connection_request: Option<coral_provider::account::ConnectionRequestId>,
    pub(super) credential_evidence: Option<coral_provider::account::CredentialEvidence>,
    pub(super) id: InvocationId,
    pub(super) workspace_id: WorkspaceId,
    pub(super) run_id: RunId,
    pub(super) claim_id: String,
    pub(super) target: ResolvedInvocationOperation,
    pub(super) call_path: String,
    pub(super) arguments_json: String,
    pub(super) admitted_at_unix_nanos: i64,
    pub(super) dispatch_started: bool,
}

pub(super) enum InvocationCompletion {
    Upstream { result_json: String },
    Failure(CoralFailure),
}

impl InvocationCompletion {
    pub(super) const fn result_kind(&self) -> crate::InvocationResultKind {
        match self {
            Self::Upstream { .. } => crate::InvocationResultKind::UpstreamOutcome,
            Self::Failure(_) => crate::InvocationResultKind::CoralFailure,
        }
    }
}

enum FenceOutcome {
    RefreshRequired,
    Permitted(FencedDispatch),
    Rejected(CoralFailure),
    RunTerminated,
}

struct FencedDispatch {
    ready: ReadyDispatch,
}

impl FencedDispatch {
    fn into_ready(self) -> ReadyDispatch {
        self.ready
    }
}
