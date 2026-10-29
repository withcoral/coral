//! Run-bound Tool Invocation admission, dispatch fence, and completion,
//! as Run Requests to the Run Owner.
//!
//! The [`ToolInvoker`] is private to one accepted Program Run. It resolves
//! a Call Path outside the Run, admits it as one owner request (answered at
//! Applied: the Invocation row and `invocation.started` ride the owner's next
//! chunk), plans away from persistence, runs its held rechecks as row work
//! inside the owner's chunk commits and the Dispatch Fence as one batched
//! check per chunk commit ([`FenceBatch`], decided by [`crate::fence`]),
//! decrypts and sends outside every lock after the pre-send check, and
//! reports the terminal Invocation through the owner: an upstream result at
//! Applied, a known failure once it is recorded. Operational failure is host-fatal rather than catchable. Known
//! failures persist the same closed [`CoralFailure`] code and safe
//! [`Rejection`] message projected to the Program, even during child shutdown. Only shutdown and
//! recovery record lost dispatching work as indeterminate; a stop that
//! reaches the owner before a fenced Invocation sends ends it as
//! `run_terminated` instead.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use coral_db::Transaction;
use coral_policy::ToolAccessState;
use coral_provider::{InvocationCaller, ResolvedInvocationTool};
use coral_telemetry::{metric, safe};
use coral_upstream::{McpRejection, McpResources, ReadyDispatch, UpstreamOutcome};
use coral_workspace::{AuthenticatedPrincipal, Principal};
use coral_workspace::{WorkspaceId, WorkspaceModule};
use tokio::sync::oneshot;
use tracing::Instrument as _;

use crate::engine::Clock;
use crate::execution::ExecutionStop;
use crate::fence::{
    self, ApprovalFact, Checked, Decision, FenceScope, FenceTarget, InvocationFacts, Refusal,
    RunFacts, SharedFacts,
};
use crate::invocation_lifecycle::{FinishOutcome, InvocationState, UnfinishedInvocation};
use crate::lifecycle::RunRecord;
use crate::owner::{Check, ChunkWork, OwnerHandle, ParkedWait};
use crate::run_repository::{OwnedEvent, OwnedEventKind};
use crate::trace::{Gate, Step, gate_span, step_span};
use crate::types::AcceptedRun;
use crate::{
    CoralFailure, Error, InvocationId, InvocationResultKind, Rejection, RunId,
    invocation_repository, run_repository,
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

    async fn fence_checks_loaded(&self) {}

    fn fence_attempt_finished(&self, _permit_returned: bool, _durable_dispatch_start: bool) {}

    async fn approval_waiting(&self) -> Result<(), Error> {
        Ok(())
    }

    fn wait_parked(&self) {}

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

    fn builtin_setup_prepared(&self) {}
}

#[cfg(test)]
pub(crate) struct NoopInvocationObserver;

#[cfg(test)]
#[async_trait]
impl InvocationObserver for NoopInvocationObserver {}

pub(crate) enum InvocationHostError {
    Rejected(Rejection),
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

/// Exact immutable Run context privately retained by one Tool invoker.
#[derive(Clone)]
pub(crate) struct ToolInvoker {
    accounts: Option<coral_provider::account::AccountModule>,
    providers: coral_provider::ProviderModule,
    policies: coral_policy::ToolPolicyModule,
    provider_publisher: Option<Arc<dyn coral_provider::WorkspaceProviderPublisher>>,
    human_interface: Option<crate::HumanInterfaceUrl>,
    database: Arc<coral_db::Database>,
    workspaces: WorkspaceModule,
    actor: AuthenticatedPrincipal,
    run_id: RunId,
    workspace_id: WorkspaceId,
    principal: Principal,
    accepted_at_unix_nanos: i64,
    deadline_at_unix_nanos: i64,
    /// How long an Approval Request this Run creates waits for a decision
    /// at most, bounded by the Run deadline.
    approval_wait_nanos: i64,
    limits: coral_value::ValueLimits,
    owner: OwnerHandle,
    stop: Arc<ExecutionStop>,
    mcp_resources: Arc<McpResources>,
    upstream: Arc<dyn UpstreamSender>,
    clock: Arc<dyn Clock>,
    /// The Run's Run Trace, which each Coral Failure names to the Program.
    trace: Option<coral_telemetry::RunTrace>,
    #[cfg(test)]
    observer: Arc<dyn InvocationObserver>,
}

impl ToolInvoker {
    #[expect(
        clippy::too_many_arguments,
        reason = "the private Run-bound invoker retains every dispatch-authority fact explicitly"
    )]
    pub(crate) fn new(
        accounts: Option<coral_provider::account::AccountModule>,
        providers: coral_provider::ProviderModule,
        policies: coral_policy::ToolPolicyModule,
        provider_publisher: Option<Arc<dyn coral_provider::WorkspaceProviderPublisher>>,
        human_interface: Option<crate::HumanInterfaceUrl>,
        database: Arc<coral_db::Database>,
        workspaces: WorkspaceModule,
        actor: AuthenticatedPrincipal,
        accepted: &AcceptedRun,
        approval_wait_nanos: i64,
        owner: OwnerHandle,
        upstream: Arc<dyn UpstreamSender>,
        clock: Arc<dyn Clock>,
        #[cfg(test)] observer: Arc<dyn InvocationObserver>,
    ) -> Self {
        Self {
            accounts,
            providers,
            policies,
            provider_publisher,
            human_interface,
            database,
            workspaces,
            actor,
            run_id: accepted.id.clone(),
            workspace_id: accepted.workspace_id.clone(),
            principal: accepted.principal.clone(),
            accepted_at_unix_nanos: accepted.accepted_at_unix_nanos,
            deadline_at_unix_nanos: accepted.deadline_at_unix_nanos,
            approval_wait_nanos,
            limits: accepted.value_limits,
            stop: owner.stop(),
            owner,
            mcp_resources: Arc::new(McpResources::default()),
            upstream,
            clock,
            trace: accepted.trace.clone(),
            #[cfg(test)]
            observer,
        }
    }

    /// The Run Trace identifier each Coral Failure names to the Program.
    pub(crate) fn trace_id(&self) -> Option<String> {
        self.trace
            .as_ref()
            .map(|trace| trace.trace_id().as_str().to_string())
    }

    pub(crate) async fn invoke(
        &self,
        call_path: &str,
        arguments: serde_json::Value,
        source_location: Option<crate::SourceLocation>,
    ) -> Result<String, InvocationHostError> {
        let arguments_json = serde_json::to_string(&arguments).map_err(Error::ProgramValueJson)?;
        self.limits
            .validate_json(&arguments_json)
            .map_err(|error| {
                InvocationHostError::Rejected(match error {
                    coral_value::ValueError::Invalid => CoralFailure::InvalidInput.into(),
                    coral_value::ValueError::TooLarge => CoralFailure::InputTooLarge.into(),
                })
            })?;
        let (target, policy, builtin) = self.resolve(call_path, &arguments).await?;
        let id = InvocationId::generate();
        let trace = crate::trace::InvocationTrace::begin(
            &id,
            target.call_path(),
            target.provider_access().id(),
        );
        let admitted = Arc::new(AdmittedInvocation {
            id,
            workspace_id: self.workspace_id.clone(),
            run_id: self.run_id.clone(),
            target,
            call_path: call_path.to_string(),
            arguments_json,
            source_location,
            admitted_at_unix_nanos: self.now()?.max(self.accepted_at_unix_nanos),
            span: trace.span().clone(),
        });
        let ended = async {
            self.owner
                .admit(Box::new(AdmissionWork {
                    admitted: admitted.clone(),
                }))
                .await?;
            trace.admitted(admitted.target.tool());
            self.invoke_admitted(&admitted, &arguments, policy, builtin, &trace)
                .await
        }
        .instrument(trace.span().clone())
        .await;
        trace.end(&ended);
        ended
    }

    /// The admitted Invocation to its end, marked on `trace` once its
    /// upstream send begins.
    async fn invoke_admitted(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        arguments: &serde_json::Value,
        policy: ToolAccessState,
        builtin: Option<crate::builtin::BuiltinCall>,
        trace: &crate::trace::InvocationTrace,
    ) -> Result<String, InvocationHostError> {
        let admitted = admitted.clone();
        let mut progress = InvocationProgress::default();
        if policy == ToolAccessState::Disabled {
            return Err(self
                .finish_failure(&admitted, &progress, CoralFailure::ToolDisabled)
                .await?);
        }
        let planned = self.planned(&admitted, arguments, builtin.as_ref());
        let mut ready = match planned {
            Ok(ready) => ready,
            Err(error) => {
                let failure: Rejection = match error {
                    coral_upstream::Error::DestinationRejected => {
                        CoralFailure::OutboundDestinationRejected.into()
                    }
                    coral_upstream::Error::InputTooLarge => CoralFailure::InputTooLarge.into(),
                    coral_upstream::Error::InvalidArguments(reason) => {
                        Rejection::invalid_input("", reason)
                    }
                    coral_upstream::Error::UnsupportedRequestMedia(_) => {
                        CoralFailure::InvalidInput.into()
                    }
                    coral_upstream::Error::ToolUnavailable => CoralFailure::ToolUnavailable.into(),
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
        // The slot this Invocation holds under the Fan-out Limit from the
        // fence's decision on: dropped when the dispatch it decided is over,
        // on every path out of here.
        let slot = loop {
            let (outcome, slot) = self
                .fence(&admitted, &mut progress)
                .instrument(step_span(&admitted.span, Step::DispatchFence))
                .await?;
            #[cfg(test)]
            self.observe_fence_attempt(&admitted, &outcome).await?;
            match outcome {
                FenceOutcome::Permitted => break slot,
                // No dispatch start exists on a denied row: the slot is free
                // before the failure is recorded.
                FenceOutcome::Rejected(rejection) => {
                    drop(slot);
                    return Err(self.finish_failure(&admitted, &progress, rejection).await?);
                }
                // The finish finds the row ended and delivers that outcome;
                // the failure named here is recorded only if the row were
                // still pending, which a superseded fence has ruled out.
                FenceOutcome::Superseded => {
                    drop(slot);
                    return Err(self
                        .finish_failure(&admitted, &progress, CoralFailure::AuthorityLost)
                        .await?);
                }
                FenceOutcome::RunTerminated => return Err(Error::ProgramHostTerminated.into()),
                FenceOutcome::RefreshRequired => {
                    // No dispatch start exists, and the refresh is an account
                    // wait: the slot is freed before the next fence. Prepare
                    // the same authorization again, outside every lock; never
                    // reselect an Account or replay JS.
                    drop(slot);
                    if let Err(error) = self
                        .prepare_credentials(&admitted, &progress)
                        .instrument(step_span(&admitted.span, Step::Credentials))
                        .await
                    {
                        return match error {
                            Error::InvocationRejected(failure) => {
                                Err(self.finish_failure(&admitted, &progress, failure).await?)
                            }
                            error => Err(error.into()),
                        };
                    }
                    ready = self
                        .planned(&admitted, arguments, builtin.as_ref())
                        .map_err(Error::from)?;
                }
            }
        };
        self.dispatch_and_finish(&admitted, &progress, ready, slot, trace)
            .await
    }

    /// [`Self::dispatch`] in its plan span, recording the protocol it plans.
    fn planned(
        &self,
        admitted: &AdmittedInvocation,
        arguments: &serde_json::Value,
        builtin: Option<&crate::builtin::BuiltinCall>,
    ) -> Result<Dispatch, coral_upstream::Error> {
        let planned = step_span(&admitted.span, Step::Plan)
            .in_scope(|| self.dispatch(admitted, arguments, builtin))?;
        let protocol = match &planned {
            Dispatch::Upstream(ReadyDispatch::Http(_)) => "http",
            Dispatch::Upstream(ReadyDispatch::Mcp(_)) => "mcp",
            Dispatch::Builtin(_) => "builtin",
        };
        admitted
            .span
            .record("coral.provider.protocol", safe::kind(protocol));
        Ok(planned)
    }

    /// Resolves the Call Path's current Tool and policy, outside the Run:
    /// the fence rechecks both before any dispatch. A Built-in Tool checks
    /// its input here, before admission, so input it rejects, such as a
    /// credential, is never stored.
    async fn resolve(
        &self,
        call_path: &str,
        arguments: &serde_json::Value,
    ) -> Result<
        (
            ResolvedInvocationTool,
            ToolAccessState,
            Option<crate::builtin::BuiltinCall>,
        ),
        Error,
    > {
        let mut transaction = self.database.begin().await?;
        let resolved_at = self.now()?.max(self.accepted_at_unix_nanos);
        let Some(target) = coral_provider::resolve_invocation_tool_in_transaction(
            &mut transaction,
            &self.workspace_id,
            InvocationCaller::new(&self.workspaces, &self.actor, resolved_at),
            call_path,
        )
        .await?
        else {
            transaction.rollback().await?;
            return Err(Error::InvocationRejected(CoralFailure::ToolNotFound.into()));
        };
        let builtin = match crate::builtin::BuiltinCall::parse(&target, arguments) {
            Some(Ok(call)) => Some(call),
            Some(Err(failure)) => {
                transaction.rollback().await?;
                return Err(Error::InvocationRejected(failure));
            }
            None => None,
        };
        let policy = coral_policy::invocation_decision_in_transaction(
            &mut transaction,
            &fence::policy_target(&self.workspace_id, &target),
            target.tool().hints(),
            target.tool().http_method(),
            coral_db::RowLock::Shared,
        )
        .await?
        .access_state();
        transaction.commit().await?;
        Ok((target, policy, builtin))
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

    /// Runs the dispatch the fence permitted and records its end. The slot
    /// is held until then: the upstream result carries its release, and
    /// every other end frees it as this returns.
    async fn dispatch_and_finish(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        mut ready: Dispatch,
        mut slot: DispatchSlot,
        trace: &crate::trace::InvocationTrace,
    ) -> Result<String, InvocationHostError> {
        // The owner's own terminal state, before anything leaves the process:
        // a stop that reached the owner ends this fenced Invocation as
        // `run_terminated` instead of sending.
        if self.owner.is_stopping() {
            self.end_before_send(admitted).await?;
            return Err(InvocationHostError::Fatal(Error::ProgramHostTerminated));
        }
        // The material the fence judged current is decrypted here, per
        // dispatch, outside every lock; a failure to open it ends this
        // Invocation and never fences again.
        if let Err(error) = self
            .authorize(admitted, progress, &mut ready)
            .instrument(step_span(&admitted.span, Step::Authorize))
            .await
        {
            return match error {
                Error::InvocationRejected(failure) => {
                    Err(self.finish_failure(admitted, progress, failure).await?)
                }
                error => Err(error.into()),
            };
        }
        let ready = match ready {
            Dispatch::Upstream(ready) => ready,
            Dispatch::Builtin(call) => {
                return self
                    .perform_builtin(admitted, progress, &call, &mut slot)
                    .await;
            }
        };
        trace.sending();
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
        let result_json = self
            .finish_result(admitted, progress, &outcome.tool_result(), &mut slot)
            .await?;
        match crate::error::UpstreamRejection::of(outcome.class()) {
            None => Ok(result_json),
            Some(rejection) => Err(InvocationHostError::Upstream {
                rejection,
                outcome_json: result_json,
            }),
        }
    }

    /// Records a complete Tool Result as the Upstream Outcome, through
    /// the owner.
    async fn finish_result(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        result: &serde_json::Value,
        slot: &mut DispatchSlot,
    ) -> Result<String, InvocationHostError> {
        let result_json = serde_json::to_string(result).map_err(Error::ProgramValueJson)?;
        if result_json.len() > self.limits.upstream_bytes()
            || self.limits.validate_json(&result_json).is_err()
        {
            return Err(self
                .finish_failure(admitted, progress, CoralFailure::ResultTooLarge)
                .await?);
        }
        match self
            .finish_upstream(admitted, progress, &result_json, slot)
            .await
        {
            Ok(()) => Ok(result_json),
            Err(Error::InvocationRejected(rejection))
                if rejection.failure() == CoralFailure::RunOutputTooLarge =>
            {
                Err(self.finish_failure(admitted, progress, rejection).await?)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// What the fence will permit: a Built-in Tool that Coral performs
    /// itself, or one planned upstream send.
    fn dispatch(
        &self,
        admitted: &AdmittedInvocation,
        arguments: &serde_json::Value,
        builtin: Option<&crate::builtin::BuiltinCall>,
    ) -> Result<Dispatch, coral_upstream::Error> {
        if let Some(call) = builtin {
            return Ok(Dispatch::Builtin(Box::new(call.clone())));
        }
        self.plan(&admitted.target, arguments)
            .map(Dispatch::Upstream)
    }

    /// Performs a Built-in Tool as the invoking Principal after the fence,
    /// where an upstream send would run, and records its Tool Result as
    /// the Upstream Outcome. Nothing is sent upstream.
    async fn perform_builtin(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        call: &crate::builtin::BuiltinCall,
        slot: &mut DispatchSlot,
    ) -> Result<String, InvocationHostError> {
        // Every runtime that runs Programs names Reef's origin; without it a
        // built-in result could not link the person anywhere.
        let Some(human_interface) = &self.human_interface else {
            return Err(self
                .finish_failure(admitted, progress, CoralFailure::ToolUnavailable)
                .await?);
        };
        let context = match self.actor.authenticated_user() {
            Some(user) => Some(coral_workspace::ChangeContext::new(
                user.clone(),
                self.now()?,
                coral_workspace::TraceId::current_or_new(),
            )),
            None => None,
        };
        let request_id = coral_provider::ManagementRequestId::parse(&admitted.id.to_string())
            .map_err(Error::from)?;
        let host = crate::builtin::BuiltinHost {
            database: &self.database,
            providers: &self.providers,
            policies: &self.policies,
            accounts: self.accounts.as_ref(),
            publisher: self.provider_publisher.as_deref(),
            actor: &self.actor,
            context: context.as_ref(),
            principal: &self.principal,
            workspace_id: &self.workspace_id,
            human_interface,
            request_id: &request_id,
            run: self,
        };
        match call.perform(&host).await {
            Ok(result) => self.finish_result(admitted, progress, &result, slot).await,
            Err(Error::InvocationRejected(failure)) => {
                Err(self.finish_failure(admitted, progress, failure).await?)
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Decrypts the selected credentials outside every lock, after the fence
    /// and the pre-send check, and authorizes the dispatch with them. A token
    /// renewed since the fence is the one dispatched.
    async fn authorize(
        &self,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
        ready: &mut Dispatch,
    ) -> Result<(), Error> {
        let Some(evidence) = &progress.credential_evidence else {
            return Ok(());
        };
        let accounts = self.accounts.as_ref().ok_or(Error::InvocationRejected(
            CoralFailure::AccountRequired.into(),
        ))?;
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
            Dispatch::Upstream(ReadyDispatch::Http(http)) => http.authorize(&secret),
            Dispatch::Upstream(ReadyDispatch::Mcp(mcp)) => mcp.authorize(&secret),
            // Built-in Tools have no Credential Route.
            Dispatch::Builtin(_) => Err(coral_upstream::Error::ToolUnavailable),
        };
        authorization.map_err(|error| {
            Error::InvocationRejected(
                if matches!(error, coral_upstream::Error::InputTooLarge) {
                    CoralFailure::InputTooLarge
                } else {
                    CoralFailure::AccountRequired
                }
                .into(),
            )
        })
    }

    fn plan(
        &self,
        target: &ResolvedInvocationTool,
        arguments: &serde_json::Value,
    ) -> Result<ReadyDispatch, coral_upstream::Error> {
        ReadyDispatch::plan(
            target.tool(),
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
        gate_span(&admitted.span, Gate::Account)
            .instrument(self.account_gate(admitted, progress))
            .await?;
        gate_span(&admitted.span, Gate::Approval)
            .instrument(self.approval_gate(admitted, progress))
            .await?;
        self.prepare_credentials(admitted, progress)
            .instrument(step_span(&admitted.span, Step::Credentials))
            .await
    }

    /// Selects the Account, waiting while a connection request is open.
    async fn account_gate(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &mut InvocationProgress,
    ) -> Result<(), Error> {
        loop {
            let (wake, woken) = oneshot::channel();
            match self
                .select_credentials(admitted, progress.clone(), wake)
                .await?
            {
                SelectionOutcome::Ready(selected) => {
                    *progress = selected;
                    break;
                }
                SelectionOutcome::Waiting {
                    progress: selected, ..
                } => {
                    *progress = selected;
                    self.parked(woken).await?;
                }
                SelectionOutcome::Rejected(failure) => {
                    return Err(Error::InvocationRejected(failure));
                }
                SelectionOutcome::RunNotCurrent => return Err(Error::ProgramHostTerminated),
            }
        }
        Ok(())
    }

    /// Finds the approval this Invocation needs, waiting while its Approval
    /// Request is undecided.
    async fn approval_gate(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &mut InvocationProgress,
    ) -> Result<(), Error> {
        loop {
            let (wake, woken) = oneshot::channel();
            match self.approval_ready(admitted, progress, wake).await? {
                GateOutcome::Ready(approval) => {
                    progress.approval = approval;
                    break;
                }
                GateOutcome::Waiting(_) => {
                    #[cfg(test)]
                    self.observer.approval_waiting().await?;
                    // No transaction or credential material is held while a human decides.
                    self.parked(woken).await?;
                }
                GateOutcome::Rejected(failure) => return Err(Error::InvocationRejected(failure)),
                GateOutcome::RunNotCurrent => return Err(Error::ProgramHostTerminated),
            }
        }
        Ok(())
    }

    /// Waits, holding nothing, until the Run Owner sends this parked
    /// Invocation back to its held recheck: the owner recorded a decision or
    /// an expiry of its Approval Request, its connection request's deadline
    /// passed, or a snapshot check found its wait ended or moot. The held
    /// recheck decides; a wake is a hint. The owner drops the wake once its
    /// Run takes no more Program requests.
    async fn parked(&self, woken: oneshot::Receiver<()>) -> Result<(), Error> {
        #[cfg(test)]
        self.observer.wait_parked();
        tokio::select! {
            biased;
            () = self.stop.requested() => Err(Error::ProgramHostTerminated),
            woken = woken => woken.map_err(|_dropped| Error::ProgramHostTerminated),
        }
    }

    /// Renews the selected credentials outside every lock, after the fence
    /// parked the Invocation on a refresh: the material is decrypted, and
    /// refreshed when expired, by the Account Module; nothing is kept here.
    async fn prepare_credentials(
        &self,
        admitted: &AdmittedInvocation,
        progress: &InvocationProgress,
    ) -> Result<(), Error> {
        let Some(evidence) = &progress.credential_evidence else {
            return Ok(());
        };
        let accounts = self.accounts.as_ref().ok_or(Error::InvocationRejected(
            CoralFailure::AccountRequired.into(),
        ))?;
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

    /// The held credential selection; `wake` is how the owner sends the
    /// Invocation back here if this parks it on a connection request.
    async fn select_credentials(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: InvocationProgress,
        wake: oneshot::Sender<()>,
    ) -> Result<SelectionOutcome, Error> {
        self.ask(|reply| {
            Box::new(SelectCredentialsWork {
                invoker: self.clone(),
                admitted: admitted.clone(),
                progress,
                reply,
                wake: Some(wake),
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
        let target = FenceTarget {
            admitted,
            credential_evidence: None,
        };
        if self.held_checked(tx, run, target).await?.is_none() {
            return Ok(SelectionOutcome::RunNotCurrent);
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
            ConnectionResolution::Waiting {
                request,
                expires_at_unix_nanos,
            } => {
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
                Ok(SelectionOutcome::Waiting {
                    progress,
                    wait: ParkedWait::Connection {
                        request,
                        expires_at_unix_nanos,
                    },
                })
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

    /// The held approval gate; `wake` is how the owner sends the Invocation
    /// back here if this parks it on its Approval Request.
    async fn approval_ready(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        wake: oneshot::Sender<()>,
    ) -> Result<GateOutcome, Error> {
        self.ask(|reply| {
            Box::new(ApprovalReadyWork {
                invoker: self.clone(),
                admitted: admitted.clone(),
                progress: progress.clone(),
                reply,
                wake: Some(wake),
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

    /// The held approval gate: the fence's facts under their shared holds,
    /// then the Approval Request this Invocation needs under the policy they
    /// show, created, expired or closed here as the gate finds it.
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
        if let Some(failure) = crate::approval::committed_failure(tx, admitted).await? {
            return Err(Error::InvocationRejected(failure.into()));
        }
        let target = FenceTarget {
            admitted,
            credential_evidence: progress.credential_evidence.as_ref(),
        };
        let Some((checked, now)) = self.held_checked(tx, run, target).await? else {
            return Ok(GateOutcome::RunNotCurrent);
        };
        let gate = crate::approval::gate(
            tx,
            admitted,
            run,
            checked.policy == ToolAccessState::ApprovalRequired,
            self.approval_wait_nanos,
            now,
            events,
        )
        .await?;
        Ok(match gate {
            crate::approval::Gate::Ready(approval) => GateOutcome::Ready(approval),
            crate::approval::Gate::Pending {
                id,
                deadline_at_unix_nanos,
            } => GateOutcome::Waiting(ParkedWait::Approval {
                approval: id,
                deadline_at_unix_nanos,
            }),
        })
    }

    /// The fence's shared facts for one Invocation, read under their shared
    /// holds inside the owner's chunk commit and checked at a clock sample
    /// taken after the reads: the policy and that sample, `None` when the Run
    /// no longer takes Program work, the known failure when a fact refuses.
    async fn held_checked(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        target: FenceTarget<'_>,
    ) -> Result<Option<(Checked, i64)>, Error> {
        gate_span(&target.admitted.span, Gate::Authority)
            .instrument(self.held_checked_facts(tx, run, target))
            .await
    }

    async fn held_checked_facts(
        &self,
        tx: &mut Transaction<'_>,
        run: &RunRecord,
        target: FenceTarget<'_>,
    ) -> Result<Option<(Checked, i64)>, Error> {
        let shared = SharedFacts::load(
            tx,
            coral_db::RowLock::Shared,
            &self.fence_scope(),
            self.now()?,
            std::slice::from_ref(&target),
        )
        .await?;
        #[cfg(test)]
        self.observer.approval_authority_checked();
        let now = self.now()?;
        match shared.checked(&self.run_facts(run), target, now) {
            Ok(checked) => Ok(Some((checked, now))),
            Err(Refusal::RunTerminated) => Ok(None),
            Err(refusal) => Err(refused(refusal)),
        }
    }

    /// Asks the owner for the Dispatch Fence: the request joins the batch of
    /// a chunk commit and answers once that chunk is recorded, with the slot
    /// the Invocation holds under the Fan-out Limit. A decision comes only
    /// from a batch that committed, and every request of a batch holds a
    /// slot; a request the owner never batched (the Run ending, the owner
    /// gone) answers with an error and held none.
    async fn fence(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &mut InvocationProgress,
    ) -> Result<(FenceOutcome, DispatchSlot), Error> {
        let (sender, receiver) = oneshot::channel();
        self.owner.fence(FenceRequest {
            invoker: self.clone(),
            fenced: FencedInvocation {
                admitted: admitted.clone(),
                progress: progress.clone(),
                reply: Replier::new(sender),
            },
        })?;
        let outcome = receiver.await.map_err(|_lost| Error::OwnerLost)??;
        if matches!(outcome, FenceOutcome::Permitted) {
            progress.dispatch_started = true;
            admitted
                .span
                .record("coral.invocation.dispatch_started", true);
        }
        Ok((outcome, DispatchSlot::new(self.owner.clone())))
    }

    pub(crate) fn fence_scope(&self) -> FenceScope<'_> {
        FenceScope {
            workspaces: &self.workspaces,
            actor: &self.actor,
            workspace_id: &self.workspace_id,
        }
    }

    pub(crate) fn run_facts<'a>(&'a self, run: &'a RunRecord) -> RunFacts<'a> {
        RunFacts {
            run,
            claim_id: self.owner.claim_id(),
            principal: &self.principal,
            deadline_at_unix_nanos: self.deadline_at_unix_nanos,
        }
    }

    /// Reports an upstream result: the owner decides it against the Run
    /// Output Budget and answers at Applied; the row and `invocation.ended`
    /// ride its next chunk. The result carries the slot's release, so the
    /// Invocation that has waited longest fences in that same chunk, after
    /// this row ends.
    async fn finish_upstream(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        result_json: &str,
        slot: &mut DispatchSlot,
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
        slot.carry_with_result();
        let applied = self
            .owner
            .invocation_result(result_json.len(), Box::new(work))
            .instrument(step_span(&admitted.span, Step::Persist))
            .await;
        if applied.is_err() {
            // The owner applied nothing (the result is over budget, or the
            // Run takes no more work): the row is still dispatching and the
            // slot stays with this task until the row's end is recorded.
            slot.keep();
        } else {
            metric::output_kept(metric::OutputKind::Result, result_json.len());
        }
        applied
    }

    /// Ends the Invocation with a known failure, recorded before the Program
    /// observes it.
    async fn finish_failure(
        &self,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        failure: impl Into<Rejection>,
    ) -> Result<InvocationHostError, Error> {
        let failure: Rejection = failure.into();
        if matches!(
            failure.failure(),
            CoralFailure::ResultTooLarge | CoralFailure::RunOutputTooLarge
        ) {
            metric::output_rejected(metric::OutputKind::Result);
        }
        let completion = InvocationCompletion::Failure(failure.clone());
        #[cfg(test)]
        self.observer.before_failure_commit().await?;
        let outcome = self
            .ask(|reply| {
                Box::new(FinishWork {
                    #[cfg(test)]
                    observer: self.observer.clone(),
                    admitted: admitted.clone(),
                    dispatch_started: progress.dispatch_started,
                    completion,
                    reply: Some(reply),
                })
            })
            .instrument(step_span(&admitted.span, Step::Persist))
            .await?;
        match outcome {
            FinishOutcome::Finished(_) | FinishOutcome::SameFailure => {}
            // Deliver the durable human decision, not a worker's stale gate failure.
            FinishOutcome::Superseded(existing) => {
                return Err(Error::InvocationRejected(existing.into()));
            }
            FinishOutcome::Lost => {
                return Err(Error::InvocationPersistence(
                    "Tool Invocation was already completed".to_string(),
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
        self.run_facts(run).is_current(now)
    }

    fn now(&self) -> Result<i64, Error> {
        self.clock.now_unix_nanos().map_err(Error::Clock)
    }
}

/// One slot under the Run's Fan-out Limit, held from the fence's decision
/// until the dispatch it decided is over. An upstream result carries the
/// release: the owner frees the slot as it applies the result, so the
/// Invocation that has waited longest fences in the same chunk the ended
/// row rides. Every other end (denied, superseded, parked on a refresh, a
/// failure, the Run ending) frees it on drop, which tells the owner.
struct DispatchSlot {
    owner: OwnerHandle,
    carried_by_result: bool,
}

impl DispatchSlot {
    fn new(owner: OwnerHandle) -> Self {
        Self {
            owner,
            carried_by_result: false,
        }
    }

    /// The upstream result about to reach the owner frees the slot as it is
    /// applied; the drop then tells the owner nothing more.
    fn carry_with_result(&mut self) {
        self.carried_by_result = true;
    }

    /// The owner refused the result and freed nothing: the slot is this
    /// task's again, freed on drop.
    fn keep(&mut self) {
        self.carried_by_result = false;
    }
}

impl Drop for DispatchSlot {
    fn drop(&mut self) {
        if !self.carried_by_result {
            self.owner.dispatch_ended();
        }
    }
}

/// The error a refusal means for a step inside the owner's chunk: the known
/// failure the Invocation ends with, or the Run's end.
fn refused(refusal: Refusal) -> Error {
    match refusal {
        Refusal::Deny(failure) => Error::InvocationRejected(failure.into()),
        Refusal::RunTerminated => Error::ProgramHostTerminated,
    }
}

/// Keeps a known failure as the step's outcome: inside the owner's chunk
/// transaction a rejection is a value the chunk records, not an error that
/// fails it.
fn catch_rejection<O>(
    result: Result<O, Error>,
    rejected: impl FnOnce(Rejection) -> O,
) -> Result<O, Error> {
    match result {
        Err(Error::InvocationRejected(failure)) => Ok(rejected(failure)),
        other => other,
    }
}

/// The known failure an Account error means, or the error itself when it is
/// not one.
pub(crate) fn account_failure(error: coral_provider::Error) -> Result<CoralFailure, Error> {
    Ok(match error {
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
        other => return Err(other.into()),
    })
}

fn account_error(error: coral_provider::Error) -> Error {
    match account_failure(error) {
        Ok(failure) => Error::InvocationRejected(failure.into()),
        Err(error) => error,
    }
}

/// One admitted Invocation's immutable evidence.
#[derive(Debug)]
pub(crate) struct AdmittedInvocation {
    pub(crate) id: InvocationId,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) run_id: RunId,
    pub(crate) target: ResolvedInvocationTool,
    pub(crate) call_path: String,
    pub(crate) arguments_json: String,
    pub(crate) source_location: Option<crate::SourceLocation>,
    pub(crate) admitted_at_unix_nanos: i64,
    /// The Invocation's `coral.invocation` span. Its gates that run inside
    /// the owner's chunk commits are its children too.
    pub(crate) span: tracing::Span,
}

/// What one Invocation's lifecycle has settled so far: its account wait, its
/// selected credentials, the approval the held gate found, and whether its
/// dispatch start is durable.
#[derive(Debug, Clone, Default)]
pub(crate) struct InvocationProgress {
    pub(crate) connection_request: Option<coral_provider::account::ConnectionRequestId>,
    pub(crate) credential_evidence: Option<coral_provider::account::CredentialEvidence>,
    /// The Approval Request's decision as the gate recorded it, which the
    /// fence decides on from owner memory: an approval never changes.
    pub(crate) approval: ApprovalFact,
    pub(crate) dispatch_started: bool,
}

pub(crate) enum InvocationCompletion {
    Upstream { result_json: String },
    Failure(Rejection),
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
    /// The Invocation parks on its User's connection request until the
    /// request ends or expires.
    Waiting {
        progress: InvocationProgress,
        wait: ParkedWait,
    },
    Ready(InvocationProgress),
    Rejected(Rejection),
    RunNotCurrent,
}

enum GateOutcome {
    /// The Invocation parks on its Approval Request until it is decided or
    /// expires.
    Waiting(ParkedWait),
    Ready(ApprovalFact),
    Rejected(Rejection),
    RunNotCurrent,
}

#[derive(Debug)]
pub(crate) enum FenceOutcome {
    RefreshRequired,
    Permitted,
    Rejected(Rejection),
    /// An outside writer ended the Invocation row during the fence (a
    /// decision, a cancel): its durable outcome stands, and the finish
    /// delivers it.
    Superseded,
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
    invoker: ToolInvoker,
    admitted: Arc<AdmittedInvocation>,
    progress: InvocationProgress,
    reply: Replier<SelectionOutcome>,
    /// How the owner sends the Invocation back here once the connection
    /// request the selection parks it on ends.
    wake: Option<oneshot::Sender<()>>,
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

    /// The last attempt's outcome decides: a recommit runs the work again.
    fn take_parked(&mut self) -> Option<Box<ParkedInvocation>> {
        let Some(SelectionOutcome::Waiting { wait, .. }) = &self.reply.outcome else {
            return None;
        };
        let wake = self.wake.take()?;
        Some(Box::new(ParkedInvocation::new(
            &self.invoker,
            &self.admitted,
            &self.progress,
            wait.clone(),
            wake,
        )))
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        self.reply.settle(recorded);
    }
}

struct ApprovalReadyWork {
    invoker: ToolInvoker,
    admitted: Arc<AdmittedInvocation>,
    progress: InvocationProgress,
    reply: Replier<GateOutcome>,
    /// How the owner sends the Invocation back here once the Approval
    /// Request the gate parks it on is decided or expires.
    wake: Option<oneshot::Sender<()>>,
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

    /// The last attempt's outcome decides: a recommit runs the work again.
    fn take_parked(&mut self) -> Option<Box<ParkedInvocation>> {
        let Some(GateOutcome::Waiting(wait)) = &self.reply.outcome else {
            return None;
        };
        let wake = self.wake.take()?;
        Some(Box::new(ParkedInvocation::new(
            &self.invoker,
            &self.admitted,
            &self.progress,
            wait.clone(),
            wake,
        )))
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        self.reply.settle(recorded);
    }
}

/// One Invocation parked on a wait, as its Run Owner keeps it until the
/// wait ends: what a snapshot check reads for it, and the wake that sends
/// it back to its held recheck. Dropping it lets the Invocation go: it reads
/// that its Run takes no more Program work.
pub(crate) struct ParkedInvocation {
    target: Arc<ParkedTarget>,
    wake: oneshot::Sender<()>,
}

/// What a snapshot check reads one parked wait against: the Invocation's
/// retained target, its selected credentials, and the wait.
struct ParkedTarget {
    invoker: ToolInvoker,
    admitted: Arc<AdmittedInvocation>,
    credential_evidence: Option<coral_provider::account::CredentialEvidence>,
    wait: ParkedWait,
}

impl ParkedTarget {
    fn target(&self) -> FenceTarget<'_> {
        FenceTarget {
            admitted: &self.admitted,
            credential_evidence: self.credential_evidence.as_ref(),
        }
    }
}

impl ParkedInvocation {
    /// The Invocation a held recheck parked on `wait`, for its owner.
    fn new(
        invoker: &ToolInvoker,
        admitted: &Arc<AdmittedInvocation>,
        progress: &InvocationProgress,
        wait: ParkedWait,
        wake: oneshot::Sender<()>,
    ) -> Self {
        Self {
            target: Arc::new(ParkedTarget {
                invoker: invoker.clone(),
                admitted: admitted.clone(),
                credential_evidence: progress.credential_evidence.clone(),
                wait,
            }),
            wake,
        }
    }

    pub(crate) fn wait(&self) -> &ParkedWait {
        &self.target.wait
    }

    /// Sends the Invocation back to its held recheck.
    pub(crate) fn wake(self) {
        // The Invocation may have stopped waiting: its Run is ending.
        let _waiting = self.wake.send(());
    }

    /// A parked Invocation for the owner's in-memory tests, which never
    /// check it.
    #[cfg(test)]
    pub(crate) fn for_apply_test(
        invoker: &ToolInvoker,
        admitted: &Arc<AdmittedInvocation>,
        wait: ParkedWait,
        wake: oneshot::Sender<()>,
    ) -> Self {
        Self::new(
            invoker,
            admitted,
            &InvocationProgress::default(),
            wait,
            wake,
        )
    }
}

impl fmt::Debug for ParkedInvocation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ParkedInvocation")
            .field("invocation", &self.target.admitted.id)
            .field("wait", &self.target.wait)
            .finish_non_exhaustive()
    }
}

/// One snapshot check of parked waits, holding nothing: each wait with the
/// serial its owner gave the park, so a finding reaches only the park it
/// read. Every wait is one Run's, so any wait's invoker reads for all.
pub(crate) struct PrecheckBatch {
    check: Check,
    waits: Vec<(u64, Arc<ParkedTarget>)>,
}

impl fmt::Debug for PrecheckBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PrecheckBatch")
            .field("check", &self.check)
            .field(
                "parks",
                &self
                    .waits
                    .iter()
                    .map(|(serial, _)| serial)
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl PrecheckBatch {
    /// The `check` of the `parked` waits by serial; `None` when none is.
    pub(crate) fn of<'a>(
        check: Check,
        parked: impl IntoIterator<Item = (&'a u64, &'a Box<ParkedInvocation>)>,
    ) -> Option<Self> {
        let waits: Vec<_> = parked
            .into_iter()
            .map(|(serial, parked)| (*serial, parked.target.clone()))
            .collect();
        (!waits.is_empty()).then_some(Self { check, waits })
    }

    pub(crate) const fn check(&self) -> Check {
        self.check
    }

    /// The parks this check reads, by serial.
    #[cfg(test)]
    pub(crate) fn parks(&self) -> Vec<u64> {
        self.waits.iter().map(|(serial, _)| *serial).collect()
    }

    /// Reads the waits in one read snapshot with no hold and returns the
    /// serials of the parks it found ended or moot. A connection check reads
    /// each connection request once and finds the ones no longer pending. The
    /// periodic precheck also reads the Dispatch Fence's facts with the
    /// shared loader and decides each wait with the shared evaluator: a wait
    /// it would not keep waiting is found. The Approval Requests and the
    /// Invocation rows are the owner's own (an outside writer touches none
    /// while the Run is nonterminal), so what the owner keeps of them stands:
    /// a request still pending, no failure recorded.
    pub(crate) async fn run(&self) -> Result<Vec<u64>, Error> {
        let Some((_, first)) = self.waits.first() else {
            return Ok(Vec::new());
        };
        let invoker = &first.invoker;
        let now = invoker.now()?;
        let mut tx = invoker.database.begin_read_snapshot().await?;
        let mut pending_until = std::collections::HashMap::new();
        for (_, target) in &self.waits {
            if let ParkedWait::Connection { request, .. } = target.wait
                && !pending_until.contains_key(&request)
            {
                let until = coral_provider::account::connection_pending_until_in_snapshot(
                    &mut tx,
                    &target.admitted.target,
                    &invoker.principal,
                    request,
                    now,
                )
                .await?;
                pending_until.insert(request, until);
            }
        }
        let connection_pending = |wait: &ParkedWait| match wait {
            ParkedWait::Connection { request, .. } => pending_until.get(request).copied().flatten(),
            ParkedWait::Approval { .. } => None,
        };
        if self.check == Check::Connections {
            tx.commit().await?;
            return Ok(self
                .waits
                .iter()
                .filter(|(_, target)| {
                    target.wait.on_connection() && connection_pending(&target.wait).is_none()
                })
                .map(|(serial, _)| *serial)
                .collect());
        }
        let run = run_repository::find_exact_in_transaction(
            &mut tx,
            &invoker.workspace_id,
            &invoker.run_id,
        )
        .await?;
        let targets: Vec<FenceTarget<'_>> = self
            .waits
            .iter()
            .map(|(_, parked)| parked.target())
            .collect();
        let shared = SharedFacts::load(
            &mut tx,
            coral_db::RowLock::None,
            &invoker.fence_scope(),
            now,
            &targets,
        )
        .await?;
        tx.commit().await?;
        let Some(run) = run else {
            return Ok(self.waits.iter().map(|(serial, _)| *serial).collect());
        };
        let now = invoker.now()?;
        let run_facts = invoker.run_facts(&run);
        Ok(self
            .waits
            .iter()
            .zip(&targets)
            .filter(|((_, parked), target)| {
                let approval = match &parked.wait {
                    ParkedWait::Approval {
                        deadline_at_unix_nanos,
                        ..
                    } => ApprovalFact::Pending {
                        deadline_at_unix_nanos: *deadline_at_unix_nanos,
                    },
                    ParkedWait::Connection { .. } => ApprovalFact::NotRequested,
                };
                let decision = fence::decide(
                    &shared,
                    &run_facts,
                    &InvocationFacts {
                        target: **target,
                        connection_pending_until_unix_nanos: connection_pending(&parked.wait),
                        approval,
                    },
                    now,
                );
                !matches!(decision, Decision::Wait { .. })
            })
            .map(|((serial, _), _)| *serial)
            .collect())
    }
}

/// One Invocation's request for the Dispatch Fence, carrying what the fence
/// decides it on from owner memory: its retained target, its selected
/// credentials and the approval the gate found, and the invoker of its Run,
/// which the batch keeps once.
pub(crate) struct FenceRequest {
    invoker: ToolInvoker,
    fenced: FencedInvocation,
}

/// One Invocation inside a batch.
struct FencedInvocation {
    admitted: Arc<AdmittedInvocation>,
    progress: InvocationProgress,
    reply: Replier<FenceOutcome>,
}

impl FencedInvocation {
    fn target(&self) -> FenceTarget<'_> {
        FenceTarget {
            admitted: &self.admitted,
            credential_evidence: self.progress.credential_evidence.as_ref(),
        }
    }
}

impl fmt::Debug for FenceRequest {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FenceRequest")
            .field("invocation", &self.fenced.admitted.id)
            .finish_non_exhaustive()
    }
}

impl FenceRequest {
    /// A request for the owner's in-memory tests, which never run it.
    #[cfg(test)]
    pub(crate) fn for_apply_test(
        invoker: ToolInvoker,
        admitted: Arc<AdmittedInvocation>,
        sender: oneshot::Sender<Result<FenceOutcome, Error>>,
    ) -> Self {
        Self {
            invoker,
            fenced: FencedInvocation {
                admitted,
                progress: InvocationProgress::default(),
                reply: Replier::new(sender),
            },
        }
    }
}

/// The Dispatch Fence of one chunk commit: every ready Invocation the owner
/// collected for it, decided together. The shared facts are read once under
/// their shared holds, the clock is sampled once after the last read, each
/// Invocation is decided in memory, and every permitted row is moved from
/// `pending` to `dispatching` in the same commit, which the owner predicates
/// on its claim and the head. The fence writes no Event.
pub(crate) struct FenceBatch {
    invoker: ToolInvoker,
    invocations: Vec<FencedInvocation>,
}

impl fmt::Debug for FenceBatch {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("FenceBatch")
            .field("invocations", &self.invocation_ids())
            .finish()
    }
}

impl FenceBatch {
    pub(crate) fn new(request: FenceRequest) -> Self {
        Self {
            invoker: request.invoker,
            invocations: vec![request.fenced],
        }
    }

    /// Adds `request`, whose Run is this batch's.
    pub(crate) fn push(&mut self, request: FenceRequest) {
        self.invocations.push(request.fenced);
    }

    pub(crate) fn len(&self) -> usize {
        self.invocations.len()
    }

    pub(crate) fn invocation_ids(&self) -> Vec<InvocationId> {
        self.invocations
            .iter()
            .map(|fenced| fenced.admitted.id.clone())
            .collect()
    }
}

#[async_trait]
impl ChunkWork for FenceBatch {
    async fn run(
        &mut self,
        transaction: &mut Transaction<'_>,
        run: &RunRecord,
        _now_unix_nanos: i64,
    ) -> Result<Vec<OwnedEvent>, Error> {
        let invoker = &self.invoker;
        let targets: Vec<FenceTarget<'_>> = self
            .invocations
            .iter()
            .map(FencedInvocation::target)
            .collect();
        let shared = SharedFacts::load(
            transaction,
            coral_db::RowLock::Shared,
            &invoker.fence_scope(),
            invoker.now()?,
            &targets,
        )
        .await?;
        #[cfg(test)]
        invoker.observer.fence_checks_loaded().await;
        // Every awaited read is done: one clock sample decides the batch and
        // dates every dispatch start it records.
        let now = invoker.now()?.max(run.accepted_at_unix_nanos);
        let run_facts = invoker.run_facts(run);
        let decisions: Vec<Decision> = self
            .invocations
            .iter()
            .zip(&targets)
            .map(|(fenced, target)| {
                fence::decide(
                    &shared,
                    &run_facts,
                    &InvocationFacts {
                        target: *target,
                        connection_pending_until_unix_nanos: None,
                        approval: fenced.progress.approval,
                    },
                    now,
                )
            })
            .collect();
        drop(targets);
        let mut permitted = Vec::new();
        for (fenced, decision) in self.invocations.iter_mut().zip(decisions) {
            match decision {
                Decision::Dispatch => permitted.push(fenced.admitted.id.clone()),
                Decision::Deny(failure) => {
                    fenced.reply.record(FenceOutcome::Rejected(failure.into()));
                }
                Decision::RefreshRequired => fenced.reply.record(FenceOutcome::RefreshRequired),
                Decision::RunTerminated => fenced.reply.record(FenceOutcome::RunTerminated),
                // The gate records no pending request; a wait here means the
                // policy now asks for a decision the Invocation never got.
                Decision::Wait { .. } => fenced.reply.record(FenceOutcome::Rejected(
                    CoralFailure::ApprovalRequired.into(),
                )),
            }
        }
        let started = invocation_repository::mark_dispatch_started_batch(
            transaction,
            &run.workspace_id,
            &run.id,
            &permitted,
            now,
        )
        .await?;
        for fenced in &mut self.invocations {
            if !permitted.contains(&fenced.admitted.id) {
                continue;
            }
            fenced
                .reply
                .record(if started.contains(&fenced.admitted.id) {
                    FenceOutcome::Permitted
                } else {
                    FenceOutcome::Superseded
                });
        }
        Ok(Vec::new())
    }

    fn settle(self: Box<Self>, recorded: Result<(), Error>) {
        for fenced in self.invocations {
            fenced
                .reply
                .settle(recorded.as_ref().copied().map_err(lost_again));
        }
    }
}

/// The loss every Invocation of a batch learns when the chunk did not land:
/// the owner settles a chunk's work with a lost claim, the Program host's
/// end, or corruption.
fn lost_again(error: &Error) -> Error {
    match error {
        Error::OwnerLost => Error::OwnerLost,
        Error::ProgramHostTerminated => Error::ProgramHostTerminated,
        Error::CorruptData(reason) => Error::CorruptData(reason.clone()),
        other => Error::InvocationPersistence(other.to_string()),
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
            generation: self.admitted.target.provider_generation_id().clone(),
            tool: self.admitted.target.tool().key().clone(),
        };
        // A row that is no longer dispatching was settled by the termination
        // itself; nothing is left to end.
        let ended = crate::invocation_lifecycle::end_for_termination(
            transaction,
            run,
            &unfinished,
            InvocationResultKind::RunTerminated,
            None,
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

#[async_trait]
impl crate::builtin::InvokingRun for ToolInvoker {
    /// Whether this Run still accepts Program work: not stopped, terminated or
    /// past its deadline.
    async fn still_current(&self) -> Result<bool, Error> {
        if self.stop.is_requested() {
            return Ok(false);
        }
        let mut tx = self.database.begin_read_snapshot().await?;
        let run =
            run_repository::find_exact_in_transaction(&mut tx, &self.workspace_id, &self.run_id)
                .await?;
        tx.commit().await?;
        let now = self.now()?;
        Ok(run.is_some_and(|run| self.run_is_current(&run, now)))
    }

    async fn stopped(&self) {
        self.stop.requested().await;
    }

    fn now(&self) -> Result<i64, Error> {
        ToolInvoker::now(self)
    }

    #[cfg(test)]
    fn setup_prepared(&self) {
        self.observer.builtin_setup_prepared();
    }
}

/// What the fence permits: one upstream send, or a Built-in Tool that
/// Coral performs itself.
enum Dispatch {
    Upstream(ReadyDispatch),
    Builtin(Box<crate::builtin::BuiltinCall>),
}
