//! Durable Program admission and Run execution for Coral.
//!
//! [`RunModule`] consumes current source selections from `coral-functions` and
//! owns accepted source snapshots and execution. It validates exact
//! Principal and Workspace attribution, compiles outside database write
//! transactions, atomically accepts one complete Program package with
//! `run.started` and the Run's worker claim, and gives every launched Run one
//! Run Owner (ADR 0018): the in-process actor that alone appends the Run
//! Ledger on every path the Program takes, in chunks, and runs the Run-bound
//! Operation Invocation and dispatch-fence choreography inside its commits. A
//! Run accepted in a caller's transaction and never launched keeps its claim
//! until the expired-lease sweep ends it.
//!
//! # Interface
//!
//! - [`RunModule::start`] admits a complete package and returns its first
//!   [`RunUpdate`], without a goal or intent parameter.
//! - [`RunModule::prepare`], [`RunModule::accept_in_transaction`], and
//!   [`RunModule::launch`] expose the same admission and execution behavior to
//!   an upper-level transaction owner. Staged acceptance is not proof of commit;
//!   launch reads durable evidence and retains normal worker and authority fences.
//! - [`RunModule::observe`] replays bounded authored Emissions/Logs with current
//!   Principal-filtered required actions and terminal data on the final output page.
//!   Bounded waiting ignores internal lifecycle records. Initial observation batches
//!   authored output until human action, terminal state, or the requested deadline.
//! - [`RunModule::cancel`] asks the Run's owner to cancel it, through the same
//!   Principal-specific visibility rules: the owner terminates active execution
//!   at once and records the intent in its next chunk. The call waits up to
//!   the shutdown grace for the owner to end the Run, then returns the state
//!   recorded by then.
//! - [`RunModule::activity_entries`] reads bounded chronological child summaries.
//!   Exact Event/Invocation anchors retain their identities. Upper owners can join
//!   authorized read snapshots through transaction facets without Task dependencies.
//!   Context impact subtracts retained Operation Result UTF-8 bytes from Emission,
//!   Log-message, and Final Value bytes. Counts commit with their evidence; summaries
//!   never load those payloads. Final Value bytes appear on the terminal child row.
//!   Running or indeterminate work has no complete total. This estimates payload
//!   impact, not tokens, protocol envelopes, repeated observations, or actual client usage.
//! - [`RunModule::inspect_run`] and [`RunModule::inspect_invocation`] independently
//!   authorize exact retained source, bounded Events, logical arguments, native outcomes,
//!   and historical request references. These reads never renew retention. Retained
//!   Account references survive active waiter detachment and grant no request authority.
//! - [`RunModule::sweep_expired_leases`] is one pass of the expired-lease sweep:
//!   every `running` Run that no process owns (its lease lapsed, or it has no
//!   claim and was accepted more than a lease ago) gets its Run Owner back,
//!   which records the timers that passed and the Run's end in one chunk.
//!   Bootstrap runs it before any listener binds and then every
//!   [`RunModule::sweep_interval`]. Coral never resumes a Run.
//! - [`RunModule::shutdown`] refuses new Runs, lets the live ones end for the
//!   Shutdown Drain, then interrupts the rest within the commit grace.
//! - [`RunModule::action_prompts`] and [`RunModule::answer_action_prompt`] let an
//!   interactive client ask the invoking User about each current action of their
//!   own Run. An approval answer decides through the same Approval rules as Reef;
//!   a connection answer changes nothing. Prompts keep no state.
//! - [`approval::ApprovalModule`] exposes human-only exact Invocation decisions;
//!   request validity and lifecycle evidence stay behind that separate Interface.
//!
//! The private Run-bound `OperationInvoker` is the sole Program-to-host Operation
//! seam. It retains exact Run, Workspace, Principal, claim, deadline, and
//! limit context; callers do not coordinate Invocation persistence or dispatch.
//!
//! Access removal is owned by Administration. Run retains live dispatch checks,
//! accepted source snapshots, historical attribution, and pending-work settlement.
//!
//! # Invariants
//!
//! Admission resolves one consistent, bounded transitive
//! source closure, initializes each linked module once before the submitted body,
//! and creates no separate Saved Function execution record. Admission failure creates
//! no Run and starts no execution. The deadline begins at durable acceptance.
//! Exact submitted and linked source, emitted JavaScript, source map, compiler
//! provenance, limits, Run, and `run.started` evidence commit together.
//! Execution code is always reloaded from that durable package. Acceptance
//! writes the Run's worker claim, so no running Run is unclaimed; the Run
//! Owner's first chunk is the execution-start mark, and after it JavaScript is
//! never resumed or replayed. The owner applies `coral.emit`, the bounded
//! console, admission and Invocation results in arrival order against its
//! exact output total and answers the Program at Applied; each chunk commits
//! when none is in flight, predicated on the owner's claim and the head, and
//! renews the lease. Above the Applied cap on unrecorded output the Program
//! waits for the chunk. Emissions and Logs remain observable after a later
//! failure. Invocation admission retains its Provider target, arguments,
//! child record, and `invocation.started` in one chunk. The Dispatch Fence is
//! one batched check per chunk commit: inside the chunk's transaction it
//! reads the Workspace, Agent/Registration, Provider, policy and Account
//! facts the chunk's ready Invocations share once, holding their rows shared
//! so fences of other Runs read beside it and an authority writer waits for
//! it, decides each Invocation in memory against those facts, the Run row as
//! the chunk read it and the approval its gate recorded, at one clock sample after
//! the last read, and records every permitted dispatch start in the same
//! commit; it reads the retained Operation by its Generation id and never
//! the Account's material. Decryption and the upstream send run outside
//! every lock, after the pre-send check; a stop that reaches the owner first
//! ends a fenced Invocation as `run_terminated` instead of sending, and a
//! fence not yet committed answers terminated at once. Missing User caller Accounts attach to durable shared connection
//! requests; each Invocation keeps its Run deadline and detaches on selection
//! or terminal cleanup. Approval is requested only after Account selection and
//! before credential resolution. Its first decision and safe lifecycle
//! evidence commit together; decline and expiry also settle the child
//! atomically. Waiting never replays a Program or changes its selected
//! account. An upstream result settles the Program Promise at Applied; a known
//! failure only once it is recorded. A claim mismatch on commit stops the
//! owner at once: it terminates the Program, cancels its dispatches, and
//! answers waiting requests `OwnerLost`. A Run cannot complete while any child
//! Invocation is nonterminal.
//! While a Run is nonterminal its owner is the only writer of its row and its
//! Events: approval decisions and cancellation reach the owner as requests,
//! and the Run and Approval Request deadlines are its own timers, applied in
//! mailbox order with them (see [`owner`]); a request for a nonterminal Run
//! with no owner in this process is answered `OwnedElsewhere`. The worker
//! lease is ownership between processes: a Run whose lease lapsed with no
//! owner here is the expired-lease sweep's, which recreates its owner, and a
//! Run whose live lease another process holds is never touched. The clock
//! decides a Run's timeout and an Approval Request's expiry only inside the
//! owner (its `apply`, and its own writes' clock sample). A connection
//! request is Account-owned: the Account module records its expiry when a holder reads it, and the
//! owner's timer at its deadline only wakes the Invocations parked on it.
//! Readers show the recorded state beside a deadline and never derive an
//! expiry or a timeout from `now`.
//! Cancellation and deadline intent stop new Program work, request uncatchable V8
//! termination, and preserve complete child outcomes that beat bounded cleanup.
//! A recovered owner takes the Run over with a new claim in the same chunk that
//! ends its children and the Run, so nothing still in flight under the old
//! claim lands. The first terminal transaction wins. User work remains
//! private; Workspace Owners can observe and cancel Workspace-owned Agent
//! Runs, while an Agent can address only that same Agent's Run in its fixed
//! Workspace. Run identifiers and cursors grant no authority.

mod action_prompts;
mod activity;
mod activity_observation;
mod admission;
pub use action_prompts::{ActionPrompt, PromptAnswer};
pub use activity::{RunEntries, RunEntry, RunEntryAnchor, RunEntryWindow};
pub use activity_observation::{ActivityObservation, ActivityWake};
pub use admission::{PreparedRun, StagedRun};
pub mod approval;
mod approval_repository;
mod builtin;
mod engine;
mod error;
mod execution;
mod fence;
mod history;
mod human_interface;
pub use history::{InvocationStatus, InvocationSummary, RunSummary};
pub use human_interface::{HumanInterfaceUrl, HumanInterfaceUrlError};
mod instance;
pub use instance::CoralInstanceId;
pub mod history_repository;
mod inspection;
mod inspection_repository;
pub use inspection::{
    InvocationDetail, InvocationOutcome, RunDetail, RunEventDetail, RunInspection, SourceSnapshot,
};
mod invocation;
mod invocation_lifecycle;
mod invocation_repository;
mod lifecycle;
mod observation;
mod owner;
mod program_output;
mod recovery;
mod required_actions;
mod required_actions_repository;
pub mod retention;
mod run_repository;
mod shutdown;
mod source_location_repository;
mod summary;
mod summary_repository;
mod termination;
mod types;
mod worker;

use std::sync::Arc;
use std::time::Duration;

use coral_db::{Database, Transaction};
use coral_workspace::{
    AuthenticatedPrincipal, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};

pub use coral_v8::{IsolateLimits, ProgramError, ProgramOperationError};
pub use coral_value::ValueLimits;
use engine::{Clock, ProgramEngine, SystemClock, V8ProgramEngine};
pub use error::{CoralFailure, Error, Rejection};
use execution::ExecutionPolicy;
#[cfg(test)]
use execution::{ExecutionObserver, NoopExecutionObserver};
#[cfg(test)]
use invocation::{InvocationObserver, NoopInvocationObserver};
use invocation::{ProductionUpstream, UpstreamSender};
use owner::Owners;
pub use required_actions::{
    ActionCursor, ActionPage, ActionSummary, QueueCursor, QueuedAction, RequiredAction,
    RequiredActionRequest, RequiredActions, RequiredActor,
};
pub use summary::{
    ActivitySummary, ActorContext, ApprovalDecisions, CallOutcomes, MAX_SUMMARY_DAYS,
    OperationCalls, StoppedOperation,
};
pub use types::{
    AppliedOutputCap, CHUNK_BYTES_CAP, CHUNK_EVENTS_CAP, COMBINED_TYPESCRIPT_MAX_BYTES,
    COMPILATION_TIMEOUT, EMITTED_JAVASCRIPT_MAX_BYTES, FanOutLimit, FinalValue, InvocationId,
    InvocationResultKind, LINKED_SAVED_FUNCTION_MAX_COUNT, PROGRAM_LOG_MESSAGE_MAX_BYTES,
    PROGRAM_SOURCE_MAX_BYTES, ProgramLogLevel, ProgramSource, ProgramValue, RUN_EVENT_PAGE_MAX,
    RUN_TIMEOUT_MAX_MS, RunCursor, RunEvent, RunEventKind, RunFailure, RunFailureCode, RunId,
    RunObservation, RunState, RunSubmission, RunTerminalOutcome, RunTimeout, RunUpdate,
    RuntimeLimits, SOURCE_MAP_MAX_BYTES, ShutdownDrain, SourceDocument, SourceLocation,
};

#[cfg(test)]
struct TestObservers {
    invocation: Arc<dyn InvocationObserver>,
    execution: Arc<dyn ExecutionObserver>,
}

#[cfg(test)]
impl TestObservers {
    fn new(invocation: Arc<dyn InvocationObserver>, execution: Arc<dyn ExecutionObserver>) -> Self {
        Self {
            invocation,
            execution,
        }
    }
}

/// Durable Program Run lifecycle Module.
#[derive(Clone)]
pub struct RunModule {
    accounts: Option<coral_provider::account::AccountModule>,
    providers: coral_provider::ProviderModule,
    policies: coral_policy::OperationPolicyModule,
    provider_publisher: Option<Arc<dyn coral_provider::WorkspaceProviderPublisher>>,
    human_interface: Option<HumanInterfaceUrl>,
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    engine: Arc<dyn ProgramEngine>,
    upstream: Arc<dyn UpstreamSender>,
    clock: Arc<dyn Clock>,
    execution_policy: ExecutionPolicy,
    value_limits: coral_value::ValueLimits,
    applied_output_cap: AppliedOutputCap,
    fan_out_limit: FanOutLimit,
    shutdown_drain: ShutdownDrain,
    /// The Coral Instance this module runs in; a Run row it claims records it.
    instance: CoralInstanceId,
    owners: Owners,
    admission: shutdown::Admission,
    /// Bounds the recovered owners' writes at once, so a mass recovery after
    /// a crash queues here instead of on the pool.
    recovery_writes: Arc<tokio::sync::Semaphore>,
    #[cfg(test)]
    invocation_observer: Arc<dyn InvocationObserver>,
    #[cfg(test)]
    execution_observer: Arc<dyn ExecutionObserver>,
}

impl RunModule {
    /// Builds the runtime with the deployment's outbound policy and runtime limits.
    /// Built-in Operations prepare Setups through `providers`, the bootstrap's
    /// Provider Module. `instance` is the Coral Instance the process runs as,
    /// which every Run it claims records.
    /// # Errors
    /// Returns an initialization error when the runtime or HTTP client cannot start.
    pub fn new(
        policy: coral_outbound::DestinationPolicy,
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        providers: coral_provider::ProviderModule,
        limits: RuntimeLimits,
        instance: CoralInstanceId,
    ) -> Result<Self, Error> {
        Ok(Self {
            providers,
            policies: coral_policy::OperationPolicyModule::new(
                database.clone(),
                workspaces.clone(),
            ),
            provider_publisher: None,
            human_interface: None,
            database,
            workspaces,
            accounts: None,
            engine: Arc::new(V8ProgramEngine::new(limits.isolate())?),
            upstream: Arc::new(ProductionUpstream::new(policy)?),
            clock: Arc::new(SystemClock),
            execution_policy: ExecutionPolicy::production(),
            value_limits: coral_value::ValueLimits::default(),
            applied_output_cap: limits.applied_output_cap(),
            fan_out_limit: limits.fan_out_limit(),
            shutdown_drain: limits.shutdown_drain(),
            instance,
            owners: Owners::default(),
            admission: shutdown::Admission::default(),
            recovery_writes: recovery::write_permits(),
            #[cfg(test)]
            invocation_observer: Arc::new(NoopInvocationObserver),
            #[cfg(test)]
            execution_observer: Arc::new(NoopExecutionObserver),
        })
    }

    #[cfg(test)]
    pub(crate) fn with_adapters(
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        providers: coral_provider::ProviderModule,
        engine: Arc<dyn ProgramEngine>,
        upstream: Arc<dyn UpstreamSender>,
        clock: Arc<dyn Clock>,
        observers: TestObservers,
    ) -> Self {
        Self {
            providers,
            policies: coral_policy::OperationPolicyModule::new(
                database.clone(),
                workspaces.clone(),
            ),
            provider_publisher: None,
            human_interface: None,
            database,
            workspaces,
            accounts: None,
            engine,
            upstream,
            clock,
            execution_policy: ExecutionPolicy::testing(),
            value_limits: coral_value::ValueLimits::default(),
            applied_output_cap: AppliedOutputCap::default(),
            fan_out_limit: FanOutLimit::default(),
            shutdown_drain: ShutdownDrain::default(),
            instance: CoralInstanceId::generate(),
            owners: Owners::default(),
            admission: shutdown::Admission::default(),
            recovery_writes: recovery::write_permits(),
            invocation_observer: observers.invocation,
            execution_observer: observers.execution,
        }
    }

    /// Enables fixed/caller credential resolution and User connection waits.
    /// Without this dependency, credential-bearing Invocations fail closed.
    #[must_use]
    pub fn with_accounts(mut self, accounts: coral_provider::account::AccountModule) -> Self {
        self.accounts = Some(accounts);
        self
    }

    /// Lets `coral.workspace.addProvider` publish a Provider that needs nobody to
    /// sign in, through the workflow Reef publishes with. Without it, such a
    /// call fails with `operation_unavailable`.
    #[must_use]
    pub fn with_provider_publisher(
        mut self,
        publisher: Arc<dyn coral_provider::WorkspaceProviderPublisher>,
    ) -> Self {
        self.provider_publisher = Some(publisher);
        self
    }

    /// The Coral Instance this module runs in.
    #[cfg(test)]
    pub(crate) const fn instance(&self) -> &CoralInstanceId {
        &self.instance
    }

    /// Names the Reef origin that built-in Operation Results link to. Only the
    /// runtime that launches Programs needs it; without it, a built-in
    /// Operation fails as `operation_unavailable`.
    #[must_use]
    pub fn with_human_interface(mut self, human_interface: HumanInterfaceUrl) -> Self {
        self.human_interface = Some(human_interface);
        self
    }

    /// Selects deployment limits for future admissions; existing Runs retain theirs.
    #[must_use]
    pub const fn with_value_limits(mut self, limits: coral_value::ValueLimits) -> Self {
        self.value_limits = limits;
        self
    }

    /// Returns the limits advertised for new Runs.
    #[must_use]
    pub const fn value_limits(&self) -> coral_value::ValueLimits {
        self.value_limits
    }

    /// Replaces the Applied cap on a built module.
    #[cfg(test)]
    pub(crate) const fn with_applied_output_cap(mut self, cap: AppliedOutputCap) -> Self {
        self.applied_output_cap = cap;
        self
    }

    /// Returns the Applied cap new Run Owners decide against.
    #[must_use]
    pub const fn applied_output_cap(&self) -> AppliedOutputCap {
        self.applied_output_cap
    }

    /// Replaces the Fan-out Limit on a built module.
    #[cfg(test)]
    pub(crate) const fn with_fan_out_limit(mut self, limit: FanOutLimit) -> Self {
        self.fan_out_limit = limit;
        self
    }

    /// Returns the Fan-out Limit new Run Owners decide dispatch starts against.
    #[must_use]
    pub const fn fan_out_limit(&self) -> FanOutLimit {
        self.fan_out_limit
    }

    async fn visibility_scope_in_transaction(
        &self,
        transaction: &mut Transaction<'_>,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
    ) -> Result<bool, Error> {
        if let Some(user) = actor.authenticated_user() {
            return match self
                .workspaces
                .check_in_transaction(transaction, user, workspace_id, WorkspaceAction::Inspect)
                .await
            {
                Ok(evidence) => Ok(evidence.effective_role() == WorkspaceRole::Owner),
                Err(
                    coral_workspace::Error::WorkspaceNotFound
                    | coral_workspace::Error::PermissionDenied(_),
                ) => Err(Error::RunNotFound),
                Err(error) => Err(error.into()),
            };
        }
        if actor.agent_workspace_id() == Some(workspace_id) {
            Ok(false)
        } else {
            Err(Error::RunNotFound)
        }
    }

    fn now(&self) -> Result<i64, Error> {
        self.clock.now_unix_nanos().map_err(Error::Clock)
    }

    /// Whether this process still hosts the Run's owner.
    #[cfg(test)]
    fn has_owner(&self, run_id: &RunId) -> bool {
        self.owners.contains(run_id)
    }

    #[cfg(test)]
    async fn count_runs(&self) -> Result<u64, Error> {
        run_repository::count_runs(&self.database).await
    }
}

fn duration_until(deadline_at_unix_nanos: i64, now_unix_nanos: i64) -> Duration {
    nanos_duration(deadline_at_unix_nanos.saturating_sub(now_unix_nanos))
}

/// `nanos` as a duration; a negative count is no wait.
fn nanos_duration(nanos: i64) -> Duration {
    Duration::from_nanos(u64::try_from(nanos).unwrap_or(0))
}

#[cfg(test)]
#[path = "../tests/domain/mod.rs"]
mod tests;
