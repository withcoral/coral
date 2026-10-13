//! Durable Program admission and Run execution for Coral.
//!
//! [`RunModule`] consumes current source selections from `coral-functions` and
//! owns accepted source snapshots and execution. It validates exact
//! Principal and Workspace attribution, compiles outside database write
//! transactions, atomically accepts one complete Program package with
//! `run.started`, claims execution at most once, and owns sequenced Run Events,
//! durable Program Emissions and Logs, and the Run-bound Operation Invocation
//! and dispatch-fence choreography.
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
//! - [`RunModule::cancel`] durably records cancellation through the same
//!   Principal-specific visibility rules, terminates active execution, and waits
//!   for bounded child shutdown before returning the winning terminal result.
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
//! - [`RunModule::recover`] performs one explicit host-maintenance pass that
//!   releases expired unstarted claims and interrupts lost started execution; it
//!   neither schedules released work nor installs startup or polling behavior.
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
//! Execution code is always reloaded from that durable package. A leased worker
//! claim can move only before the separately durable execution-start mark; after
//! that mark JavaScript is never resumed or replayed. `coral.emit` and the bounded
//! console append one sequenced Event under the current claim before returning to
//! Program code; Emissions
//! and Logs remain observable after a later failure. Invocation admission
//! atomically retains its Provider target, arguments, child record, and
//! `invocation.started`. One short
//! final transaction holds current claim/Run, host-fed Organization authority,
//! Workspace and Agent/Registration, Provider, policy, exact Account authorization
//! and token-material revision, and Approval evidence. It validates
//! the retained Provider Generation rather than requiring it to remain active.
//! After every awaited check it resamples time and validates Run, claim,
//! Registration, and Approval deadlines immediately before the single-use fence.
//! Missing User caller Accounts attach to durable shared connection requests;
//! each Invocation keeps its Run deadline and detaches on selection or terminal
//! cleanup. Approval is requested only after Account selection and before credential
//! resolution. Its first decision and safe lifecycle evidence commit together;
//! decline and expiry also settle the child atomically. Waiting never replays a
//! Program or changes its selected account. Sending holds no database
//! transaction, and `invocation.ended` commits before the Program Promise settles.
//! A terminal-persistence failure terminates host execution without settling the
//! Promise, and a Run cannot complete while any child Invocation is nonterminal.
//! Cancellation and deadline intent stop new Program work, request uncatchable V8
//! termination, and preserve complete child outcomes that beat bounded cleanup.
//! Recovery revokes a stale started claim before settling its children and Run.
//! The first terminal transaction wins. User work remains private; Workspace
//! Owners can observe and cancel Workspace-owned Agent
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
mod history;
mod human_interface;
pub use history::{InvocationStatus, InvocationSummary, RunSummary};
pub use human_interface::{HumanInterfaceUrl, HumanInterfaceUrlError};
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
mod program_output;
mod required_actions;
mod required_actions_repository;
pub mod retention;
mod run_repository;
mod termination;
mod types;
mod worker;

use std::sync::Arc;
use std::time::Duration;

use coral_db::{Database, Transaction};
use coral_workspace::{
    AuthenticatedPrincipal, WorkspaceAction, WorkspaceId, WorkspaceModule, WorkspaceRole,
};

pub use coral_v8::{IsolateLimits, ProgramError};
pub use coral_value::ValueLimits;
use engine::{Clock, ProgramEngine, SystemClock, V8ProgramEngine};
pub use error::{CoralFailure, Error, Rejection};
use execution::{ActiveExecutions, ExecutionPolicy};
#[cfg(test)]
use execution::{ExecutionObserver, NoopExecutionObserver};
#[cfg(test)]
use invocation::{InvocationObserver, NoopInvocationObserver};
use invocation::{ProductionUpstream, UpstreamSender};
pub use required_actions::{
    ActionCursor, ActionPage, ActionSummary, QueueCursor, QueuedAction, RequiredAction,
    RequiredActionRequest, RequiredActions, RequiredActor,
};
pub use types::{
    COMBINED_TYPESCRIPT_MAX_BYTES, COMPILATION_TIMEOUT, EMITTED_JAVASCRIPT_MAX_BYTES, FinalValue,
    InvocationId, InvocationResultKind, LINKED_SAVED_FUNCTION_MAX_COUNT,
    PROGRAM_LOG_MESSAGE_MAX_BYTES, PROGRAM_SOURCE_MAX_BYTES, ProgramLogLevel, ProgramSource,
    ProgramValue, RUN_EVENT_PAGE_MAX, RUN_TIMEOUT_MAX_MS, RunCursor, RunEvent, RunEventKind,
    RunFailure, RunFailureCode, RunId, RunObservation, RunState, RunSubmission, RunTerminalOutcome,
    RunTimeout, RunUpdate, SOURCE_MAP_MAX_BYTES,
};

static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// Returns the Program Run Module's domain-owned migration plan.
#[must_use]
pub const fn migrator() -> &'static sqlx::migrate::Migrator {
    &MIGRATOR
}

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
    provider_publisher: Option<Arc<dyn coral_provider::WorkspaceProviderPublisher>>,
    human_interface: Option<HumanInterfaceUrl>,
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    engine: Arc<dyn ProgramEngine>,
    upstream: Arc<dyn UpstreamSender>,
    clock: Arc<dyn Clock>,
    execution_policy: ExecutionPolicy,
    value_limits: coral_value::ValueLimits,
    active_executions: ActiveExecutions,
    #[cfg(test)]
    invocation_observer: Arc<dyn InvocationObserver>,
    #[cfg(test)]
    execution_observer: Arc<dyn ExecutionObserver>,
}

impl RunModule {
    /// Builds the runtime with deployment-selected outbound policy and isolate limits.
    /// Built-in Operations prepare Setups through `providers`, the bootstrap's
    /// Provider Module.
    /// # Errors
    /// Returns an initialization error when the runtime or HTTP client cannot start.
    pub fn new(
        policy: coral_outbound::DestinationPolicy,
        database: Arc<Database>,
        workspaces: WorkspaceModule,
        providers: coral_provider::ProviderModule,
        isolate_limits: IsolateLimits,
    ) -> Result<Self, Error> {
        Ok(Self {
            providers,
            provider_publisher: None,
            human_interface: None,
            database,
            workspaces,
            accounts: None,
            engine: Arc::new(V8ProgramEngine::new(isolate_limits)?),
            upstream: Arc::new(ProductionUpstream::new(policy)?),
            clock: Arc::new(SystemClock),
            execution_policy: ExecutionPolicy::production(),
            value_limits: coral_value::ValueLimits::default(),
            active_executions: ActiveExecutions::default(),
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
            active_executions: ActiveExecutions::default(),
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

    /// Lets `coral.admin.createProvider` publish a Provider that needs nobody to
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

    #[cfg(test)]
    fn has_active_execution(&self, run_id: &RunId) -> bool {
        self.active_executions.contains(run_id)
    }

    #[cfg(test)]
    async fn count_runs(&self) -> Result<u64, Error> {
        run_repository::count_runs(&self.database).await
    }
}

fn duration_until(deadline_at_unix_nanos: i64, now_unix_nanos: i64) -> Duration {
    let remaining = deadline_at_unix_nanos.saturating_sub(now_unix_nanos);
    Duration::from_nanos(u64::try_from(remaining).unwrap_or(0))
}

#[cfg(test)]
#[path = "../tests/domain/mod.rs"]
mod tests;
