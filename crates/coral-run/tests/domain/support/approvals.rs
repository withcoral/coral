//! Approval waits, required-action waits and a Run held at its approval gate.
use crate::approval::{ApprovalDecision, ApprovalModule};
use crate::tests::*;

/// Asserts that the Program caught an Invocation failure and returned its code.
pub(crate) fn assert_final_code(update: &crate::RunUpdate, code: &str) {
    assert!(
        matches!(update.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(code)),
        "{update:?}"
    );
}

pub(crate) async fn wait_for_approval(
    approvals: &ApprovalModule,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    run: &crate::RunId,
) -> crate::approval::ApprovalRequest {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = approvals.list_for_run(actor, workspace, run).await.unwrap();
            if let Some(request) = requests.first() {
                return request.clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("Invocation waits on durable approval")
}

pub(crate) async fn wait_for_actions(
    runs: &RunModule,
    actor: &AuthenticatedPrincipal,
    workspace: &coral_workspace::WorkspaceId,
    run: &crate::RunId,
    count: usize,
    invocations: usize,
) -> crate::RunUpdate {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let update = runs
                .observe(actor, workspace, run, &immediate(None, 100))
                .await
                .unwrap();
            if update.required_actions().len() == count
                && retained_events(runs, actor, workspace, run)
                    .await
                    .iter()
                    .filter(|event| matches!(event.kind(), RunEventKind::InvocationStarted { .. }))
                    .count()
                    == invocations
            {
                return update;
            }
            assert_eq!(
                update.state(),
                RunState::Running,
                "Run ended before human work: {update:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("required actions become visible")
}

/// Reads the action queue as transports do: authority and page share one snapshot.
pub(crate) trait ActionQueue {
    async fn required_actions(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: Option<&coral_workspace::WorkspaceId>,
        after: Option<&crate::QueueCursor>,
        limit: u16,
    ) -> Result<crate::RequiredActions, Error>;
}

impl ActionQueue for RunModule {
    async fn required_actions(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace: Option<&coral_workspace::WorkspaceId>,
        after: Option<&crate::QueueCursor>,
        limit: u16,
    ) -> Result<crate::RequiredActions, Error> {
        let mut tx = self.database.begin_read_snapshot().await?;
        let authority = self
            .workspaces
            .action_history_authority_in_transaction(&mut tx, actor, workspace)
            .await?;
        let page = self
            .required_actions_in_transaction(&mut tx, &authority, after, limit)
            .await?;
        tx.commit().await?;
        Ok(page)
    }
}

/// An approval-gated Operation whose Invocations stop at the approval wait.
pub(crate) struct ApprovalGatedOperation {
    pub(crate) fixture: TestModules,
    pub(crate) operation: OperationFixture,
    gate: Arc<FenceGate>,
    release: oneshot::Sender<()>,
}

impl ApprovalGatedOperation {
    pub(crate) async fn new(user_id: &str) -> Self {
        let (gate, release) = FenceGate::blocking();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
            Arc::new(RejectingUpstream),
            Arc::new(ApprovalWaitGate(gate.clone())),
        )
        .await;
        let operation = fixture.user_operation(user_id, "Approval waits").await;
        fixture.require_approval(&operation).await;
        Self {
            fixture,
            operation,
            gate,
            release,
        }
    }

    /// Starts a Run as `actor` and holds it at its approval wait.
    pub(crate) async fn hold(self, actor: AuthenticatedPrincipal) -> HeldApproval {
        let Self {
            fixture,
            operation,
            gate,
            release,
        } = self;
        let approvals = fixture.approvals();
        let accepted = fixture
            .runs
            .start(&actor, &operation.workspace_id, submission("invoke"))
            .await
            .unwrap();
        wait_until(|| gate.entered()).await;
        let run = accepted.run_id().clone();
        let waiting = fixture
            .runs
            .observe(&actor, &operation.workspace_id, &run, &immediate(None, 100))
            .await
            .unwrap();
        assert_eq!(waiting.required_actions().len(), 1);
        HeldApproval {
            fixture,
            operation,
            approvals,
            actor,
            run,
            waiting,
            _release: release,
        }
    }
}

/// A Run held at its approval wait, so the worker commits nothing more on its own.
pub(crate) struct HeldApproval {
    pub(crate) fixture: TestModules,
    pub(crate) operation: OperationFixture,
    pub(crate) approvals: ApprovalModule,
    /// The Principal that started the Run.
    pub(crate) actor: AuthenticatedPrincipal,
    pub(crate) run: crate::RunId,
    pub(crate) waiting: crate::RunUpdate,
    _release: oneshot::Sender<()>,
}

impl HeldApproval {
    /// Holds a Run that the Operation's User started.
    pub(crate) async fn new(user_id: &str) -> Self {
        Self::under(user_id, crate::execution::ExecutionPolicy::testing()).await
    }

    /// Holds a Run whose Approval Request expires `approval_wait` after its
    /// creation, by the owner's timer.
    pub(crate) async fn expiring(user_id: &str, approval_wait: Duration) -> Self {
        Self::under(
            user_id,
            crate::execution::ExecutionPolicy::testing_with_approval_wait(approval_wait),
        )
        .await
    }

    async fn under(user_id: &str, policy: crate::execution::ExecutionPolicy) -> Self {
        let mut setup = ApprovalGatedOperation::new(user_id).await;
        setup.fixture.runs.execution_policy = policy;
        let owner = setup.operation.principal.clone();
        setup.hold(owner).await
    }

    /// Waits up to `wait` for anything newer than the held snapshot.
    pub(crate) fn observe_changes(
        &self,
        wait: Duration,
    ) -> impl std::future::Future<Output = Result<crate::RunUpdate, Error>> + '_ {
        let observation = immediate(Some(self.waiting.next_cursor().clone()), 100)
            .with_wait(wait)
            .unwrap();
        async move {
            self.fixture
                .runs
                .observe(
                    &self.actor,
                    &self.operation.workspace_id,
                    &self.run,
                    &observation,
                )
                .await
        }
    }

    pub(crate) fn action(&self) -> &crate::RequiredAction {
        self.waiting.required_actions().first().unwrap()
    }

    /// The Operation's User approves the held Invocation.
    pub(crate) async fn approve(&self) {
        let crate::RequiredActionRequest::InvocationApproval(id) = &self.action().request else {
            panic!("approval")
        };
        self.approvals
            .decide(
                &self.operation.principal,
                &self.operation.workspace_id,
                id,
                ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"d".repeat(32)).unwrap(),
            )
            .await
            .unwrap();
    }
}
