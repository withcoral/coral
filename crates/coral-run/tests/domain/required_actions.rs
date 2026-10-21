use super::*;
use crate::approval::ApprovalDecision;

const WAIT_FOR_ACCOUNT: &str = "await coral.providers.weather.getForecast({});";

/// A Run that waited on its User's Account connection request.
struct RetainedConnection {
    fixture: TestModules,
    operation: OperationFixture,
    _directory: tempfile::TempDir,
    accounts: coral_provider::account::AccountModule,
    runs: RunModule,
    run: crate::RunId,
    original: coral_provider::account::ConnectionRequestId,
}

impl RetainedConnection {
    async fn new() -> Self {
        let fixture = TestModules::new(Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
        ))
        .await;
        let operation = fixture
            .user_operation("user:retained-connection", "Retained connection selection")
            .await;
        let (directory, accounts) =
            configure_user_caller(&fixture, &operation, "https://upstream.example").await;
        let runs = fixture.runs.clone().with_accounts(accounts.clone());
        let (run, original) =
            Self::waiting_run(&runs, &operation.principal, &operation.workspace_id).await;
        Self {
            fixture,
            operation,
            _directory: directory,
            accounts,
            runs,
            run,
            original,
        }
    }

    /// Starts a Run as `actor` and returns it with the request it waits on.
    async fn waiting_run(
        runs: &RunModule,
        actor: &AuthenticatedPrincipal,
        workspace: &coral_workspace::WorkspaceId,
    ) -> (crate::RunId, coral_provider::account::ConnectionRequestId) {
        let run = runs
            .start(actor, workspace, submission(WAIT_FOR_ACCOUNT))
            .await
            .unwrap()
            .run_id()
            .clone();
        let waiting = wait_for_actions(runs, actor, workspace, &run, 1, 1).await;
        let crate::RequiredActionRequest::AccountConnection(request) =
            waiting.required_actions().first().unwrap().request
        else {
            panic!("the Run waits on an Account connection")
        };
        (run, request)
    }

    /// Cancels the waiting Run and declines its request.
    async fn end_original(&self) {
        self.runs
            .cancel(
                &self.operation.principal,
                &self.operation.workspace_id,
                &self.run,
                &immediate(None, 1),
            )
            .await
            .unwrap();
        self.accounts
            .decline_connection(
                &change(self.operation.user.clone(), 50, '5'),
                &self.operation.workspace_id,
                self.original,
            )
            .await
            .unwrap();
    }

    /// The Run detail when `actor` selects `request` on `run`.
    async fn select(
        &self,
        actor: &AuthenticatedPrincipal,
        run: &crate::RunId,
        request: coral_provider::account::ConnectionRequestId,
    ) -> Result<crate::RunDetail, Error> {
        self.runs
            .inspect_run(
                actor,
                &self.operation.workspace_id,
                run,
                &crate::RunInspection {
                    account_connection: Some(request),
                    ..Default::default()
                },
            )
            .await
    }

    async fn selected(
        &self,
        run: &crate::RunId,
        request: coral_provider::account::ConnectionRequestId,
    ) -> Option<coral_provider::account::ConnectionRequestId> {
        self.select(&self.operation.principal, run, request)
            .await
            .unwrap()
            .account_connection
    }
}

/// A Run keeps its connection selection after it stops waiting and after the
/// request is resolved, though nothing is actionable any more.
#[tokio::test(flavor = "multi_thread")]
async fn connection_selection_survives_detachment_and_resolution() {
    let retained = RetainedConnection::new().await;
    let (user, run, original) = (
        &retained.operation.principal,
        &retained.run,
        retained.original,
    );
    retained
        .runs
        .cancel(
            user,
            &retained.operation.workspace_id,
            run,
            &immediate(None, 1),
        )
        .await
        .unwrap();
    let detached = retained.select(user, run, original).await.unwrap();
    assert_eq!(detached.account_connection, Some(original));
    assert_eq!(detached.summary.action_page.unwrap().total, 0);
    retained.end_original().await;
    assert_eq!(
        retained.selected(run, original).await,
        Some(original),
        "historical selection does not require a live waiter or pending state"
    );
}

/// A selection applies only to the Run that waited on that request, and only
/// its User can make it.
#[tokio::test(flavor = "multi_thread")]
async fn connection_selection_matches_only_the_run_that_waited_on_it() {
    let retained = RetainedConnection::new().await;
    let (user, workspace) = (
        &retained.operation.principal,
        &retained.operation.workspace_id,
    );
    let unrelated = retained
        .runs
        .start(user, workspace, submission("return 42;"))
        .await
        .unwrap();
    assert_eq!(
        retained
            .selected(unrelated.run_id(), retained.original)
            .await,
        None
    );
    let other_user = organization_user(
        retained.fixture.database.clone(),
        "user:other-connection",
        OrganizationRole::Member,
    )
    .await;
    retained
        .fixture
        .workspaces
        .set_membership(
            &change(retained.operation.user.clone(), 60, '6'),
            workspace,
            &other_user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let other = AuthenticatedPrincipal::user(other_user);
    let (_, other_request) =
        RetainedConnection::waiting_run(&retained.runs, &other, workspace).await;
    assert_eq!(
        retained.selected(&retained.run, other_request).await,
        None,
        "another User's request never matches"
    );
    assert!(matches!(
        retained
            .select(&other, &retained.run, retained.original)
            .await,
        Err(Error::RunNotFound)
    ));
}

/// A later Run's replacement request never retargets the original Run, and a
/// request's recorded expiry removes its actionability but not its retained
/// reference. The deadline passing on the clock records nothing: the request
/// stays a current action until a holder records its expiry.
#[tokio::test(flavor = "multi_thread")]
async fn connection_selection_never_retargets_to_a_replacement_request() {
    let retained = RetainedConnection::new().await;
    retained.end_original().await;
    let (user, workspace) = (
        &retained.operation.principal,
        &retained.operation.workspace_id,
    );
    let (replacement_run, replacement) =
        RetainedConnection::waiting_run(&retained.runs, user, workspace).await;
    assert_ne!(replacement, retained.original);
    assert_eq!(
        retained.selected(&retained.run, replacement).await,
        None,
        "never retarget the original Run to the current replacement"
    );
    assert_eq!(
        retained.selected(&replacement_run, replacement).await,
        Some(replacement)
    );
    let deadline = retained
        .runs
        .observe(user, workspace, &replacement_run, &immediate(None, 1))
        .await
        .unwrap()
        .required_actions()
        .first()
        .unwrap()
        .deadline_at_unix_nanos;
    retained
        .fixture
        .clock
        .next
        .store(deadline, Ordering::SeqCst);
    let past_deadline = retained
        .select(user, &replacement_run, replacement)
        .await
        .unwrap();
    assert_eq!(
        past_deadline.summary.action_page.unwrap().total,
        1,
        "a reader shows the recorded pending request beside its deadline"
    );
    // The User's own read of the request records its expiry.
    assert_eq!(
        retained
            .accounts
            .get_connection_request(
                &change(retained.operation.user.clone(), deadline, '7'),
                workspace,
                replacement,
            )
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::Expired
    );
    let expired = retained
        .select(user, &replacement_run, replacement)
        .await
        .unwrap();
    assert_eq!(
        expired.account_connection,
        Some(replacement),
        "request expiry removes actionability, not the retained reference"
    );
    assert_eq!(expired.summary.action_page.unwrap().total, 0);
    assert_eq!(
        retained.selected(&retained.run, retained.original).await,
        Some(retained.original),
        "a terminal historical decision keeps its original identity"
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one gated observation checks action-only wake-up, unchanged wait expiry, and terminal cleanup"
)]
async fn required_actions_show_pending_approval_and_clear_without_a_run_event() {
    let (gate, release) = FenceGate::blocking();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        Arc::new(RejectingUpstream),
        Arc::new(ApprovalWaitGate(gate.clone())),
    )
    .await;
    let operation = fixture
        .user_operation("user:actions", "Required actions")
        .await;
    fixture.require_approval(&operation).await;
    let approvals = fixture.approvals();
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("invoke"),
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    let waiting = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert_eq!(waiting.state(), RunState::Running);
    assert_eq!(waiting.required_actions().len(), 1);
    let queue = fixture
        .runs
        .required_actions(&operation.principal, None, None, 1)
        .await
        .unwrap();
    assert_eq!(queue.action_count, 1);
    assert_eq!(queue.actions.first().unwrap().run.id, *accepted.run_id());
    assert!(queue.next.is_none());
    let requests = approvals
        .list_for_run(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
        )
        .await
        .unwrap();
    let observation = immediate(Some(waiting.next_cursor().clone()), 100)
        .with_wait(Duration::from_secs(2))
        .unwrap();
    let pending = fixture.runs.observe(
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
        &observation,
    );
    tokio::pin!(pending);
    let activity_before = retained_activity(
        &fixture.database,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await;
    tokio::time::timeout(Duration::from_millis(20), &mut pending)
        .await
        .expect_err("unchanged actions keep observation waiting");
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
            &requests.first().unwrap().id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"a".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    let activity_after = retained_activity(
        &fixture.database,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(
        activity_after > activity_before,
        "approval decisions count without fabricating Run Events"
    );
    approvals
        .decide(
            &operation.principal,
            &operation.workspace_id,
            &requests.first().unwrap().id,
            ApprovalDecision::Decline,
            None,
            &TraceId::parse(&"b".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        retained_activity(
            &fixture.database,
            &operation.workspace_id,
            accepted.run_id()
        )
        .await,
        activity_after,
        "reads and repeated no-op decisions do not renew retention"
    );
    let resolved = tokio::time::timeout(Duration::from_secs(1), pending)
        .await
        .unwrap()
        .unwrap();
    assert!(resolved.required_actions().is_empty());
    assert_eq!(
        fixture
            .runs
            .required_actions(&operation.principal, None, None, 1)
            .await
            .unwrap()
            .action_count,
        0
    );
    assert!(resolved.events().is_empty());
    assert_ne!(resolved.next_cursor(), waiting.next_cursor());
    let no_change = immediate(Some(resolved.next_cursor().clone()), 100)
        .with_wait(Duration::from_millis(30))
        .unwrap();
    let deadline_update = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &no_change,
        )
        .await
        .unwrap();
    assert_eq!(
        deadline_update, resolved,
        "wait expiry returns the current unchanged snapshot"
    );
    immediate(None, 1)
        .with_wait(Duration::from_secs(31))
        .expect_err("bounded observation wait");
    let terminal_wait = immediate(Some(resolved.next_cursor().clone()), 100)
        .with_wait(Duration::from_secs(2))
        .unwrap();
    release.send(()).unwrap();
    let changed = tokio::time::timeout(
        Duration::from_secs(1),
        fixture.runs.observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &terminal_wait,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert!(
        changed.state().is_terminal(),
        "terminal state wakes observation without lifecycle chatter"
    );
    assert!(changed.events().is_empty());
    let terminal = wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(terminal.required_actions().is_empty());
}

/// The spec requires that reads and streams do not renew retention. A wake-up
/// rereads authority and deadlines; a heartbeat rereads only the authority part.
#[tokio::test]
async fn activity_observation_rereads_do_not_renew_retention() {
    let held = HeldApproval::new("user:retention").await;
    let (fixture, operation) = (&held.fixture, &held.operation);
    let retained =
        async || retained_activity(&fixture.database, &operation.workspace_id, &held.run).await;
    let before = retained().await;
    let mut observation = fixture
        .runs
        .observe_activity(&operation.principal)
        .await
        .unwrap();
    // A committed policy change wakes the observer, which reschedules deadlines.
    fixture
        .policies
        .set_access_state(
            &change(operation.user.clone(), 41, '4'),
            operation.policy_target.clone(),
            OperationAccessState::Enabled,
        )
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), observation.next())
            .await
            .unwrap()
            .unwrap(),
        crate::ActivityWake::Changed
    );
    assert_eq!(retained().await, before);
}

/// Action windows seek by request identity, so resolving the anchor or its
/// predecessors never skips or repeats a later action.
#[tokio::test(flavor = "multi_thread")]
async fn action_window_cursor_survives_resolution_of_its_anchor_and_predecessor() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let operation = fixture
        .user_operation("user:action-window", "Action window")
        .await;
    publish_approval_provider(&fixture, &operation).await;
    let (actor, workspace) = (&operation.principal, &operation.workspace_id);
    // Settle each call independently so resolved approvals leave the Run running.
    let accepted = fixture
        .runs
        .start(
            actor,
            workspace,
            submission(
                "await Promise.allSettled([1, 2, 3].map(() => coral.providers.public.getForecast({})));",
            ),
        )
        .await
        .unwrap();
    let run = accepted.run_id();
    wait_for_actions(&fixture.runs, actor, workspace, run, 3, 3).await;
    let window = async |after: Option<&crate::ActionSummary>| {
        fixture
            .runs
            .inspect_run(
                actor,
                workspace,
                run,
                &crate::RunInspection {
                    action_after: after.map(|action| crate::ActionCursor(action.request.clone())),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .summary
            .action_page
            .unwrap()
    };
    let requests = |page: &crate::ActionPage| {
        page.actions
            .iter()
            .map(|action| action.request.clone())
            .collect::<Vec<_>>()
    };
    let all = window(None).await;
    assert_eq!(all.total, 3);
    assert!(all.next_action.is_none());
    let [first, anchor, last] = all.actions.as_slice() else {
        panic!("three current approvals")
    };
    assert_eq!(
        requests(&window(Some(anchor)).await),
        std::slice::from_ref(&last.request)
    );
    let approvals = fixture.approvals();
    for action in [first, anchor] {
        let crate::RequiredActionRequest::InvocationApproval(id) = &action.request else {
            panic!("approval")
        };
        approvals
            .decide(
                actor,
                workspace,
                id,
                ApprovalDecision::Decline,
                None,
                &TraceId::parse(&"c".repeat(32)).unwrap(),
            )
            .await
            .unwrap();
    }
    let resolved = window(Some(anchor)).await;
    assert_eq!(resolved.total, 1);
    assert_eq!(requests(&resolved), std::slice::from_ref(&last.request));
    assert!(resolved.next_action.is_none());
    assert!(matches!(
        crate::ActionCursor::parse("not-an-action-cursor"),
        Err(Error::InvalidInput(_))
    ));
    fixture
        .runs
        .cancel(actor, workspace, run, &immediate(None, 1))
        .await
        .unwrap();
}

/// A Run's action window holds at most 100 cards, but its total counts every
/// current request and the cursor reaches the rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_run_action_window_is_bounded_but_counts_every_request() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let operation = fixture
        .user_operation("user:action-bound", "Action bound")
        .await;
    publish_approval_provider(&fixture, &operation).await;
    let (actor, workspace) = (&operation.principal, &operation.workspace_id);
    let accepted = fixture
        .runs
        .start(
            actor,
            workspace,
            submission(
                "await Promise.allSettled(Array.from({length: 101}, () => coral.providers.public.getForecast({})));",
            ),
        )
        .await
        .unwrap();
    let run = accepted.run_id();
    let window = async |after: Option<crate::ActionCursor>| {
        fixture
            .runs
            .inspect_run(
                actor,
                workspace,
                run,
                &crate::RunInspection {
                    action_after: after,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
            .summary
            .action_page
            .unwrap()
    };
    // Each observation wakes on the next change to the Run's current actions.
    let mut cursor = None;
    let first = loop {
        let page = window(None).await;
        if page.total == 101 {
            break page;
        }
        let update = fixture
            .runs
            .observe(
                actor,
                workspace,
                run,
                &immediate(cursor, 100)
                    .with_wait(Duration::from_secs(5))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(update.state(), RunState::Running, "{update:?}");
        cursor = Some(update.next_cursor().clone());
    };
    assert_eq!(first.actions.len(), 100);
    let rest = window(first.next_action.clone()).await;
    assert_eq!(rest.total, 101);
    let [last] = rest.actions.as_slice() else {
        panic!("the 101st action remains reachable: {rest:?}")
    };
    assert!(rest.next_action.is_none());
    assert!(
        !first
            .actions
            .iter()
            .any(|action| action.request == last.request)
    );
    fixture
        .runs
        .cancel(actor, workspace, run, &immediate(None, 1))
        .await
        .unwrap();
}

/// One Run whose four concurrent Invocations wait on human work: two share one
/// Account connection request and two wait for approval.
struct MixedActions {
    fixture: TestModules,
    operation: OperationFixture,
    _directory: tempfile::TempDir,
    accounts: coral_provider::account::AccountModule,
    runs: RunModule,
    run: crate::RunId,
    /// The Run's update once all three requests are visible.
    waiting: crate::RunUpdate,
    request: coral_provider::account::ConnectionRequest,
}

impl MixedActions {
    async fn new() -> Self {
        let fixture = TestModules::new(Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
        ))
        .await;
        let operation = fixture
            .user_operation("user:concurrent-actions", "Concurrent human actions")
            .await;
        let (directory, accounts) =
            configure_user_caller(&fixture, &operation, "https://upstream.example").await;
        publish_approval_provider(&fixture, &operation).await;
        fixture.require_approval(&operation).await;
        let runs = fixture.runs.clone().with_accounts(accounts.clone());
        let accepted = runs.start(&operation.principal, &operation.workspace_id, submission("await Promise.all([coral.providers.weather.getForecast({}), coral.providers.weather.getForecast({}), coral.providers.public.getForecast({}), coral.providers.public.getForecast({})]);"
        )).await.unwrap();
        let run = accepted.run_id().clone();
        let waiting = wait_for_actions(
            &runs,
            &operation.principal,
            &operation.workspace_id,
            &run,
            3,
            4,
        )
        .await;
        let request = wait_for_connection(&accounts, &operation).await;
        Self {
            fixture,
            operation,
            _directory: directory,
            accounts,
            runs,
            run,
            waiting,
            request,
        }
    }

    fn user(&self) -> &AuthenticatedPrincipal {
        &self.operation.principal
    }

    fn scope(&self) -> &coral_workspace::WorkspaceId {
        &self.operation.workspace_id
    }

    /// Activity rows once every Invocation, including the second Account
    /// waiter, shows that it requires action.
    async fn waiting_rows(&self) -> crate::RunEntries {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let page = self
                    .runs
                    .activity_entries(
                        self.user(),
                        self.scope(),
                        &self.run,
                        &crate::RunEntryWindow::Start,
                        100,
                        None,
                    )
                    .await
                    .unwrap();
                let waiting = page
                    .entries
                    .iter()
                    .filter(|entry| {
                        matches!(entry, crate::RunEntry::Invocation(row) if row.requires_action)
                    })
                    .count();
                if waiting == 4 {
                    break page;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the shared request attaches its second waiter")
    }

    /// The User connects; both Account waiters then wait for approval.
    async fn connect(&self) -> crate::RunUpdate {
        let context = change(self.operation.user.clone(), 50, '5');
        let authorization = self
            .accounts
            .start_user_authorization(
                &context,
                self.scope(),
                &self.operation.provider_id,
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                None,
            )
            .await
            .unwrap();
        self.accounts
            .submit_secret(
                &context,
                self.scope(),
                authorization.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new("required-action-secret-canary".into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        wait_for_actions(&self.runs, self.user(), self.scope(), &self.run, 4, 4).await
    }
}

/// The queue lists each current request once, most urgent first: two waiters
/// on one Account connection and a second Run joining it add no entries, on
/// any page, and detaching that Run leaves the request actionable.
#[tokio::test(flavor = "multi_thread")]
async fn the_queue_counts_each_shared_request_once_across_waiters_runs_and_pages() {
    let mixed = MixedActions::new().await;
    let (runs, user, scope) = (&mixed.runs, mixed.user(), mixed.scope());
    let rows = mixed.waiting_rows().await;
    assert_eq!(
        rows.entries.len(),
        4,
        "both Account waiters and both approvals remain child rows"
    );
    assert!(rows.run.requires_action);
    let queue = runs.required_actions(user, None, None, 10).await.unwrap();
    assert_eq!(queue.action_count, 3, "count requests, not Runs or waiters");
    assert_eq!(queue.actions.len(), 3, "one entry per request");
    assert_eq!(
        queue
            .actions
            .iter()
            .filter(|queued| matches!(
                queued.action.request,
                crate::RequiredActionRequest::AccountConnection(_)
            ))
            .count(),
        1,
        "two approval decisions and one shared connection"
    );
    assert!(
        queue
            .actions
            .iter()
            .zip(queue.actions.iter().skip(1))
            .all(|(earlier, later)| earlier.action.deadline_at_unix_nanos
                <= later.action.deadline_at_unix_nanos),
        "most urgent first"
    );
    assert!(queue.actions.iter().all(|queued| {
        queued.run.id == mixed.run && queued.run.workspace_label == "Concurrent human actions"
    }));

    let second = runs
        .start(
            user,
            scope,
            submission("await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    wait_for_actions(runs, user, scope, second.run_id(), 1, 1).await;
    let bounded = runs.required_actions(user, None, None, 1).await.unwrap();
    assert_eq!(bounded.actions.len(), 1);
    assert!(bounded.next.is_some());
    assert_eq!(
        bounded.action_count, 3,
        "the shared connection counts once across Runs and pages"
    );
    assert_eq!(runs.required_action_count(user, None).await.unwrap(), 3);
    runs.cancel(user, scope, second.run_id(), &immediate(None, 1))
        .await
        .unwrap();
    assert_eq!(
        runs.required_action_count(user, None).await.unwrap(),
        3,
        "detaching one Run leaves the shared request actionable"
    );
}

/// Each action names its exact request, the invoking User and the Run page,
/// and is the same on every Event page. Waiting adds no Run Events.
#[tokio::test(flavor = "multi_thread")]
async fn required_actions_name_their_request_and_ignore_event_paging() {
    let mixed = MixedActions::new().await;
    let (runs, user, scope, waiting) = (&mixed.runs, mixed.user(), mixed.scope(), &mixed.waiting);
    let connection_actions = waiting
        .required_actions()
        .iter()
        .filter(|action| {
            matches!(
                action.request,
                crate::RequiredActionRequest::AccountConnection(_)
            )
        })
        .collect::<Vec<_>>();
    let [connection_action] = connection_actions.as_slice() else {
        panic!("one shared Account connection action")
    };
    assert_eq!(
        connection_action.request,
        crate::RequiredActionRequest::AccountConnection(mixed.request.id)
    );
    assert_eq!(
        connection_action.deadline_at_unix_nanos,
        mixed.request.expires_at_unix_nanos
    );
    for action in waiting.required_actions() {
        assert_eq!(action.required_actor, crate::RequiredActor::InvokingUser);
        assert_eq!(
            action.run_status_path,
            format!("/workspaces/{scope}/runs/{}", mixed.run)
        );
    }
    let replay = runs
        .observe(user, scope, &mixed.run, &immediate(None, 1))
        .await
        .unwrap();
    assert_eq!(
        replay.required_actions(),
        waiting.required_actions(),
        "actions are independent of Event paging"
    );
    assert_eq!(
        replay.event_prefix(0).required_actions(),
        waiting.required_actions()
    );
    assert!(waiting.events().is_empty());
    let kinds = retained_events(runs, user, scope, &mixed.run)
        .await
        .iter()
        .map(|event| match event.kind() {
            RunEventKind::Started => "started",
            RunEventKind::InvocationStarted { .. } => "invocation",
            _ => "other",
        })
        .collect::<Vec<_>>();
    assert_eq!(
        kinds,
        [
            "started",
            "invocation",
            "invocation",
            "invocation",
            "invocation"
        ],
        "waiting for humans adds no Run Event"
    );
}

/// Neither another designated Owner nor an Agent in the Workspace sees another
/// User's actions or their Run.
#[tokio::test(flavor = "multi_thread")]
async fn human_actions_are_concealed_from_owners_and_agents() {
    let mixed = MixedActions::new().await;
    let (runs, scope) = (&mixed.runs, mixed.scope());
    let other_owner = AuthenticatedPrincipal::user(
        mixed
            .fixture
            .workspace_user(
                scope,
                &mixed.operation.user,
                "user:action-outsider",
                WorkspaceRole::Owner,
            )
            .await,
    );
    let agent = registered_agent(&mixed.fixture.workspaces, mixed.user(), scope).await;
    assert_eq!(
        runs.required_actions(&other_owner, None, None, 10)
            .await
            .unwrap()
            .action_count,
        0,
        "ownership never exposes another User's actions"
    );
    assert_eq!(
        runs.required_action_count(&other_owner, None)
            .await
            .unwrap(),
        0
    );
    let denied = runs.required_action_count(&agent, None).await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                _
            )))
        ),
        "Agent callers cannot read human counts: {denied:?}"
    );
    for outsider in [&other_owner, &agent] {
        assert_run_concealed(runs, outsider, scope, &mixed.run).await;
        assert_run_concealed(
            runs,
            outsider,
            scope,
            &crate::RunId::parse("00000000-0000-0000-0000-000000000001").unwrap(),
        )
        .await;
        assert!(matches!(
            runs.observe(
                outsider,
                scope,
                &mixed.run,
                &immediate(Some(mixed.waiting.next_cursor().clone()), 1)
            )
            .await,
            Err(Error::RunNotFound)
        ));
    }
}

/// Connecting hands both Account waiters to their approvals. The handoff adds
/// no Run Event and exposes no credential.
#[tokio::test(flavor = "multi_thread")]
async fn connecting_hands_waiting_invocations_to_approval_without_a_run_event() {
    let mixed = MixedActions::new().await;
    let (runs, user, scope) = (&mixed.runs, mixed.user(), mixed.scope());
    let before = retained_events(runs, user, scope, &mixed.run).await;
    let handoff = mixed.connect().await;
    assert!(handoff.required_actions().iter().all(|action| matches!(
        action.request,
        crate::RequiredActionRequest::InvocationApproval(_)
    )));
    assert_eq!(
        retained_events(runs, user, scope, &mixed.run).await,
        before,
        "Account-to-approval handoff creates no Run Event"
    );
    assert_ne!(handoff.next_cursor(), mixed.waiting.next_cursor());
    assert!(!format!("{handoff:?}").contains("canary"));
    assert_eq!(
        mixed
            .accounts
            .list_connection_requests(&change(mixed.operation.user.clone(), 50, '5'), scope)
            .await
            .unwrap(),
        vec![],
        "the connected request is no longer pending"
    );
}

/// Cancellation clears every action, but each Invocation keeps the requests it
/// waited on, and the Run detail stays bound to the original connection.
#[tokio::test(flavor = "multi_thread")]
async fn cancellation_clears_actions_but_keeps_each_invocations_requests() {
    let mixed = MixedActions::new().await;
    let (runs, user, scope) = (&mixed.runs, mixed.user(), mixed.scope());
    let rows = mixed.waiting_rows().await;
    mixed.connect().await;
    let terminal = runs
        .cancel(
            user,
            scope,
            &mixed.run,
            &immediate(Some(mixed.waiting.next_cursor().clone()), 1),
        )
        .await
        .unwrap();
    assert_eq!(terminal.state(), RunState::Canceled);
    assert!(terminal.required_actions().is_empty());
    let ended = runs
        .activity_entries(
            user,
            scope,
            &mixed.run,
            &crate::RunEntryWindow::Start,
            100,
            None,
        )
        .await
        .unwrap();
    assert!(!ended.run.requires_action);
    assert!(
        ended
            .entries
            .iter()
            .all(|entry| !matches!(entry, crate::RunEntry::Invocation(row) if row.requires_action))
    );
    let detail = runs
        .inspect_run(
            user,
            scope,
            &mixed.run,
            &crate::RunInspection {
                event_limit: 100,
                account_connection: Some(mixed.request.id),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(
        detail.account_connection,
        Some(mixed.request.id),
        "the connected selection stays bound to the original request after detachment"
    );
    assert!(detail.events.iter().any(|event| matches!(event.kind(), RunEventKind::CancellationRequested { principal } if principal == user.principal())));
    assert!(matches!(
        detail.terminal,
        Some(RunTerminalOutcome::Canceled)
    ));
    let mut retained_connections = 0;
    for entry in rows.entries {
        let crate::RunEntry::Invocation(row) = entry else {
            panic!("operation row")
        };
        let exact = runs.inspect_invocation(user, scope, &row.id).await.unwrap();
        assert!(!exact.summary.requires_action);
        if exact
            .requests
            .contains(&crate::RequiredActionRequest::AccountConnection(
                mixed.request.id,
            ))
        {
            retained_connections += 1;
        }
    }
    assert_eq!(
        retained_connections, 2,
        "both Account waiters keep their connection reference"
    );
    assert_eq!(
        mixed
            .accounts
            .get_connection_request(
                &change(mixed.operation.user.clone(), 50, '5'),
                scope,
                mixed.request.id
            )
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::Connected
    );
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one observation scenario proves Agent summaries, Owner visibility, and mid-wait authority loss"
)]
async fn required_actions_disclose_owner_work_to_agents_without_granting_approval_access() {
    let fixture = TestModules::new(Arc::new(InvokingEngine::new(
        "coral.providers.weather.getForecast",
    )))
    .await;
    let operation = fixture
        .user_operation("user:agent-owner", "Agent required actions")
        .await;
    fixture.require_approval(&operation).await;
    let agent = registered_agent(
        &fixture.workspaces,
        &operation.principal,
        &operation.workspace_id,
    )
    .await;
    let accepted = fixture
        .runs
        .start(&agent, &operation.workspace_id, submission("invoke"))
        .await
        .unwrap();
    wait_for_actions(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
        1,
        1,
    )
    .await;
    let waiting = fixture
        .runs
        .observe(
            &agent,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    let [action] = waiting.required_actions() else {
        panic!("one Agent approval action")
    };
    assert_eq!(action.required_actor, crate::RequiredActor::WorkspaceOwner);
    let crate::RequiredActionRequest::InvocationApproval(id) = &action.request else {
        panic!("Agents never receive Account connection actions")
    };
    let denied = fixture.runs.required_actions(&agent, None, None, 10).await;
    assert!(
        matches!(
            denied,
            Err(Error::Workspace(coral_workspace::Error::PermissionDenied(
                _
            )))
        ),
        "{denied:?}"
    );
    let queue = fixture
        .runs
        .required_actions(&operation.principal, None, None, 10)
        .await
        .unwrap();
    assert_eq!(queue.actions.len(), 1);
    assert_eq!(queue.action_count, 1);
    assert_eq!(
        fixture
            .runs
            .required_action_count(&operation.principal, Some(&operation.workspace_id))
            .await
            .unwrap(),
        1
    );
    let [queued] = queue.actions.as_slice() else {
        panic!("one Agent approval")
    };
    assert_eq!(queued.run.id, *accepted.run_id());
    assert_eq!(queued.run.agent_label.as_deref(), Some("Worker"));
    assert_eq!(
        queued.action.request,
        crate::RequiredActionRequest::InvocationApproval(id.clone())
    );
    let approvals = fixture.approvals();
    assert!(matches!(
        approvals.get(&agent, &operation.workspace_id, id).await,
        Err(Error::ApprovalNotFound)
    ));
    assert!(matches!(
        approvals
            .decide(
                &agent,
                &operation.workspace_id,
                id,
                ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"a".repeat(32)).unwrap()
            )
            .await,
        Err(Error::ApprovalNotFound)
    ));
    let second_owner = fixture
        .workspace_user(
            &operation.workspace_id,
            &operation.user,
            "user:second-owner",
            WorkspaceRole::Owner,
        )
        .await;
    for owner in [
        &operation.principal,
        &AuthenticatedPrincipal::user(second_owner.clone()),
    ] {
        let update = fixture
            .runs
            .observe(
                owner,
                &operation.workspace_id,
                accepted.run_id(),
                &immediate(None, 100),
            )
            .await
            .unwrap();
        assert_eq!(
            update, waiting,
            "every designated Owner sees the same safe snapshot"
        );
    }
    let observation = immediate(Some(waiting.next_cursor().clone()), 100)
        .with_wait(Duration::from_secs(2))
        .unwrap();
    let observing = fixture.runs.observe(
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
        &observation,
    );
    tokio::pin!(observing);
    tokio::time::timeout(Duration::from_millis(20), &mut observing)
        .await
        .expect_err("unchanged snapshot waits");
    fixture
        .workspaces
        .set_membership(
            &change(second_owner, 50, '5'),
            &operation.workspace_id,
            &operation.user,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(1), observing)
                .await
                .unwrap(),
            Err(Error::RunNotFound)
        ),
        "a cursor does not cache Owner authority"
    );
    assert_eq!(
        fixture
            .runs
            .required_action_count(&operation.principal, None)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        fixture
            .runs
            .required_actions(&operation.principal, None, None, 10)
            .await
            .unwrap()
            .action_count,
        0
    );
    let terminal = fixture
        .runs
        .cancel(
            &agent,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(Some(waiting.next_cursor().clone()), 100),
        )
        .await
        .unwrap();
    assert_eq!(terminal.state(), RunState::Canceled);
    assert!(terminal.required_actions().is_empty());
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "shared-request cleanup matrix exercises cancellation/deadline against decline/expiry through domain interfaces"
)]
async fn required_actions_cleanup_shared_requests_under_termination_and_resolution_races() {
    for deadline in [false, true] {
        for expire in [false, true] {
            let fixture = TestModules::new(Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
            ))
            .await;
            let operation = fixture
                .user_operation("user:shared-actions", "Shared action cleanup")
                .await;
            let (_directory, accounts) =
                configure_user_caller(&fixture, &operation, "https://upstream.example").await;
            let mut runs = fixture.runs.clone().with_accounts(accounts.clone());
            runs.execution_policy = crate::execution::ExecutionPolicy::testing_with_claim_lease(
                Duration::from_secs(30),
            );
            let source = "try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }";
            let first = runs
                .start(
                    &operation.principal,
                    &operation.workspace_id,
                    RunSubmission::new(
                        ProgramSource::parse(source).unwrap(),
                        RunTimeout::from_millis(10_000).unwrap(),
                    ),
                )
                .await
                .unwrap();
            let second = runs
                .start(
                    &operation.principal,
                    &operation.workspace_id,
                    submission(source),
                )
                .await
                .unwrap();
            let first_waiting = wait_for_actions(
                &runs,
                &operation.principal,
                &operation.workspace_id,
                first.run_id(),
                1,
                1,
            )
            .await;
            let second_waiting = wait_for_actions(
                &runs,
                &operation.principal,
                &operation.workspace_id,
                second.run_id(),
                1,
                1,
            )
            .await;
            let [action] = first_waiting.required_actions() else {
                panic!("one Account connection action")
            };
            assert_eq!(
                second_waiting.required_actions().first().unwrap().request,
                action.request
            );
            let crate::RequiredActionRequest::AccountConnection(id) = action.request else {
                panic!("Account connection action")
            };
            let page = runs
                .required_actions(&operation.principal, Some(&operation.workspace_id), None, 1)
                .await
                .unwrap();
            let [queued] = page.actions.as_slice() else {
                panic!("one connection spans both Runs and is listed once")
            };
            assert_eq!(queued.action.request, action.request);
            assert!(page.next.is_none());
            assert_eq!(page.action_count, 1, "one connection spans both Runs");
            let context = change(operation.user.clone(), 50, '5');
            let request = accounts
                .get_connection_request(&context, &operation.workspace_id, id)
                .await
                .unwrap();
            let cancel_observation = immediate(Some(first_waiting.next_cursor().clone()), 1);
            let stop = async {
                if deadline {
                    // Advance the Run deadline, but stay within the 30-second worker lease.
                    fixture.clock.next.store(20_000_000_000, Ordering::SeqCst);
                }
                runs.cancel(
                    &operation.principal,
                    &operation.workspace_id,
                    first.run_id(),
                    &cancel_observation,
                )
                .await
                .unwrap()
            };
            let resolve = async {
                if expire {
                    accounts
                        .get_connection_request(
                            &change(
                                operation.user.clone(),
                                request.expires_at_unix_nanos + 1_000_000_000,
                                '6',
                            ),
                            &operation.workspace_id,
                            id,
                        )
                        .await
                        .unwrap()
                } else {
                    accounts
                        .decline_connection(&context, &operation.workspace_id, id)
                        .await
                        .unwrap()
                }
            };
            let (terminal, resolved) = tokio::join!(stop, resolve);
            if expire {
                let events = accounts
                    .list_user_lifecycle_events(&context, &operation.workspace_id)
                    .await
                    .unwrap();
                let event = events
                    .iter()
                    .find(|event| event.target_id() == id.to_string())
                    .unwrap();
                assert_eq!(
                    event.occurred_at_unix_nanos(),
                    request.expires_at_unix_nanos,
                    "late inspection cannot advance expiry activity to observation time"
                );
            }
            assert!(terminal.state().is_terminal());
            assert!(terminal.required_actions().is_empty());
            assert_eq!(
                resolved.state,
                if expire {
                    coral_provider::account::ConnectionState::Expired
                } else {
                    coral_provider::account::ConnectionState::Declined
                }
            );
            let mut cleanup = fixture.database.begin().await.unwrap();
            crate::retention::delete_in_transaction(
                &mut cleanup,
                &operation.workspace_id,
                first.run_id(),
                operation.principal.principal(),
            )
            .await
            .unwrap();
            cleanup.commit().await.unwrap();
            assert_eq!(
                accounts
                    .get_connection_request(&context, &operation.workspace_id, id)
                    .await
                    .unwrap()
                    .state,
                resolved.state,
                "Run cleanup must retain the other Run's shared Account connection workflow"
            );
            let second_terminal = wait_for_terminal(
                &runs,
                &operation.principal,
                &operation.workspace_id,
                second.run_id(),
            )
            .await;
            assert!(second_terminal.required_actions().is_empty());
            assert_eq!(second_terminal.state(), RunState::Succeeded);
            let Some(RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value),
            }) = second_terminal.terminal_outcome()
            else {
                panic!("caught request outcome")
            };
            assert_eq!(
                value.as_json(),
                &serde_json::json!(if expire {
                    "account_connection_expired"
                } else {
                    "account_connection_declined"
                })
            );
        }
    }
}

/// Publishes `public.getForecast` as POST, so it needs approval by default.
async fn publish_approval_provider(fixture: &TestModules, operation: &OperationFixture) {
    let context = change(operation.user.clone(), 40, '4');
    let setup = fixture.providers.prepare_openapi_fixture(&context, &operation.workspace_id,
        OpenApiDocument::parse(r#"{"openapi":"3.1.0","info":{"title":"Public weather","version":"1"},"servers":[{"url":"https://upstream.example"}],"paths":{"/forecast":{"post":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}}"#).unwrap(),
    ).await.unwrap();
    fixture
        .providers
        .publish_workspace_provider(
            &context,
            WorkspaceProviderPublicationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                operation.workspace_id.clone(),
                setup.id().clone(),
                ProviderNamespace::parse("public").unwrap(),
                CredentialRoute::None,
            ),
        )
        .await
        .unwrap()
        .published_provider()
        .expect("public Provider");
}

async fn retained_activity(
    database: &coral_db::Database,
    workspace: &coral_workspace::WorkspaceId,
    run: &crate::RunId,
) -> i64 {
    let mut tx = database.begin().await.unwrap();
    let time = crate::retention::hold_in_transaction(&mut tx, workspace, run)
        .await
        .unwrap()
        .unwrap()
        .last_activity_at_unix_nanos();
    tx.commit().await.unwrap();
    time
}

#[tokio::test(flavor = "multi_thread")]
async fn the_queue_lists_each_request_most_urgent_first_across_runs() {
    let fixture = TestModules::new(Arc::new(InvokingEngine::new(
        "coral.providers.weather.getForecast",
    )))
    .await;
    let operation = fixture
        .user_operation("user:queue-order", "Queue order")
        .await;
    fixture.require_approval(&operation).await;
    let (user, workspace) = (&operation.principal, &operation.workspace_id);
    let mut runs = Vec::new();
    for timeout in [60_000, 10_000, 30_000_u64] {
        let accepted = fixture
            .runs
            .start(
                user,
                workspace,
                RunSubmission::new(
                    ProgramSource::parse("invoke").unwrap(),
                    RunTimeout::from_millis(timeout).unwrap(),
                ),
            )
            .await
            .unwrap();
        wait_for_actions(&fixture.runs, user, workspace, accepted.run_id(), 1, 1).await;
        runs.push((timeout, accepted.run_id().clone()));
    }
    runs.sort_by_key(|(timeout, _)| *timeout);
    let by_deadline = runs.into_iter().map(|(_, run)| run).collect::<Vec<_>>();
    let first = fixture
        .runs
        .required_actions(user, None, None, 2)
        .await
        .unwrap();
    assert_eq!(first.action_count, 3);
    let rest = fixture
        .runs
        .required_actions(user, None, first.next.as_ref(), 2)
        .await
        .unwrap();
    assert!(rest.next.is_none());
    let order = first
        .actions
        .iter()
        .chain(&rest.actions)
        .map(|queued| queued.run.id.clone())
        .collect::<Vec<_>>();
    assert_eq!(
        order, by_deadline,
        "the shortest deadline comes first, whatever the Run identity"
    );
    let cursor = first.next.unwrap();
    assert_eq!(
        crate::QueueCursor::parse(&cursor.to_string()).unwrap(),
        cursor
    );
}
