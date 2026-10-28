//! Caller Account connection requests that gate Invocations.
use super::*;

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one real two-Run journey covers shared connection, independent cancellation, and completion"
)]
async fn caller_connection_is_shared_and_canceling_one_run_does_not_cancel_another() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer caller-canary",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at("user:connecting", "Caller wait", &server.uri())
        .await;
    let context = change(tool.user.clone(), 40, '4');
    let scope = tool.workspace_id.clone();
    let (_dir, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let source = "return await coral.providers.weather.getForecast({});";
    let first = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    let second = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    let request = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let requests = accounts
                .list_connection_requests(&context, &tool.workspace_id)
                .await
                .unwrap();
            let first_detail = runs
                .observe(
                    &tool.principal,
                    &tool.workspace_id,
                    first.run_id(),
                    &immediate(None, 100),
                )
                .await
                .unwrap();
            let second_detail = runs
                .observe(
                    &tool.principal,
                    &tool.workspace_id,
                    second.run_id(),
                    &immediate(None, 100),
                )
                .await
                .unwrap();
            if requests.len() == 1
                && first_detail.required_actions().len() == 1
                && second_detail.required_actions().len() == 1
            {
                break requests.first().expect("one request").clone();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("two admitted calls share one durable request");
    runs.cancel(
        &tool.principal,
        &tool.workspace_id,
        first.run_id(),
        &immediate(None, 100),
    )
    .await
    .unwrap();
    assert_eq!(
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, first.run_id())
            .await
            .state(),
        RunState::Canceled
    );
    let authorization = accounts
        .start_user_authorization(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("caller-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let terminal =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, second.run_id()).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
    assert!(!format!("{terminal:?}").contains("caller-canary"));
    assert_eq!(
        accounts
            .get_connection_request(&context, &tool.workspace_id, request.id)
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::Connected
    );
    assert_connection_completion_event(&accounts, &context, &tool.workspace_id, request.id).await;
}

/// A Run whose only Invocation waits on its User's connection request.
struct WaitingConnection {
    server: MockServer,
    fixture: TestModules,
    tool: ToolFixture,
    _directory: tempfile::TempDir,
    accounts: coral_provider::account::AccountModule,
    context: ChangeContext,
    runs: RunModule,
    run: crate::RunId,
    request: coral_provider::account::ConnectionRequest,
}

impl WaitingConnection {
    async fn new() -> Self {
        let server = MockServer::start().await;
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let tool = fixture
            .user_tool_at("user:gate-loss", "Connection gate", &server.uri())
            .await;
        let (directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
        let runs = fixture.live_runs(Some(accounts.clone()));
        let accepted = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission(
                    "try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }",
                ),
            )
            .await
            .unwrap();
        let request = wait_for_connection(&accounts, &tool).await;
        Self {
            server,
            context: change(tool.user.clone(), 40, '4'),
            run: accepted.run_id().clone(),
            fixture,
            tool,
            _directory: directory,
            accounts,
            runs,
            request,
        }
    }

    fn scope(&self) -> &coral_workspace::WorkspaceId {
        &self.tool.workspace_id
    }

    async fn start_connecting(&self) -> coral_provider::account::Authorization {
        self.accounts
            .start_user_authorization(
                &self.context,
                self.scope(),
                &self.tool.provider_id,
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                None,
            )
            .await
            .unwrap()
    }

    async fn submit(
        &self,
        authorization: &coral_provider::account::Authorization,
        secret: &str,
    ) -> coral_provider::account::AuthorizationState {
        self.accounts
            .submit_secret(
                &self.context,
                self.scope(),
                authorization.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new(secret.into()).unwrap(),
                ),
            )
            .await
            .unwrap()
            .state
    }

    async fn request_state(&self) -> coral_provider::account::ConnectionState {
        self.accounts
            .get_connection_request(&self.context, self.scope(), self.request.id)
            .await
            .unwrap()
            .state
    }

    async fn terminal(&self) -> crate::RunUpdate {
        wait_for_terminal(&self.runs, &self.tool.principal, self.scope(), &self.run).await
    }
}

/// What ends a connection request while an Invocation waits on it.
#[derive(Debug, Clone, Copy)]
enum GateLoss {
    Decline,
    Expiry,
    Route,
    Authority,
    Disconnect,
}

impl GateLoss {
    const ALL: [Self; 5] = [
        Self::Decline,
        Self::Expiry,
        Self::Route,
        Self::Authority,
        Self::Disconnect,
    ];

    const fn code(self) -> &'static str {
        match self {
            Self::Decline => "account_connection_declined",
            Self::Expiry => "account_connection_expired",
            Self::Route => "credential_route_changed",
            Self::Authority => "dispatch_authority_lost",
            Self::Disconnect => "account_authorization_changed",
        }
    }

    async fn apply(self, waiting: &WaitingConnection) {
        use coral_provider::account::ConnectionState;
        let (fixture, accounts, context) = (&waiting.fixture, &waiting.accounts, &waiting.context);
        let (scope, request) = (waiting.scope(), waiting.request.id);
        match self {
            Self::Decline => {
                let (first, replay) = tokio::join!(
                    accounts.decline_connection(context, scope, request),
                    accounts.decline_connection(context, scope, request),
                );
                let first = first.unwrap();
                assert_eq!(first.state, ConnectionState::Declined);
                assert_eq!(replay.unwrap(), first, "a concurrent decline replays");
            }
            Self::Expiry => {
                let at_deadline = change(
                    waiting.tool.user.clone(),
                    waiting.request.expires_at_unix_nanos,
                    '6',
                );
                assert_eq!(
                    accounts
                        .get_connection_request(&at_deadline, scope, request)
                        .await
                        .unwrap()
                        .state,
                    ConnectionState::Expired
                );
            }
            Self::Route => move_to_shared_account(waiting).await,
            Self::Authority => {
                fixture.workspaces.archive(context, scope).await.unwrap();
            }
            Self::Disconnect => {
                let account = accounts
                    .get_account(
                        context,
                        scope,
                        &waiting.tool.provider_id,
                        &coral_provider::account::AccountScope::User,
                    )
                    .await
                    .unwrap()
                    .account
                    .unwrap();
                accounts
                    .disconnect(context, scope, &waiting.tool.provider_id, account.id)
                    .await
                    .unwrap();
            }
        }
    }
}

/// The owner moves the Provider to a shared Account through the public
/// Provider API, which changes the credential route.
async fn move_to_shared_account(waiting: &WaitingConnection) {
    let (providers, context, scope) = (
        &waiting.fixture.providers,
        &waiting.context,
        waiting.scope(),
    );
    let document = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Private weather","version":"2"},
        "servers":[{"url":waiting.server.uri()}], "security":[{"bearer":[]}],
        "components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    });
    let setup = providers
        .prepare_update_fixture(
            context,
            scope,
            &waiting.tool.provider_id,
            OpenApiDocument::parse(&document.to_string()).unwrap(),
        )
        .await
        .unwrap();
    let review = providers
        .review_setup_configuration(context.actor(), scope, setup.id())
        .await
        .unwrap();
    let confirmed = providers
        .confirm_setup_configuration(
            context,
            scope,
            setup.id(),
            review.revision,
            review.confirmed.unwrap(),
        )
        .await
        .unwrap();
    let grant = waiting
        .accounts
        .authorize_setup(
            context,
            scope,
            setup.id(),
            confirmed.revision,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("shared-route-canary".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    providers
        .regenerate_provider(
            context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                scope.clone(),
                waiting.tool.provider_id.clone(),
                setup.id().clone(),
                confirmed.revision,
            )
            .with_account_setting(CredentialRoute::Fixed)
            .with_account_grant(grant.provisional_grant_id.unwrap())
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
}

/// Each loss ends the waiting Invocation once, with a catchable code, and
/// nothing reaches the upstream or falls back to another Account.
#[tokio::test(flavor = "multi_thread")]
async fn connection_gate_losses_settle_the_invocation_once_without_sending() {
    for loss in GateLoss::ALL {
        let waiting = WaitingConnection::new().await;
        loss.apply(&waiting).await;
        let terminal = waiting.terminal().await;
        assert_final_code(&terminal, loss.code());
        let events = retained_events(
            &waiting.runs,
            &waiting.tool.principal,
            waiting.scope(),
            &waiting.run,
        )
        .await;
        let count = |started: bool| {
            events
                .iter()
                .filter(|event| match event.kind() {
                    RunEventKind::InvocationStarted { .. } => started,
                    RunEventKind::InvocationEnded { .. } => !started,
                    _ => false,
                })
                .count()
        };
        assert_eq!(
            (count(true), count(false)),
            (1, 1),
            "{loss:?} settles one Invocation without replay"
        );
        assert!(
            waiting.server.received_requests().await.unwrap().is_empty(),
            "{loss:?} never sends"
        );
    }
}

/// Once ended, a request replays a decline unchanged and records exactly one
/// terminal event. The User's own actions are attributed to them.
#[tokio::test(flavor = "multi_thread")]
async fn an_ended_request_records_one_terminal_event_and_replays_unchanged() {
    // Archiving the Workspace ends the Run, not the request.
    for loss in [
        GateLoss::Decline,
        GateLoss::Expiry,
        GateLoss::Route,
        GateLoss::Disconnect,
    ] {
        let waiting = WaitingConnection::new().await;
        loss.apply(&waiting).await;
        waiting.terminal().await;
        let (accounts, context, scope) = (&waiting.accounts, &waiting.context, waiting.scope());
        // Reads settle expired or invalidated requests even after their last waiter ends.
        let ended = accounts
            .get_connection_request(context, scope, waiting.request.id)
            .await
            .unwrap();
        assert_eq!(
            accounts
                .decline_connection(context, scope, waiting.request.id)
                .await
                .unwrap(),
            ended,
            "{loss:?}"
        );
        let events = accounts
            .list_user_lifecycle_events(context, scope)
            .await
            .unwrap();
        let [event] = events
            .iter()
            .filter(|event| event.target_id() == waiting.request.id.to_string())
            .collect::<Vec<_>>()[..]
        else {
            panic!("{loss:?}: one request terminal event, including replay")
        };
        assert_eq!(
            event.action(),
            coral_provider::LifecycleAction::AccountConnectionCompleted
        );
        assert_eq!(event.changed_field_paths(), &["state"]);
        let by_user = matches!(loss, GateLoss::Decline | GateLoss::Route);
        assert_eq!(
            event.actor(),
            &if by_user {
                coral_provider::LifecycleActor::User {
                    user_id: context.actor().user_id().clone(),
                    authority_source: coral_workspace::AuthoritySource::WorkspaceMembership,
                }
            } else {
                coral_provider::LifecycleActor::Coral
            },
            "{loss:?}"
        );
        assert_eq!(
            event.outcome(),
            if matches!(loss, GateLoss::Decline) {
                assert_eq!(event.trace_id(), context.trace_id());
                coral_provider::LifecycleOutcome::Accepted
            } else {
                coral_provider::LifecycleOutcome::Failed
            },
            "{loss:?}"
        );
        TraceId::parse(event.trace_id().as_str()).expect("safe W3C trace correlation");
    }
}

/// The owner arms a timer at each connection request's deadline when an
/// Invocation parks on it, beside the Run deadline's; when it fires, the
/// owner sends the parked Invocation back to its held recheck, which records
/// what the request became.
#[tokio::test(flavor = "multi_thread")]
async fn the_owner_arms_a_timer_at_a_connection_requests_deadline() {
    let server = MockServer::start().await;
    let probe = Arc::new(TimerProbe::default());
    let fixture = TestModules::with_execution_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
    let tool = fixture
        .user_tool_at("user:connection-timer", "Connection timer", &server.uri())
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let runs = fixture.runs.clone().with_accounts(accounts.clone());
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                "try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }",
            ),
        )
        .await
        .unwrap();
    let request = wait_for_connection(&accounts, &tool).await;
    let timer = crate::owner::Timer::Connection(request.id);
    wait_until(|| probe.armed(&timer).is_some()).await;
    let after_nanos = probe.armed(&timer).unwrap();
    let now = fixture.clock.next.load(Ordering::SeqCst);
    assert!(
        (request.expires_at_unix_nanos - now - after_nanos).abs() < 1_000_000,
        "the timer fires at the request's deadline: {after_nanos} vs {}",
        request.expires_at_unix_nanos - now
    );
    assert!(
        probe.armed(&crate::owner::Timer::RunDeadline).is_some(),
        "the Run deadline is the owner's timer too"
    );
    let terminal = runs
        .cancel(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert_eq!(terminal.state(), RunState::Canceled);
}

/// A declined or expired request binds no later connection: the User still
/// connects, but the request stays ended.
#[tokio::test(flavor = "multi_thread")]
async fn a_declined_or_expired_request_does_not_bind_a_later_connection() {
    for (loss, ended) in [
        (
            GateLoss::Decline,
            coral_provider::account::ConnectionState::Declined,
        ),
        (
            GateLoss::Expiry,
            coral_provider::account::ConnectionState::Expired,
        ),
    ] {
        let waiting = WaitingConnection::new().await;
        let connecting = waiting.start_connecting().await;
        loss.apply(&waiting).await;
        waiting.terminal().await;
        assert_eq!(
            waiting.submit(&connecting, "late-connection-canary").await,
            coral_provider::account::AuthorizationState::Succeeded,
            "{loss:?}"
        );
        assert_eq!(waiting.request_state().await, ended, "{loss:?}");
        assert_eq!(
            waiting
                .accounts
                .get_account(
                    &waiting.context,
                    waiting.scope(),
                    &waiting.tool.provider_id,
                    &coral_provider::account::AccountScope::User
                )
                .await
                .unwrap()
                .account
                .unwrap()
                .state,
            coral_provider::account::AccountState::Ready,
            "{loss:?}"
        );
        let events = waiting
            .accounts
            .list_user_lifecycle_events(&waiting.context, waiting.scope())
            .await
            .unwrap();
        assert!(!format!("{events:?}").contains("late-connection-canary"));
    }
}

/// Disconnecting fences a waiting request before its first credentials
/// arrive. Later independent work may connect the same Account but cannot
/// revive the request or its Invocation.
#[tokio::test(flavor = "multi_thread")]
async fn disconnect_fences_a_waiting_request_before_its_credentials_arrive() {
    let waiting = WaitingConnection::new().await;
    let connecting = waiting.start_connecting().await;
    GateLoss::Disconnect.apply(&waiting).await;
    assert_eq!(
        waiting.request_state().await,
        coral_provider::account::ConnectionState::AuthorizationChanged
    );
    let independent = waiting.start_connecting().await;
    assert_eq!(
        waiting
            .submit(&independent, "independent-account-canary")
            .await,
        coral_provider::account::AuthorizationState::Succeeded
    );
    assert_eq!(
        waiting.submit(&connecting, "late-connection-canary").await,
        coral_provider::account::AuthorizationState::Failed
    );
    assert_final_code(&waiting.terminal().await, GateLoss::Disconnect.code());
    assert_eq!(
        waiting.request_state().await,
        coral_provider::account::ConnectionState::AuthorizationChanged,
        "the request is not revived"
    );
}

/// Another User, even a designated Workspace Owner, can neither read nor decline a
/// User's connection request, nor see its lifecycle events.
#[tokio::test(flavor = "multi_thread")]
async fn connection_requests_are_concealed_from_other_users() {
    let waiting = WaitingConnection::new().await;
    let (accounts, scope, request) = (&waiting.accounts, waiting.scope(), waiting.request.id);
    let stranger = change(
        waiting
            .fixture
            .workspace_user(
                scope,
                &waiting.tool.user,
                "user:other-owner",
                WorkspaceRole::Owner,
            )
            .await,
        40,
        '5',
    );
    assert!(matches!(
        accounts
            .get_connection_request(&stranger, scope, request)
            .await,
        Err(coral_provider::Error::AccountNotFound)
    ));
    assert!(matches!(
        accounts.decline_connection(&stranger, scope, request).await,
        Err(coral_provider::Error::AccountNotFound)
    ));
    assert_eq!(
        waiting.request_state().await,
        coral_provider::account::ConnectionState::Pending,
        "a concealed decline changes nothing"
    );
    GateLoss::Decline.apply(&waiting).await;
    assert!(
        accounts
            .list_user_lifecycle_events(&stranger, scope)
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        !waiting
            .fixture
            .providers
            .list_lifecycle_events(stranger.actor(), scope)
            .await
            .unwrap()
            .iter()
            .any(|event| event.target_id() == request.to_string())
    );
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one durable journey checks independent deadlines, empty-request survival, restart, and later Account reuse"
)]
async fn caller_connection_outlives_run_deadlines_and_empty_waiter_sets() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer after-deadline",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at(
            "user:deadline-connection",
            "Independent deadlines",
            &server.uri(),
        )
        .await;
    let (directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let mut runs = fixture.live_runs(None).with_accounts(accounts.clone());
    runs.clock = fixture.clock.clone();
    runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_secs(30));
    let source = "return await coral.providers.weather.getForecast({});";
    let short = RunSubmission::new(
        ProgramSource::parse(source).unwrap(),
        RunTimeout::from_millis(10_000).unwrap(),
    );
    let first = runs
        .start(&tool.principal, &tool.workspace_id, short)
        .await
        .unwrap();
    let request = wait_for_connection(&accounts, &tool).await;
    let second = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    let second_waiting = wait_for_actions(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        second.run_id(),
        1,
        1,
    )
    .await;
    assert_eq!(
        second_waiting.required_actions().first().unwrap().request,
        crate::RequiredActionRequest::AccountConnection(request.id)
    );
    let queue = runs
        .required_actions(&tool.principal, None, None, 10)
        .await
        .unwrap();
    let [queued] = queue.actions.as_slice() else {
        panic!("one shared connection decision, listed once")
    };
    assert_eq!(
        queued.run.id,
        *first.run_id(),
        "the entry shows the Run whose deadline comes first"
    );
    let first_card = &queued.action;
    assert!(
        first_card.deadline_at_unix_nanos
            < second_waiting
                .required_actions()
                .first()
                .unwrap()
                .deadline_at_unix_nanos,
        "presentation does not shorten the shared request lifetime"
    );
    assert_eq!(first_card.call_path, "coral.providers.weather.getForecast");
    // Expire only the short Run, not either worker's 30-second claim lease.
    fixture.clock.next.store(20_000_000_000, Ordering::SeqCst);
    let terminal =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, first.run_id()).await;
    assert_eq!(terminal.state(), RunState::TimedOut);
    let surviving = runs
        .observe(
            &tool.principal,
            &tool.workspace_id,
            second.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert_eq!(surviving.state(), RunState::Running);
    assert_eq!(
        surviving.required_actions(),
        second_waiting.required_actions(),
        "another Run's deadline does not detach this Run's shared action"
    );
    let context = change(tool.user.clone(), 20_000_000_000, '7');
    assert_eq!(
        accounts
            .list_connection_requests(&context, &tool.workspace_id)
            .await
            .unwrap(),
        vec![request.clone()]
    );
    runs.cancel(
        &tool.principal,
        &tool.workspace_id,
        second.run_id(),
        &immediate(None, 100),
    )
    .await
    .unwrap();
    assert_eq!(
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, second.run_id())
            .await
            .state(),
        RunState::Canceled
    );
    let reopened = coral_provider::account::AccountModule::new(
        fixture.providers.clone(),
        coral_credentials::CredentialVault::open(&directory.path().join("key")).unwrap(),
    );
    assert_eq!(
        reopened
            .get_connection_request(&context, &tool.workspace_id, request.id)
            .await
            .unwrap(),
        request
    );
    let authorization = reopened
        .start_user_authorization(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    reopened
        .submit_secret(
            &context,
            &tool.workspace_id.clone(),
            authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("after-deadline".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    let later = runs
        .start(&tool.principal, &tool.workspace_id, submission(source))
        .await
        .unwrap();
    assert_eq!(
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, later.run_id())
            .await
            .state(),
        RunState::Succeeded
    );
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one regeneration race proves an obsolete secret cannot authorize or dispatch the waiting Invocation"
)]
async fn caller_connection_cannot_complete_an_obsolete_contract_after_regeneration() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer pinned-credential",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(0)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at(
            "user:pinned-connection",
            "Pinned connection contract",
            &server.uri(),
        )
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let context = change(tool.user.clone(), 40, '4');
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("return await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    wait_for_connection(&accounts, &tool).await;
    let scope = tool.workspace_id.clone();
    let authorization = accounts
        .start_user_authorization(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    let changed = serde_json::json!({
        "openapi":"3.1.0", "info":{"title":"Changed credentials","version":"2"},
        "servers":[{"url":server.uri()}], "security":[{"key":[]}],
        "components":{"securitySchemes":{"key":{"type":"apiKey","in":"header","name":"x-api-key"}}},
        "paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"Forecast"}}}}}
    });
    let setup = fixture
        .providers
        .prepare_openapi_fixture(
            &context,
            &tool.workspace_id,
            OpenApiDocument::parse(&changed.to_string()).unwrap(),
        )
        .await
        .unwrap();
    fixture
        .providers
        .regenerate_provider(
            &context,
            ProviderRegenerationRequest::new(
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                scope.clone(),
                tool.provider_id.clone(),
                setup.id().clone(),
                1,
            )
            .with_incompatible_configuration_confirmation(),
        )
        .await
        .unwrap();
    let completed = accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("pinned-credential".into()).unwrap(),
            ),
        )
        .await
        .unwrap();
    assert_eq!(
        completed.state,
        coral_provider::account::AuthorizationState::Canceled
    );
    assert_eq!(completed.failure_code(), Some("configuration_changed"));
    assert_eq!(completed.account_id, None);
    let account = accounts
        .get_account(
            &context,
            &scope,
            &tool.provider_id,
            &coral_provider::account::AccountScope::User,
        )
        .await
        .unwrap()
        .account
        .unwrap();
    assert_eq!(
        account.state,
        coral_provider::account::AccountState::Disconnected
    );
    assert_eq!(account.connected_at_unix_nanos, None);
    assert_eq!(account.configured_by_user_id, None);
    let terminal = wait_for_terminal(&runs, &tool.principal, &scope, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::Failed);
    let observed = runs
        .observe(
            &tool.principal,
            &scope,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .unwrap();
    assert_eq!(
        retained_events(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            observed.run_id()
        )
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::InvocationStarted { .. }))
        .count(),
        1
    );
    assert!(
        server
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method != "GET")
    );
}

#[tokio::test(flavor = "multi_thread")]
#[expect(
    clippy::too_many_lines,
    reason = "one caller-account journey checks exact Account selection and authorization ABA rejection at the dispatch fence"
)]
async fn caller_connection_reconnect_preserves_account_and_fences_prepared_dispatch() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .and(wiremock::matchers::header(
            "authorization",
            "Bearer existing-account",
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature": 18})),
        )
        .expect(1)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at("user:existing-account", "Existing account", &server.uri())
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let context = change(tool.user.clone(), 40, '4');
    let scope = tool.workspace_id.clone();

    let mut runs = fixture.live_runs(None).with_accounts(accounts.clone());
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("return await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    let request = wait_for_connection(&accounts, &tool).await;
    let authorization = accounts
        .start_user_authorization(
            &context,
            &scope,
            &tool.provider_id,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    let original = accounts
        .submit_secret(
            &context,
            &scope,
            authorization.id,
            coral_credentials::CredentialValues::token(
                coral_credentials::Secret::new("existing-account".into()).unwrap(),
            ),
        )
        .await
        .unwrap()
        .account_id
        .unwrap();
    let connected = accounts
        .get_connection_request(&context, &scope, request.id)
        .await
        .unwrap();
    assert_eq!(
        connected.state,
        coral_provider::account::ConnectionState::Connected
    );
    assert_eq!(
        accounts
            .decline_connection(&context, &tool.workspace_id, request.id)
            .await
            .unwrap(),
        connected
    );
    assert_connection_completion_event(&accounts, &context, &tool.workspace_id, request.id).await;
    assert_eq!(
        wait_for_terminal(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id()
        )
        .await
        .state(),
        RunState::Succeeded
    );
    let (gate, release) = FenceGate::blocking();
    runs.invocation_observer = gate.clone();
    let later = runs.start(&tool.principal, &tool.workspace_id, submission("try { await coral.providers.weather.getForecast({}); } catch (error) { return error.code; }")).await.unwrap();
    wait_until(|| gate.entered()).await;
    for secret in ["different-account", "existing-account"] {
        let reconnect = accounts
            .start_reconnect(
                &context,
                &scope,
                &tool.provider_id,
                original,
                ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
                None,
            )
            .await
            .unwrap();
        let completed = accounts
            .submit_secret(
                &context,
                &scope,
                reconnect.id,
                coral_credentials::CredentialValues::token(
                    coral_credentials::Secret::new(secret.into()).unwrap(),
                ),
            )
            .await
            .unwrap();
        assert_eq!(completed.account_id, Some(original));
    }
    release.send(()).unwrap();
    let terminal =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, later.run_id()).await;
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = terminal.terminal_outcome()
    else {
        panic!("Account replacement must settle the original Invocation");
    };
    assert_eq!(
        value.as_json(),
        &serde_json::json!("account_authorization_changed")
    );
}

/// A completed connection wakes exactly the Run parked on it: the change
/// signal rings both parked Runs' owners, each reads just its own pending
/// connection requests, and only the one whose request ended sends its
/// Invocation back to its held recheck, within the wake gap. That Invocation
/// dispatches after one fence. The other owner writes nothing: its Run's
/// ledger head stays where it was, and its request stays pending.
#[tokio::test(flavor = "multi_thread")]
async fn a_completed_connection_wakes_exactly_its_parked_run_within_the_gap_after_one_fence() {
    let server = MockServer::start().await;
    forecast_once(&server, "first-canary").await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let first = fixture
        .user_tool_at("user:first-connecting", "First caller wait", &server.uri())
        .await;
    let second = fixture
        .user_tool_at(
            "user:second-connecting",
            "Second caller wait",
            &server.uri(),
        )
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &first, &server.uri()).await;
    let (_unused_directory, _unused_accounts) =
        configure_user_caller(&fixture, &second, &server.uri()).await;
    let probe = WaitProbe::new();
    let wake_gap = Duration::from_secs(1);
    let mut runs = fixture.live_runs(Some(accounts.clone()));
    runs.invocation_observer = probe.clone();
    runs.execution_observer = probe.clone();
    // The production cadence: the periodic precheck is 15 s away.
    runs.execution_policy = runs
        .execution_policy
        .with_wakeups(Duration::from_secs(15), wake_gap);
    let source = "return await coral.providers.weather.getForecast({});";
    let first_run = runs
        .start(&first.principal, &first.workspace_id, submission(source))
        .await
        .unwrap();
    let second_run = runs
        .start(&second.principal, &second.workspace_id, submission(source))
        .await
        .unwrap();
    wait_for_connection(&accounts, &first).await;
    let second_request = wait_for_connection(&accounts, &second).await;
    probe.until_parked(2).await;
    let second_head = fixture
        .run_record(&second.workspace_id, second_run.run_id())
        .await
        .chunk_sequence;

    connect_caller(&accounts, &first, "first-canary").await;
    // The completion committed before this instant.
    let connected_at = tokio::time::Instant::now();
    let terminal = wait_for_terminal(
        &runs,
        &first.principal,
        &first.workspace_id,
        first_run.run_id(),
    )
    .await;

    assert_eq!(terminal.state(), RunState::Succeeded);
    let found_at = probe
        .first_found(crate::owner::Check::Connections)
        .expect("a connection check found the request ended");
    // At most one gap after the ring, plus the read itself.
    let found_after = found_at.saturating_duration_since(connected_at);
    assert!(
        found_after <= wake_gap + Duration::from_millis(500),
        "found within the gap: {found_after:?}"
    );
    assert_eq!(probe.fences(), 1, "one fence");
    let prechecks = probe.prechecks();
    assert!(
        prechecks
            .iter()
            .all(|(check, _)| *check == crate::owner::Check::Connections),
        "no periodic precheck: {prechecks:?}"
    );
    assert_eq!(
        prechecks.iter().map(|(_, found)| found).sum::<usize>(),
        1,
        "one read found one ended request: {prechecks:?}"
    );
    let bystander = fixture
        .run_record(&second.workspace_id, second_run.run_id())
        .await;
    assert_eq!(bystander.state, RunState::Running);
    assert_eq!(
        bystander.chunk_sequence, second_head,
        "the other Run's owner wrote nothing"
    );
    assert_eq!(
        accounts
            .get_connection_request(
                &change(second.user.clone(), 40, '4'),
                &second.workspace_id,
                second_request.id
            )
            .await
            .unwrap()
            .state,
        coral_provider::account::ConnectionState::Pending
    );
    runs.cancel(
        &second.principal,
        &second.workspace_id,
        second_run.run_id(),
        &immediate(None, 100),
    )
    .await
    .unwrap();
}

/// A wake-up is a hint: with the change listener down, the periodic
/// precheck still finds a completed connection and the Invocation
/// dispatches. During the outage the owner's subscription rings nothing and
/// subscribes again only a wake gap apart, which here outlasts the finding.
#[tokio::test(flavor = "multi_thread")]
async fn with_the_change_listener_down_the_periodic_precheck_still_finds_a_completed_connection() {
    coral_test_support::database_only!(Postgres);
    let server = MockServer::start().await;
    forecast_once(&server, "outage-canary").await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let tool = fixture
        .user_tool_at("user:outage-connecting", "Caller wait", &server.uri())
        .await;
    let (_directory, accounts) = configure_user_caller(&fixture, &tool, &server.uri()).await;
    let probe = WaitProbe::new();
    let mut runs = fixture.live_runs(Some(accounts.clone()));
    runs.invocation_observer = probe.clone();
    runs.execution_observer = probe.clone();
    runs.execution_policy = runs
        .execution_policy
        .with_wakeups(Duration::from_millis(200), Duration::from_secs(3));
    let run = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission("return await coral.providers.weather.getForecast({});"),
        )
        .await
        .unwrap();
    wait_for_connection(&accounts, &tool).await;
    probe.until_parked(1).await;
    fixture
        .database
        .interrupt_change_notifications()
        .await
        .unwrap();
    wait_until(|| fixture.database.subscribe_changes().is_err()).await;

    connect_caller(&accounts, &tool, "outage-canary").await;
    let terminal =
        wait_for_terminal(&runs, &tool.principal, &tool.workspace_id, run.run_id()).await;

    assert_eq!(terminal.state(), RunState::Succeeded);
    let prechecks = probe.prechecks();
    assert!(
        prechecks.contains(&(crate::owner::Check::Periodic, 1)),
        "the periodic precheck found it: {prechecks:?}"
    );
    assert!(
        !prechecks.contains(&(crate::owner::Check::Connections, 1)),
        "no change wake-up found it: {prechecks:?}"
    );
}
