//! `coral.workspace.addProvider` prepares and configures a Provider Setup. The
//! call returns the Reef page where a person previews and publishes it, and
//! never waits for that person.
//! `coral.workspace.listProviders` lists the Providers a Program can check first.
//! `coral.workspace.getProvider` reads one Provider's Provider Configuration.
//! `coral.workspace.getPolicy` reads one Provider's exact Tool rules.
//! `coral.workspace.updatePolicy` sets them.
use super::*;
use coral_provider::{ProviderSetupId, ProviderSetupState};

pub(super) const DOCUMENT: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1"},"security":[],"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"OK"}}}}}}"#;
/// The same service behind a bearer token, which a person enters.
pub(super) const PRIVATE_DOCUMENT: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1"},"security":[{"bearer":[]}],"components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"OK"}}}}}}"#;

/// A Program that calls `addProvider` with exactly `arguments` and returns
/// its Tool Result, or the JavaScript expression `caught` for the Coral
/// Failure it catches, such as `error.code`.
fn add_provider(arguments: &serde_json::Value, caught: &str) -> RunSubmission {
    submission(
        &"
        try {
            return await coral.workspace.addProvider(ARGUMENTS);
        } catch (error) {
            if (!(error instanceof coral.ToolError)) throw error;
            return CAUGHT;
        }
    "
        .replace("CAUGHT", caught)
        .replace("ARGUMENTS", &arguments.to_string()),
    )
}

/// Arguments that `addProvider` accepts for a source that needs no
/// credential, with `extra` replacing or adding fields.
fn valid_with(extra: serde_json::Value) -> serde_json::Value {
    let mut arguments = serde_json::json!({
        "namespace": "weather",
        "source": document_source(),
        "credentialRoute": "none",
    });
    let serde_json::Value::Object(extra) = extra else {
        panic!("extra arguments are an object");
    };
    arguments.as_object_mut().unwrap().extend(extra);
    arguments
}

pub(super) fn document_source() -> serde_json::Value {
    serde_json::json!({ "kind": "openapi_document", "document": DOCUMENT })
}

/// The workflow Reef publishes and renames with, Universal Search included,
/// that Coral renames through too.
pub(super) fn workflow(fixture: &TestModules) -> coral_administration::ProviderWorkflow {
    coral_administration::ProviderWorkflow::new(
        fixture.database.clone(),
        fixture.providers.clone(),
        coral_search::SearchIndex::new(fixture.database.clone(), Duration::from_mins(5)),
    )
}

/// A Workspace whose Owner installed the Built-in Provider, over the real V8 engine.
pub(super) struct AdminScope {
    pub(super) fixture: TestModules,
    pub(super) upstream: Arc<ScriptedUpstream>,
    pub(super) workspace_id: coral_workspace::WorkspaceId,
    pub(super) owner: AuthenticatedPrincipal,
    _vault: tempfile::TempDir,
}

impl AdminScope {
    pub(super) async fn new(user: &str) -> Self {
        let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
        let fixture = TestModules::with_upstream(
            Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                    .expect("V8 engine"),
            ),
            upstream.clone(),
        )
        .await;
        let (workspace_id, owner) = fixture.user_scope(user, "Add a provider").await;
        let mut fixture = fixture;
        // People sign in through the same accounts as in production.
        let (vault, accounts) = fixture.accounts();
        fixture.runs = fixture
            .runs
            .clone()
            .with_provider_renamer(Arc::new(workflow(&fixture)))
            .with_accounts(accounts);
        let mut transaction = fixture.database.begin().await.unwrap();
        fixture
            .providers
            .install_builtin_in_transaction(
                &mut transaction,
                &change(owner.authenticated_user().unwrap().clone(), 11, '2'),
                &workspace_id,
            )
            .await
            .expect("install the Built-in Provider");
        transaction.commit().await.unwrap();
        Self {
            fixture,
            upstream,
            workspace_id,
            owner,
            _vault: vault,
        }
    }

    pub(super) fn owner_user(&self) -> AuthenticatedUser {
        self.owner.authenticated_user().unwrap().clone()
    }

    async fn set_access(&self, state: ToolAccessState) {
        self.set_access_to("coral.workspace.addProvider", state)
            .await;
    }

    /// Sets the Tool Access State of the Tool at `call_path`.
    pub(super) async fn set_access_to(&self, call_path: &str, state: ToolAccessState) {
        self.fixture
            .policies
            .set_access_state(
                &change(self.owner_user(), 12, '3'),
                self.policy_target(call_path).await,
                state,
            )
            .await
            .expect("set the Tool Access State");
    }

    /// The policy target of the Tool at `call_path`.
    pub(super) async fn policy_target(&self, call_path: &str) -> ToolPolicyTarget {
        let mut transaction = self.fixture.database.begin().await.unwrap();
        let target = coral_provider::resolve_invocation_tool_in_transaction(
            &mut transaction,
            &self.workspace_id,
            coral_provider::InvocationCaller::new(&self.fixture.workspaces, &self.owner, 12),
            call_path,
        )
        .await
        .unwrap()
        .unwrap_or_else(|| panic!("{call_path} resolves"));
        transaction.rollback().await.unwrap();
        ToolPolicyTarget::new(
            self.workspace_id.clone(),
            target.provider_access().id().clone(),
            target.tool().key().clone(),
        )
    }

    /// The access state of the Tool under `call_path`, and whether a
    /// rule sets it.
    async fn access(&self, call_path: &str) -> (ToolAccessState, bool) {
        let decision = self
            .fixture
            .policies
            .decision(&self.owner_user(), &self.policy_target(call_path).await)
            .await
            .unwrap();
        (decision.access_state(), decision.is_explicit())
    }

    /// Tool-policy lifecycle events.
    async fn policy_events(&self) -> Vec<coral_policy::AdministrativeLifecycleEvent> {
        self.fixture
            .policies
            .list_lifecycle_events(&self.owner_user(), &self.workspace_id)
            .await
            .unwrap()
    }

    pub(super) async fn start(
        &self,
        actor: &AuthenticatedPrincipal,
        run: RunSubmission,
    ) -> crate::RunId {
        self.fixture
            .runs
            .start(actor, &self.workspace_id, run)
            .await
            .expect("accept Program")
            .run_id()
            .clone()
    }

    pub(super) async fn terminal(
        &self,
        actor: &AuthenticatedPrincipal,
        run: &crate::RunId,
    ) -> crate::RunUpdate {
        wait_for_terminal(&self.fixture.runs, actor, &self.workspace_id, run).await
    }

    /// Runs `run` as the Owner and returns its Final Value.
    pub(super) async fn outcome(&self, run: RunSubmission) -> serde_json::Value {
        let run = self.start(&self.owner, run).await;
        final_value(&self.terminal(&self.owner, &run).await)
    }

    /// `actor`, who started the Run, approves its one pending Invocation.
    pub(super) async fn approve(&self, actor: &AuthenticatedPrincipal, run: &crate::RunId) {
        let approvals = self.fixture.approvals();
        let request = wait_for_approval(&approvals, actor, &self.workspace_id, run).await;
        approvals
            .decide(
                actor,
                &self.workspace_id,
                &request.id,
                crate::approval::ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"d".repeat(32)).unwrap(),
            )
            .await
            .expect("approve addProvider");
    }

    /// Provider-domain lifecycle events: Setups, Publications and Providers.
    pub(super) async fn lifecycle_events(
        &self,
    ) -> Vec<coral_provider::AdministrativeLifecycleEvent> {
        self.fixture
            .providers
            .list_lifecycle_events(&self.owner_user(), &self.workspace_id)
            .await
            .unwrap()
    }

    /// Whether the Run recorded an Invocation, which stores its arguments.
    async fn recorded_an_invocation(&self, run: &crate::RunId) -> bool {
        self.fixture
            .runs
            .activity_entries(
                &self.owner,
                &self.workspace_id,
                run,
                &crate::RunEntryWindow::Start,
                100,
                None,
            )
            .await
            .unwrap()
            .entries
            .iter()
            .any(|entry| matches!(entry, crate::RunEntry::Invocation(_)))
    }

    /// The configuration confirmed for the latest Setup.
    async fn last_confirmed_configuration(&self) -> coral_catalog::ProviderConfiguration {
        let setup = self.created_setups().await.pop().expect("a Setup");
        self.fixture
            .providers
            .review_setup_configuration(&self.owner_user(), &self.workspace_id, &setup)
            .await
            .expect("review the Setup")
            .confirmed
            .expect("a confirmed configuration")
    }

    /// The Provider that holds the `weather` namespace.
    pub(super) async fn weather(&self) -> Option<coral_provider::Provider> {
        self.fixture
            .providers
            .inspect_providers(&self.owner_user(), &self.workspace_id)
            .await
            .unwrap()
            .into_iter()
            .find(|provider| provider.call_root().as_str() == "coral.providers.weather")
    }

    /// Publishes the Setup that `result` links to, as the person does in Reef
    /// under the namespace and route the Program asked for.
    pub(super) async fn person_publishes(&self, result: &serde_json::Value) {
        let setup = linked_setup(result);
        let request = self
            .fixture
            .providers
            .get_setup(&self.owner_user(), &self.workspace_id, &setup)
            .await
            .unwrap()
            .request()
            .cloned()
            .expect("the Program's request");
        workflow(&self.fixture)
            .publish_workspace_provider(
                &change(self.owner_user(), 20, '5'),
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse(&setup.to_string()).unwrap(),
                    self.workspace_id.clone(),
                    setup,
                    request.namespace,
                    request.route,
                ),
            )
            .await
            .unwrap()
            .published_provider()
            .expect("published Provider");
    }

    /// Publishes the Update Setup that `result` links to, as the person does on
    /// Reef's update page.
    pub(super) async fn person_publishes_update(&self, result: &serde_json::Value) {
        let setup = linked_setup(result);
        let provider = self.weather().await.expect("the weather Provider");
        let revision = self
            .fixture
            .providers
            .review_setup_configuration(&self.owner_user(), &self.workspace_id, &setup)
            .await
            .unwrap()
            .revision;
        let context = change(self.owner_user(), 21, '5')
            .with_etag(&provider.etag())
            .unwrap();
        let publication = workflow(&self.fixture)
            .regenerate_provider(
                &context,
                ProviderRegenerationRequest::new(
                    ManagementRequestId::parse(&setup.to_string()).unwrap(),
                    self.workspace_id.clone(),
                    provider.id().clone(),
                    setup,
                    revision,
                ),
            )
            .await
            .unwrap();
        assert_eq!(
            publication.outcome(),
            coral_provider::ProviderPublicationOutcome::Succeeded
        );
    }

    /// Every Setup created so far, from its creation evidence.
    pub(super) async fn created_setups(&self) -> Vec<ProviderSetupId> {
        self.lifecycle_events()
            .await
            .iter()
            .filter(|event| event.action() == coral_provider::LifecycleAction::SetupCreated)
            .map(|event| ProviderSetupId::parse(event.target_id()).unwrap())
            .collect()
    }

    /// The Setups created since `before` was read.
    pub(super) async fn setups_created_since(
        &self,
        before: &[ProviderSetupId],
    ) -> Vec<ProviderSetupId> {
        let mut created = self.created_setups().await;
        created.retain(|setup| !before.contains(setup));
        created
    }

    /// Asserts that a denied call changed nothing since `before` was read: it
    /// published nothing, and every Setup it prepared has ended.
    pub(super) async fn assert_denial_changed_nothing(
        &self,
        before: &[coral_provider::AdministrativeLifecycleEvent],
        case: &str,
    ) {
        for event in self.lifecycle_events().await {
            if before.contains(&event) {
                continue;
            }
            assert_ne!(
                event.action(),
                coral_provider::LifecycleAction::PublicationCompleted,
                "{case}"
            );
            if event.action() == coral_provider::LifecycleAction::SetupCreated {
                let setup = ProviderSetupId::parse(event.target_id()).unwrap();
                let state = self
                    .fixture
                    .providers
                    .get_setup(&self.owner_user(), &self.workspace_id, &setup)
                    .await
                    .unwrap()
                    .state();
                assert_eq!(state, ProviderSetupState::Canceled, "{case}");
            }
        }
    }

    /// Publishes the credential-free Weather document under `weather`, as an
    /// Owner does in Reef.
    pub(super) async fn publish_weather(&self) -> coral_provider::Provider {
        let context = change(self.owner_user(), 20, '5');
        let setup = self
            .fixture
            .providers
            .prepare_openapi_fixture(
                &context,
                &self.workspace_id,
                OpenApiDocument::parse(DOCUMENT).unwrap(),
            )
            .await
            .unwrap();
        self.fixture
            .providers
            .publish_workspace_provider(
                &context,
                WorkspaceProviderPublicationRequest::new(
                    ManagementRequestId::parse("00000000-0000-4000-8000-000000000020").unwrap(),
                    self.workspace_id.clone(),
                    setup.id().clone(),
                    ProviderNamespace::parse("weather").unwrap(),
                    CredentialRoute::None,
                ),
            )
            .await
            .unwrap()
            .published_provider()
            .expect("published Provider")
            .clone()
    }

    /// How many Tools the Workspace's Built-in Provider has.
    async fn builtin_tools(&self) -> usize {
        let builtin = self
            .fixture
            .providers
            .inspect_providers(&self.owner_user(), &self.workspace_id)
            .await
            .unwrap()
            .into_iter()
            .find(|provider| provider.call_root().as_str() == "coral.workspace")
            .expect("the Workspace has its Built-in Provider");
        self.fixture
            .providers
            .inspect_catalog_page(
                &self.owner_user(),
                &self.workspace_id,
                builtin.id(),
                None,
                100,
            )
            .await
            .unwrap()
            .len()
    }
}

/// The bearer-protected OpenAPI document served at `/openapi.json`.
async fn private_document_server() -> MockServer {
    document_server(PRIVATE_DOCUMENT).await
}

/// A server that serves `document` at `/openapi.json`.
pub(super) async fn document_server(document: &'static str) -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(document))
        .mount(&server)
        .await;
    server
}

/// The `openapi_url` source for the document `server` serves.
pub(super) fn url_source(server: &MockServer) -> serde_json::Value {
    serde_json::json!({
        "kind": "openapi_url",
        "url": format!("{}/openapi.json", server.uri()),
    })
}

/// The Setup that a `needs_person` result links to.
pub(super) fn linked_setup(result: &serde_json::Value) -> ProviderSetupId {
    assert_eq!(
        result.pointer("/data/status"),
        Some(&serde_json::json!("needs_person")),
        "{result}"
    );
    result
        .pointer("/data/url")
        .and_then(serde_json::Value::as_str)
        .and_then(|link| link.split_once("setup="))
        .and_then(|(_page, query)| ProviderSetupId::parse(query.split('&').next()?).ok())
        .unwrap_or_else(|| panic!("a link to one Setup: {result}"))
}

pub(super) async fn fetches(server: &MockServer) -> usize {
    server.received_requests().await.unwrap_or_default().len()
}

pub(super) fn final_value(update: &crate::RunUpdate) -> serde_json::Value {
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = update.terminal_outcome()
    else {
        panic!("a Final Value: {update:?}")
    };
    value.as_json().clone()
}

// Catches: addProvider waiting on the person who finishes its Setup, Coral
// publishing a Provider no person previewed, or a result other than the Reef
// page for that Setup.
#[tokio::test]
async fn add_provider_returns_the_reef_page_that_finishes_its_setup() {
    for (access, route, document) in [
        (ToolAccessState::ApprovalRequired, "fixed", PRIVATE_DOCUMENT),
        (ToolAccessState::Enabled, "fixed", PRIVATE_DOCUMENT),
        (ToolAccessState::Enabled, "none", DOCUMENT),
    ] {
        let scope = AdminScope::new("user:add-provider").await;
        scope.set_access(access).await;
        let server = document_server(document).await;
        let source = url_source(&server);
        let before = scope.created_setups().await;
        let run = scope
            .start(&scope.owner, add_provider(&serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": route }), "error.code"))
            .await;
        if access == ToolAccessState::ApprovalRequired {
            wait_for_approval(
                &scope.fixture.approvals(),
                &scope.owner,
                &scope.workspace_id,
                &run,
            )
            .await;
            assert_eq!(fetches(&server).await, 0, "no fetch before approval");
            assert!(
                scope.setups_created_since(&before).await.is_empty(),
                "no Setup before approval"
            );
            scope.approve(&scope.owner, &run).await;
        }
        let terminal = scope.terminal(&scope.owner, &run).await;
        assert_eq!(terminal.state(), RunState::Succeeded, "{access:?}");
        let created = scope.setups_created_since(&before).await;
        let [setup] = created.as_slice() else {
            panic!("{access:?}: one new Setup: {created:?}")
        };
        // The Setup holds what the Program asked for, so the link names only the Setup.
        let url = format!(
            "{TEST_REEF}/workspaces/{}/providers/new?setup={setup}",
            scope.workspace_id,
        );
        assert_eq!(
            final_value(&terminal),
            serde_json::json!({
                "data": { "status": "needs_person", "url": url, "namespace": "weather" },
                "metadata": { "kind": "builtin" },
            }),
            "{access:?}"
        );
        assert!(
            scope.weather().await.is_none(),
            "{access:?} {route}: nothing is published"
        );
        // A Program can find the Setup again while a person finishes it.
        let listed = scope
            .start(&scope.owner, workspace_call("listProviders", "{}"))
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &listed).await).pointer("/data/setups"),
            Some(&serde_json::json!([{
                "callRoot": "coral.providers.weather",
                "credentialRoute": route,
                "state": "ready",
                "url": url,
            }])),
            "{access:?}"
        );
        assert_eq!(
            scope
                .fixture
                .providers
                .get_setup(&scope.owner_user(), &scope.workspace_id, setup)
                .await
                .unwrap()
                .state(),
            // Coral confirmed the configuration; the person only enters the credential.
            ProviderSetupState::Ready,
            "{access:?}"
        );
        assert_eq!(fetches(&server).await, 1, "{access:?}");
        assert_eq!(scope.upstream.sends.load(Ordering::SeqCst), 0);
    }
}

// Catches: a source that fails to answer telling the Program to correct
// arguments that may be right, or leaving a Setup open behind the failure.
#[tokio::test]
async fn a_source_that_fails_to_answer_is_not_the_programs_input_to_correct() {
    let scope = AdminScope::new("user:source-fails").await;
    scope.set_access(ToolAccessState::Enabled).await;
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/unavailable.json"))
        .respond_with(ResponseTemplate::new(503))
        .mount(&server)
        .await;
    let closed = {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.local_addr().unwrap()
    };
    for url in [
        format!("{}/unavailable.json", server.uri()),
        format!("http://{closed}/openapi.json"),
    ] {
        let before = scope.created_setups().await;
        let run = scope
            .start(
                &scope.owner,
                add_provider(
                    &serde_json::json!({
                        "namespace": "weather",
                        "source": { "kind": "openapi_url", "url": url },
                        "credentialRoute": "none",
                    }),
                    "error.code",
                ),
            )
            .await;
        assert_final_code(
            &scope.terminal(&scope.owner, &run).await,
            "upstream_incomplete",
        );
        for setup in scope.setups_created_since(&before).await {
            let state = scope
                .fixture
                .providers
                .get_setup(&scope.owner_user(), &scope.workspace_id, &setup)
                .await
                .unwrap()
                .state();
            assert!(!state.is_open(), "{url} {setup}: {state:?}");
        }
    }
    assert!(
        !scope
            .fixture
            .providers
            .inspect_providers(&scope.owner_user(), &scope.workspace_id)
            .await
            .unwrap()
            .iter()
            .any(|provider| provider.call_root().as_str() == "coral.providers.weather")
    );
}

// Catches: a namespace already in use failing the call with anything but the
// conflict the Program can act on.
#[tokio::test]
async fn a_namespace_in_use_is_a_conflict() {
    let scope = AdminScope::new("user:namespace-in-use").await;
    scope.set_access(ToolAccessState::Enabled).await;
    scope
        .fixture
        .providers
        .publish_openapi_fixture(
            &change(scope.owner_user(), 13, '4'),
            &scope.workspace_id,
            OpenApiDocument::parse(DOCUMENT).unwrap(),
            "weather",
            CredentialRoute::None,
        )
        .await
        .expect("an earlier Provider takes the namespace");
    let run = scope
        .start(
            &scope.owner,
            add_provider(
                &serde_json::json!({
                    "namespace": "weather",
                    "source": document_source(),
                    "credentialRoute": "none",
                }),
                "error.code",
            ),
        )
        .await;
    assert_final_code(
        &scope.terminal(&scope.owner, &run).await,
        "provider_namespace_conflict",
    );
}

/// Ends the Run just after addProvider prepares its Setup.
struct RunEndsAfterSetup {
    clock: Arc<TestClock>,
}

#[async_trait]
impl InvocationObserver for RunEndsAfterSetup {
    fn builtin_setup_prepared(&self) {
        // Past the Run timeout, not the claim lease.
        self.clock.next.store(100_000_000_000, Ordering::SeqCst);
    }
}

// Catches: Coral linking a person to a Setup for a Run that can no longer
// return the link, or leaving that Run's Setup open.
#[tokio::test]
async fn a_run_that_ends_before_the_call_returns_leaves_nothing_behind() {
    let mut scope = AdminScope::new("user:ended-before-return").await;
    scope.fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_mins(15));
    scope.fixture.runs.invocation_observer = Arc::new(RunEndsAfterSetup {
        clock: scope.fixture.clock.clone(),
    });
    scope.set_access(ToolAccessState::Enabled).await;
    let before = scope.created_setups().await;
    let run = scope
        .start(
            &scope.owner,
            add_provider(
                &serde_json::json!({
                    "namespace": "weather",
                    "source": document_source(),
                    "credentialRoute": "none",
                }),
                "error.code",
            ),
        )
        .await;
    assert_eq!(
        scope.terminal(&scope.owner, &run).await.state(),
        RunState::TimedOut
    );
    let created = scope.setups_created_since(&before).await;
    let [setup] = created.as_slice() else {
        panic!("the call prepares one Setup before the Run ends")
    };
    let state = scope
        .fixture
        .providers
        .get_setup(&scope.owner_user(), &scope.workspace_id, setup)
        .await
        .unwrap()
        .state();
    assert!(!state.is_open(), "{setup}: {state:?}");
}

// Catches: input the contract rejects being stored with the Invocation,
// waiting for an Owner's approval, or reaching a fetch or a Setup, before it fails.
#[tokio::test]
async fn invalid_input_fails_before_storage_approval_fetch_or_setup() {
    let scope = AdminScope::new("user:invalid-input").await;
    let server = private_document_server().await;
    let fetchable = url_source(&server);
    let arguments = |namespace: &str, source: &serde_json::Value| serde_json::json!({ "namespace": namespace, "source": source, "credentialRoute": "fixed" });
    let authenticated = |authentication: serde_json::Value| serde_json::json!({ "namespace": "weather", "source": fetchable, "credentialRoute": "fixed", "authentication": authentication });
    let before = scope.lifecycle_events().await.len();
    // The contract's own tests hold each rule; one shape and one value rule
    // show that a rejection comes before anything else.
    for arguments in [
        arguments("not-an-identifier", &fetchable),
        // A method that takes no settings takes no credential beside it.
        authenticated(serde_json::json!({ "method": "bearer", "token": "secret" })),
    ] {
        // addProvider waits for approval by default; input it can judge alone never does.
        let run = scope
            .start(&scope.owner, add_provider(&arguments, "error.code"))
            .await;
        assert_final_code(&scope.terminal(&scope.owner, &run).await, "invalid_input");
        assert!(
            scope
                .fixture
                .approvals()
                .list_for_run(&scope.owner, &scope.workspace_id, &run)
                .await
                .unwrap()
                .is_empty(),
            "{arguments}: no approval request"
        );
        assert!(
            !scope.recorded_an_invocation(&run).await,
            "{arguments}: no Invocation stores the arguments"
        );
    }
    assert_eq!(fetches(&server).await, 0);
    assert_eq!(scope.lifecycle_events().await.len(), before);
}

// Catches: an older Coral that a rolling deploy runs beside a newer one
// failing an Invocation as corrupt data, or asking an Owner to approve an
// Tool it cannot perform, after the newer Coral updated at boot.
#[tokio::test]
async fn a_tool_this_version_does_not_perform_fails_before_approval() {
    let scope = AdminScope::new("user:unknown-tool").await;
    let newer_catalog = coral_catalog::BuiltinCatalog::with_unknown_tools([(
        "listThings",
        coral_catalog::ToolHints::default(),
    )])
    .expect("the newer catalog prepares");
    let newer = ProviderModule::new(
        coral_outbound::DestinationPolicy::default(),
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        newer_catalog,
    );
    // A process of the newer Coral boots and updates the Workspace.
    let [outdated] = newer
        .outdated_builtin_generations()
        .await
        .unwrap()
        .try_into()
        .expect("the Workspace's Built-in Provider pins the older catalog");
    let mut transaction = scope.fixture.database.begin().await.unwrap();
    assert!(
        newer
            .update_builtin_catalog_in_transaction(&mut transaction, &outdated, 20)
            .await
            .unwrap(),
        "the newer Coral publishes its catalog"
    );
    transaction.commit().await.unwrap();

    let run = scope
        .start(
            &scope.owner,
            submission(
                "
                try {
                    return await coral.workspace.listThings({});
                } catch (error) {
                    if (!(error instanceof coral.ToolError)) throw error;
                    return error.code;
                }
            ",
            ),
        )
        .await;

    assert_final_code(
        &scope.terminal(&scope.owner, &run).await,
        "tool_unavailable",
    );
    assert!(
        scope
            .fixture
            .approvals()
            .list_for_run(&scope.owner, &scope.workspace_id, &run)
            .await
            .unwrap()
            .is_empty(),
        "no approval request"
    );
    assert!(!scope.recorded_an_invocation(&run).await);
}

// Catches: a Program unable to configure how a person signs in as Reef's
// configuration step does, a link that opens the wrong account question, or
// Coral publishing an application whose secret only a person can give.
#[tokio::test]
async fn the_program_configures_how_a_person_signs_in() {
    let scope = AdminScope::new("user:sign-in-configuration").await;
    scope.set_access(ToolAccessState::Enabled).await;
    let bot_header = serde_json::json!({
        "method": "header_credentials",
        "headers": { "authorization": { "source": "field", "field": "token", "prefix": "Bot " } },
    });
    let bot_token = serde_json::json!({ "token": { "label": "Bot token", "sensitive": true } });
    let bot = serde_json::json!({ "authentication": bot_header, "credentialFields": bot_token });
    let device = serde_json::json!({ "authentication": {
        "method": "oauth_device_code",
        "device_authorization_url": "https://weather.example/device",
        "token_url": "https://weather.example/token",
        "scopes": [],
        "client": { "client_id": "weather-app" },
    } });
    let confidential = serde_json::json!({ "authentication": {
        "method": "oauth_authorization_code",
        "authorization_url": "https://weather.example/authorize",
        "token_url": "https://weather.example/token",
        "scopes": ["read"],
        "client": {
            "client_id": "weather-app",
            "authentication": "client_secret_post",
        },
    } });
    for (index, (route, choices, needs_person)) in [
        ("fixed", bot, true),
        // The source declares no field for the header to send.
        (
            "fixed",
            serde_json::json!({ "authentication": bot_header }),
            false,
        ),
        // Device code never uses PKCE, whatever the Program wrote.
        ("fixed", device, true),
        // Each person's own Account needs a sign-in too, and only the person
        // can give a confidential application's secret.
        ("caller", confidential, true),
    ]
    .into_iter()
    .enumerate()
    {
        let mut arguments = serde_json::json!({
            "namespace": format!("weather{index}"),
            "source": document_source(),
            "credentialRoute": route,
        });
        arguments
            .as_object_mut()
            .unwrap()
            .extend(choices.as_object().unwrap().clone());
        let run = scope
            .start(&scope.owner, add_provider(&arguments, "error.code"))
            .await;
        let value = final_value(&scope.terminal(&scope.owner, &run).await);
        if !needs_person {
            assert_eq!(value, serde_json::json!("invalid_input"), "{arguments}");
            continue;
        }
        // Reef opens the account question from the route the Setup holds.
        let setup = linked_setup(&value);
        let request = scope
            .fixture
            .providers
            .get_setup(&scope.owner_user(), &scope.workspace_id, &setup)
            .await
            .unwrap()
            .request()
            .cloned()
            .expect("the Program's request");
        assert_eq!(
            (request.namespace.as_str(), request.route),
            (
                format!("weather{index}").as_str(),
                CredentialRoute::from_storage_str(route).unwrap()
            ),
            "{arguments}"
        );
        let configuration = scope.last_confirmed_configuration().await;
        match configuration.authentication.expect("a method") {
            coral_catalog::AuthenticationConfiguration::HeaderCredentials { .. } => {
                let field = configuration
                    .credential_fields
                    .get("token")
                    .expect("the token field");
                assert_eq!((field.label.as_str(), field.sensitive), ("Bot token", true));
            }
            coral_catalog::AuthenticationConfiguration::OAuthDeviceCode { client, .. } => {
                let client = client.expect("the Program's application");
                assert_eq!(client.client_id, "weather-app");
                assert_eq!(client.pkce, coral_catalog::OAuthPkcePolicy::Disabled);
            }
            coral_catalog::AuthenticationConfiguration::OAuthAuthorizationCode {
                client, ..
            } => {
                let client = client.expect("the Program's application");
                assert_eq!(
                    client.authentication,
                    coral_catalog::OAuthClientAuthenticationMethod::ClientSecretPost
                );
            }
            other => panic!("unexpected method {other:?}"),
        }
    }
}

/// Serves the credential-free Weather document, but holds each response until
/// released, so a test can change the Workspace while Coral fetches it.
pub(super) struct HeldDocument {
    pub(super) entered: Arc<tokio::sync::Notify>,
    pub(super) release: Mutex<mpsc::Receiver<()>>,
}

impl wiremock::Respond for HeldDocument {
    fn respond(&self, _request: &wiremock::Request) -> ResponseTemplate {
        self.entered.notify_one();
        // The mock server answers on its own thread, so this blocks only it.
        let _released = self.release.lock().unwrap().recv();
        ResponseTemplate::new(200).set_body_string(DOCUMENT)
    }
}

// Catches: an Owner demoted after approving their own addProvider, while Coral
// fetches the source, still preparing a Setup, or the lost authority reaching
// the Program as anything but a failure it catches.
#[tokio::test]
async fn an_owner_demoted_while_coral_performs_the_call_loses_it() {
    let scope = AdminScope::new("user:demoting-owner").await;
    scope.set_access(ToolAccessState::ApprovalRequired).await;
    let demoted = scope
        .fixture
        .workspace_user(
            &scope.workspace_id,
            &scope.owner_user(),
            "user:demoted-owner",
            WorkspaceRole::Owner,
        )
        .await;
    let actor = AuthenticatedPrincipal::user(demoted.clone());
    let server = MockServer::start().await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, released) = mpsc::channel();
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(HeldDocument {
            entered: entered.clone(),
            release: Mutex::new(released),
        })
        .mount(&server)
        .await;
    let before = scope.lifecycle_events().await;
    let run = scope
        .start(
            &actor,
            add_provider(
                &valid_with(serde_json::json!({ "source": {
                    "kind": "openapi_url",
                    "url": format!("{}/openapi.json", server.uri()),
                } })),
                "error.code",
            ),
        )
        .await;
    scope.approve(&actor, &run).await;
    entered.notified().await;
    scope
        .fixture
        .workspaces
        .set_membership(
            &change(scope.owner_user(), 30, '6'),
            &scope.workspace_id,
            &demoted,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    release.send(()).unwrap();

    assert_final_code(
        &scope.terminal(&actor, &run).await,
        "dispatch_authority_lost",
    );
    scope
        .assert_denial_changed_nothing(&before, "demoted while Coral fetched")
        .await;
}

/// An MCP server at `/mcp` that answers every request with `challenge`, and
/// the protected-resource and authorization-server metadata a sign-in reads.
/// The resource metadata names `resource` as the server's identity; with no
/// `resource`, the server publishes none.
async fn protected_mcp_server(challenge: &str, resource: Option<&str>) -> MockServer {
    let server = MockServer::start().await;
    let uri = server.uri();
    let challenge = challenge.replace("{uri}", &uri);
    Mock::given(method("POST"))
        .and(path("/mcp"))
        .respond_with(
            ResponseTemplate::new(401).insert_header("www-authenticate", challenge.as_str()),
        )
        .mount(&server)
        .await;
    if let Some(resource) = resource {
        Mock::given(method("GET"))
            .and(path("/.well-known/oauth-protected-resource/mcp"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "resource": resource.replace("{uri}", &uri),
                "authorization_servers": [uri],
            })))
            .mount(&server)
            .await;
    }
    Mock::given(method("GET"))
        .and(path("/.well-known/oauth-authorization-server"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "issuer": uri,
            "authorization_endpoint": format!("{uri}/authorize"),
            "token_endpoint": format!("{uri}/token"),
            "registration_endpoint": format!("{uri}/register"),
            "code_challenge_methods_supported": ["S256"],
        })))
        .mount(&server)
        .await;
    server
}

// Catches: an OAuth-protected MCP server that Coral links a person to unread, or
// whose sign-in Coral cannot discover reaching the Program as anything but
// the source to correct.
#[tokio::test]
async fn an_oauth_protected_mcp_server_needs_a_person_to_sign_in() {
    const BEARER: &str =
        r#"Bearer resource_metadata="{uri}/.well-known/oauth-protected-resource/mcp""#;
    let scope = AdminScope::new("user:protected-mcp").await;
    scope.set_access(ToolAccessState::Enabled).await;
    let discovery = |code: &str| {
        format!(" at `source`: Coral could not discover how to sign in to the MCP server ({code})")
    };
    for (index, (challenge, resource, route, rejected)) in [
        // A person signs in before Coral can read the server, on either route.
        (BEARER, Some("{uri}/mcp"), "fixed", None),
        (BEARER, Some("{uri}/mcp"), "caller", None),
        // The server publishes no metadata, so nobody can tell how to sign in.
        (
            r#"Bearer realm="mcp""#,
            None,
            "fixed",
            Some(
                " at `authentication`: The source does not say how to sign in. Give \
                 `authentication`."
                    .to_owned(),
            ),
        ),
        (
            BEARER,
            Some("{uri}/mcp"),
            "none",
            Some(
                " at `credentialRoute`: the source needs a credential; use `fixed` or `caller`"
                    .to_owned(),
            ),
        ),
        // The metadata names another server, so its sign-in is not this one's.
        (
            BEARER,
            Some("{uri}/elsewhere"),
            "fixed",
            Some(discovery("resource_mismatch")),
        ),
        (
            r#"Basic realm="mcp""#,
            Some("{uri}/mcp"),
            "fixed",
            Some(discovery("challenge_unsupported")),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let server = protected_mcp_server(challenge, resource).await;
        let arguments = serde_json::json!({
            "namespace": format!("tools{index}"),
            "source": { "kind": "mcp_url", "url": format!("{}/mcp", server.uri()) },
            "credentialRoute": route,
        });
        let before = scope.lifecycle_events().await;
        let setups = scope.created_setups().await;
        let run = scope
            .start(
                &scope.owner,
                add_provider(&arguments, "{ code: error.code, message: error.message }"),
            )
            .await;
        let value = final_value(&scope.terminal(&scope.owner, &run).await);
        if let Some(message) = rejected {
            assert_eq!(
                value,
                serde_json::json!({
                    "code": "invalid_input",
                    "message": format!("Tool arguments are invalid{message}"),
                }),
                "{arguments}"
            );
            scope
                .assert_denial_changed_nothing(&before, &arguments.to_string())
                .await;
            continue;
        }
        assert_eq!(
            value.pointer("/data/status"),
            Some(&serde_json::json!("needs_person")),
            "{arguments}: {value}"
        );
        let created = scope.setups_created_since(&setups).await;
        let [setup] = created.as_slice() else {
            panic!("{arguments}: one new Setup: {created:?}")
        };
        assert_eq!(
            scope
                .fixture
                .providers
                .get_setup(&scope.owner_user(), &scope.workspace_id, setup)
                .await
                .unwrap()
                .state(),
            ProviderSetupState::AwaitingAuthorization,
            "{arguments}"
        );
    }
    assert_eq!(scope.upstream.sends.load(Ordering::SeqCst), 0);
}

/// An `invalid_input` message names each argument to correct and what to write,
/// whether the contract rejects it or Coral does once it reads the source. It
/// never repeats a submitted value, though it names a header or field the
/// Program wrote. The contract's own tests hold each of its
/// rules; one problem and several show how a message carries them.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table lists every argument check and the reason it gives"
)]
async fn an_invalid_argument_names_what_to_correct() {
    const CANARY: &str = "sk-canary";
    let scope = AdminScope::new("user:invalid-argument-reason").await;
    scope.set_access(ToolAccessState::Enabled).await;
    for (arguments, message) in [
        (
            valid_with(serde_json::json!({ "namespace": "not-an-identifier" })),
            " at `namespace`: use a JavaScript identifier of at most 128 bytes, such as `weather`",
        ),
        (
            valid_with(
                serde_json::json!({ "source": { "kind": "openapi_document", "document": CANARY } }),
            ),
            " at `source`: Coral cannot build a Provider from the source (invalid_openapi_document)",
        ),
        (
            valid_with(serde_json::json!({ "requestHeaders": { "Host": "weather.example" } })),
            " at `requestHeaders.Host`: Use a header that Coral does not set for credentials or transport.",
        ),
        (
            serde_json::json!({
                "namespace": "not-an-identifier",
                "source": document_source(),
            }),
            ": `namespace`: use a JavaScript identifier of at most 128 bytes, such as `weather`; \
             `credentialRoute`: choose `none`, `fixed` or `caller`",
        ),
        (
            valid_with(serde_json::json!({
                "requestHeaders": { "Host": "weather.example", "Connection": "close" },
            })),
            ": `requestHeaders.Connection`: Use a header that Coral does not set for credentials \
             or transport.; `requestHeaders.Host`: Use a header that Coral does not set for \
             credentials or transport.",
        ),
        (
            valid_with(serde_json::json!({
                "credentialRoute": "fixed",
                "authentication": {
                    "method": "oauth_authorization_code",
                    "authorization_url": "https://weather.example/authorize",
                    "token_url": "https://weather.example/token",
                    "scopes": [],
                    "client": { "client_id": " " },
                },
            })),
            " at `authentication.client`: Review the OAuth application, PKCE policy and \
             authorization parameters.",
        ),
        // A person could not sign in: nothing names the application, and Coral
        // cannot register one with this service.
        (
            valid_with(serde_json::json!({
                "credentialRoute": "fixed",
                "authentication": {
                    "method": "oauth_authorization_code",
                    "authorization_url": "https://weather.example/authorize",
                    "token_url": "https://weather.example/token",
                    "scopes": [],
                },
            })),
            " at `authentication.client`: Coral cannot register an OAuth application with this \
             service; register one with the service and give its `client_id`",
        ),
        (
            valid_with(serde_json::json!({
                "credentialRoute": "caller",
                "authentication": {
                    "method": "oauth_device_code",
                    "device_authorization_url": "https://weather.example/device",
                    "token_url": "https://weather.example/token",
                    "scopes": [],
                },
            })),
            " at `authentication.client`: Coral cannot register an OAuth application with this \
             service; register one with the service and give its `client_id`",
        ),
        (
            valid_with(serde_json::json!({
                "credentialRoute": "fixed",
                "authentication": {
                    "method": "header_credentials",
                    "headers": { "authorization": { "source": "field", "field": "token" } },
                },
                "credentialFields": { "token": { "label": " ", "sensitive": true } },
            })),
            " at `credentialFields.token`: Name each field in lowercase, give it a label, and \
             send it.",
        ),
        (
            valid_with(serde_json::json!({
                "servers": {
                    "#/servers": {
                        "kind": "declared",
                        "url": "https://elsewhere.example",
                        "variables": {},
                    },
                },
            })),
            " at `servers.#/servers`: Select a server and complete its declared variables, or \
             supply a valid base URL.",
        ),
        (
            valid_with(serde_json::json!({ "credentialRoute": "fixed" })),
            " at `credentialRoute`: the source declares no credential; use `none`, or give \
             `authentication` to say how a person signs in",
        ),
        (
            valid_with(serde_json::json!({
                "source": { "kind": "openapi_document", "document": PRIVATE_DOCUMENT },
            })),
            " at `credentialRoute`: the source needs a credential; use `fixed` or `caller`",
        ),
    ] {
        let before = scope.lifecycle_events().await;
        let run = scope
            .start(
                &scope.owner,
                add_provider(&arguments, "{ code: error.code, message: error.message }"),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            serde_json::json!({
                "code": "invalid_input",
                "message": format!("Tool arguments are invalid{message}"),
            }),
            "{arguments}"
        );
        scope
            .assert_denial_changed_nothing(&before, &arguments.to_string())
            .await;
    }
}

// Catches: a Built-in Tool's reach that ignores the Workspace action it
// declares, a host that performs listProviders or getPolicy only for a User,
// or a read that waits for approval.
#[tokio::test]
async fn members_and_agents_list_providers_and_get_policy_but_reach_no_owner_tool() {
    let scope = AdminScope::new("user:admin-owner").await;
    scope.set_access(ToolAccessState::Enabled).await;
    scope.publish_weather().await;
    let member = organization_user(
        scope.fixture.database.clone(),
        "user:admin-member",
        OrganizationRole::Member,
    )
    .await;
    scope
        .fixture
        .workspaces
        .set_membership(
            &change(scope.owner_user(), 14, '5'),
            &scope.workspace_id,
            &member,
            WorkspaceRole::Member,
        )
        .await
        .unwrap();
    let agent =
        registered_agent(&scope.fixture.workspaces, &scope.owner, &scope.workspace_id).await;
    let before = scope.lifecycle_events().await;
    let policy_before = scope.policy_events().await;
    for principal in [AuthenticatedPrincipal::user(member), agent] {
        for (tool, arguments) in [
            (
                "addProvider",
                serde_json::json!({ "namespace": "other", "source": document_source(), "credentialRoute": "none" }),
            ),
            ("getProvider", serde_json::json!({ "namespace": "weather" })),
            (
                "updatePolicy",
                serde_json::json!({ "namespace": "weather", "tools": { "getForecast": "disabled" } }),
            ),
        ] {
            let run = scope
                .start(&principal, workspace_call(tool, &arguments.to_string()))
                .await;
            assert_eq!(
                final_value(&scope.terminal(&principal, &run).await),
                serde_json::json!({
                    "code": "tool_not_found",
                    "message": "Tool Call Path was not found",
                }),
                "{tool}"
            );
        }

        let run = scope
            .start(&principal, workspace_call("listProviders", "{}"))
            .await;
        let listed = final_value(&scope.terminal(&principal, &run).await);
        let call_roots = listed
            .pointer("/data/providers")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("{principal:?} lists providers: {listed}"))
            .iter()
            .map(|provider| provider.get("callRoot").cloned())
            .collect::<Vec<_>>();
        assert_eq!(
            call_roots,
            [
                Some(serde_json::json!("coral.workspace")),
                Some(serde_json::json!("coral.providers.weather"))
            ],
            "{principal:?}"
        );
        assert_eq!(
            listed.pointer("/data/setups"),
            Some(&serde_json::json!([])),
            "{principal:?}"
        );

        let run = scope
            .start(
                &principal,
                workspace_call("getPolicy", r#"{ "namespace": "weather" }"#),
            )
            .await;
        let read = final_value(&scope.terminal(&principal, &run).await);
        assert_eq!(
            read.pointer("/data/namespace"),
            Some(&serde_json::json!("weather")),
            "{principal:?} reads the policy: {read}"
        );
    }
    assert_eq!(scope.lifecycle_events().await, before);
    assert_eq!(scope.policy_events().await, policy_before);
}

#[tokio::test]
async fn the_source_fetch_uses_the_bootstrap_destination_policy() {
    let server = private_document_server().await;
    let source = url_source(&server);
    // The default deployment policy rejects the loopback test server.
    let scope = AdminScope::new("user:outbound-policy").await;
    scope.set_access(ToolAccessState::Enabled).await;
    let before = scope.created_setups().await;
    let production = RunModule::new(
        DestinationPolicy::default(),
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        coral_provider::ProviderModule::with_running_catalog(
            DestinationPolicy::default(),
            scope.fixture.database.clone(),
            scope.fixture.workspaces.clone(),
        ),
        crate::RuntimeLimits::default(),
        crate::CoralInstanceId::generate(),
        crate::RunInbox::table(scope.fixture.database.clone()),
    )
    .expect("start the real Program runtime")
    .with_human_interface(test_reef());
    let run = production
        .start(
            &scope.owner,
            &scope.workspace_id,
            add_provider(&serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "fixed" }), "error.code"),
        )
        .await
        .unwrap()
        .run_id()
        .clone();
    assert_final_code(
        &wait_for_terminal(&production, &scope.owner, &scope.workspace_id, &run).await,
        "outbound_destination_rejected",
    );
    assert_eq!(fetches(&server).await, 0);
    assert!(scope.setups_created_since(&before).await.is_empty());

    // A policy that permits loopback fetches the source once and prepares one Setup.
    let run = scope
        .start(&scope.owner, add_provider(&serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "fixed" }), "error.code"))
        .await;
    assert_eq!(
        scope.terminal(&scope.owner, &run).await.state(),
        RunState::Succeeded
    );
    assert_eq!(fetches(&server).await, 1);
    assert_eq!(scope.setups_created_since(&before).await.len(), 1);
}

/// A Program that calls the Built-in `tool` with `arguments`, the
/// JavaScript argument list, and returns its Tool Result or the failure
/// it catches.
pub(super) fn workspace_call(tool: &str, arguments: &str) -> RunSubmission {
    submission(
        &"
        try {
            return await coral.workspace.TOOL(ARGUMENTS);
        } catch (error) {
            if (!(error instanceof coral.ToolError)) throw error;
            return { code: error.code, message: error.message };
        }
    "
        .replace("TOOL", tool)
        .replace("ARGUMENTS", arguments),
    )
}

// Catches: a result that misleads a Program about taken Call Roots, such as
// a disabled Provider not marked disabled, a tools count that includes
// a Tool the Provider's state or policy stops the caller calling, a read
// that waits for approval, or an argument the call silently ignores.
#[tokio::test]
async fn list_providers_lists_every_provider_without_approval() {
    let scope = AdminScope::new("user:list-providers").await;
    scope.set_access(ToolAccessState::Disabled).await;
    let weather = scope.publish_weather().await;
    scope
        .fixture
        .providers
        .disable_provider(
            &change(scope.owner_user(), 21, '6')
                .with_etag(&weather.etag())
                .unwrap(),
            &scope.workspace_id,
            weather.id(),
        )
        .await
        .unwrap();
    let builtin_tools = scope.builtin_tools().await;
    let listed = serde_json::json!({
        "data": {
            "providers": [
                {
                    "callRoot": "coral.workspace",
                    "namespace": null,
                    "displayName": "Coral",
                    "state": "active",
                    "credentialRoute": "none",
                    "authentication": "none",
                    // Policy disables addProvider.
                    "tools": builtin_tools - 1,
                    "accountRequired": false,
                },
                {
                    "callRoot": "coral.providers.weather",
                    "namespace": "weather",
                    "displayName": "Weather",
                    "state": "disabled",
                    "credentialRoute": "none",
                    "authentication": "none",
                    "tools": 0,
                    "accountRequired": false,
                },
            ],
            "setups": [],
        },
        "metadata": { "kind": "builtin" },
    });

    for (arguments, expected) in [
        ("{}", listed.clone()),
        // A call with no argument sends an empty object.
        ("", listed),
        (
            r#"{ "namespace": "weather" }"#,
            serde_json::json!({
                "code": "invalid_input",
                "message": "Tool arguments are invalid at `namespace`: unknown field; \
                            pass an empty object",
            }),
        ),
        (
            "[]",
            serde_json::json!({
                "code": "invalid_input",
                "message": "Tool arguments are invalid: pass an empty object",
            }),
        ),
    ] {
        let run = scope
            .start(&scope.owner, workspace_call("listProviders", arguments))
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            expected,
            "{arguments}"
        );
        assert!(
            scope
                .fixture
                .approvals()
                .list_for_run(&scope.owner, &scope.workspace_id, &run)
                .await
                .unwrap()
                .is_empty(),
            "{arguments}: no approval request"
        );
    }
}

/// An MCP server that lists one read-only tool to anyone.
async fn public_mcp_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .respond_with(|request: &wiremock::Request| {
            let request: serde_json::Value = request.body_json().expect("an MCP request");
            let result = match request.get("method").and_then(serde_json::Value::as_str) {
                Some("initialize") => serde_json::json!({
                    "protocolVersion": "2025-11-25",
                    "capabilities": { "tools": {} },
                    "serverInfo": { "name": "forecasts", "title": "Forecasts", "version": "1" },
                }),
                Some("tools/list") => serde_json::json!({ "tools": [{
                    "name": "lookup",
                    "inputSchema": { "type": "object" },
                    "annotations": { "readOnlyHint": true },
                }] }),
                // A notification has no response body.
                _ => return ResponseTemplate::new(202),
            };
            ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "jsonrpc": "2.0", "id": request.get("id"), "result": result,
            }))
        })
        .mount(&server)
        .await;
    server
}

// Catches: a read that differs from the configuration a Program saved, or
// that returns an uploaded document.
#[tokio::test]
async fn get_provider_reads_back_the_configuration_a_program_saved() {
    let scope = AdminScope::new("user:get-provider").await;
    scope.set_access(ToolAccessState::Enabled).await;
    let document_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(DOCUMENT))
        .mount(&document_server)
        .await;
    let mcp_server = public_mcp_server().await;
    let document_url = format!("{}/openapi.json", document_server.uri());
    let declared = serde_json::json!({
        "#/servers": { "kind": "declared", "url": "https://weather.example", "variables": {} },
    });
    for (namespace, source, read_source, servers, request_headers) in [
        (
            "weatherUrl",
            serde_json::json!({ "kind": "openapi_url", "url": document_url }),
            serde_json::json!({ "kind": "openapi_url", "url": document_url }),
            declared.clone(),
            serde_json::json!({}),
        ),
        (
            "forecasts",
            serde_json::json!({ "kind": "mcp_url", "url": mcp_server.uri() }),
            serde_json::json!({ "kind": "mcp_url", "url": mcp_server.uri() }),
            // Coral saves the inspected server URL in its normalized form.
            serde_json::json!({
                "#/servers": { "kind": "custom", "url": format!("{}/", mcp_server.uri()) },
            }),
            serde_json::json!({ "x-tenant": "acme" }),
        ),
        (
            "weatherDocument",
            document_source(),
            serde_json::json!({ "kind": "openapi_document" }),
            declared.clone(),
            serde_json::json!({ "x-trace": "on" }),
        ),
    ] {
        let created = scope
            .start(
                &scope.owner,
                add_provider(
                    &serde_json::json!({
                        "namespace": namespace,
                        "source": source,
                        "credentialRoute": "none",
                        "displayName": "Saved weather",
                        "requestHeaders": request_headers,
                    }),
                    "error.code",
                ),
            )
            .await;
        let created = final_value(&scope.terminal(&scope.owner, &created).await);
        scope.person_publishes(&created).await;

        let run = scope
            .start(
                &scope.owner,
                workspace_call(
                    "getProvider",
                    &serde_json::json!({ "namespace": namespace }).to_string(),
                ),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            serde_json::json!({
                "data": {
                    "namespace": namespace,
                    "state": "active",
                    "displayName": "Saved weather",
                    "source": read_source,
                    "credentialRoute": "none",
                    "servers": servers,
                    "authentication": { "method": "none" },
                    "requestHeaders": request_headers,
                    "credentialFields": {},
                },
                "metadata": { "kind": "builtin" },
            }),
            "{namespace}"
        );
    }
}

// Catches: a namespace that no Provider holds read as something other than
// not found.
#[tokio::test]
async fn get_provider_and_get_policy_name_an_absent_provider() {
    let scope = AdminScope::new("user:get-provider-absent").await;
    for tool in ["getProvider", "getPolicy"] {
        let run = scope
            .start(
                &scope.owner,
                workspace_call(tool, r#"{ "namespace": "forecasts" }"#),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            serde_json::json!({ "code": "provider_not_found", "message": "Provider was not found" }),
            "{tool}"
        );
    }
}

// Catches: an updatePolicy call that runs before an Owner approves it, or a
// result other than the state each sent path holds.
#[tokio::test]
async fn update_policy_sets_the_sent_paths_once_an_owner_approves() {
    let scope = AdminScope::new("user:update-policy").await;
    scope.publish_weather().await;
    let before = scope.policy_events().await;
    assert_eq!(
        scope.access("coral.workspace.updatePolicy").await,
        (ToolAccessState::ApprovalRequired, false),
        "updatePolicy waits for approval by default"
    );
    let run = scope
        .start(
            &scope.owner,
            workspace_call(
                "updatePolicy",
                r#"{ namespace: "weather", tools: { getForecast: "disabled" } }"#,
            ),
        )
        .await;
    wait_for_approval(
        &scope.fixture.approvals(),
        &scope.owner,
        &scope.workspace_id,
        &run,
    )
    .await;
    assert_eq!(
        scope.access("coral.providers.weather.getForecast").await,
        (ToolAccessState::Enabled, false),
        "nothing changes before approval"
    );

    scope.approve(&scope.owner, &run).await;
    assert_eq!(
        final_value(&scope.terminal(&scope.owner, &run).await),
        serde_json::json!({
            "data": { "namespace": "weather", "tools": { "getForecast": "disabled" } },
            "metadata": { "kind": "builtin" },
        })
    );
    assert_eq!(
        scope.access("coral.providers.weather.getForecast").await,
        (ToolAccessState::Disabled, true)
    );
    let events = scope.policy_events().await;
    assert_eq!(
        events
            .iter()
            .filter(|event| !before.contains(event))
            .map(coral_policy::AdministrativeLifecycleEvent::actor_user_id)
            .collect::<Vec<_>>(),
        [scope.owner_user().user_id()],
        "one change, by the invoking Owner"
    );
}

// Catches: an argument problem that waits for an Owner's approval, or is
// stored with an Invocation, before it fails. coral-catalog owns each problem.
#[tokio::test]
async fn update_policy_rejects_an_argument_problem_before_admission() {
    let scope = AdminScope::new("user:update-policy-arguments").await;
    scope.publish_weather().await;
    let before = scope.policy_events().await;
    let run = scope
        .start(
            &scope.owner,
            workspace_call("updatePolicy", r#"{ namespace: "weather", tools: {} }"#),
        )
        .await;
    assert_eq!(
        final_value(&scope.terminal(&scope.owner, &run).await),
        serde_json::json!({
            "code": "invalid_input",
            "message": "Tool arguments are invalid at `tools`: name 1 to 100 Tools",
        }),
    );
    assert!(!scope.recorded_an_invocation(&run).await);
    assert_eq!(scope.policy_events().await, before);
}

// Catches: a valid path applied beside one that names no Tool, a failure
// that does not name the path, or a namespace that reaches a Built-in
// Tool, updatePolicy's own included.
#[tokio::test]
async fn update_policy_changes_nothing_when_a_target_is_missing() {
    let scope = AdminScope::new("user:update-policy-missing").await;
    scope.publish_weather().await;
    scope
        .set_access_to("coral.workspace.updatePolicy", ToolAccessState::Enabled)
        .await;
    let before = scope.policy_events().await;
    for (arguments, expected) in [
        (
            serde_json::json!({
                "namespace": "weather",
                "tools": { "getForecast": "disabled", "getAlerts": "disabled" },
            }),
            serde_json::json!({
                "code": "tool_not_found",
                "message": "Tool Call Path was not found at `tools.getAlerts`: no \
                            Tool of weather has this path",
            }),
        ),
        (
            serde_json::json!({
                "namespace": "workspace",
                "tools": { "updatePolicy": "disabled" },
            }),
            serde_json::json!({ "code": "provider_not_found", "message": "Provider was not found" }),
        ),
    ] {
        let run = scope
            .start(
                &scope.owner,
                workspace_call("updatePolicy", &arguments.to_string()),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            expected,
            "{arguments}"
        );
    }
    assert_eq!(scope.policy_events().await, before);
    assert_eq!(
        scope.access("coral.workspace.updatePolicy").await,
        (ToolAccessState::Enabled, true)
    );
}
