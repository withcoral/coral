//! `coral.workspace.createProvider` prepares and configures a Provider Setup. Coral
//! publishes it when nobody needs to sign in; otherwise the call returns the
//! Reef page where a person finishes it. The call never waits for that person.
//! `coral.workspace.listProviders` lists the Providers a Program can check first.
use super::*;
use coral_provider::{ProviderSetupId, ProviderSetupState};

const DOCUMENT: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1"},"security":[],"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"OK"}}}}}}"#;
/// The same service behind a bearer token, which a person enters.
const PRIVATE_DOCUMENT: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1"},"security":[{"bearer":[]}],"components":{"securitySchemes":{"bearer":{"type":"http","scheme":"bearer"}}},"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"OK"}}}}}}"#;

/// A Program that calls `createProvider` with exactly `arguments` and returns
/// its Operation Result, or the JavaScript expression `caught` for the Coral
/// Failure it catches, such as `error.code`.
fn create_provider(arguments: &serde_json::Value, caught: &str) -> RunSubmission {
    submission(
        &"
        try {
            return await coral.workspace.createProvider(ARGUMENTS);
        } catch (error) {
            if (!(error instanceof coral.OperationError)) throw error;
            return CAUGHT;
        }
    "
        .replace("CAUGHT", caught)
        .replace("ARGUMENTS", &arguments.to_string()),
    )
}

/// Arguments that `createProvider` accepts for a source that needs no
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

fn document_source() -> serde_json::Value {
    serde_json::json!({ "kind": "openapi_document", "document": DOCUMENT })
}

/// The workflow Reef publishes with, Universal Search included, that Coral
/// publishes through too.
fn workflow(fixture: &TestModules) -> coral_administration::ProviderWorkflow {
    coral_administration::ProviderWorkflow::new(
        fixture.database.clone(),
        fixture.providers.clone(),
        coral_search::SearchIndex::new(fixture.database.clone(), Duration::from_mins(5)),
    )
}

/// A Workspace whose Owner installed the Built-in Provider, over the real V8 engine.
struct AdminScope {
    fixture: TestModules,
    upstream: Arc<ScriptedUpstream>,
    workspace_id: coral_workspace::WorkspaceId,
    owner: AuthenticatedPrincipal,
    _vault: tempfile::TempDir,
}

impl AdminScope {
    async fn new(user: &str) -> Self {
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
            .with_provider_publisher(Arc::new(workflow(&fixture)))
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

    fn owner_user(&self) -> AuthenticatedUser {
        self.owner.authenticated_user().unwrap().clone()
    }

    async fn set_access(&self, state: OperationAccessState) {
        let mut transaction = self.fixture.database.begin().await.unwrap();
        let target = coral_provider::resolve_invocation_operation_in_transaction(
            &mut transaction,
            &self.workspace_id,
            coral_provider::InvocationCaller::new(&self.fixture.workspaces, &self.owner, 12),
            "coral.workspace.createProvider",
        )
        .await
        .unwrap()
        .expect("the Built-in Operation resolves");
        transaction.rollback().await.unwrap();
        self.fixture
            .policies
            .set_access_state(
                &change(self.owner_user(), 12, '3'),
                OperationPolicyTarget::new(
                    self.workspace_id.clone(),
                    target.provider_access().id().clone(),
                    target.operation().key().clone(),
                ),
                state,
            )
            .await
            .expect("set createProvider access");
    }

    async fn start(&self, actor: &AuthenticatedPrincipal, run: RunSubmission) -> crate::RunId {
        self.fixture
            .runs
            .start(actor, &self.workspace_id, run)
            .await
            .expect("accept Program")
            .run_id()
            .clone()
    }

    async fn terminal(
        &self,
        actor: &AuthenticatedPrincipal,
        run: &crate::RunId,
    ) -> crate::RunUpdate {
        wait_for_terminal(&self.fixture.runs, actor, &self.workspace_id, run).await
    }

    /// The Owner approves the Run's one pending Invocation.
    async fn approve(&self, run: &crate::RunId) {
        let approvals = self.fixture.approvals();
        let request = wait_for_approval(&approvals, &self.owner, &self.workspace_id, run).await;
        approvals
            .decide(
                &self.owner,
                &self.workspace_id,
                &request.id,
                crate::approval::ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"d".repeat(32)).unwrap(),
            )
            .await
            .expect("approve createProvider");
    }

    /// Provider-domain lifecycle events: Setups, Publications and Providers.
    async fn lifecycle_events(&self) -> Vec<coral_provider::AdministrativeLifecycleEvent> {
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

    /// Every Setup created so far, from its creation evidence.
    async fn created_setups(&self) -> Vec<ProviderSetupId> {
        self.lifecycle_events()
            .await
            .iter()
            .filter(|event| event.action() == coral_provider::LifecycleAction::SetupCreated)
            .map(|event| ProviderSetupId::parse(event.target_id()).unwrap())
            .collect()
    }

    /// The Setups created since `before` was read.
    async fn setups_created_since(&self, before: &[ProviderSetupId]) -> Vec<ProviderSetupId> {
        let mut created = self.created_setups().await;
        created.retain(|setup| !before.contains(setup));
        created
    }

    /// Asserts that a denied call changed nothing since `before` was read: it
    /// published nothing, and every Setup it prepared has ended.
    async fn assert_denial_changed_nothing(
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
    async fn publish_weather(&self) -> coral_provider::Provider {
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

    /// How many Operations the Workspace's Built-in Provider has.
    async fn builtin_operations(&self) -> usize {
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
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(PRIVATE_DOCUMENT))
        .mount(&server)
        .await;
    server
}

/// The `openapi_url` source for the document `server` serves.
fn url_source(server: &MockServer) -> serde_json::Value {
    serde_json::json!({
        "kind": "openapi_url",
        "url": format!("{}/openapi.json", server.uri()),
    })
}

async fn fetches(server: &MockServer) -> usize {
    server.received_requests().await.unwrap_or_default().len()
}

fn final_value(update: &crate::RunUpdate) -> serde_json::Value {
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = update.terminal_outcome()
    else {
        panic!("a Final Value: {update:?}")
    };
    value.as_json().clone()
}

// Catches: createProvider waiting on the person who finishes its Setup, or
// returning anything but the Reef page for that Setup.
#[tokio::test]
async fn create_provider_returns_the_reef_page_that_finishes_its_setup() {
    for access in [
        OperationAccessState::ApprovalRequired,
        OperationAccessState::Enabled,
    ] {
        let scope = AdminScope::new("user:create-provider").await;
        scope.set_access(access).await;
        let server = private_document_server().await;
        let source = url_source(&server);
        let before = scope.created_setups().await;
        let run = scope
            .start(&scope.owner, create_provider(&serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "fixed" }), "error.code"))
            .await;
        if access == OperationAccessState::ApprovalRequired {
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
            scope.approve(&run).await;
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
                "data": { "status": "needs_sign_in", "url": url, "namespace": "weather" },
                "metadata": { "kind": "builtin" },
            }),
            "{access:?}"
        );
        // A Program can find the Setup again while a person finishes it.
        let listed = scope.start(&scope.owner, list_providers("{}")).await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &listed).await).pointer("/data/setups"),
            Some(&serde_json::json!([{
                "namespace": "weather",
                "credentialRoute": "fixed",
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

// Catches: Coral asking a person to finish a Provider that nobody needs to
// sign in to, or a published result that leaves the Program guessing.
#[tokio::test]
async fn coral_publishes_a_provider_that_needs_nobody_to_sign_in() {
    let scope = AdminScope::new("user:self-publish").await;
    scope.set_access(OperationAccessState::Enabled).await;
    let run = scope
        .start(
            &scope.owner,
            create_provider(
                &serde_json::json!({
                    "namespace": "weather",
                    "source": document_source(),
                    "credentialRoute": "none",
                }),
                "error.code",
            ),
        )
        .await;
    let terminal = scope.terminal(&scope.owner, &run).await;
    assert_eq!(
        final_value(&terminal),
        serde_json::json!({
            "data": {
                "status": "published",
                "namespace": "weather",
                "displayName": "Weather",
                "credentialRoute": "none",
                "authentication": "none",
                "operations": 1,
                "accountRequired": false,
            },
            "metadata": { "kind": "builtin" },
        })
    );
    assert!(
        scope
            .fixture
            .providers
            .inspect_providers(&scope.owner_user(), &scope.workspace_id)
            .await
            .unwrap()
            .iter()
            .any(|provider| provider.call_root().as_str() == "coral.providers.weather")
    );
}

/// Publishes through the real workflow, but under the `fixed` route with no
/// shared credential, so the Publication fails as a real one does.
struct FailingPublisher(coral_administration::ProviderWorkflow);

#[async_trait]
impl coral_provider::WorkspaceProviderPublisher for FailingPublisher {
    async fn publish_workspace_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<coral_provider::ProviderPublication, coral_provider::Error> {
        self.0
            .publish_workspace_provider(
                context,
                WorkspaceProviderPublicationRequest::new(
                    request.request_id().clone(),
                    request.workspace_id().clone(),
                    request.setup_id().clone(),
                    request.namespace().clone(),
                    CredentialRoute::Fixed,
                ),
            )
            .await
    }
}

/// Cancels the Setup, as an Owner in Reef would, then forwards the request to
/// the real workflow.
struct SetupCanceledFirst {
    workflow: coral_administration::ProviderWorkflow,
    accounts: coral_provider::account::AccountModule,
    _vault: tempfile::TempDir,
}

#[async_trait]
impl coral_provider::WorkspaceProviderPublisher for SetupCanceledFirst {
    async fn publish_workspace_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<coral_provider::ProviderPublication, coral_provider::Error> {
        self.accounts
            .cancel_setup(context, request.workspace_id(), request.setup_id())
            .await?;
        self.workflow
            .publish_workspace_provider(context, request)
            .await
    }
}

/// How Coral's Publication fails.
#[derive(Debug, Clone, Copy)]
enum PublicationFault {
    /// The Publication fails, as one under a route the source cannot take does.
    Failed,
    /// The Setup is canceled before Coral publishes it.
    SetupCanceled,
}

// Catches: a Publication Coral attempts that fails cancelling the Run, instead
// of failing the call with a code the Program can catch.
#[tokio::test]
async fn a_failed_publication_is_a_failure_the_program_catches() {
    for fault in [PublicationFault::Failed, PublicationFault::SetupCanceled] {
        let mut scope = AdminScope::new("user:failed-publication").await;
        scope.set_access(OperationAccessState::Enabled).await;
        let publisher: Arc<dyn coral_provider::WorkspaceProviderPublisher> = match fault {
            PublicationFault::Failed => Arc::new(FailingPublisher(workflow(&scope.fixture))),
            PublicationFault::SetupCanceled => {
                let (vault, accounts) = scope.fixture.accounts();
                Arc::new(SetupCanceledFirst {
                    workflow: workflow(&scope.fixture),
                    accounts,
                    _vault: vault,
                })
            }
        };
        scope.fixture.runs = scope
            .fixture
            .runs
            .clone()
            .with_provider_publisher(publisher);
        let run = scope
            .start(
                &scope.owner,
                create_provider(
                    &serde_json::json!({
                        "namespace": "weather",
                        "source": document_source(),
                        "credentialRoute": "none",
                    }),
                    "error.code",
                ),
            )
            .await;
        let terminal = scope.terminal(&scope.owner, &run).await;
        assert_eq!(terminal.state(), RunState::Succeeded, "{fault:?}");
        assert_final_code(&terminal, "provider_publication_failed");
        assert!(
            !scope
                .fixture
                .providers
                .inspect_providers(&scope.owner_user(), &scope.workspace_id)
                .await
                .unwrap()
                .iter()
                .any(|provider| provider.call_root().as_str() == "coral.providers.weather"),
            "{fault:?}"
        );
    }
}

// Catches: a configuration the source cannot take leaving its Setup open after
// the call fails, where nobody would ever finish it.
#[tokio::test]
async fn a_configuration_the_source_cannot_take_leaves_no_setup_open() {
    let scope = AdminScope::new("user:route-mismatch").await;
    scope.set_access(OperationAccessState::Enabled).await;
    let before = scope.created_setups().await;
    let run = scope
        .start(
            &scope.owner,
            create_provider(
                &serde_json::json!({
                    "namespace": "weather",
                    "source": { "kind": "openapi_document", "document": PRIVATE_DOCUMENT },
                    "credentialRoute": "none",
                }),
                "error.code",
            ),
        )
        .await;
    assert_final_code(&scope.terminal(&scope.owner, &run).await, "invalid_input");
    let created = scope.setups_created_since(&before).await;
    let [setup] = created.as_slice() else {
        panic!("one new Setup: {created:?}")
    };
    assert_eq!(
        scope
            .fixture
            .providers
            .get_setup(&scope.owner_user(), &scope.workspace_id, setup)
            .await
            .unwrap()
            .state(),
        ProviderSetupState::Canceled
    );
}

// Catches: a source that fails to answer telling the Program to correct
// arguments that may be right, or leaving a Setup open behind the failure.
#[tokio::test]
async fn a_source_that_fails_to_answer_is_not_the_programs_input_to_correct() {
    let scope = AdminScope::new("user:source-fails").await;
    scope.set_access(OperationAccessState::Enabled).await;
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
                create_provider(
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

// Catches: a namespace already in use reaching a fetch and a Setup before the
// Publication finally fails on it.
#[tokio::test]
async fn a_namespace_in_use_fails_before_any_fetch_or_setup() {
    let scope = AdminScope::new("user:namespace-in-use").await;
    scope.set_access(OperationAccessState::Enabled).await;
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
    let server = private_document_server().await;
    let before = scope.created_setups().await;
    let run = scope
        .start(
            &scope.owner,
            create_provider(
                &serde_json::json!({
                    "namespace": "weather",
                    "source": url_source(&server),
                    "credentialRoute": "fixed",
                }),
                "error.code",
            ),
        )
        .await;
    assert_final_code(
        &scope.terminal(&scope.owner, &run).await,
        "provider_namespace_conflict",
    );
    assert_eq!(fetches(&server).await, 0);
    assert!(scope.setups_created_since(&before).await.is_empty());
}

/// Publishes another Provider under the requested namespace, then forwards the
/// request to the real workflow, as a second Owner racing the call would.
struct NamespaceTakenFirst(coral_administration::ProviderWorkflow, ProviderModule);

#[async_trait]
impl coral_provider::WorkspaceProviderPublisher for NamespaceTakenFirst {
    async fn publish_workspace_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<coral_provider::ProviderPublication, coral_provider::Error> {
        self.1
            .publish_openapi_fixture(
                context,
                request.workspace_id(),
                OpenApiDocument::parse(DOCUMENT).unwrap(),
                request.namespace().as_str(),
                CredentialRoute::None,
            )
            .await?;
        self.0.publish_workspace_provider(context, request).await
    }
}

// Catches: a namespace taken while Coral publishes failing the call as an
// ordinary failed Publication, instead of the conflict the Program can act on.
#[tokio::test]
async fn a_namespace_taken_while_coral_publishes_is_a_conflict() {
    let mut scope = AdminScope::new("user:namespace-race").await;
    scope.set_access(OperationAccessState::Enabled).await;
    scope.fixture.runs =
        scope
            .fixture
            .runs
            .clone()
            .with_provider_publisher(Arc::new(NamespaceTakenFirst(
                workflow(&scope.fixture),
                scope.fixture.providers.clone(),
            )));
    let run = scope
        .start(
            &scope.owner,
            create_provider(
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

/// Ends the Run just after createProvider prepares its Setup.
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

// Catches: Coral publishing a Provider, or linking a person to a Setup, for a
// Run that can no longer return either, or leaving that Run's Setup open.
#[tokio::test]
async fn a_run_that_ends_before_the_call_returns_leaves_nothing_behind() {
    for (route, document) in [("none", DOCUMENT), ("fixed", PRIVATE_DOCUMENT)] {
        a_run_ends_before_the_call_returns(route, document).await;
    }
}

async fn a_run_ends_before_the_call_returns(route: &str, document: &str) {
    let mut scope = AdminScope::new("user:ended-before-return").await;
    scope.fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_mins(15));
    scope.fixture.runs.invocation_observer = Arc::new(RunEndsAfterSetup {
        clock: scope.fixture.clock.clone(),
    });
    scope.set_access(OperationAccessState::Enabled).await;
    let before = scope.created_setups().await;
    let run = scope
        .start(
            &scope.owner,
            create_provider(
                &serde_json::json!({
                    "namespace": "weather",
                    "source": { "kind": "openapi_document", "document": document },
                    "credentialRoute": route,
                }),
                "error.code",
            ),
        )
        .await;
    assert_eq!(
        scope.terminal(&scope.owner, &run).await.state(),
        RunState::TimedOut
    );
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
    let created = scope.setups_created_since(&before).await;
    let [setup] = created.as_slice() else {
        panic!("{route}: the call prepares one Setup before the Run ends")
    };
    let state = scope
        .fixture
        .providers
        .get_setup(&scope.owner_user(), &scope.workspace_id, setup)
        .await
        .unwrap()
        .state();
    assert!(!state.is_open(), "{route} {setup}: {state:?}");
}

/// Publishes through the real workflow, but stalls before or after the
/// Publication until released, as a worker that dies there would.
struct StallingPublisher {
    workflow: coral_administration::ProviderWorkflow,
    publishes_first: bool,
    entered: AtomicBool,
    release: tokio::sync::Notify,
}

#[async_trait]
impl coral_provider::WorkspaceProviderPublisher for StallingPublisher {
    async fn publish_workspace_provider(
        &self,
        context: &ChangeContext,
        request: WorkspaceProviderPublicationRequest,
    ) -> Result<coral_provider::ProviderPublication, coral_provider::Error> {
        let publication = if self.publishes_first {
            Some(
                self.workflow
                    .publish_workspace_provider(context, request.clone())
                    .await,
            )
        } else {
            None
        };
        self.entered.store(true, Ordering::SeqCst);
        self.release.notified().await;
        match publication {
            Some(publication) => publication,
            None => {
                self.workflow
                    .publish_workspace_provider(context, request)
                    .await
            }
        }
    }
}

// Catches: a worker lost while Coral publishes leaving createProvider
// indeterminate, when Coral's own Publication shows what happened, or a
// Provider deleted since then failing the Run's termination.
#[tokio::test]
async fn recovery_settles_a_lost_publication_from_its_publication() {
    for (publishes_first, deleted_since, expected) in [
        (true, false, crate::InvocationResultKind::UpstreamOutcome),
        (true, true, crate::InvocationResultKind::RunTerminated),
        (false, false, crate::InvocationResultKind::RunTerminated),
    ] {
        let mut scope = AdminScope::new("user:lost-publication").await;
        let publisher = Arc::new(StallingPublisher {
            workflow: workflow(&scope.fixture),
            publishes_first,
            entered: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        scope.fixture.runs = scope
            .fixture
            .runs
            .clone()
            .with_provider_publisher(publisher.clone());
        scope.set_access(OperationAccessState::Enabled).await;
        let run = scope
            .start(
                &scope.owner,
                create_provider(
                    &serde_json::json!({
                        "namespace": "weather",
                        "source": document_source(),
                        "credentialRoute": "none",
                    }),
                    "error.code",
                ),
            )
            .await;
        wait_until(|| publisher.entered.load(Ordering::SeqCst)).await;
        if deleted_since {
            let provider = scope
                .fixture
                .providers
                .inspect_providers(&scope.owner_user(), &scope.workspace_id)
                .await
                .unwrap()
                .into_iter()
                .find(|provider| provider.call_root().as_str() == "coral.providers.weather")
                .expect("Coral published the Provider");
            scope
                .fixture
                .providers
                .delete_provider(
                    &change(scope.owner_user(), 13, '4'),
                    &scope.workspace_id,
                    provider.id(),
                )
                .await
                .expect("an Owner deletes the Provider");
        }
        let accepted_at =
            retained_events(&scope.fixture.runs, &scope.owner, &scope.workspace_id, &run)
                .await
                .first()
                .expect("run.started")
                .occurred_at_unix_nanos();
        scope
            .fixture
            .expire_worker_claim(&scope.workspace_id, &run, accepted_at)
            .await;
        scope
            .fixture
            .clock
            .next
            .store(accepted_at + 2_000_000_000, Ordering::SeqCst);
        scope
            .fixture
            .runs
            .recover()
            .await
            .expect("recover the lost worker");
        let ended: Vec<_> =
            retained_events(&scope.fixture.runs, &scope.owner, &scope.workspace_id, &run)
                .await
                .iter()
                .filter_map(|event| match event.kind() {
                    RunEventKind::InvocationEnded { result_kind, .. } => Some(*result_kind),
                    _ => None,
                })
                .collect();
        assert_eq!(
            ended,
            [expected],
            "publishes first: {publishes_first}, deleted since: {deleted_since}"
        );
        publisher.release.notify_one();
        wait_until(|| !scope.fixture.runs.has_owner(&run)).await;
    }
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
        // createProvider waits for approval by default; input it can judge alone never does.
        let run = scope
            .start(&scope.owner, create_provider(&arguments, "error.code"))
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
// Operation it cannot perform, after the newer Coral updated at boot.
#[tokio::test]
async fn an_operation_this_version_does_not_perform_fails_before_approval() {
    let scope = AdminScope::new("user:unknown-operation").await;
    let newer_catalog = coral_catalog::BuiltinCatalog::with_unknown_operations([(
        "listThings",
        coral_catalog::OperationHints::default(),
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
                    if (!(error instanceof coral.OperationError)) throw error;
                    return error.code;
                }
            ",
            ),
        )
        .await;

    assert_final_code(
        &scope.terminal(&scope.owner, &run).await,
        "operation_unavailable",
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
#[expect(
    clippy::too_many_lines,
    reason = "one table checks each sign-in method's configuration and the request it records"
)]
async fn the_program_configures_how_a_person_signs_in() {
    let scope = AdminScope::new("user:sign-in-configuration").await;
    scope.set_access(OperationAccessState::Enabled).await;
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
    for (index, (route, choices, needs_sign_in)) in [
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
            .start(&scope.owner, create_provider(&arguments, "error.code"))
            .await;
        let value = final_value(&scope.terminal(&scope.owner, &run).await);
        if !needs_sign_in {
            assert_eq!(value, serde_json::json!("invalid_input"), "{arguments}");
            continue;
        }
        assert_eq!(
            value.pointer("/data/status"),
            Some(&serde_json::json!("needs_sign_in")),
            "{arguments}: {value}"
        );
        // Reef opens the account question from the route the Setup holds.
        let setup = value
            .pointer("/data/url")
            .and_then(serde_json::Value::as_str)
            .and_then(|link| link.split_once("?setup="))
            .and_then(|(_page, setup)| ProviderSetupId::parse(setup).ok())
            .unwrap_or_else(|| panic!("{arguments}: a link to one Setup: {value}"));
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
    scope.set_access(OperationAccessState::Enabled).await;
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
                create_provider(&arguments, "{ code: error.code, message: error.message }"),
            )
            .await;
        assert_eq!(
            final_value(&scope.terminal(&scope.owner, &run).await),
            serde_json::json!({
                "code": "invalid_input",
                "message": format!("Operation arguments are invalid{message}"),
            }),
            "{arguments}"
        );
        scope
            .assert_denial_changed_nothing(&before, &arguments.to_string())
            .await;
    }
}

// Catches: a Built-in Operation's reach that ignores the Workspace action it
// declares, or a host that performs listProviders only for a User.
#[tokio::test]
async fn members_and_agents_list_providers_but_cannot_reach_create_provider() {
    let scope = AdminScope::new("user:admin-owner").await;
    scope.set_access(OperationAccessState::Enabled).await;
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
    for principal in [AuthenticatedPrincipal::user(member), agent] {
        let run = scope
            .start(&principal, create_provider(&serde_json::json!({ "namespace": "other", "source": document_source(), "credentialRoute": "none" }), "error.code"))
            .await;
        assert_final_code(
            &scope.terminal(&principal, &run).await,
            "operation_not_found",
        );

        let run = scope.start(&principal, list_providers("{}")).await;
        let listed = final_value(&scope.terminal(&principal, &run).await);
        let namespaces = listed
            .pointer("/data/providers")
            .and_then(serde_json::Value::as_array)
            .unwrap_or_else(|| panic!("{principal:?} lists providers: {listed}"))
            .iter()
            .map(|provider| provider.get("namespace").cloned())
            .collect::<Vec<_>>();
        assert_eq!(
            namespaces,
            [
                Some(serde_json::Value::Null),
                Some(serde_json::json!("weather"))
            ],
            "{principal:?}"
        );
        assert_eq!(
            listed.pointer("/data/setups"),
            Some(&serde_json::json!([])),
            "{principal:?}"
        );
    }
    assert_eq!(scope.lifecycle_events().await, before);
}

#[tokio::test]
async fn the_source_fetch_uses_the_bootstrap_destination_policy() {
    let server = private_document_server().await;
    let source = url_source(&server);
    // The default deployment policy rejects the loopback test server.
    let scope = AdminScope::new("user:outbound-policy").await;
    scope.set_access(OperationAccessState::Enabled).await;
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
    )
    .expect("start the real Program runtime")
    .with_human_interface(test_reef());
    let run = production
        .start(
            &scope.owner,
            &scope.workspace_id,
            create_provider(&serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "fixed" }), "error.code"),
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
        .start(&scope.owner, create_provider(&serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "fixed" }), "error.code"))
        .await;
    assert_eq!(
        scope.terminal(&scope.owner, &run).await.state(),
        RunState::Succeeded
    );
    assert_eq!(fetches(&server).await, 1);
    assert_eq!(scope.setups_created_since(&before).await.len(), 1);
}

/// A Program that calls `listProviders` with `arguments`, the JavaScript
/// argument list, and returns its Operation Result or the failure it catches.
fn list_providers(arguments: &str) -> RunSubmission {
    submission(
        &"
        try {
            return await coral.workspace.listProviders(ARGUMENTS);
        } catch (error) {
            if (!(error instanceof coral.OperationError)) throw error;
            return { code: error.code, message: error.message };
        }
    "
        .replace("ARGUMENTS", arguments),
    )
}

// Catches: a result that misleads a Program about taken namespaces, such as
// the Built-in Provider under a namespace an Owner can take or a disabled
// Provider not marked disabled, a read that waits for approval, or an
// argument the call silently ignores.
#[tokio::test]
async fn list_providers_lists_every_provider_without_approval() {
    let scope = AdminScope::new("user:list-providers").await;
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
    let builtin_operations = scope.builtin_operations().await;
    let listed = serde_json::json!({
        "data": {
            "providers": [
                {
                    "namespace": null,
                    "displayName": "Coral",
                    "state": "active",
                    "credentialRoute": "none",
                    "authentication": "none",
                    "operations": builtin_operations,
                    "accountRequired": false,
                },
                {
                    "namespace": "weather",
                    "displayName": "Weather",
                    "state": "disabled",
                    "credentialRoute": "none",
                    "authentication": "none",
                    "operations": 1,
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
                "message": "Operation arguments are invalid at `namespace`: unknown field; \
                            pass an empty object",
            }),
        ),
        (
            "[]",
            serde_json::json!({
                "code": "invalid_input",
                "message": "Operation arguments are invalid: pass an empty object",
            }),
        ),
    ] {
        let run = scope.start(&scope.owner, list_providers(arguments)).await;
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
