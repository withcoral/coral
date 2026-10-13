//! `coral.admin.createProvider` prepares a Provider Setup and returns the Reef
//! page where a person finishes it. The call never waits for that person.
use super::*;
use coral_provider::{ProviderSetupId, ProviderSetupState};

const DOCUMENT: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"1"},"security":[],"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"OK"}}}}}}"#;

/// A Program that calls `createProvider` and returns its Operation Result, or
/// the code of a caught Coral Failure.
fn create_provider(namespace: &str, source: &serde_json::Value) -> RunSubmission {
    submission(
        &"
        try {
            return await coral.admin.createProvider({ namespace: NAMESPACE, source: SOURCE });
        } catch (error) {
            if (!(error instanceof coral.OperationError)) throw error;
            return error.code;
        }
    "
        .replace("NAMESPACE", &serde_json::json!(namespace).to_string())
        .replace("SOURCE", &source.to_string()),
    )
}

fn document_source() -> serde_json::Value {
    serde_json::json!({ "kind": "openapi_document", "document": DOCUMENT })
}

/// A Workspace whose Owner installed the Built-in Provider, over the real V8 engine.
struct AdminScope {
    fixture: TestModules,
    upstream: Arc<ScriptedUpstream>,
    workspace_id: coral_workspace::WorkspaceId,
    owner: AuthenticatedPrincipal,
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
        let catalog = coral_provider::prepare_builtin_catalog().unwrap();
        let mut transaction = fixture.database.begin().await.unwrap();
        fixture
            .providers
            .install_builtin_in_transaction(
                &mut transaction,
                &change(owner.authenticated_user().unwrap().clone(), 11, '2'),
                &workspace_id,
                catalog,
            )
            .await
            .expect("install the Built-in Provider");
        transaction.commit().await.unwrap();
        Self {
            fixture,
            upstream,
            workspace_id,
            owner,
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
            "coral.admin.createProvider",
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
}

/// An OpenAPI document served at `/openapi.json`.
async fn document_server() -> MockServer {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/openapi.json"))
        .respond_with(ResponseTemplate::new(200).set_body_string(DOCUMENT))
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
        let server = document_server().await;
        let source = url_source(&server);
        let before = scope.created_setups().await;
        let run = scope
            .start(&scope.owner, create_provider("weather", &source))
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
        assert_eq!(
            final_value(&terminal),
            serde_json::json!({
                "data": {
                    "status": "needs_sign_in",
                    "url": format!(
                        "{TEST_REEF}/workspaces/{}/providers/new?setup={setup}&namespace=weather",
                        scope.workspace_id,
                    ),
                    "namespace": "weather",
                },
                "metadata": { "kind": "builtin" },
            }),
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
            ProviderSetupState::AwaitingConfiguration,
            "{access:?}"
        );
        assert_eq!(fetches(&server).await, 1, "{access:?}");
        assert_eq!(scope.upstream.sends.load(Ordering::SeqCst), 0);
    }
}

// Catches: input Coral can judge on its own waiting for an Owner's approval,
// or reaching a fetch or a Setup, before it fails.
#[tokio::test]
async fn invalid_input_fails_before_approval_fetch_or_setup() {
    let scope = AdminScope::new("user:invalid-input").await;
    let server = document_server().await;
    let fetchable = url_source(&server);
    let before = scope.lifecycle_events().await.len();
    for (namespace, source) in [
        ("not-an-identifier", fetchable.clone()),
        ("", fetchable.clone()),
        ("coral.providers.weather", fetchable.clone()),
        (
            "weather",
            serde_json::json!({ "kind": "openapi_url", "url": "not a url" }),
        ),
        (
            "weather",
            serde_json::json!({ "kind": "openapi_url", "url": "ftp://weather.example/openapi.json" }),
        ),
        (
            "weather",
            serde_json::json!({ "kind": "mcp_url", "url": "file:///etc/passwd" }),
        ),
        // A Setup takes no credentials or fragment in its source URL.
        (
            "weather",
            serde_json::json!({ "kind": "openapi_url", "url": "https://user:secret@weather.example/openapi.json" }),
        ),
        (
            "weather",
            serde_json::json!({ "kind": "mcp_url", "url": "https://weather.example/mcp#tools" }),
        ),
    ] {
        // createProvider waits for approval by default; input it can judge alone never does.
        let run = scope
            .start(&scope.owner, create_provider(namespace, &source))
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
            "{namespace} {source}: no approval request"
        );
    }
    assert_eq!(fetches(&server).await, 0);
    assert_eq!(scope.lifecycle_events().await.len(), before);
}

#[tokio::test]
async fn members_and_agents_cannot_reach_create_provider_and_change_nothing() {
    let scope = AdminScope::new("user:admin-owner").await;
    scope.set_access(OperationAccessState::Enabled).await;
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
    // Built-in Operations stay concealed from non-Owners.
    for principal in [AuthenticatedPrincipal::user(member), agent] {
        let run = scope
            .start(&principal, create_provider("weather", &document_source()))
            .await;
        assert_final_code(
            &scope.terminal(&principal, &run).await,
            "operation_not_found",
        );
    }
    assert_eq!(scope.lifecycle_events().await, before);
}

#[tokio::test]
async fn the_source_fetch_uses_the_bootstrap_destination_policy() {
    let server = document_server().await;
    let source = url_source(&server);
    // The default deployment policy rejects the loopback test server.
    let scope = AdminScope::new("user:outbound-policy").await;
    scope.set_access(OperationAccessState::Enabled).await;
    let before = scope.created_setups().await;
    let production = RunModule::new(
        DestinationPolicy::default(),
        scope.fixture.database.clone(),
        scope.fixture.workspaces.clone(),
        coral_provider::ProviderModule::new(
            DestinationPolicy::default(),
            scope.fixture.database.clone(),
            scope.fixture.workspaces.clone(),
        ),
        crate::IsolateLimits::default(),
    )
    .expect("start the real Program runtime")
    .with_human_interface(test_reef());
    let run = production
        .start(
            &scope.owner,
            &scope.workspace_id,
            create_provider("weather", &source),
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
        .start(&scope.owner, create_provider("weather", &source))
        .await;
    assert_eq!(
        scope.terminal(&scope.owner, &run).await.state(),
        RunState::Succeeded
    );
    assert_eq!(fetches(&server).await, 1);
    assert_eq!(scope.setups_created_since(&before).await.len(), 1);
}
