use super::*;
use coral_credentials::{CredentialField, CredentialValues, SecretReference};
use coral_provider::account::{AccountModule, AccountScope};
use coral_test_support::InMemorySecretStore;
use coral_workspace::OrganizationRole;

const PATH: &str = "team/weather";

struct StoreBacked {
    server: MockServer,
    fixture: TestModules,
    tool: ToolFixture,
    store: Arc<InMemorySecretStore>,
    runs: RunModule,
    _directory: tempfile::TempDir,
    accounts: AccountModule,
}

/// A `fixed` weather Provider whose one field the store holds at `PATH`.
async fn store_backed(engine: Arc<dyn ProgramEngine>) -> StoreBacked {
    let server = MockServer::start().await;
    let fixture = TestModules::new(engine).await;
    let mut tool = fixture
        .user_tool_at("user:store-backed", "Store backed", &server.uri())
        .await;
    let (directory, accounts) = Box::pin(configure_shared_account(
        &fixture,
        &mut tool,
        &server.uri(),
        "typed-canary",
    ))
    .await;
    let store = Arc::new(InMemorySecretStore::new());
    store.set(PATH, "token", "stored-canary");
    let accounts = accounts.with_secret_stores(coral_test_support::secret_stores([(
        "team",
        Arc::clone(&store) as _,
    )]));
    let governor = coral_test_support::provision_user(
        fixture.database.clone(),
        "user:governor",
        OrganizationRole::Owner,
    )
    .await;
    fixture
        .workspaces
        .grant_secret_store(&change(governor, 40, '4'), &tool.workspace_id, "team")
        .await
        .unwrap();
    let context = change(tool.user.clone(), 41, '4');
    let account = accounts
        .get_account(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    let reconnect = accounts
        .start_reconnect(
            &context,
            &tool.workspace_id,
            &tool.provider_id,
            account,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_fields(
            &context,
            &tool.workspace_id,
            reconnect.id,
            std::collections::BTreeMap::from([(
                CredentialValues::TOKEN.to_owned(),
                CredentialField::Reference(
                    SecretReference::new("team".into(), PATH.into(), Some("token".into())).unwrap(),
                ),
            )]),
        )
        .await
        .unwrap();
    let runs = fixture.live_runs(None).with_accounts(accounts.clone());
    StoreBacked {
        server,
        fixture,
        tool,
        store,
        runs,
        _directory: directory,
        accounts,
    }
}

impl StoreBacked {
    async fn expect_bearer(&self, token: &str) {
        Mock::given(method("GET"))
            .and(path("/forecast"))
            .and(wiremock::matchers::header(
                "authorization",
                format!("Bearer {token}").as_str(),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"temperature":18})),
            )
            .expect(1)
            .mount(&self.server)
            .await;
    }

    /// Runs one Program to its end and returns the Run.
    async fn run(&self, program: &str) -> crate::RunUpdate {
        let started = self
            .runs
            .start(
                &self.tool.principal,
                &self.tool.workspace_id,
                submission(program),
            )
            .await
            .unwrap();
        wait_for_terminal(
            &self.runs,
            &self.tool.principal,
            &self.tool.workspace_id,
            started.run_id(),
        )
        .await
    }
}

const CAUGHT: &str = "try { await coral.providers.weather.getForecast({}); return 'sent'; } \
                      catch (error) { return [error.code, error.message]; }";

fn caught(run: &crate::RunUpdate) -> (String, String) {
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = run.terminal_outcome()
    else {
        panic!("the Program catches the failure: {run:?}");
    };
    let [code, message] = value.as_json().as_array().unwrap().as_slice() else {
        panic!("{value:?}");
    };
    (
        code.as_str().unwrap().to_owned(),
        message.as_str().unwrap().to_owned(),
    )
}

// Catches: a value Coral keeps, or one read once, so rotation has no effect.
#[tokio::test]
async fn an_invocation_sends_the_stores_current_value_and_a_rotated_one_next() {
    let backed = store_backed(Arc::new(ImmediateEngine::default())).await;
    backed.expect_bearer("stored-canary").await;
    let first = backed
        .run("return await coral.providers.weather.getForecast({});")
        .await;
    assert_eq!(first.state(), RunState::Succeeded);
    assert!(!format!("{first:?}").contains("stored-canary"));

    backed.store.set(PATH, "token", "rotated-canary");
    backed.expect_bearer("rotated-canary").await;
    let second = backed
        .run("return await coral.providers.weather.getForecast({});")
        .await;
    assert_eq!(second.state(), RunState::Succeeded);
}

// Catches: the read during preparation, which runs before enforcement decides,
// reaching the store for an Invocation enforcement then denies.
#[tokio::test]
async fn an_invocation_enforcement_denies_never_reads_the_store() {
    let mut backed = store_backed(Arc::new(ImmediateEngine::default())).await;
    let (gate, release) = EnforcementGate::blocking();
    backed.runs.invocation_observer = gate.clone();
    let reads = backed.store.reads();
    let started = backed
        .runs
        .start(
            &backed.tool.principal,
            &backed.tool.workspace_id,
            submission(CAUGHT),
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    backed
        .fixture
        .policies
        .set_access_state(
            &change(backed.tool.user.clone(), 50, '5'),
            backed.tool.policy_target.clone(),
            coral_policy::ToolAccessState::Disabled,
        )
        .await
        .unwrap();
    release.send(()).unwrap();
    let run = wait_for_terminal(
        &backed.runs,
        &backed.tool.principal,
        &backed.tool.workspace_id,
        started.run_id(),
    )
    .await;

    assert_eq!(caught(&run).0, "tool_disabled");
    assert_eq!(
        backed.store.reads(),
        reads,
        "a denied Invocation reads nothing"
    );
    assert!(backed.server.received_requests().await.unwrap().is_empty());
}

// Catches: a failure that names the path, falls back, or sends the call anyway.
#[tokio::test]
async fn a_failed_read_ends_the_invocation_without_naming_the_path() {
    let backed = store_backed(Arc::new(ImmediateEngine::default())).await;
    for (failure, code) in [
        (
            coral_secret_store::Error::Unreachable,
            "secret_store_unreachable",
        ),
        (
            coral_secret_store::Error::EntryNotFound,
            "secret_entry_not_found",
        ),
        (
            coral_secret_store::Error::KeyNotFound,
            "secret_key_not_found",
        ),
        (
            coral_secret_store::Error::PermissionDenied,
            "secret_store_permission_denied",
        ),
        (
            coral_secret_store::Error::MalformedResponse,
            "secret_store_malformed_response",
        ),
    ] {
        backed.store.fail_with(Some(failure));
        let run = backed.run(CAUGHT).await;
        let (seen, message) = caught(&run);
        assert_eq!(seen, code);
        assert!(!format!("{message} {run:?}").contains(PATH), "{message}");
    }
    assert!(backed.server.received_requests().await.unwrap().is_empty());
}

// Catches: pending work that goes on to read a reference nobody approved, after an
// Owner changes it during the Invocation.
#[tokio::test]
async fn changing_a_reference_stops_a_pending_invocation() {
    let mut backed = store_backed(Arc::new(ImmediateEngine::default())).await;
    backed.store.set("team/other", "token", "other-canary");
    let (gate, release) = EnforcementGate::blocking();
    backed.runs.invocation_observer = gate.clone();
    let started = backed
        .runs
        .start(
            &backed.tool.principal,
            &backed.tool.workspace_id,
            submission(CAUGHT),
        )
        .await
        .unwrap();
    wait_until(|| gate.entered()).await;
    let context = change(backed.tool.user.clone(), 50, '5');
    let accounts = &backed.accounts;
    let account = accounts
        .get_account(
            &context,
            &backed.tool.workspace_id,
            &backed.tool.provider_id,
            &AccountScope::Shared,
        )
        .await
        .unwrap()
        .account
        .unwrap()
        .id;
    let reconnect = accounts
        .start_reconnect(
            &context,
            &backed.tool.workspace_id,
            &backed.tool.provider_id,
            account,
            ManagementRequestId::parse(&uuid::Uuid::new_v4().to_string()).unwrap(),
            None,
        )
        .await
        .unwrap();
    accounts
        .submit_fields(
            &context,
            &backed.tool.workspace_id,
            reconnect.id,
            std::collections::BTreeMap::from([(
                CredentialValues::TOKEN.to_owned(),
                CredentialField::Reference(
                    SecretReference::new("team".into(), "team/other".into(), Some("token".into()))
                        .unwrap(),
                ),
            )]),
        )
        .await
        .unwrap();
    let reads = backed.store.reads();
    release.send(()).unwrap();
    let run = wait_for_terminal(
        &backed.runs,
        &backed.tool.principal,
        &backed.tool.workspace_id,
        started.run_id(),
    )
    .await;

    assert_eq!(caught(&run).0, "account_required");
    assert_eq!(
        backed.store.reads(),
        reads,
        "the stopped call reads nothing"
    );
    assert!(backed.server.received_requests().await.unwrap().is_empty());
}
