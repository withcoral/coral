//! `coral.workspace.updateProvider` changes an Owner-created Provider through
//! an Update Setup, as Reef's update page does. The call returns Reef's update
//! page, where a person reviews and publishes the change. A rename alone keeps
//! the Generation, as Reef's settings page does.
use super::provider_setup::{
    AdminScope, DOCUMENT, HeldDocument, PRIVATE_DOCUMENT, document_server, document_source,
    fetches, final_value, url_source, workflow, workspace_call,
};
use super::*;

/// A Program that calls `updateProvider` with exactly `arguments` and returns
/// its Tool Result, or the failure it catches.
fn update_provider(arguments: &serde_json::Value) -> RunSubmission {
    workspace_call("updateProvider", &arguments.to_string())
}

/// Runs a Built-in Tool and returns its result, or the failure it catches.
async fn call(scope: &AdminScope, tool: &str, arguments: &serde_json::Value) -> serde_json::Value {
    scope
        .outcome(workspace_call(tool, &arguments.to_string()))
        .await
}

/// What `getProvider` returns for `namespace`.
async fn saved(scope: &AdminScope, namespace: &str) -> serde_json::Value {
    call(
        scope,
        "getProvider",
        &serde_json::json!({ "namespace": namespace }),
    )
    .await
}

/// The Publications completed since `before` was read.
async fn publications_since(
    scope: &AdminScope,
    before: &[coral_provider::AdministrativeLifecycleEvent],
) -> Vec<coral_provider::AdministrativeLifecycleEvent> {
    scope
        .lifecycle_events()
        .await
        .into_iter()
        .filter(|event| {
            !before.contains(event)
                && event.action() == coral_provider::LifecycleAction::PublicationCompleted
        })
        .collect()
}

// Catches: an update that resets a field the call omits, or that loses a kept
// Tool's Call Path or Tool Access State once the person publishes it.
#[tokio::test]
async fn a_published_change_keeps_what_the_call_omits() {
    let scope = AdminScope::new("user:update-provider").await;
    scope.publish_weather().await;
    let forecast = "coral.providers.weather.getForecast";
    scope
        .set_access_to(forecast, ToolAccessState::Disabled)
        .await;
    let before = saved(&scope, "weather").await;

    let run = scope
        .start(
            &scope.owner,
            update_provider(&serde_json::json!({
                "namespace": "weather",
                "displayName": "Forecasts",
                "requestHeaders": { "x-trace": "on" },
            })),
        )
        .await;
    scope.approve(&scope.owner, &run).await;
    let result = final_value(&scope.terminal(&scope.owner, &run).await);
    scope.person_publishes_update(&result).await;

    let mut expected = before;
    *expected.pointer_mut("/data/displayName").unwrap() = serde_json::json!("Forecasts");
    *expected.pointer_mut("/data/requestHeaders").unwrap() = serde_json::json!({ "x-trace": "on" });
    assert_eq!(saved(&scope, "weather").await, expected);
    // The kept Tool keeps its Call Path, so it resolves, and its policy.
    let decision = scope
        .fixture
        .policies
        .decision(&scope.owner_user(), &scope.policy_target(forecast).await)
        .await
        .unwrap();
    assert_eq!(decision.access_state(), ToolAccessState::Disabled);
}

/// The Weather document with a second Tool, `getAlerts`.
const WITH_ALERTS: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"2"},"security":[],"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast","responses":{"200":{"description":"OK"}}}},"/alerts":{"get":{"operationId":"getAlerts","responses":{"200":{"description":"OK"}}}}}}"#;
/// [`WITH_ALERTS`] with no responses for `getForecast`, which cannot run then.
const BROKEN_FORECAST: &str = r#"{"openapi":"3.1.0","info":{"title":"Weather","version":"2"},"security":[],"servers":[{"url":"https://weather.example"}],"paths":{"/forecast":{"get":{"operationId":"getForecast"}},"/alerts":{"get":{"operationId":"getAlerts","responses":{"200":{"description":"OK"}}}}}}"#;

fn openapi_document(document: &str) -> serde_json::Value {
    serde_json::json!({ "kind": "openapi_document", "document": document })
}

/// An Owner whose Programs call addProvider and updateProvider without
/// approval, so each test reads the call's own outcome.
async fn unattended(user: &str) -> AdminScope {
    let scope = AdminScope::new(user).await;
    for tool in ["addProvider", "updateProvider"] {
        scope
            .set_access_to(&format!("coral.workspace.{tool}"), ToolAccessState::Enabled)
            .await;
    }
    scope
}

/// Creates the credential-free `weather` Provider from `source`: a Program
/// adds it and a person publishes it.
async fn create_weather(scope: &AdminScope, source: serde_json::Value) -> coral_provider::Provider {
    let created = call(
        scope,
        "addProvider",
        &serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "none" }),
    )
    .await;
    scope.person_publishes(&created).await;
    scope.weather().await.expect("the weather Provider")
}

// Catches: a call that changes only the display name opening an Update Setup
// or publishing a new Generation, where Reef's settings page renames in place.
#[tokio::test]
async fn a_rename_alone_keeps_the_generation() {
    let scope = unattended("user:update-rename").await;
    create_weather(&scope, document_source()).await;
    let before_setups = scope.created_setups().await;
    let before = scope.lifecycle_events().await;
    let mut expected = saved(&scope, "weather").await;

    // An Update Setup trims the name it confirms, so a rename does too.
    let result = call(
        &scope,
        "updateProvider",
        &serde_json::json!({ "namespace": "weather", "displayName": " Forecasts " }),
    )
    .await;

    assert_eq!(
        result.pointer("/data/status"),
        Some(&serde_json::json!("published")),
        "{result}"
    );
    assert_eq!(
        result.pointer("/data/displayName"),
        Some(&serde_json::json!("Forecasts"))
    );
    *expected.pointer_mut("/data/displayName").unwrap() = serde_json::json!("Forecasts");
    assert_eq!(saved(&scope, "weather").await, expected);
    assert!(scope.setups_created_since(&before_setups).await.is_empty());
    assert!(publications_since(&scope, &before).await.is_empty());
}

// Catches: a refresh that reads a URL other than the one saved when the call
// runs.
#[tokio::test]
async fn refresh_reads_the_source_url_saved_when_the_call_runs() {
    let scope = unattended("user:update-refresh").await;
    let first = document_server(DOCUMENT).await;
    let second = document_server(WITH_ALERTS).await;
    create_weather(&scope, url_source(&first)).await;
    let replaced = call(
        &scope,
        "updateProvider",
        &serde_json::json!({ "namespace": "weather", "source": url_source(&second) }),
    )
    .await;
    scope.person_publishes_update(&replaced).await;
    assert_eq!(
        saved(&scope, "weather").await.pointer("/data/source"),
        Some(&url_source(&second))
    );

    let (first_before, second_before) = (fetches(&first).await, fetches(&second).await);
    let refreshed = call(
        &scope,
        "updateProvider",
        &serde_json::json!({ "namespace": "weather", "refresh": true }),
    )
    .await;
    scope.person_publishes_update(&refreshed).await;
    assert_eq!(
        (fetches(&first).await, fetches(&second).await),
        (first_before, second_before + 1),
        "the refresh reads the saved URL, and only it"
    );
}

/// How a test arranges the `weather` Provider before the call.
#[derive(Debug, Clone, Copy)]
enum Saved {
    /// Created from the credential-free Weather document.
    Document,
    /// Created from the document with `getAlerts`.
    WithAlerts,
    /// Published by the Owner from the document that takes a bearer token,
    /// which each caller gives. A Program cannot publish a Provider that
    /// needs a sign-in.
    PerCaller,
}

// Catches: Coral publishing a change no person reviewed, even one that keeps
// every Tool and the route, or a link that confirms the change for the person,
// opens another task than Reef starts for the same change, or loses the route
// the Program asked for.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table checks the page each change opens"
)]
async fn every_change_returns_the_update_page() {
    let confidential = serde_json::json!({
        "method": "oauth_authorization_code",
        "authorization_url": "https://weather.example/authorize",
        "token_url": "https://weather.example/token",
        "scopes": ["read"],
        "client": { "client_id": "weather-app", "authentication": "client_secret_post" },
    });
    // Each link opens Reef's update page on `task`, followed by `then`.
    for (arranged, changes, task, then) in [
        // A change that keeps the route, the contract and every Tool.
        (
            Saved::Document,
            serde_json::json!({ "requestHeaders": { "x-trace": "on" } }),
            "configuration",
            "",
        ),
        // A source that only adds a Tool.
        (
            Saved::Document,
            serde_json::json!({ "source": openapi_document(WITH_ALERTS) }),
            "refresh",
            "",
        ),
        // The route the Provider has.
        (
            Saved::Document,
            serde_json::json!({ "credentialRoute": "none" }),
            "accounts",
            "&account=none",
        ),
        (
            Saved::WithAlerts,
            serde_json::json!({ "source": document_source() }),
            "refresh",
            "",
        ),
        // A source that keeps a Tool Programs call now, but breaks it.
        (
            Saved::Document,
            serde_json::json!({ "source": openapi_document(BROKEN_FORECAST) }),
            "refresh",
            "",
        ),
        // A way to sign in, on a route that sends no credential.
        (
            Saved::Document,
            serde_json::json!({ "authentication": { "method": "bearer" } }),
            "configuration",
            "",
        ),
        // A new read that breaks access opens on the configuration it keeps.
        (
            Saved::Document,
            serde_json::json!({
                "source": openapi_document(PRIVATE_DOCUMENT),
                "authentication": { "method": "bearer" },
                "credentialRoute": "fixed",
            }),
            "refresh",
            "&configure=1&account=fixed",
        ),
        // Only the person enters the application's secret.
        (
            Saved::Document,
            serde_json::json!({ "authentication": confidential, "credentialRoute": "caller" }),
            "configuration",
            "&account=caller",
        ),
        // A shared account needs a person to connect it.
        (
            Saved::PerCaller,
            serde_json::json!({ "credentialRoute": "fixed" }),
            "accounts",
            "&account=fixed",
        ),
    ] {
        let scope = unattended("user:update-needs-person").await;
        let provider = match arranged {
            Saved::Document => create_weather(&scope, document_source()).await,
            Saved::WithAlerts => create_weather(&scope, openapi_document(WITH_ALERTS)).await,
            Saved::PerCaller => scope
                .fixture
                .providers
                .publish_openapi_fixture(
                    &change(scope.owner_user(), 30, '6'),
                    &scope.workspace_id,
                    OpenApiDocument::parse(PRIVATE_DOCUMENT).unwrap(),
                    "weather",
                    CredentialRoute::Caller,
                )
                .await
                .unwrap(),
        };
        let before_setups = scope.created_setups().await;
        let before = scope.lifecycle_events().await;
        let saved_before = saved(&scope, "weather").await;
        let mut arguments = serde_json::json!({ "namespace": "weather" });
        arguments
            .as_object_mut()
            .unwrap()
            .extend(changes.as_object().unwrap().clone());

        let result = call(&scope, "updateProvider", &arguments).await;

        let created = scope.setups_created_since(&before_setups).await;
        let [setup] = created.as_slice() else {
            panic!("{changes}: one Update Setup: {created:?}")
        };
        // The link never confirms an incompatible configuration for the person.
        let url = format!(
            "{TEST_REEF}/workspaces/{}/providers/{}/update?setup={setup}&task={task}&etag={}{then}",
            scope.workspace_id,
            provider.id(),
            provider.etag(),
        );
        assert_eq!(
            result,
            serde_json::json!({
                "data": { "status": "needs_person", "url": url, "namespace": "weather" },
                "metadata": { "kind": "builtin" },
            }),
            "{changes}"
        );
        assert!(
            publications_since(&scope, &before).await.is_empty(),
            "{changes}: nothing is published"
        );
        assert_eq!(saved(&scope, "weather").await, saved_before, "{changes}");
        // The person finishes the Setup that Coral configured.
        assert!(
            scope
                .fixture
                .providers
                .get_setup(&scope.owner_user(), &scope.workspace_id, setup)
                .await
                .unwrap()
                .state()
                .is_open(),
            "{changes}"
        );
    }
}

// Catches: a rejected update that leaves a Setup open or changes the
// Provider, or one rejected with a code the Program cannot act on.
#[tokio::test]
async fn each_rejection_names_its_cause_and_changes_nothing() {
    let scope = unattended("user:update-rejected").await;
    create_weather(&scope, document_source()).await;
    for (arguments, expected) in [
        (
            serde_json::json!({ "namespace": "forecasts", "displayName": "Forecasts" }),
            serde_json::json!({ "code": "provider_not_found", "message": "Provider was not found" }),
        ),
        (
            serde_json::json!({ "namespace": "weather", "refresh": true }),
            serde_json::json!({
                "code": "invalid_input",
                "message": "Tool arguments are invalid at `refresh`: the Provider was read \
                            from an uploaded document, which Coral cannot read again; send \
                            `source` with the new document instead",
            }),
        ),
        (
            // The route must suit the source, as addProvider's must.
            serde_json::json!({ "namespace": "weather", "credentialRoute": "caller" }),
            serde_json::json!({
                "code": "invalid_input",
                "message": "Tool arguments are invalid at `credentialRoute`: the source \
                            declares no credential; use `none`, or give `authentication` to say \
                            how a person signs in",
            }),
        ),
        (
            // A configuration the source cannot take fails once Coral reads it.
            serde_json::json!({
                "namespace": "weather",
                "servers": { "#/paths/~1missing/servers": { "kind": "custom", "url": "https://weather.example" } },
            }),
            serde_json::json!({ "code": "invalid_input" }),
        ),
        (
            // A rename takes the name rule Reef's settings page does.
            serde_json::json!({ "namespace": "weather", "displayName": " " }),
            serde_json::json!({
                "code": "invalid_input",
                "message": "Tool arguments are invalid at `displayName`: Provider display \
                            name must be non-blank, contain no control or format characters, \
                            and be at most 256 bytes",
            }),
        ),
    ] {
        let before = scope.lifecycle_events().await;
        let saved_before = saved(&scope, "weather").await;
        let mut result = call(&scope, "updateProvider", &arguments).await;
        if expected.get("message").is_none() {
            result.as_object_mut().unwrap().remove("message");
        }
        assert_eq!(result, expected, "{arguments}");
        scope
            .assert_denial_changed_nothing(&before, &arguments.to_string())
            .await;
        assert_eq!(saved(&scope, "weather").await, saved_before, "{arguments}");
    }
}

/// Renames the Provider first, as an Owner racing the call would, then forwards
/// the rename under the etag that race made stale.
struct RenamedFirst(coral_administration::ProviderWorkflow);

#[async_trait]
impl coral_provider::ProviderRenamer for RenamedFirst {
    async fn rename_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        workspace: &coral_workspace::WorkspaceId,
        provider: &coral_provider::ProviderId,
        display_name: coral_provider::ProviderDisplayName,
    ) -> Result<coral_provider::Provider, coral_provider::Error> {
        self.0
            .rename_provider(
                context,
                workspace,
                provider,
                coral_provider::ProviderDisplayName::parse("Raced").unwrap(),
            )
            .await?;
        self.0
            .rename_provider(context, workspace, provider, display_name)
            .await
    }
}

// Catches: a rename an Owner raced ending the Run instead of failing the call
// with a code the Program can catch.
#[tokio::test]
async fn a_rename_an_owner_raced_is_a_failure_the_program_catches() {
    let mut scope = unattended("user:update-rename-raced").await;
    create_weather(&scope, document_source()).await;
    scope.fixture.runs = scope
        .fixture
        .runs
        .clone()
        .with_provider_renamer(Arc::new(RenamedFirst(workflow(&scope.fixture))));

    let mut result = call(
        &scope,
        "updateProvider",
        &serde_json::json!({ "namespace": "weather", "displayName": "Forecasts" }),
    )
    .await;

    result.as_object_mut().unwrap().remove("message");
    assert_eq!(
        result,
        serde_json::json!({ "code": "provider_publication_failed" })
    );
    assert_eq!(
        saved(&scope, "weather").await.pointer("/data/displayName"),
        Some(&serde_json::json!("Raced"))
    );
}

/// Renames through the real workflow, but stalls first until released, as a
/// worker that dies there would.
struct StallingRenamer {
    workflow: coral_administration::ProviderWorkflow,
    entered: AtomicBool,
    release: tokio::sync::Notify,
}

#[async_trait]
impl coral_provider::ProviderRenamer for StallingRenamer {
    async fn rename_provider(
        &self,
        context: &coral_workspace::ChangeContext,
        workspace: &coral_workspace::WorkspaceId,
        provider: &coral_provider::ProviderId,
        display_name: coral_provider::ProviderDisplayName,
    ) -> Result<coral_provider::Provider, coral_provider::Error> {
        self.entered.store(true, Ordering::SeqCst);
        self.release.notified().await;
        self.workflow
            .rename_provider(context, workspace, provider, display_name)
            .await
    }
}

/// Expires the worker claim on `run`, sweeps it as a lost worker, and returns
/// how its one Invocation ended.
async fn lose_worker(scope: &AdminScope, run: &crate::RunId) -> crate::InvocationResultKind {
    let accepted_at = retained_events(&scope.fixture.runs, &scope.owner, &scope.workspace_id, run)
        .await
        .first()
        .expect("run.started")
        .occurred_at_unix_nanos();
    scope
        .fixture
        .expire_worker_claim(&scope.workspace_id, run, accepted_at)
        .await;
    scope
        .fixture
        .clock
        .next
        .store(accepted_at + 2_000_000_000, Ordering::SeqCst);
    scope
        .fixture
        .next_process()
        .sweep_expired_leases()
        .await
        .expect("sweep the lost worker's lapsed lease");
    wait_for_terminal(&scope.fixture.runs, &scope.owner, &scope.workspace_id, run).await;
    let ended: Vec<_> =
        retained_events(&scope.fixture.runs, &scope.owner, &scope.workspace_id, run)
            .await
            .iter()
            .filter_map(|event| match event.kind() {
                RunEventKind::InvocationEnded { result_kind, .. } => Some(*result_kind),
                _ => None,
            })
            .collect();
    let [ended] = ended.as_slice() else {
        panic!("one ended Invocation: {ended:?}")
    };
    *ended
}

// Catches: a worker lost while Coral prepares a Setup leaving the call
// indeterminate, though it publishes nothing.
#[tokio::test]
async fn a_lost_setup_call_settles_as_the_run_ending_first() {
    for tool in ["addProvider", "updateProvider"] {
        let scope = unattended("user:lost-setup").await;
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
        let arguments = if tool == "addProvider" {
            serde_json::json!({
                "namespace": "weather",
                "source": url_source(&server),
                "credentialRoute": "none",
            })
        } else {
            scope.publish_weather().await;
            serde_json::json!({ "namespace": "weather", "source": url_source(&server) })
        };
        let run = scope
            .start(&scope.owner, workspace_call(tool, &arguments.to_string()))
            .await;
        entered.notified().await;

        let ended = lose_worker(&scope, &run).await;

        assert_eq!(ended, crate::InvocationResultKind::RunTerminated, "{tool}");
        release.send(()).unwrap();
        wait_until(|| !scope.fixture.runs.has_owner(&run)).await;
    }
}

// Catches: a lost rename, which nothing records, claiming the Run ended first.
#[tokio::test]
async fn a_lost_rename_is_indeterminate() {
    let mut scope = unattended("user:lost-rename").await;
    scope.publish_weather().await;
    let renamer = Arc::new(StallingRenamer {
        workflow: workflow(&scope.fixture),
        entered: AtomicBool::new(false),
        release: tokio::sync::Notify::new(),
    });
    scope.fixture.runs = scope
        .fixture
        .runs
        .clone()
        .with_provider_renamer(renamer.clone());
    let run = scope
        .start(
            &scope.owner,
            update_provider(
                &serde_json::json!({ "namespace": "weather", "displayName": "Forecasts" }),
            ),
        )
        .await;
    wait_until(|| renamer.entered.load(Ordering::SeqCst)).await;

    let ended = lose_worker(&scope, &run).await;

    assert_eq!(ended, crate::InvocationResultKind::Indeterminate);
    renamer.release.notify_one();
    wait_until(|| !scope.fixture.runs.has_owner(&run)).await;
}
