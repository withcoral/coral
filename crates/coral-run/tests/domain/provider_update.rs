//! `coral.workspace.updateProvider` changes an Owner-created Provider through
//! an Update Setup and a regeneration Publication, as Reef's update page does.
//! Coral publishes the change when nobody needs to act; otherwise the call
//! returns Reef's update page, where a person finishes it. A rename alone keeps
//! the Generation, as Reef's settings page does.
use super::provider_setup::{
    AdminScope, DOCUMENT, PRIVATE_DOCUMENT, SetupCanceledFirst, StallingPublisher, document_server,
    document_source, fetches, final_value, url_source, workflow, workspace_call,
};
use super::*;

/// A Program that calls `updateProvider` with exactly `arguments` and returns
/// its Operation Result, or the failure it catches.
fn update_provider(arguments: &serde_json::Value) -> RunSubmission {
    workspace_call("updateProvider", &arguments.to_string())
}

/// Runs a Built-in Operation and returns its result, or the failure it catches.
async fn call(
    scope: &AdminScope,
    operation: &str,
    arguments: &serde_json::Value,
) -> serde_json::Value {
    scope
        .outcome(workspace_call(operation, &arguments.to_string()))
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
// Operation's Call Path or Operation Access State.
#[tokio::test]
async fn coral_publishes_a_change_nobody_needs_to_act_on() {
    let scope = AdminScope::new("user:update-provider").await;
    scope.publish_weather().await;
    let forecast = "coral.providers.weather.getForecast";
    scope
        .set_access_to(forecast, OperationAccessState::Disabled)
        .await;
    let before = saved(&scope, "weather").await;
    let events = scope.lifecycle_events().await;

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

    assert_eq!(
        result,
        serde_json::json!({
            "data": {
                "status": "published",
                "namespace": "weather",
                "displayName": "Forecasts",
                "credentialRoute": "none",
                "authentication": "none",
                // The kept Operation stays disabled, so the Program cannot call it.
                "operations": 0,
                "accountRequired": false,
            },
            "metadata": { "kind": "builtin" },
        })
    );
    let mut expected = before;
    *expected.pointer_mut("/data/displayName").unwrap() = serde_json::json!("Forecasts");
    *expected.pointer_mut("/data/requestHeaders").unwrap() = serde_json::json!({ "x-trace": "on" });
    assert_eq!(saved(&scope, "weather").await, expected);
    // The Publication names the Owner whose Program asked for it.
    let published = publications_since(&scope, &events).await;
    let [published] = published.as_slice() else {
        panic!("one Publication: {published:?}")
    };
    assert!(
        matches!(
            published.actor(),
            coral_provider::LifecycleActor::User { user_id, .. }
                if user_id == scope.owner_user().user_id()
        ),
        "{published:?}"
    );
    // The kept Operation keeps its Call Path, so it resolves, and its policy.
    let decision = scope
        .fixture
        .policies
        .decision(&scope.owner_user(), &scope.policy_target(forecast).await)
        .await
        .unwrap();
    assert_eq!(decision.access_state(), OperationAccessState::Disabled);
}

/// The Weather document with a second Operation, `getAlerts`.
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
    for operation in ["addProvider", "updateProvider"] {
        scope
            .set_access_to(
                &format!("coral.workspace.{operation}"),
                OperationAccessState::Enabled,
            )
            .await;
    }
    scope
}

/// Creates the credential-free `weather` Provider from `source`.
async fn create_weather(scope: &AdminScope, source: serde_json::Value) -> coral_provider::Provider {
    let created = call(
        scope,
        "addProvider",
        &serde_json::json!({ "namespace": "weather", "source": source, "credentialRoute": "none" }),
    )
    .await;
    assert_eq!(
        created.pointer("/data/status"),
        Some(&serde_json::json!("published")),
        "{created}"
    );
    scope
        .fixture
        .providers
        .inspect_providers(&scope.owner_user(), &scope.workspace_id)
        .await
        .unwrap()
        .into_iter()
        .find(|provider| provider.call_root().as_str() == "coral.providers.weather")
        .expect("the weather Provider")
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
// runs, or a source that only adds Operations waiting for a person.
#[tokio::test]
async fn refresh_reads_the_source_url_saved_when_the_call_runs() {
    let scope = unattended("user:update-refresh").await;
    let first = document_server(DOCUMENT).await;
    let second = document_server(WITH_ALERTS).await;
    create_weather(&scope, url_source(&first)).await;

    // A new source that only adds an Operation publishes at once.
    let replaced = call(
        &scope,
        "updateProvider",
        &serde_json::json!({ "namespace": "weather", "source": url_source(&second) }),
    )
    .await;
    assert_eq!(
        replaced.pointer("/data/status"),
        Some(&serde_json::json!("published")),
        "{replaced}"
    );
    assert_eq!(
        replaced.pointer("/data/operations"),
        Some(&serde_json::json!(2))
    );

    let (first_before, second_before) = (fetches(&first).await, fetches(&second).await);
    let refreshed = call(
        &scope,
        "updateProvider",
        &serde_json::json!({ "namespace": "weather", "refresh": true }),
    )
    .await;
    assert_eq!(
        refreshed.pointer("/data/status"),
        Some(&serde_json::json!("published")),
        "{refreshed}"
    );
    assert_eq!(
        (fetches(&first).await, fetches(&second).await),
        (first_before, second_before + 1),
        "the refresh reads the saved URL, and only it"
    );
    assert_eq!(
        saved(&scope, "weather").await.pointer("/data/source"),
        Some(&url_source(&second))
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

// Catches: Coral publishing a change only a person can confirm (a source that
// drops or breaks an Operation Programs call now, a new way to sign in, a new Credential
// Route, or an application's secret), or a link that confirms the change for
// the person, opens another task than Reef starts for the same change, or
// loses the route the Program asked for.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table checks the page each change that needs a person opens"
)]
async fn a_change_that_needs_a_person_returns_the_update_page() {
    let confidential = serde_json::json!({
        "method": "oauth_authorization_code",
        "authorization_url": "https://weather.example/authorize",
        "token_url": "https://weather.example/token",
        "scopes": ["read"],
        "client": { "client_id": "weather-app", "authentication": "client_secret_post" },
    });
    // Each link opens Reef's update page on `task`, followed by `then`.
    for (arranged, changes, task, then) in [
        (
            Saved::WithAlerts,
            serde_json::json!({ "source": document_source() }),
            "refresh",
            "",
        ),
        // A source that keeps an Operation Programs call now, but breaks it.
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
                "data": { "status": "needs_sign_in", "url": url, "namespace": "weather" },
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
                "message": "Operation arguments are invalid at `refresh`: the Provider was read \
                            from an uploaded document, which Coral cannot read again; send \
                            `source` with the new document instead",
            }),
        ),
        (
            // The route must suit the source, as addProvider's must.
            serde_json::json!({ "namespace": "weather", "credentialRoute": "caller" }),
            serde_json::json!({
                "code": "invalid_input",
                "message": "Operation arguments are invalid at `credentialRoute`: the source \
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
                "message": "Operation arguments are invalid at `displayName`: Provider display \
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

// Catches: a Publication that fails after Coral decided to publish, here one
// whose Setup an Owner canceled first, ending the Run or changing the
// Provider instead of failing the call with a code the Program can catch.
#[tokio::test]
async fn a_failed_publication_is_a_failure_the_program_catches() {
    let mut scope = unattended("user:update-publication-failed").await;
    create_weather(&scope, document_source()).await;
    let (vault, accounts) = scope.fixture.accounts();
    scope.fixture.runs =
        scope
            .fixture
            .runs
            .clone()
            .with_provider_publisher(Arc::new(SetupCanceledFirst {
                workflow: workflow(&scope.fixture),
                accounts,
                _vault: vault,
            }));
    let before = scope.lifecycle_events().await;
    let saved_before = saved(&scope, "weather").await;
    let arguments =
        serde_json::json!({ "namespace": "weather", "requestHeaders": { "x-trace": "on" } });

    let mut result = call(&scope, "updateProvider", &arguments).await;

    result.as_object_mut().unwrap().remove("message");
    assert_eq!(
        result,
        serde_json::json!({ "code": "provider_publication_failed" })
    );
    scope
        .assert_denial_changed_nothing(&before, &arguments.to_string())
        .await;
    assert_eq!(saved(&scope, "weather").await, saved_before);
}

// Catches: a worker lost while Coral publishes an update leaving the call
// indeterminate, when Coral's own Publication shows what happened, a
// Provider deleted since then failing the Run's termination, or a lost rename,
// which nothing records, claiming the Run ended first.
#[tokio::test]
async fn recovery_settles_a_lost_update_from_its_publication() {
    let regenerates =
        serde_json::json!({ "namespace": "weather", "requestHeaders": { "x-trace": "on" } });
    let renames = serde_json::json!({ "namespace": "weather", "displayName": "Forecasts" });
    for (arguments, publishes_first, deleted_since, expected) in [
        (
            &regenerates,
            true,
            false,
            crate::InvocationResultKind::UpstreamOutcome,
        ),
        (
            &regenerates,
            true,
            true,
            crate::InvocationResultKind::RunTerminated,
        ),
        (
            &regenerates,
            false,
            false,
            crate::InvocationResultKind::RunTerminated,
        ),
        (
            &renames,
            true,
            false,
            crate::InvocationResultKind::Indeterminate,
        ),
    ] {
        let mut scope = unattended("user:lost-update").await;
        let weather = scope.publish_weather().await;
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
        let run = scope.start(&scope.owner, update_provider(arguments)).await;
        wait_until(|| publisher.entered.load(Ordering::SeqCst)).await;
        if deleted_since {
            scope
                .fixture
                .providers
                .delete_provider(
                    &change(scope.owner_user(), 13, '4'),
                    &scope.workspace_id,
                    weather.id(),
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
            .next_process()
            .sweep_expired_leases()
            .await
            .expect("sweep the lost worker's lapsed lease");
        wait_for_terminal(&scope.fixture.runs, &scope.owner, &scope.workspace_id, &run).await;
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
            "{arguments}, publishes first: {publishes_first}, deleted since: {deleted_since}"
        );
        publisher.release.notify_one();
        wait_until(|| !scope.fixture.runs.has_owner(&run)).await;
    }
}
