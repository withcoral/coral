use coral_functions::SavedFunctionModule;
use coral_run::{
    IsolateLimits, ProgramSource, RunModule, RunSubmission, RunTimeout, RuntimeLimits,
};
use coral_search::UniversalSearchModule;
use coral_task::{TaskCommands, TaskModule};
use coral_test_support::TestDatabase;
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, OrganizationModule, WorkspaceDisplayName,
    WorkspaceModule, local_organization_owner,
};

use super::*;
use serde_json::{Value, json};

async fn accepted_unlaunched_run() -> (TestDatabase, McpServer, RunId) {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let user = local_organization_owner();
    let change = ChangeContext::new(user.clone(), 1, TraceId::current_or_new());
    OrganizationModule::new(database.clone())
        .provision_login(&change)
        .await
        .unwrap();
    let workspaces = WorkspaceModule::new(database.clone());
    let workspace = workspaces
        .create(&change, WorkspaceDisplayName::parse("MCP waits").unwrap())
        .await
        .unwrap();
    let actor = AuthenticatedPrincipal::user(user);
    let tasks = TaskModule::new(database.clone(), workspaces.clone());
    let functions = SavedFunctionModule::new(
        database.clone(),
        workspaces.clone(),
        IsolateLimits::default(),
    )
    .unwrap();
    let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
    let index = coral_search::SearchIndex::inert(database.clone());
    let runs = RunModule::new(
        coral_outbound::DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
        coral_provider::ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        ),
        RuntimeLimits::default(),
        coral_run::CoralInstanceId::generate(),
        coral_run::RunInbox::table(database.clone()),
    )
    .unwrap();
    // Commit acceptance without launching execution. There can be no authored output,
    // human actions, or terminal transition to wake the observation early.
    let prepared = runs
        .prepare(
            &actor,
            workspace.id(),
            RunSubmission::new(
                ProgramSource::parse("return 1;").unwrap(),
                RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let mut transaction = database.begin().await.unwrap();
    let staged = runs
        .accept_in_transaction(&mut transaction, prepared)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let commands = TaskCommands::new(tasks.clone(), functions, search, index, runs.clone());
    let server = McpServer::new(
        McpRuntime {
            modules: crate::McpModules::new(
                workspaces,
                tasks,
                commands,
                runs,
                crate::HumanInterfaceUrl::parse("https://reef.example.test").unwrap(),
            ),
            principal: actor,
        },
        workspace.id().clone(),
    );
    (fixture, server, staged.run_id().clone())
}

/// Calls `wait` on `run_id` and returns its structured update.
async fn wait(server: &McpServer, run_id: &RunId, wait_ms: Option<Value>) -> Value {
    let mut arguments = json!({"run_id": run_id.to_string()});
    if let Some(wait_ms) = wait_ms {
        arguments
            .as_object_mut()
            .unwrap()
            .insert("wait_ms".into(), wait_ms);
    }
    let request =
        CallToolRequestParams::new("wait").with_arguments(arguments.as_object().unwrap().clone());
    let Ok(CallToolResponse::Complete(result)) =
        server.dispatch(request, Channel::Unavailable).await
    else {
        panic!("wait without prompting completes")
    };
    assert_ne!(result.is_error, Some(true));
    result.structured_content.unwrap()
}

/// `RunModule::observe` owns waiting; this covers how `wait_ms` selects it.
#[tokio::test]
async fn omitted_null_and_zero_wait_observe_immediately_but_positive_wait_waits() {
    const POSITIVE_WAIT_MS: u64 = 200;
    let (_fixture, server, run_id) = accepted_unlaunched_run().await;

    // Freeze virtual time: real database I/O still completes, but a handler
    // that waits on a timer never finishes, and nextest times it out. The
    // spinner stops Tokio from auto-advancing the paused clock during that I/O.
    tokio::time::pause();
    let keep_time_frozen = tokio::spawn(async {
        loop {
            tokio::task::yield_now().await;
        }
    });
    let mut updates = Vec::new();
    for wait_ms in [None, Some(Value::Null), Some(json!(0))] {
        updates.push(wait(&server, &run_id, wait_ms).await);
    }
    keep_time_frozen.abort();
    tokio::time::resume();

    // Nothing changes this Run, so only the deadline ends a positive wait.
    // Load can only delay the reply, so this lower bound cannot flake.
    let started = tokio::time::Instant::now();
    updates.push(wait(&server, &run_id, Some(json!(POSITIVE_WAIT_MS))).await);
    assert!(
        started.elapsed() >= Duration::from_millis(POSITIVE_WAIT_MS),
        "positive wait must not read immediately"
    );

    let first = updates.first().unwrap();
    assert_eq!(first.get("state"), Some(&json!("running")));
    assert_eq!(first.get("events"), Some(&json!([])));
    for update in &updates {
        assert_eq!(
            update, first,
            "observation does not change Run state or consume output"
        );
    }
}

/// A Run Owner that stopped before answering, a Run owned by another
/// process, and a request not yet recorded by its owner each project one
/// stable code; none is an internal error.
#[test]
fn owner_lost_owned_elsewhere_and_not_yet_recorded_project_stable_retryable_codes() {
    let cases = [
        (coral_run::Error::OwnerLost, "run_owner_lost"),
        (coral_run::Error::OwnedElsewhere, "run_owned_elsewhere"),
        (
            coral_run::Error::RunRequestNotYetRecorded,
            "run_request_not_yet_recorded",
        ),
    ];
    for (error, code) in cases {
        let label = format!("{error:?}");
        let super::DispatchError::Tool(failure) = super::run_failure(error) else {
            panic!("{label}: a tool failure, not a protocol error");
        };
        let projected =
            serde_json::to_value(&failure).expect("string-only tool failures serialize");
        assert_eq!(
            projected.get("code").and_then(serde_json::Value::as_str),
            Some(code),
            "{label}"
        );
        assert!(
            projected
                .get("message")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|message| !message.is_empty()),
            "{label}"
        );
    }
}
