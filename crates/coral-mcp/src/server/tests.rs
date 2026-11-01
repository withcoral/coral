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

struct ServerFixture {
    /// Keeps the database alive while the server uses it.
    database: TestDatabase,
    server: McpServer,
    runs: RunModule,
}

async fn server_fixture() -> ServerFixture {
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
    let commands = TaskCommands::new(tasks.clone(), functions, search, index, runs.clone());
    let server = McpServer::new(
        McpRuntime {
            modules: crate::McpModules::new(
                workspaces,
                tasks,
                commands,
                runs.clone(),
                crate::HumanInterfaceUrl::parse("https://reef.example.test").unwrap(),
            ),
            principal: actor,
        },
        workspace.id().clone(),
    );
    ServerFixture {
        database: fixture,
        server,
        runs,
    }
}

async fn accepted_unlaunched_run() -> (TestDatabase, McpServer, RunId) {
    let ServerFixture {
        database: fixture,
        server,
        runs,
    } = server_fixture().await;
    // Commit acceptance without launching execution. There can be no authored output,
    // human actions, or terminal transition to wake the observation early.
    let prepared = runs
        .prepare(
            &server.runtime.principal,
            &server.workspace_id,
            RunSubmission::new(
                ProgramSource::parse("return 1;").unwrap(),
                RunTimeout::from_millis(60_000).unwrap(),
            ),
        )
        .await
        .unwrap();
    let database = fixture.database();
    let mut transaction = database.begin().await.unwrap();
    let staged = runs
        .accept_in_transaction(&mut transaction, prepared)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
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

/// Starts a Task directly, as the first `exec` of a unit of work would.
async fn started_task(server: &McpServer) -> String {
    server
        .runtime
        .modules
        .tasks
        .start(
            &server.runtime.principal,
            &server.workspace_id,
            TaskIntent::parse("End the turn").unwrap(),
            now_unix_nanos().ok().unwrap(),
        )
        .await
        .unwrap()
        .id()
        .to_string()
}

/// Calls `end_task` with `extra` fields beside `task_id`.
async fn end_task(server: &McpServer, task: &str, extra: &Value) -> CallToolResult {
    let mut arguments = json!({"task_id": task});
    arguments
        .as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let request = CallToolRequestParams::new("end_task")
        .with_arguments(arguments.as_object().unwrap().clone());
    let Ok(CallToolResponse::Complete(result)) =
        server.dispatch(request, Channel::Unavailable).await
    else {
        panic!("end_task completes")
    };
    assert_ne!(result.is_error, Some(true));
    result
}

/// The result's `_meta` as a client reads it, if it has any.
fn result_meta(result: &CallToolResult) -> Option<Value> {
    serde_json::to_value(result).unwrap().get("_meta").cloned()
}

/// A success whose reply the agent says it gave ends the turn. Anything less
/// sends the model one more call, because ending the turn then could leave
/// the user with no reply at all.
#[tokio::test]
async fn only_a_durable_success_with_its_reply_given_ends_the_turn() {
    let fixture = server_fixture().await;
    let server = &fixture.server;
    let end_turn = json!({"claude/endTurn": true});

    let task = started_task(server).await;
    let ended = end_task(
        server,
        &task,
        &json!({"outcome": "success", "reply_given": true}),
    )
    .await;
    assert_eq!(result_meta(&ended), Some(end_turn));

    for extra in [
        json!({"outcome": "success"}),
        json!({"outcome": "success", "reply_given": false}),
        json!({"outcome": "success", "reply_given": null}),
        json!({"outcome": "failure", "reply_given": true}),
    ] {
        let task = started_task(server).await;
        let ended = end_task(server, &task, &extra).await;
        assert_eq!(result_meta(&ended), None, "{extra}");
    }

    // The first call wins, so asking for success again changes nothing and
    // the recorded failure keeps the turn open.
    let task = started_task(server).await;
    end_task(server, &task, &json!({"outcome": "failure"})).await;
    let repeated = end_task(
        server,
        &task,
        &json!({"outcome": "success", "reply_given": true}),
    )
    .await;
    assert_eq!(result_meta(&repeated), None);
    assert_eq!(
        repeated.structured_content.unwrap().get("outcome"),
        Some(&json!("failure"))
    );
}

/// The key sits beside the result: what the model and output schema read does
/// not change with it.
#[tokio::test]
async fn ending_the_turn_keeps_the_same_content_and_structured_content() {
    let fixture = server_fixture().await;
    let server = &fixture.server;
    let task = started_task(server).await;
    let plain = end_task(server, &task, &json!({"outcome": "success"})).await;
    let ending = end_task(
        server,
        &task,
        &json!({"outcome": "success", "reply_given": true}),
    )
    .await;
    assert!(result_meta(&ending).is_some());

    let mut serialized = serde_json::to_value(&ending).unwrap();
    serialized.as_object_mut().unwrap().remove("_meta");
    assert_eq!(serialized, serde_json::to_value(&plain).unwrap());
}

/// Input schemas no longer spell out identifiers, so a malformed one reaches
/// the server, which rejects it as invalid input before acting on it.
#[tokio::test]
async fn the_server_rejects_a_malformed_identifier_on_every_tool() {
    const MALFORMED: &str = "not-a-task";
    let fixture = server_fixture().await;
    for (tool, arguments) in [
        (
            "end_task",
            json!({"task_id": MALFORMED, "outcome": "success"}),
        ),
        (
            "exec",
            json!({"task_id": MALFORMED, "intent": "Run", "program": "return 1;"}),
        ),
        (
            "save_function",
            json!({
                "task_id": MALFORMED, "intent": "Save", "path": "a.b",
                "description": "Saves.", "source": "export default () => 1;"
            }),
        ),
        (
            "delete_function",
            json!({"task_id": MALFORMED, "intent": "Delete", "path": "a.b"}),
        ),
        ("wait", json!({"run_id": MALFORMED})),
        ("cancel", json!({"run_id": MALFORMED})),
    ] {
        let request =
            CallToolRequestParams::new(tool).with_arguments(arguments.as_object().unwrap().clone());
        let Err(DispatchError::Protocol(error)) =
            fixture.server.dispatch(request, Channel::Unavailable).await
        else {
            panic!("{tool} rejects the identifier as invalid input")
        };
        assert_eq!(error.code, rmcp::model::ErrorCode::INVALID_PARAMS, "{tool}");
        assert!(
            error.message.contains("identifier must be"),
            "{tool} says what a valid identifier looks like: {}",
            error.message
        );
    }
}

/// A Run Owner that stopped before answering and a request not yet recorded
/// by its owner each project one stable code; neither is an internal error.
#[test]
fn owner_lost_and_not_yet_recorded_project_stable_retryable_codes() {
    let cases = [
        (coral_run::Error::OwnerLost, "run_owner_lost"),
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
        let projected = crate::projection::failure(&failure, &TraceId::current_or_new())
            .structured_content
            .expect("structured failure");
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
