use std::sync::Arc;

use coral_run::{ProgramSource, RunModule, RunSubmission, RunTimeout, RuntimeLimits};
use coral_search::UniversalSearchModule;
use coral_task::{TaskCommands, TaskModule};
use coral_test_support::TestDatabase;
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, OrganizationModule, TraceId, WorkspaceDisplayName,
    WorkspaceModule, local_organization_owner,
};

use super::*;
use serde_json::{Value, json};

/// A Run Module over `database`. No test here launches a Run's execution.
fn run_module(database: &Arc<coral_db::Database>, workspaces: &WorkspaceModule) -> RunModule {
    RunModule::new(
        coral_outbound::DestinationPolicy::default(),
        database.clone(),
        workspaces.clone(),
        coral_provider::ProviderModule::with_running_catalog(
            coral_outbound::DestinationPolicy::default(),
            database.clone(),
            workspaces.clone(),
        ),
        RuntimeLimits::default()
            .with_compilation_timeout(coral_test_support::TEST_COMPILATION_TIMEOUT),
        coral_run::CoralInstanceId::generate(),
        coral_run::RunInbox::table(database.clone()),
    )
    .unwrap()
}

/// `principal`'s MCP server on `workspace`, over the real modules.
fn server_on(
    database: &Arc<coral_db::Database>,
    workspaces: WorkspaceModule,
    runs: RunModule,
    principal: AuthenticatedPrincipal,
    workspace: WorkspaceId,
) -> McpServer {
    let tasks = TaskModule::new(database.clone(), workspaces.clone());
    let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
    let commands = TaskCommands::new(tasks.clone(), search.clone(), runs.clone());
    McpServer::new(
        McpRuntime {
            modules: crate::McpModules::new(
                workspaces,
                tasks,
                commands,
                runs,
                search,
                crate::HumanInterfaceUrl::parse("https://reef.example.test").unwrap(),
            ),
            principal,
        },
        workspace,
    )
}

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
    let runs = run_module(&database, &workspaces);
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
    let server = server_on(&database, workspaces, runs, actor, workspace.id().clone());
    (fixture, server, staged.run_id().clone())
}

// Catches: a caller that lost the Workspace being served the tools without
// namespaces, as when the lookup only failed, instead of the denial every
// later call of its repeats.
#[tokio::test]
async fn a_caller_that_lost_the_workspace_is_refused_the_tool_list() {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    let user = local_organization_owner();
    OrganizationModule::new(database.clone())
        .provision_login(&ChangeContext::new(
            user.clone(),
            1,
            TraceId::current_or_new(),
        ))
        .await
        .unwrap();
    let workspaces = WorkspaceModule::new(database.clone());
    let runs = run_module(&database, &workspaces);
    // No Workspace has this id, as after the caller's one is gone.
    let gone = WorkspaceId::parse("6f1d2c3b-5e4a-4d7c-9b8a-0123456789ab").unwrap();
    let server = server_on(
        &database,
        workspaces,
        runs,
        AuthenticatedPrincipal::user(user),
        gone,
    );
    let Err(denied) = server.listed_tools().await else {
        panic!("a caller that lost the Workspace gets no tools")
    };
    assert_eq!(denied.code, rmcp::model::ErrorCode::INVALID_REQUEST);
    assert_eq!(denied.message, "Permission was denied");
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
    let Ok(CallToolResponse::Complete(result)) = server.dispatch(request, None).await else {
        panic!("wait completes")
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

/// A launch that fails after an `exec` with `goal` started its Task keeps the
/// failure's code and names the Task, so the agent continues it with
/// `task_id` instead of starting a second Task with `goal`.
#[test]
fn a_failure_after_goal_started_its_task_names_the_task() {
    let task = TaskId::parse("alpha-bravo-charlie-delta").unwrap();
    let super::DispatchError::Tool(failure) = super::started_task_failure(
        super::task_failure(coral_task::Error::Run(coral_run::Error::CorruptData(
            "unreadable Run".into(),
        ))),
        &task,
    ) else {
        panic!("a tool failure, not a protocol error");
    };
    let projected = crate::projection::failure(&failure, &TraceId::current_or_new())
        .structured_content
        .expect("structured failure");
    assert_eq!(
        projected.get("code").and_then(Value::as_str),
        Some("internal_error")
    );
    assert_eq!(
        projected.get("message").and_then(Value::as_str),
        Some(
            "Coral could not complete the tool call. This call started Task alpha-bravo-charlie-delta: pass it as `task_id` to later calls instead of `goal`."
        )
    );
}
