//! The `agent` tool's contract: listing, the turn page, `wait` and `cancel` by
//! `turn_id`, and the tools the agent itself may call. Turn rules belong to
//! `coral-agent`'s own tests.

use std::sync::Arc;

use async_trait::async_trait;
use coral_agent::{
    AgentModule, Model, ModelError, ModelRequest, ModelResponse, StopReason, Toolbox,
};
use coral_functions::SavedFunctionModule;
use coral_run::{IsolateLimits, RunModule, RuntimeLimits};
use coral_search::UniversalSearchModule;
use coral_task::{TaskCommands, TaskModule};
use coral_test_support::TestDatabase;
use coral_workspace::{
    AuthenticatedPrincipal, ChangeContext, OrganizationModule, WorkspaceDisplayName,
    WorkspaceModule, local_organization_owner,
};
use serde_json::{Map, Value, json};

use super::*;

const PROGRAM: &str = "return 41 + 1;";

/// The value at a JSON pointer, or null.
fn at<'a>(value: &'a Value, pointer: &str) -> &'a Value {
    value.pointer(pointer).unwrap_or(&Value::Null)
}

/// Runs one `exec` with the thread's `task_id`, then answers from its result
/// and names its Run.
struct ExecThenAnswer;

#[async_trait]
impl Model for ExecThenAnswer {
    async fn respond(&self, request: &ModelRequest) -> Result<ModelResponse, ModelError> {
        let last = request.messages.last().expect("a message");
        let tool_result = last
            .content
            .iter()
            .find(|block| at(block, "/type") == "tool_result");
        if let Some(result) = tool_result {
            let page: Value =
                serde_json::from_str(at(result, "/content").as_str().expect("text")).expect("json");
            let answer = format!(
                "The answer is {}.\nrun_id: {}",
                at(&page, "/final_value/value"),
                at(&page, "/run_id").as_str().expect("run_id")
            );
            return Ok(ModelResponse {
                content: vec![json!({"type": "text", "text": answer})],
                stop_reason: StopReason::EndTurn,
                usage: None,
            });
        }
        let first = request.messages.first().expect("first message");
        let task_id = first
            .content
            .first()
            .and_then(|block| at(block, "/text").as_str())
            .and_then(|text| text.strip_prefix("task_id: "))
            .expect("task_id block");
        Ok(ModelResponse {
            content: vec![json!({
                "type": "tool_use",
                "id": "toolu_1",
                "name": "exec",
                "input": {"task_id": task_id, "intent": "Add the numbers", "program": PROGRAM},
            })],
            stop_reason: StopReason::ToolUse,
            usage: None,
        })
    }
}

/// Never answers, so its turn stays running.
struct Silent;

#[async_trait]
impl Model for Silent {
    async fn respond(&self, _request: &ModelRequest) -> Result<ModelResponse, ModelError> {
        std::future::pending().await
    }
}

async fn server(model: Option<Arc<dyn Model>>) -> (TestDatabase, McpServer) {
    let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
    let database = fixture.database();
    coral_search::prepare_storage(&database).await.unwrap();
    let user = local_organization_owner();
    let change = ChangeContext::new(user.clone(), 1, TraceId::current_or_new());
    OrganizationModule::new(database.clone())
        .provision_login(&change)
        .await
        .unwrap();
    let workspaces = WorkspaceModule::new(database.clone());
    let workspace = workspaces
        .create(&change, WorkspaceDisplayName::parse("MCP agent").unwrap())
        .await
        .unwrap();
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
    )
    .unwrap();
    let commands = TaskCommands::new(tasks.clone(), functions, search, index, runs.clone());
    let mut modules = crate::McpModules::new(
        workspaces.clone(),
        tasks.clone(),
        commands,
        runs,
        crate::HumanInterfaceUrl::parse("https://reef.example.test").unwrap(),
    );
    if let Some(model) = model {
        modules = modules.with_agents(AgentModule::new(database, tasks, workspaces, model));
    }
    let server = McpServer::new(
        McpRuntime {
            modules,
            principal: AuthenticatedPrincipal::user(user),
        },
        workspace.id().clone(),
    );
    (fixture, server)
}

async fn call(server: &McpServer, name: &str, arguments: Value) -> Result<Value, DispatchError> {
    let Value::Object(arguments) = arguments else {
        panic!("object arguments")
    };
    let request = CallToolRequestParams::new(name.to_owned()).with_arguments(arguments);
    match server.dispatch(request, Channel::Unavailable).await? {
        CallToolResponse::Complete(result) => {
            assert_ne!(result.is_error, Some(true), "{result:?}");
            let structured = result.structured_content.unwrap();
            assert_matches_output_schema(name, &structured);
            Ok(structured)
        }
        _ => panic!("no prompting without a channel"),
    }
}

/// Clients such as Claude Code reject a result that its tool's advertised
/// output schema does not accept.
fn assert_matches_output_schema(name: &str, structured: &Value) {
    let tool = crate::arguments::tools(true)
        .into_iter()
        .find(|tool| tool.name == name)
        .expect("advertised tool");
    let schema = Value::Object((*tool.output_schema.expect("output schema")).clone());
    let validator = jsonschema::draft202012::options()
        .should_validate_formats(true)
        .build(&schema)
        .expect("output schema compiles");
    let problems = validator
        .iter_errors(structured)
        .map(|error| error.to_string())
        .collect::<Vec<_>>();
    assert!(
        problems.is_empty(),
        "{name} result breaks its schema: {problems:?}\n{structured}"
    );
}

#[tokio::test]
async fn agent_is_offered_only_when_configured() {
    let (_without, without) = server(None).await;
    let (_with, with) = server(Some(Arc::new(Silent))).await;

    let instructions = |server: &McpServer| server.get_info().instructions.unwrap_or_default();
    assert!(!instructions(&without).contains("`agent`"));
    assert!(instructions(&with).contains("`agent`"));
    let unconfigured = call(&without, "agent", json!({"message": "List my PRs"})).await;
    assert!(
        matches!(&unconfigured, Err(DispatchError::Protocol(error)) if error.message.contains("not found")),
        "an unlisted tool is not callable"
    );
}

#[tokio::test]
async fn agent_answers_through_a_turn_page_that_wait_follows_to_the_end() {
    let (_fixture, server) = server(Some(Arc::new(ExecThenAnswer))).await;

    let first = call(
        &server,
        "agent",
        json!({"message": "What is 41 + 1?", "wait_ms": 0}),
    )
    .await
    .unwrap_or_else(|_error| panic!("agent starts a turn"));
    let turn_id = at(&first, "/turn_id").as_str().expect("turn_id").to_owned();
    assert!(at(&first, "/task_id").is_string(), "agent started a Task");

    let mut events = at(&first, "/events")
        .as_array()
        .cloned()
        .unwrap_or_default();
    let mut page = first;
    for _attempt in 0..20 {
        if at(&page, "/state") != "running" {
            break;
        }
        page = call(
            &server,
            "wait",
            json!({"turn_id": turn_id, "cursor": at(&page, "/next_cursor"), "wait_ms": 30_000}),
        )
        .await
        .unwrap_or_else(|_error| panic!("wait observes the turn"));
        events.extend(at(&page, "/events").as_array().cloned().unwrap_or_default());
    }

    assert_eq!(at(&page, "/state"), "completed", "{page}");
    assert_eq!(at(&page, "/final"), "The answer is 42.");
    assert_eq!(at(&page, "/evidence/program"), PROGRAM);
    assert!(at(&page, "/evidence/run_id").is_string());
    assert_eq!(
        at(&page, "/evidence/value"),
        42,
        "the caller can check the answer"
    );
    assert_eq!(
        events
            .iter()
            .map(|event| (
                event["kind"].clone(),
                event["tool"].clone(),
                event["intent"].clone()
            ))
            .collect::<Vec<_>>(),
        [(json!("agent.step"), json!("exec"), json!("Add the numbers"))],
        "each step arrives once across pages"
    );
}

#[tokio::test]
async fn wait_and_cancel_name_exactly_one_run_or_turn() {
    let (_fixture, server) = server(Some(Arc::new(Silent))).await;
    let started = call(
        &server,
        "agent",
        json!({"message": "List my PRs", "wait_ms": 0}),
    )
    .await
    .unwrap_or_else(|_error| panic!("agent starts a turn"));
    let turn_id = at(&started, "/turn_id").clone();
    let run_id = json!("00000000-0000-4000-8000-000000000000");

    for tool in ["wait", "cancel"] {
        for arguments in [json!({}), json!({"run_id": run_id, "turn_id": turn_id})] {
            let rejected = call(&server, tool, arguments).await;
            assert!(
                matches!(&rejected, Err(DispatchError::Protocol(error)) if error.message.contains("exactly one")),
                "{tool} must name one target"
            );
        }
    }
    let canceled = call(&server, "cancel", json!({"turn_id": turn_id}))
        .await
        .unwrap_or_else(|_error| panic!("cancel ends the turn"));
    assert_eq!(at(&canceled, "/state"), "canceled");
}

#[tokio::test]
async fn the_agent_learns_namespaces_and_calls_only_discovery_and_run_tools() {
    let (_fixture, server) = server(Some(Arc::new(Silent))).await;
    let toolbox = crate::agent_toolbox::McpToolbox::new(server);

    // The fixture connects no Provider.
    assert_eq!(
        toolbox.briefing().await.as_deref(),
        Some("No Provider namespaces are searchable.")
    );
    let tools = toolbox.tools();
    let names = tools
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();
    assert_eq!(names, ["search", "describe", "exec", "wait", "cancel"]);
    for tool in &tools {
        assert!(
            tool.input_schema.pointer("/properties/turn_id").is_none(),
            "{} offers no turn_id",
            tool.name
        );
    }
    for forbidden in ["agent", "start_task", "end_task", "save_function"] {
        let outcome = toolbox.call(forbidden, Value::Object(Map::new())).await;
        assert!(outcome.is_error, "{forbidden} is refused");
        assert!(outcome.text.contains("unknown_tool"));
    }
}
