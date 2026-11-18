use std::sync::Arc;
use std::time::Duration;

use coral_app::{McpInterfaceDependencies, McpInterfaceFactory as _};
use coral_db::{Clock, Database, UnixNanos};
use coral_mcp::{HumanInterfaceUrl, McpCaller, McpInterface, McpModules, ToolCallError};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_provider::ProviderModule;
use coral_run::{CoralInstanceId, RunInbox, RunModule, RuntimeLimits};
use coral_search::UniversalSearchModule;
use coral_task::{AttemptKind, TaskCommands, TaskId, TaskIntent, TaskModule};
use coral_test_support::{
    EVENTUALLY, TEST_COMPILATION_TIMEOUT, TestDatabase, change, provision_user,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule,
};
use rmcp::model::{CallToolRequestParams, CallToolResponse, Tool};
use sea_query::{Alias, Expr, ExprTrait as _, Order, Query};
use serde_json::{Value, json};
use wiremock::matchers::{any, body_string_contains, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::model::{Block, Message, ModelClient, Role, ToolCall};
use crate::sub_agents::{Error, SubAgents, TurnState};
use crate::{MODEL_CALL_TIMEOUT, SubAgentFactory, repository};

const POLICY: DestinationPolicy =
    DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny);
const MODEL: &str = "sailfish-1";
const MESSAGE: &str = "How many invoices are open?";

/// The clock of every Sub-agent here: noon on 18 November 2026, UTC.
#[derive(Debug)]
struct Noon;

impl Clock for Noon {
    fn now(&self) -> UnixNanos {
        UnixNanos::new(1_795_003_200_000_000_000)
    }
}

/// A lead in a Workspace of its own, over the real modules, and the HTTP
/// stub that stands in for the model.
struct Lead {
    _fixture: TestDatabase,
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    modules: McpModules,
    actor: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    model: MockServer,
}

impl Lead {
    async fn new() -> Self {
        let fixture = TestDatabase::new(coral_db::migration_plan()).await.unwrap();
        let database = fixture.database();
        let user = provision_user(database.clone(), "lead", OrganizationRole::Owner).await;
        let workspaces = WorkspaceModule::new(database.clone());
        let workspace_id = workspace(&workspaces, &user, "Lead").await;
        let providers =
            ProviderModule::with_running_catalog(POLICY, database.clone(), workspaces.clone());
        let runs = RunModule::new(
            POLICY,
            database.clone(),
            workspaces.clone(),
            providers,
            RuntimeLimits::default().with_compilation_timeout(TEST_COMPILATION_TIMEOUT),
            CoralInstanceId::generate(),
            RunInbox::table(database.clone()),
        )
        .unwrap();
        let tasks = TaskModule::new(database.clone(), workspaces.clone());
        let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
        let commands = TaskCommands::new(tasks.clone(), search.clone(), runs.clone());
        let reef = HumanInterfaceUrl::parse("https://reef.example.test").unwrap();
        Self {
            modules: McpModules::new(workspaces.clone(), tasks, commands, runs, search, reef),
            workspaces,
            actor: AuthenticatedPrincipal::user(user),
            workspace_id,
            model: MockServer::start().await,
            database,
            _fixture: fixture,
        }
    }

    /// What bootstrap hands the factory, in an environment that sets only
    /// `MODEL_KEY`.
    fn dependencies(&self) -> McpInterfaceDependencies {
        McpInterfaceDependencies {
            database: self.database.clone(),
            modules: self.modules.clone(),
            outbound_policy: POLICY,
            env: Arc::new(|name: &str| Ok((name == "MODEL_KEY").then(|| "key".to_owned()))),
            clock: Arc::new(Noon),
            stopping: tokio::sync::watch::channel(false).1,
        }
    }

    fn base_url(&self) -> String {
        format!("{}/v1", self.model.uri())
    }

    /// The tools that the factory builds from a section that names the stub.
    fn tools(&self) -> Arc<dyn McpInterface> {
        let section = format!("base_url = '{}'\nmodel = '{MODEL}'", self.base_url());
        SubAgentFactory
            .build(&toml::from_str(&section).unwrap(), self.dependencies())
            .unwrap()
    }

    /// The Sub-agents of the same database, which call the same stub.
    fn sub_agents(&self) -> SubAgents {
        let http = coral_outbound::Client::new(POLICY, MODEL_CALL_TIMEOUT).unwrap();
        let base_url = self.base_url().parse().unwrap();
        let model = ModelClient::new(http, &base_url, MODEL.to_owned(), None);
        let modules = self.modules.clone();
        let (tasks, commands, runs) = (modules.tasks, modules.commands, modules.runs);
        SubAgents::new(
            self.database.clone(),
            tasks,
            commands,
            runs,
            model,
            Arc::new(Noon),
        )
    }

    /// A new Task of the lead's.
    async fn task(&self) -> TaskId {
        let goal = TaskIntent::parse("Count the invoices").unwrap();
        let tasks = &self.modules.tasks;
        let started = tasks.start(&self.actor, &self.workspace_id, goal, UnixNanos::new(2));
        started.await.unwrap().id().clone()
    }

    /// Starts a Sub-agent in `task_id` and returns its `sub_agent_id` and
    /// how its Turn stands once it ends or 30 seconds pass.
    async fn turn(&self, tools: &dyn McpInterface, task_id: &TaskId) -> (String, Value) {
        let caller = self.caller();
        let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
        let started = result(call(tools, &caller, "agent", agent).await);
        let sub_agent_id = started
            .get("sub_agent_id")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned();
        let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
        let waited = result(call(tools, &caller, "wait", wait).await);
        (sub_agent_id, waited)
    }

    async fn thread(&self, sub_agent_id: &str) -> Vec<Message> {
        let thread = repository::thread(&*self.database, &self.workspace_id, sub_agent_id);
        thread.await.unwrap()
    }

    /// The model and output tokens recorded beside each message of the thread.
    async fn model_calls(&self, sub_agent_id: &str) -> Vec<(Option<String>, Option<i64>)> {
        let statement = Query::select()
            .columns([Alias::new("model"), Alias::new("output_tokens")])
            .from(Alias::new("sub_agent_thread_messages"))
            .and_where(Expr::col(Alias::new("sub_agent_id")).eq(sub_agent_id))
            .order_by(Alias::new("position"), Order::Asc)
            .to_owned();
        self.database.fetch_all(statement).await.unwrap()
    }

    fn caller(&self) -> McpCaller {
        McpCaller::fixture(
            self.actor.clone(),
            self.workspace_id.clone(),
            UnixNanos::new(2),
        )
    }

    /// The stub answers every request with `answer`.
    async fn model_answers(&self, answer: ResponseTemplate) {
        Mock::given(any())
            .respond_with(answer)
            .mount(&self.model)
            .await;
    }
}

/// A new Workspace of `owner`.
async fn workspace(
    workspaces: &WorkspaceModule,
    owner: &AuthenticatedUser,
    name: &str,
) -> WorkspaceId {
    let name = WorkspaceDisplayName::parse(name).unwrap();
    let created = change(owner.clone(), 1, 'a');
    let workspace = workspaces.create(&created, name).await.unwrap();
    workspace.id().clone()
}

/// The model server's answer whose reply is `text`.
fn reply(text: &str) -> ResponseTemplate {
    completion(&json!({"role": "assistant", "content": text}))
}

/// The model server's answer whose reply makes each tool call.
fn tool_calls(calls: &[ToolCall]) -> ResponseTemplate {
    let calls: Vec<Value> = calls
        .iter()
        .map(|call| {
            let function = json!({"name": call.name, "arguments": call.arguments});
            json!({"id": call.id, "type": "function", "function": function})
        })
        .collect();
    completion(&json!({"role": "assistant", "content": null, "tool_calls": calls}))
}

fn completion(message: &Value) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({
        "model": MODEL,
        "choices": [{"message": message}],
        "usage": {"prompt_tokens": 10, "completion_tokens": 2},
    }))
}

fn tool_call(id: &str, name: &str, arguments: &Value) -> ToolCall {
    ToolCall {
        id: id.to_owned(),
        name: name.to_owned(),
        arguments: arguments.to_string(),
    }
}

/// A reply that the stub holds back past the end of the test.
fn no_reply() -> ResponseTemplate {
    reply("late").set_delay(EVENTUALLY)
}

async fn call(
    tools: &dyn McpInterface,
    caller: &McpCaller,
    name: &'static str,
    arguments: Value,
) -> Result<Value, ToolCallError> {
    let Value::Object(arguments) = arguments else {
        panic!("tool arguments are an object");
    };
    let request = CallToolRequestParams::new(name).with_arguments(arguments);
    match tools.call(caller, request, None).await? {
        CallToolResponse::Complete(result) => Ok(result.structured_content.unwrap()),
        _ => panic!("{name} completes"),
    }
}

/// The structured result of a call that Coral completed.
fn result(called: Result<Value, ToolCallError>) -> Value {
    match called {
        Ok(result) => result,
        Err(ToolCallError::Protocol(error)) => panic!("refused: {}", error.message),
        Err(ToolCallError::Tool(_)) => panic!("failed"),
    }
}

/// The names of the arguments that a tool takes, and of those it requires.
fn arguments(tools: &[Tool], name: &str) -> (Vec<String>, Vec<Value>) {
    let tool = tools.iter().find(|tool| tool.name == name).unwrap();
    let schema = &tool.input_schema;
    let taken = schema.get("properties").and_then(Value::as_object);
    let required = schema.get("required").and_then(Value::as_array);
    (
        taken.unwrap().keys().cloned().collect(),
        required.cloned().unwrap_or_default(),
    )
}

// Catches: a section that starts Coral with a setting it cannot use, and a
// startup that needs the model to answer.
#[tokio::test]
async fn startup_checks_the_section_and_sends_the_model_nothing() {
    let lead = Lead::new().await;
    let factory = SubAgentFactory;
    let named = format!("base_url = '{}'\nmodel = '{MODEL}'\n", lead.base_url());
    for (section, refusal) in [
        (named.clone(), None),
        (
            format!("{named}api_key_env = 'MODEL_KEY'\ncontext_window = 48000"),
            None,
        ),
        (
            format!("{named}reasoning = 'high'"),
            Some("unknown field `reasoning`"),
        ),
        ("model = 'm'".to_owned(), Some("missing field `base_url`")),
        (
            "base_url = 'sailfish.example/v1'\nmodel = 'm'".to_owned(),
            Some("base_url: must be an absolute http or https URL"),
        ),
        (
            "base_url = 'https://sailfish.example/v1?key=k'\nmodel = 'm'".to_owned(),
            Some("base_url: must be an absolute http or https URL without a query or fragment"),
        ),
        (
            "base_url = 'https://sailfish.example/v1#chat'\nmodel = 'm'".to_owned(),
            Some("base_url: must be an absolute http or https URL without a query or fragment"),
        ),
        (
            "base_url = 'https://sailfish.example/v1'\nmodel = ' '".to_owned(),
            Some("model: must not be empty"),
        ),
        (
            format!("{named}context_window = 47999"),
            Some("context_window: must be at least 48000"),
        ),
        (
            format!("{named}api_key_env = 'UNSET_KEY'"),
            Some("api_key_env: environment variable is not set"),
        ),
    ] {
        let built = factory.build(&toml::from_str(&section).unwrap(), lead.dependencies());
        let refused = built.err().map(|error| error.to_string());
        match refusal {
            Some(refusal) => assert!(
                refused
                    .as_deref()
                    .is_some_and(|text| text.contains(refusal)),
                "{section}: {refused:?}"
            ),
            None => assert_eq!(refused, None, "{section}"),
        }
    }
    assert!(lead.model.received_requests().await.unwrap().is_empty());
}

// Catches: a lead served without `agent`, or a `wait` or `cancel` that refuses
// a `sub_agent_id`.
#[tokio::test]
async fn a_lead_is_served_agent_beside_cores_tools() {
    let lead = Lead::new().await;
    let tools = lead.tools();
    let caller = lead.caller();
    let core_tools = lead.modules.core_interface().tools(&caller).await.unwrap();

    let served = tools.tools(&caller).await.unwrap();
    let names = |tools: &[Tool]| -> Vec<String> {
        tools.iter().map(|tool| tool.name.to_string()).collect()
    };
    let with_agent = [names(&core_tools), vec!["agent".to_owned()]].concat();
    assert_eq!(names(&served), with_agent);
    for name in ["wait", "cancel"] {
        let (core_taken, _) = arguments(&core_tools, name);
        let (taken, required) = arguments(&served, name);
        let with_sub_agent_id = [core_taken, vec!["sub_agent_id".to_owned()]].concat();
        assert_eq!(taken, with_sub_agent_id, "{name}");
        assert!(!required.contains(&json!("run_id")), "{name}");
    }
}

// Catches: a Turn that hides why the model gave no reply, one that completes
// with an empty answer, and one that never stops calling the model.
#[tokio::test]
async fn a_turn_without_an_answer_says_why() {
    let lead = Lead::new().await;
    let (tools, task_id) = (lead.tools(), lead.task().await);
    let refused = "the model server refused the request with status 400 Bad Request";
    let no_answer = "the model gave no answer: send the message again";
    let too_many =
        "the Sub-agent made 60 model calls without an answer: send it a narrower message";
    let again = tool_calls(&[tool_call("call_1", "nothing", &json!({}))]);
    for (answer, failure, model_calls) in [
        (ResponseTemplate::new(400), refused, 1),
        (reply(""), no_answer, 1),
        (again, too_many, 60),
    ] {
        lead.model_answers(answer).await;

        let (sub_agent_id, waited) = lead.turn(&*tools, &task_id).await;

        let failed = json!({"sub_agent_id": sub_agent_id, "state": "failed", "failure": failure});
        assert_eq!(waited, failed);
        let requests = lead.model.received_requests().await.unwrap();
        assert_eq!(requests.len(), model_calls);
        lead.model.reset().await;
    }
}

// Catches: a Run that the Sub-agent starts outside the lead's Task, an
// answer read before the Run's result reached the model, and a thread that
// loses, repeats or reorders a message or a model call's record.
#[tokio::test]
async fn a_turn_runs_a_program_in_the_leads_task_and_answers_from_it() {
    let lead = Lead::new().await;
    let (tools, task_id) = (lead.tools(), lead.task().await);
    let program = "return ['pelican', 'heron'].join(' and ').toUpperCase();";
    let arguments =
        json!({"task_id": task_id.as_str(), "intent": "Read the birds", "program": program});
    let exec = tool_call("call_1", "exec", &arguments);
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .and(body_string_contains("PELICAN AND HERON"))
        .respond_with(reply("Pelican and heron."))
        .mount(&lead.model)
        .await;
    lead.model_answers(tool_calls(std::slice::from_ref(&exec)))
        .await;

    let (sub_agent_id, waited) = lead.turn(&*tools, &task_id).await;

    let answer = "Pelican and heron.";
    let completed = json!({"sub_agent_id": sub_agent_id, "state": "completed", "answer": answer});
    assert_eq!(waited, completed);
    let tasks = &lead.modules.tasks;
    let attempts = tasks.interactions(&lead.actor, &lead.workspace_id, &task_id, None, 10);
    let attempts = attempts.await.unwrap();
    let runs: Vec<_> = attempts
        .iter()
        .map(|attempt| (attempt.kind, attempt.run.is_some()))
        .collect();
    assert_eq!(runs, [(AttemptKind::Submit, true)]);
    let thread = lead.thread(&sub_agent_id).await;
    let result = match thread.get(2).map(|results| results.content.as_slice()) {
        Some([Block::ToolResult { content, .. }]) => content.clone(),
        _ => panic!("the third message is the result of exec: {thread:?}"),
    };
    let brief = format!(
        "task_id: {}\ndate: 2026-11-18\n\n{MESSAGE}",
        task_id.as_str()
    );
    let assistant = |content| Message {
        role: Role::Assistant,
        content,
    };
    let text = Block::Text {
        text: answer.to_owned(),
    };
    assert_eq!(
        thread,
        [
            Message::user(brief),
            assistant(vec![Block::ToolUse(exec.clone())]),
            Message::tool_results(vec![exec.result(result)]),
            assistant(vec![text]),
        ]
    );
    let model = Some(MODEL.to_owned());
    let records = [
        (None, None),
        (model.clone(), Some(2)),
        (None, None),
        (model, Some(2)),
    ];
    assert_eq!(lead.model_calls(&sub_agent_id).await, records);
}

// Catches: tool calls of one reply that run one after another. If they did,
// `wait` would hold the Turn for 30 seconds before `cancel` ended its Run.
#[tokio::test]
async fn the_tool_calls_of_one_reply_run_at_the_same_time() {
    let lead = Lead::new().await;
    let (tools, task_id) = (lead.tools(), lead.task().await);
    let endless = json!({"task_id": task_id.as_str(), "intent": "Loop", "program": "while (true) {}", "wait_ms": 0});
    let started = result(call(&*tools, &lead.caller(), "exec", endless).await);
    let run_id = started.get("run_id").unwrap();
    let wait = tool_call(
        "call_1",
        "wait",
        &json!({"run_id": run_id, "wait_ms": 30_000}),
    );
    let cancel = tool_call("call_2", "cancel", &json!({"run_id": run_id}));
    Mock::given(any())
        .respond_with(tool_calls(&[wait, cancel]))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    lead.model_answers(reply("Stopped.")).await;

    let (sub_agent_id, waited) = lead.turn(&*tools, &task_id).await;

    assert_eq!(waited.get("state"), Some(&json!("completed")), "{waited}");
    let thread = lead.thread(&sub_agent_id).await;
    let waited = match thread.get(2).map(|results| results.content.as_slice()) {
        Some([Block::ToolResult { content, .. }, _]) => content.clone(),
        _ => panic!("the third message holds both results: {thread:?}"),
    };
    assert!(waited.contains("canceled"), "{waited}");
}

// Catches: a Sub-agent that works outside the lead's Task, and a call that
// no longer reaches core once the Sub-agent is on.
#[tokio::test]
async fn sub_agents_share_the_leads_task_and_core_still_ends_it() {
    let lead = Lead::new().await;
    lead.model_answers(reply("Twelve.")).await;
    let (tools, caller) = (lead.tools(), lead.caller());

    let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
    let first = result(call(&*tools, &caller, "agent", agent).await);
    let task_id = first.get("task_id").unwrap();
    let agent = json!({"task_id": task_id, "message": MESSAGE});
    let second = result(call(&*tools, &caller, "agent", agent).await);
    let end = json!({"task_id": task_id, "outcome": "success"});
    let ended = result(call(&*tools, &caller, "end_task", end).await);

    assert_eq!(second.get("task_id"), Some(task_id));
    assert_ne!(second.get("sub_agent_id"), first.get("sub_agent_id"));
    assert_eq!(ended.get("outcome"), Some(&json!("success")));
}

// Catches: a Sub-agent that another caller, or its own caller in another
// Workspace, can read or stop.
#[tokio::test]
async fn another_callers_sub_agent_reads_as_not_found() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let sub_agents = lead.sub_agents();
    let (actor, workspace_id) = (&lead.actor, &lead.workspace_id);
    let goal = TaskIntent::parse("Count the invoices").unwrap();
    let started = lead
        .modules
        .tasks
        .start(actor, workspace_id, goal, UnixNanos::new(2));
    let task_id = started.await.unwrap().id().clone();
    let started = sub_agents.start(actor, workspace_id, &task_id, MESSAGE.to_owned());
    let sub_agent_id = started.await.unwrap();
    let other_user = provision_user(lead.database.clone(), "other", OrganizationRole::Owner).await;
    let other_workspace_id = workspace(&lead.workspaces, &other_user, "Other").await;
    let other_actor = AuthenticatedPrincipal::user(other_user);

    for (actor, workspace_id) in [(&other_actor, workspace_id), (actor, &other_workspace_id)] {
        let waited = sub_agents.wait(actor, workspace_id, &sub_agent_id, Duration::ZERO);
        let waited = waited.await;
        assert!(matches!(waited, Err(Error::SubAgentNotFound)), "{waited:?}");
        let canceled = sub_agents.cancel(actor, workspace_id, &sub_agent_id).await;
        assert!(
            matches!(canceled, Err(Error::SubAgentNotFound)),
            "{canceled:?}"
        );
    }

    let unchanged = sub_agents.wait(actor, workspace_id, &sub_agent_id, Duration::ZERO);
    assert_eq!(unchanged.await.unwrap(), TurnState::Running);
}

// Catches: a canceled Turn that keeps its lease, takes a late reply as its
// answer, or shares its Sub-agent with a second open Turn.
#[tokio::test]
async fn cancel_ends_a_running_turn_for_good() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let (tools, caller, sub_agents) = (lead.tools(), lead.caller(), lead.sub_agents());
    let (database, workspace_id) = (&lead.database, &lead.workspace_id);
    let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
    let started = result(call(&*tools, &caller, "agent", agent).await);
    let sub_agent_id = started.get("sub_agent_id").and_then(Value::as_str);
    let sub_agent_id = sub_agent_id.unwrap();
    let by_id = json!({"sub_agent_id": sub_agent_id});
    let turn = sub_agents.turn(&lead.actor, workspace_id, sub_agent_id);
    let turn_id = turn.await.unwrap().id;
    let lease_end = UnixNanos::new(i64::MAX);

    let renewed = repository::renew_lease(database, workspace_id, &turn_id, lease_end).await;
    assert!(renewed.unwrap(), "a running Turn renews its lease");
    let mut transaction = database.begin().await.unwrap();
    let second =
        repository::insert_turn(&mut transaction, workspace_id, sub_agent_id, "2", lease_end).await;
    assert!(second.is_err_and(|error| error.is_unique_violation()));
    drop(transaction);
    let running = result(call(&*tools, &caller, "wait", by_id.clone()).await);
    assert_eq!(
        running,
        json!({"sub_agent_id": sub_agent_id, "state": "running"})
    );

    let canceled = result(call(&*tools, &caller, "cancel", by_id.clone()).await);

    let stopped = json!({"sub_agent_id": sub_agent_id, "state": "canceled"});
    assert_eq!(canceled, stopped);
    let renewed = repository::renew_lease(database, workspace_id, &turn_id, lease_end).await;
    assert!(!renewed.unwrap(), "the next renewal stops the model call");
    let late = TurnState::Completed {
        answer: "late".to_owned(),
    };
    let ended = sub_agents.end_turn(workspace_id, &turn_id, &late).await;
    assert!(!ended.unwrap(), "the first ending stands");
    assert_eq!(result(call(&*tools, &caller, "wait", by_id).await), stopped);
}
