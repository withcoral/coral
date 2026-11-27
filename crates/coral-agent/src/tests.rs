use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use coral_agent_model::AgentModels;
use coral_app::{SubAgentDependencies, SubAgentToolsFactory as _};
use coral_catalog::{ArgumentProblem, BuiltinArguments, BuiltinTool};
use coral_credentials::CredentialVault;
use coral_db::{Clock, Database, UnixNanos};
use coral_mcp::{HumanInterfaceUrl, McpCaller, McpModules, ToolCallError};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_provider::ProviderModule;
use coral_run::{
    CoralInstanceId, RunId, RunModule, RunStop, RuntimeLimits, SubAgentCaller, SubAgentError,
    SubAgentTools,
};
use coral_search::UniversalSearchModule;
use coral_task::{AttemptKind, AttributedCommand, TaskCommands, TaskId, TaskIntent, TaskModule};
use coral_test_support::{
    EVENTUALLY, ManualClock, TEST_COMPILATION_TIMEOUT, TestDatabase, change, provision_user,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, IfMatch, OrganizationModule, OrganizationRole,
    WorkspaceDisplayName, WorkspaceId, WorkspaceModule,
};
use rmcp::ErrorData;
use rmcp::model::{CallToolRequestParams, CallToolResponse};
use sea_query::{Alias, Expr, ExprTrait as _, Order, Query};
use serde_json::{Map, Value, json};
use wiremock::matchers::{any, body_string_contains, method, path};
use wiremock::{Mock, MockServer, Request, Respond, ResponseTemplate};

use crate::model::{Block, Message, ModelClient, Protocol, Role, ToolCall};
use crate::run_tools::Caller;
use crate::sub_agents::{
    Error, ModelRefusal, ModelSource, RunningTurn, SubAgents, TURN_LEASE, TURN_MAX, TurnModel,
    TurnState,
};
use crate::telemetry::TurnTrace;
use crate::tools::AgentTools;
use crate::{MODEL_CALL_TIMEOUT, SubAgentFactory, repository};

mod compaction;
mod consent;
mod models;
mod telemetry;

const POLICY: DestinationPolicy =
    DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny);
const MODEL: &str = "sailfish-1";
const MESSAGE: &str = "How many invoices are open?";

/// Where the clock of every Sub-agent here starts: noon on 18 November 2026,
/// UTC.
const NOON: UnixNanos = UnixNanos::new(1_795_003_200_000_000_000);

/// A lead in a Workspace of its own, over the real modules, and the HTTP
/// stub that stands in for the model.
struct Lead {
    /// Serves the Runs on a Run Executor in this process.
    _serving: coral_run::testing::Serving,
    _fixture: TestDatabase,
    database: Arc<Database>,
    workspaces: WorkspaceModule,
    modules: McpModules,

    actor: AuthenticatedPrincipal,
    workspace_id: WorkspaceId,
    model: MockServer,
    _vault_dir: tempfile::TempDir,
    agent_models: AgentModels,
    clock: ManualClock,
    /// Shuts the Coral Instance down.
    stop: tokio::sync::watch::Sender<bool>,
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
        )
        .unwrap();
        let reef = HumanInterfaceUrl::parse("https://reef.example.test").unwrap();
        let runs = runs.with_human_interface(reef.clone());
        let (runs, serving) = coral_run::testing::serve_on_run_executor(runs).await;
        let tasks = TaskModule::new(database.clone(), workspaces.clone());
        let search = UniversalSearchModule::new(database.clone(), workspaces.clone());
        let commands = TaskCommands::new(tasks.clone(), search.clone(), runs.clone());
        let vault_dir = tempfile::tempdir().unwrap();
        let vault = CredentialVault::open(&vault_dir.path().join("key")).unwrap();
        Self {
            agent_models: AgentModels::new(database.clone(), vault, POLICY),
            _vault_dir: vault_dir,
            modules: McpModules::new(workspaces.clone(), tasks, commands, runs, search, reef),
            _serving: serving,
            workspaces,
            actor: AuthenticatedPrincipal::user(user),
            workspace_id,
            model: MockServer::start().await,
            clock: ManualClock::starting_at(NOON),
            stop: tokio::sync::watch::Sender::new(false),
            database,
            _fixture: fixture,
        }
    }

    /// What bootstrap hands the factory.
    fn dependencies(&self) -> SubAgentDependencies {
        SubAgentDependencies {
            database: self.database.clone(),
            agent_models: self.agent_models.clone(),
            modules: self.modules.clone(),
            outbound_policy: POLICY,
            clock: Arc::new(self.clock.clone()),
            stopping: self.stop.subscribe(),
        }
    }

    fn base_url(&self) -> String {
        format!("{}/v1", self.model.uri())
    }

    /// The Tools, which call the stub for every Workspace.
    fn tools(&self) -> Arc<dyn SubAgentTools> {
        self.tools_with_window(None)
    }

    /// The Tools, over a stub whose context window is `window`.
    fn tools_with_window(&self, window: Option<u32>) -> Arc<dyn SubAgentTools> {
        let model = FixedModel::new(model_client_with_window(&self.model, window));
        Arc::new(AgentTools {
            database: self.database.clone(),
            tasks: self.modules.tasks.clone(),
            sub_agents: self.sub_agents_of(Arc::new(model)),
            human_interface: self.modules.human_interface().clone(),
        })
    }

    /// The Tools as a Workspace that has them serves them: wired into the
    /// lead's Runs, in a Workspace whose Built-in Provider holds them.
    async fn served_tools(&self) -> Arc<dyn SubAgentTools> {
        let (database, workspaces) = (self.database.clone(), self.workspaces.clone());
        let catalog = coral_catalog::BuiltinCatalog::with_sub_agent().unwrap();
        let graphs = coral_catalog::SchemaGraphCache::with_default_budget();
        let providers = ProviderModule::new(POLICY, database, workspaces, catalog, graphs);
        let installed = providers.install_missing_coral_defined(NOON).await.unwrap();
        assert!(
            installed
                .into_iter()
                .all(|one| one.into_installed().is_ok())
        );
        let tools = self.tools();
        self.modules.runs.set_sub_agent_tools(tools.clone());
        tools
    }

    /// The Sub-agents of the same database, which call the same stub.
    fn sub_agents(&self) -> SubAgents {
        self.sub_agents_of(Arc::new(FixedModel::new(model_client(&self.model))))
    }

    /// The Sub-agents of the same database, which call what `models` gives.
    fn sub_agents_of(&self, models: Arc<dyn ModelSource>) -> SubAgents {
        let modules = self.modules.clone();
        let (tasks, commands, runs) = (modules.tasks, modules.commands, modules.runs);
        let (clock, stopping) = (Arc::new(self.clock.clone()), self.stop.subscribe());
        SubAgents::new(
            self.database.clone(),
            tasks,
            commands,
            runs,
            modules.search,
            models,
            clock,
            stopping,
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
    async fn turn(&self, tools: &dyn SubAgentTools, task_id: &TaskId) -> (String, Value) {
        let caller = self.caller();
        let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
        let started = result(call(tools, &caller, "agent", agent).await);
        let sub_agent_id = started
            .get("sub_agent_id")
            .and_then(Value::as_str)
            .unwrap()
            .to_owned();
        let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
        let waited = result(call(tools, &caller, "agent", wait).await);
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

    /// The state of the Sub-agent's one Turn, and when it started and ended.
    async fn turn_times(
        &self,
        sub_agent_id: &str,
    ) -> (String, Option<UnixNanos>, Option<UnixNanos>) {
        let columns = ["state", "started_at_unix_nanos", "ended_at_unix_nanos"];
        let statement = Query::select()
            .columns(columns.map(Alias::new))
            .from(Alias::new("sub_agent_turns"))
            .and_where(Expr::col(Alias::new("sub_agent_id")).eq(sub_agent_id))
            .to_owned();
        self.database
            .fetch_optional(statement)
            .await
            .unwrap()
            .unwrap()
    }

    fn caller(&self) -> LeadCaller {
        let (actor, workspace_id) = (self.actor.clone(), self.workspace_id.clone());
        LeadCaller {
            mcp: McpCaller::fixture(actor, workspace_id, UnixNanos::new(2)),
            modules: self.modules.clone(),
        }
    }

    /// The stub answers every request with `answer`.
    async fn model_answers(&self, answer: impl Respond + 'static) {
        Mock::given(any())
            .respond_with(answer)
            .mount(&self.model)
            .await;
    }
}

/// A model that every Workspace may use.
struct FixedModel(TurnModel);

impl FixedModel {
    fn new(client: ModelClient) -> Self {
        Self(TurnModel {
            id: uuid::Uuid::nil(),
            client: Arc::new(client),
        })
    }
}

#[async_trait::async_trait]
impl ModelSource for FixedModel {
    async fn current(&self, _workspace: &WorkspaceId) -> Result<TurnModel, ModelRefusal> {
        Ok(self.0.clone())
    }

    async fn allows(&self, _workspace: &WorkspaceId, _id: uuid::Uuid) -> Result<(), ModelRefusal> {
        Ok(())
    }
}

/// A client of the model that `server` stands in for.
fn model_client(server: &MockServer) -> ModelClient {
    model_client_with_window(server, None)
}

fn model_client_with_window(server: &MockServer, window: Option<u32>) -> ModelClient {
    ModelClient::new(
        coral_outbound::Client::new(POLICY, MODEL_CALL_TIMEOUT).unwrap(),
        Protocol::ChatCompletions,
        &format!("{}/v1", server.uri()).parse().unwrap(),
        MODEL.to_owned(),
        None,
        window,
        Map::new(),
    )
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

/// The lead as it calls: over MCP, and from a Run of its own.
struct LeadCaller {
    mcp: McpCaller,
    modules: McpModules,
}

/// Calls core's tool `name`, or a Sub-agent Tool from a Run of the lead's: in
/// the Task of `task_id` or `goal`, and asked to stop at once without
/// `wait_ms`. With `stop`, that Run was canceled.
async fn call(
    tools: &dyn SubAgentTools,
    caller: &LeadCaller,
    name: &'static str,
    arguments: Value,
) -> Result<Value, ToolCallError> {
    let Value::Object(mut arguments) = arguments else {
        panic!("tool arguments are an object");
    };
    let Some(tool) = BuiltinTool::from_key(name) else {
        let request = CallToolRequestParams::new(name).with_arguments(arguments);
        let core = caller.modules.core_interface();
        return match core.call(&caller.mcp, request, None).await? {
            CallToolResponse::Complete(result) => Ok(result.structured_content.unwrap()),
            _ => panic!("{name} completes"),
        };
    };
    let (actor, workspace_id) = (caller.mcp.principal(), caller.mcp.workspace_id());
    let intent = TaskIntent::parse("Call the agent").unwrap();
    let task_id = match (arguments.remove("task_id"), arguments.remove("goal")) {
        (Some(task_id), _) => Some(TaskId::parse(task_id.as_str().unwrap()).unwrap()),
        (None, Some(goal)) => {
            let goal = TaskIntent::parse(goal.as_str().unwrap()).unwrap();
            let started = caller
                .modules
                .tasks
                .start(actor, workspace_id, goal, UnixNanos::new(2));
            Some(started.await.unwrap().id().clone())
        }
        // The call starts no Sub-agent, so its Run needs no Task.
        (None, None) => None,
    };
    let run_id = match task_id.clone() {
        Some(task_id) => {
            let call = AttributedCommand::new(actor.clone(), workspace_id.clone(), task_id, intent);
            let run = caller.modules.commands.submit(&call, "return 0;", 60_000);
            run.await.unwrap().run_id().clone()
        }
        None => RunId::parse("a-run-of-the-leads").unwrap(),
    };
    let waits = arguments.get("wait_ms").is_some_and(|wait| wait != 0);
    let canceled = arguments.remove("stop").is_some();
    let run = SubAgentCaller {
        actor,
        workspace_id,
        run_id: &run_id,
        stopped: Box::pin(async move {
            if canceled {
                return RunStop::Canceled;
            }
            if waits {
                std::future::pending::<()>().await;
            }
            RunStop::Ended
        }),
    };
    let refused =
        |reason: &str| ToolCallError::Protocol(ErrorData::invalid_params(reason.to_owned(), None));
    let outcome = match BuiltinArguments::parse(tool, &Value::Object(arguments)) {
        Ok(BuiltinArguments::Agent(arguments)) => tools.agent(run, &arguments).await,
        Ok(BuiltinArguments::CancelAgent(arguments)) => tools.cancel(run, &arguments).await,
        Ok(_) => panic!("{name} is no Sub-agent Tool"),
        Err(problems) => {
            return Err(refused(
                problems.first().map_or("", ArgumentProblem::reason),
            ));
        }
    };
    let mut result = match outcome {
        Ok(result) => serde_json::to_value(result).unwrap(),
        Err(SubAgentError::InvalidInput { reason, .. } | SubAgentError::Unavailable(reason)) => {
            return Err(refused(reason));
        }
        Err(SubAgentError::Host(error)) => panic!("{error}"),
    };
    if let (Some(task_id), Some(fields)) = (task_id, result.as_object_mut()) {
        fields.insert("task_id".to_owned(), json!(task_id.as_str()));
    }
    Ok(result)
}

/// The structured result of a call that Coral completed.
fn result(called: Result<Value, ToolCallError>) -> Value {
    match called {
        Ok(result) => result,
        Err(ToolCallError::Protocol(error)) => panic!("refused: {}", error.message),
        Err(ToolCallError::Tool(_)) => panic!("failed"),
    }
}

/// The field `name` of a result, or null.
fn field<'a>(result: &'a Value, name: &str) -> &'a Value {
    result.get(name).unwrap_or(&Value::Null)
}

/// A Sub-agent's page without its `next_cursor`, which names its Turn.
fn without_cursor(mut page: Value) -> Value {
    page.as_object_mut().unwrap().remove("next_cursor");
    page
}

/// The message of a call that the protocol refused.
fn refusal(called: Result<Value, ToolCallError>) -> String {
    match called {
        Err(ToolCallError::Protocol(error)) => error.message.into_owned(),
        _ => panic!("the call is refused"),
    }
}

// Catches: a Program that cannot reach the Sub-agent, a Sub-agent outside the
// Task of the Run that called it, and a call that returns before the Turn ends.
#[tokio::test]
async fn coral_agent_returns_the_sub_agents_answer_to_the_program_that_calls_it() {
    let lead = Lead::new().await;
    lead.model_answers(reply("Twelve.")).await;
    let (tools, task_id) = (lead.served_tools().await, lead.task().await);
    let program = format!("return (await coral.agent({{ message: '{MESSAGE}' }})).data;");
    let exec = json!({"task_id": task_id.as_str(), "intent": "Delegate", "program": program, "wait_ms": 30_000});

    let run = result(call(&*tools, &lead.caller(), "exec", exec).await);

    let answered = run.pointer("/final_value/value").unwrap_or(&Value::Null);
    let completed = (&json!("completed"), &json!("Twelve."));
    assert_eq!(
        (field(answered, "state"), field(answered, "answer")),
        completed,
        "{run}"
    );
    let sub_agent_id = field(answered, "sub_agent_id").as_str().unwrap();
    let in_task = repository::task_id(&*lead.database, &lead.workspace_id, sub_agent_id);
    assert_eq!(in_task.await.unwrap(), Some(task_id));
}

// Catches: a Sub-agent whose Program starts another Sub-agent, and a refusal
// that the Program cannot read.
#[tokio::test]
async fn a_program_that_a_sub_agent_started_cannot_call_coral_agent() {
    let lead = Lead::new().await;
    let (tools, task_id) = (lead.served_tools().await, lead.task().await);
    let program = "try { return await coral.agent({ message: 'Deeper' }); } catch (error) { return [error.code, error.message]; }";
    let exec = json!({"task_id": task_id.as_str(), "intent": "Delegate", "program": program});

    let run = lead.first_result(&*tools, &task_id, tool_call("call_1", "exec", &exec));
    let run = run.await;

    let refused =
        "Tool is unavailable: an agent started this run, and an agent cannot call an agent";
    assert_eq!(
        run.get("final_value"),
        Some(&json!(["tool_unavailable", refused])),
        "{run}"
    );
    let sub_agents = Query::select()
        .column(Alias::new("id"))
        .from(Alias::new("sub_agent_records"))
        .to_owned();
    let sub_agents: Vec<(String,)> = lead.database.fetch_all(sub_agents).await.unwrap();
    assert_eq!(sub_agents.len(), 1, "nothing started");
    assert_eq!(lead.model.received_requests().await.unwrap().len(), 2);
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

        let failed = json!({"sub_agent_id": sub_agent_id, "state": "failed", "failure": failure, "steps": []});
        assert_eq!(without_cursor(waited), failed);
        let requests = lead.model.received_requests().await.unwrap();
        assert_eq!(requests.len(), model_calls);
        lead.model.reset().await;
    }
}

// Catches: a Run that the Sub-agent starts outside the lead's Task, an
// answer read before the Run's result reached the model, a result stored
// without its Run, and a thread that loses, repeats or reorders a message or
// a model call's record.
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
    let completed = (&json!("completed"), &json!(answer));
    assert_eq!(
        (field(&waited, "state"), field(&waited, "answer")),
        completed
    );
    let tasks = &lead.modules.tasks;
    let attempts = tasks.interactions(&lead.actor, &lead.workspace_id, &task_id, None, 10);
    let attempts = attempts.await.unwrap();
    let [_the_leads_run, attempt] = attempts.as_slice() else {
        panic!("the lead's Run, then one attempt: {attempts:?}");
    };
    assert_eq!(attempt.kind, AttemptKind::Submit);
    let thread = lead.thread(&sub_agent_id).await;
    let result = match thread.get(2).map(|results| results.content.as_slice()) {
        Some([Block::ToolResult { content, .. }]) => content.clone(),
        _ => panic!("the third message is the result of exec: {thread:?}"),
    };
    let brief = format!(
        "task_id: {}\ndatetime: 2026-11-18 12:00 UTC (Wednesday)\n\n{MESSAGE}",
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
            Message::tool_results(vec![exec.result(result, false, attempt.run.clone())]),
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
        let waited = sub_agents.wait(actor, workspace_id, &sub_agent_id, None, Duration::ZERO);
        let waited = waited.await;
        assert!(matches!(waited, Err(Error::SubAgentNotFound)), "{waited:?}");
        let canceled = sub_agents.cancel(actor, workspace_id, &sub_agent_id).await;
        assert!(
            matches!(canceled, Err(Error::SubAgentNotFound)),
            "{canceled:?}"
        );
    }

    let unchanged = sub_agents.wait(actor, workspace_id, &sub_agent_id, None, Duration::ZERO);
    assert_eq!(unchanged.await.unwrap().state, TurnState::Running);
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
    let second = repository::insert_turn(
        &mut transaction,
        workspace_id,
        sub_agent_id,
        "2",
        NOON,
        lease_end,
    )
    .await;
    assert!(second.is_err_and(|error| error.is_unique_violation()));
    drop(transaction);
    let running = result(call(&*tools, &caller, "agent", by_id.clone()).await);
    assert_eq!(
        without_cursor(running),
        json!({"sub_agent_id": sub_agent_id, "state": "running", "steps": []})
    );

    let canceled = result(call(&*tools, &caller, "cancelAgent", by_id.clone()).await);

    let stopped = json!({"sub_agent_id": sub_agent_id, "state": "canceled", "steps": []});
    assert_eq!(without_cursor(canceled), stopped);
    let renewed = repository::renew_lease(database, workspace_id, &turn_id, lease_end).await;
    assert!(!renewed.unwrap(), "the next renewal stops the model call");
    let late = TurnState::Completed {
        answer: "late".to_owned(),
    };
    let ended = sub_agents.end_turn(workspace_id, sub_agent_id, &turn_id, &late);
    assert!(!ended.await.unwrap(), "the first ending stands");
    assert_eq!(
        without_cursor(result(call(&*tools, &caller, "agent", by_id).await)),
        stopped
    );
}

// Catches: a Turn that runs on, or reads as running, once its Coral Instance
// begins to shut down.
#[tokio::test]
async fn shutdown_interrupts_a_running_turn() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let (tools, caller) = (lead.tools(), lead.caller());
    let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
    let started = result(call(&*tools, &caller, "agent", agent).await);
    let sub_agent_id = started.get("sub_agent_id").unwrap();
    let endless = json!({"task_id": started.get("task_id"), "intent": "Loop", "program": "while (true) {}", "wait_ms": 0});
    let endless = result(call(&*tools, &caller, "exec", endless).await);
    let run_id = field(&endless, "run_id");
    lead.exec_step(sub_agent_id, "Loop", run_id).await;

    lead.stop.send_replace(true);

    let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
    let waited = result(call(&*tools, &caller, "agent", wait).await);
    let steps = json!([{"intent": "Loop", "run_id": run_id}]);
    let interrupted = json!({"sub_agent_id": sub_agent_id, "state": "interrupted", "steps": steps});
    assert_eq!(without_cursor(waited), interrupted);
    let run = json!({"run_id": run_id, "wait_ms": 1_000});
    let run = result(call(&*tools, &caller, "wait", run).await);
    assert_eq!(
        field(&run, "state"),
        "running",
        "the Turn left its Run alone"
    );
}

// Catches: a Turn that calls the model again after the lead ended its Task,
// or that then cancels the Runs it started.
#[tokio::test]
async fn a_turn_whose_task_ends_stops_as_canceled() {
    let lead = Lead::new().await;
    let (tools, caller, task_id) = (lead.tools(), lead.caller(), lead.task().await);
    let endless = json!({"task_id": task_id.as_str(), "intent": "Loop", "program": "while (true) {}", "wait_ms": 0});
    Mock::given(any())
        .respond_with(tool_calls(&[tool_call("call_1", "exec", &endless)]))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    let nothing = tool_calls(&[tool_call("call_2", "nothing", &json!({}))]);
    let (arrived, release) = lead.hold_first_reply(nothing.clone()).await;
    lead.model_answers(nothing).await;
    let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
    let started = result(call(&*tools, &caller, "agent", agent).await);
    arrived.await.unwrap();

    let end = json!({"task_id": task_id.as_str(), "outcome": "success"});
    result(call(&*tools, &caller, "end_task", end).await);
    release.send(()).unwrap();

    let wait = json!({"sub_agent_id": field(&started, "sub_agent_id"), "wait_ms": 30_000});
    let waited = result(call(&*tools, &caller, "agent", wait).await);
    assert_eq!(field(&waited, "state"), "canceled", "{waited}");
    assert_eq!(lead.model.received_requests().await.unwrap().len(), 2);
    let run = json!({"run_id": waited.pointer("/steps/0/run_id"), "wait_ms": 1_000});
    let run = result(call(&*tools, &caller, "wait", run).await);
    assert_eq!(field(&run, "state"), "running", "{run}");
}

// Catches: a Turn that calls the model again after its caller lost access to
// its Task.
#[tokio::test]
async fn a_turn_whose_caller_lost_access_stops_as_canceled() {
    let lead = Lead::new().await;
    let nothing = tool_calls(&[tool_call("call_1", "nothing", &json!({}))]);
    let (arrived, release) = lead.hold_first_reply(nothing.clone()).await;
    lead.model_answers(nothing).await;
    let (sub_agents, task_id, actor) = (lead.sub_agents(), lead.task().await, &lead.actor);
    let started = sub_agents.start(actor, &lead.workspace_id, &task_id, MESSAGE.to_owned());
    let sub_agent_id = started.await.unwrap();
    arrived.await.unwrap();

    let owner = provision_user(lead.database.clone(), "owner", OrganizationRole::Owner).await;
    let lead_id = actor.authenticated_user().unwrap().user_id();
    OrganizationModule::new(lead.database.clone())
        .set_enabled(&change(owner, 3, 'b'), lead_id, &IfMatch::any(), false)
        .await
        .unwrap();
    release.send(()).unwrap();

    let wait = Duration::from_secs(30);
    let waited = sub_agents.wait(actor, &lead.workspace_id, &sub_agent_id, None, wait);
    assert_eq!(waited.await.unwrap().state, TurnState::Canceled);
    assert_eq!(lead.model.received_requests().await.unwrap().len(), 1);
}

// Catches: a Run that goes on after its Sub-agent's Turn was stopped, by
// `cancelAgent`, by a cancellation of the Run that waits on the Sub-agent, or
// by the Turn's time limit. Also a stop that changes a Run that had ended,
// one that stores the reply of the Turn's last model call, and a call that
// waits past its `wait_ms`.
#[tokio::test]
async fn stopping_a_sub_agent_cancels_the_runs_its_turn_left_running() {
    enum Stop {
        CancelAgent,
        CanceledRun,
        TimeLimit,
    }
    for (stop, ended_as) in [
        (Stop::CancelAgent, "canceled"),
        (Stop::CanceledRun, "canceled"),
        (Stop::TimeLimit, "failed"),
    ] {
        let lead = Lead::new().await;
        let (tools, caller, task_id) = (lead.tools(), lead.caller(), lead.task().await);
        let exec = |id, intent, program, wait_ms: u64| {
            let task_id = task_id.as_str();
            let arguments = json!({"task_id": task_id, "intent": intent, "program": program, "wait_ms": wait_ms});
            tool_call(id, "exec", &arguments)
        };
        let execs = [
            exec("call_1", "Loop", "while (true) {}", 0),
            exec("call_2", "Count", "return 1;", 30_000),
        ];
        Mock::given(any())
            .respond_with(tool_calls(&execs))
            .up_to_n_times(1)
            .mount(&lead.model)
            .await;
        let (arrived, release) = lead.hold_first_reply(reply("Done.")).await;
        let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
        let started = result(call(&*tools, &caller, "agent", agent).await);
        arrived.await.unwrap();
        let sub_agent_id = field(&started, "sub_agent_id");
        let asked_at = std::time::Instant::now();
        let look = json!({"sub_agent_id": sub_agent_id, "wait_ms": 50});
        let running = result(call(&*tools, &caller, "agent", look).await);
        assert!(asked_at.elapsed() < Duration::from_secs(10), "its wait_ms");

        match stop {
            Stop::CancelAgent => {
                let by_id = json!({"sub_agent_id": sub_agent_id});
                result(call(&*tools, &caller, "cancelAgent", by_id).await);
            }
            Stop::CanceledRun => {
                let waiting = json!({"sub_agent_id": sub_agent_id, "stop": "canceled"});
                result(call(&*tools, &caller, "agent", waiting).await);
            }
            Stop::TimeLimit => {
                tokio::time::pause();
                tokio::time::advance(TURN_MAX).await;
                tokio::time::resume();
            }
        }
        let ended = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
        let ended = result(call(&*tools, &caller, "agent", ended).await);
        release.send(()).unwrap();

        assert_eq!(field(&ended, "state"), ended_as, "{ended}");
        let steps = field(&running, "steps");
        let (looped, counted) = (field(&steps[0], "run_id"), field(&steps[1], "run_id"));
        let wait = json!({"run_id": looped, "wait_ms": 30_000});
        let looped = result(call(&*tools, &caller, "wait", wait).await);
        assert_eq!(field(&looped, "state"), "canceled", "{looped}");
        let read = json!({"run_id": counted});
        let counted = result(call(&*tools, &caller, "wait", read).await);
        assert_eq!(field(&counted, "state"), "succeeded", "{counted}");
        let sub_agent_id = sub_agent_id.as_str().unwrap();
        assert_eq!(
            lead.thread(sub_agent_id).await.len(),
            3,
            "brief, calls, results"
        );
    }
}

// Catches: a cancel of a Sub-agent whose Turn has ended that leaves the
// Turn's Run running, because the Turn no longer holds it in memory.
#[tokio::test]
async fn cancel_cancels_the_runs_of_a_turn_that_has_ended() {
    let lead = Lead::new().await;
    let (tools, caller, task_id) = (lead.tools(), lead.caller(), lead.task().await);
    let endless = json!({"task_id": task_id.as_str(), "intent": "Loop", "program": "while (true) {}", "wait_ms": 0});
    Mock::given(any())
        .respond_with(tool_calls(&[tool_call("call_1", "exec", &endless)]))
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    lead.model_answers(reply("It is still running.")).await;
    let (sub_agent_id, completed) = lead.turn(&*tools, &task_id).await;
    assert_eq!(field(&completed, "state"), "completed", "{completed}");

    let by_id = json!({"sub_agent_id": sub_agent_id});
    result(call(&*tools, &caller, "cancelAgent", by_id).await);

    let steps = field(&completed, "steps");
    let wait = json!({"run_id": field(&steps[0], "run_id"), "wait_ms": 30_000});
    let run = result(call(&*tools, &caller, "wait", wait).await);
    assert_eq!(field(&run, "state"), "canceled", "{run}");
}

// Catches: a Run that goes on after the lead canceled the Sub-agent because
// the Sub-agent's `exec` still waited on it, and a reply right after the
// cancel whose Turn reads that `exec` without a result. The renewal that
// stops the Turn comes 10 seconds after it starts.
#[tokio::test]
async fn cancel_cancels_the_run_that_an_exec_still_waits_on() {
    let lead = Lead::new().await;
    let (tools, caller, task_id) = (lead.tools(), lead.caller(), lead.task().await);
    let paid = "Count only the paid invoices.";
    let answers = Mock::given(body_string_contains(paid)).respond_with(reply("Seven."));
    answers.mount(&lead.model).await;
    let endless = json!({"task_id": task_id.as_str(), "intent": "Loop", "program": "while (true) {}", "wait_ms": 30_000});
    let exec = tool_calls(&[tool_call("call_1", "exec", &endless)]);
    Mock::given(any())
        .respond_with(exec)
        .up_to_n_times(1)
        .mount(&lead.model)
        .await;
    lead.model_answers(no_reply()).await;
    let agent = json!({"task_id": task_id.as_str(), "message": MESSAGE});
    let started = result(call(&*tools, &caller, "agent", agent).await);
    let (tasks, deadline) = (
        &lead.modules.tasks,
        tokio::time::Instant::now() + EVENTUALLY,
    );
    let mut changes = lead.database.wait_for_changes(deadline);
    let run_id = loop {
        let read_at = tokio::time::Instant::now();
        let attempts = tasks.interactions(&lead.actor, &lead.workspace_id, &task_id, None, 10);
        if let Some(run_id) = attempts
            .await
            .unwrap()
            .into_iter()
            .filter_map(|attempt| attempt.run)
            .nth(1)
        {
            break run_id;
        }
        assert!(changes.pause(read_at).await, "the exec started no Run");
    };

    let sub_agent_id = field(&started, "sub_agent_id");
    result(
        call(
            &*tools,
            &caller,
            "cancelAgent",
            json!({"sub_agent_id": sub_agent_id}),
        )
        .await,
    );
    let message = json!({"sub_agent_id": sub_agent_id, "message": paid});
    result(call(&*tools, &caller, "agent", message).await);
    let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
    let answered = result(call(&*tools, &caller, "agent", wait).await);

    assert_eq!(field(&answered, "answer"), "Seven.", "{answered}");
    let requests = lead.model.received_requests().await.unwrap();
    let second = requests
        .get(1)
        .map(|request| String::from_utf8_lossy(&request.body));
    assert!(second.is_some_and(|body| body.contains("No result")));
    let wait = json!({"run_id": run_id.as_str(), "wait_ms": 30_000});
    let waited = result(call(&*tools, &caller, "wait", wait).await);
    assert_eq!(field(&waited, "state"), "canceled", "{waited}");
}

// Catches: a Turn that reads as running for ever once its Coral Instance
// stopped without shutting down, and one interrupted while its lease holds.
#[tokio::test]
async fn the_next_read_interrupts_a_turn_whose_lease_lapsed() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let (tools, caller) = (lead.tools(), lead.caller());
    let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
    let started = result(call(&*tools, &caller, "agent", agent).await);
    let sub_agent_id = started.get("sub_agent_id").and_then(Value::as_str);
    let sub_agent_id = sub_agent_id.unwrap();
    let by_id = json!({"sub_agent_id": sub_agent_id});
    let sub_agents = lead.sub_agents();
    let turn = sub_agents.turn(&lead.actor, &lead.workspace_id, sub_agent_id);
    let lease_end = turn.await.unwrap().lease_expires_at;

    lead.clock.set(UnixNanos::new(lease_end.get() - 1));
    let held = result(call(&*tools, &caller, "agent", by_id.clone()).await);
    assert_eq!(held.get("state"), Some(&json!("running")));
    lead.clock.set(UnixNanos::new(i64::MAX));
    let lapsed = result(call(&*tools, &caller, "cancelAgent", by_id).await);

    let interrupted = json!({"sub_agent_id": sub_agent_id, "state": "interrupted", "steps": []});
    assert_eq!(without_cursor(lapsed), interrupted);
}

/// A clock that reads one nanosecond later each time.
#[derive(Debug)]
struct Ticking(AtomicI64);

impl Clock for Ticking {
    fn now(&self) -> UnixNanos {
        UnixNanos::new(self.0.fetch_add(1, Ordering::SeqCst))
    }
}

// Catches: a wait that reads a Turn as running until its own deadline after
// the Turn's lease lapsed, since no write marks the lapse.
#[tokio::test]
async fn a_wait_returns_once_the_lease_of_its_turn_lapses() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
    let started = result(call(&*lead.tools(), &lead.caller(), "agent", agent).await);
    let mut dependencies = lead.dependencies();
    let lease_end = NOON.saturating_add(TURN_LEASE).get();
    dependencies.clock = Arc::new(Ticking(AtomicI64::new(lease_end - 1)));
    let other = SubAgentFactory.build(dependencies);

    let wait = json!({"sub_agent_id": field(&started, "sub_agent_id"), "wait_ms": 30_000});
    let asked_at = tokio::time::Instant::now();
    let waited = result(call(&*other.unwrap(), &lead.caller(), "agent", wait).await);
    assert!(
        asked_at.elapsed() < Duration::from_secs(30),
        "the wait ran out"
    );
    assert_eq!(field(&waited, "state"), "interrupted", "{waited}");
}

// Catches: an ending that records no end time, or a start or an end dated by
// another clock reading than the one that started or ended the Turn.
#[tokio::test]
async fn a_turn_records_when_it_started_and_when_it_ended() {
    let held = NOON.saturating_add(Duration::from_secs(10));
    let lapsed = UnixNanos::new(i64::MAX);
    for (answer, state, ended_at) in [
        (reply("Twelve."), "completed", held),
        (ResponseTemplate::new(400), "failed", held),
        (no_reply(), "canceled", held),
        (no_reply(), "interrupted", lapsed),
    ] {
        let lead = Lead::new().await;
        let (sub_agents, task_id) = (lead.sub_agents(), lead.task().await);
        let (actor, workspace_id) = (&lead.actor, &lead.workspace_id);
        let clock = lead.clock.clone();
        // The model answers once the clock reads the end.
        let answer = move |_: &Request| {
            clock.set(ended_at);
            answer.clone()
        };
        lead.model_answers(answer).await;
        let started = sub_agents.start(actor, workspace_id, &task_id, MESSAGE.to_owned());
        let sub_agent_id = started.await.unwrap();

        lead.clock.set(ended_at);
        if state == "canceled" {
            let running = ("running".to_owned(), Some(NOON), None);
            assert_eq!(lead.turn_times(&sub_agent_id).await, running);
            let canceled = sub_agents.cancel(actor, workspace_id, &sub_agent_id);
            canceled.await.unwrap();
        }
        let wait = Duration::from_secs(30);
        let waited = sub_agents.wait(actor, workspace_id, &sub_agent_id, None, wait);
        waited.await.unwrap();

        let ended = (state.to_owned(), Some(NOON), Some(ended_at));
        assert_eq!(lead.turn_times(&sub_agent_id).await, ended);
    }
}

// Catches: an interrupted Turn that leaves tool calls without results, which
// the model refuses in the thread of the next Turn.
#[tokio::test]
async fn an_interrupted_turn_closes_the_calls_it_left_without_results() {
    let lead = Lead::new().await;
    lead.model_answers(no_reply()).await;
    let (sub_agents, task_id) = (lead.sub_agents(), lead.task().await);
    let (actor, workspace_id) = (&lead.actor, &lead.workspace_id);
    let started = sub_agents.start(actor, workspace_id, &task_id, MESSAGE.to_owned());
    let sub_agent_id = started.await.unwrap();
    let turn = sub_agents
        .turn(actor, workspace_id, &sub_agent_id)
        .await
        .unwrap();
    // The Turn's instance stored the model's calls, then stopped.
    let calls = [
        tool_call("a", "exec", &json!({})),
        tool_call("b", "search", &json!({})),
    ];
    let asked = Message {
        role: Role::Assistant,
        content: calls.iter().cloned().map(Block::ToolUse).collect(),
    };
    let task = lead.modules.tasks.get(actor, workspace_id, &task_id);
    let task = task.await.unwrap();
    let running = RunningTurn {
        trace: TurnTrace::begin(&sub_agent_id, actor, workspace_id, task.telemetry_id()),
        model: FixedModel::new(model_client(&lead.model)).0,
        id: turn.id,
        sub_agent_id: sub_agent_id.clone(),
        caller: Caller {
            principal: actor.clone(),
            workspace_id: workspace_id.clone(),
            task_id,
            started_runs: std::sync::Mutex::default(),
        },
    };
    let mut transaction = lead.database.begin().await.unwrap();
    repository::insert_message(&mut transaction, &running, 1, &asked, None)
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    lead.clock.set(UnixNanos::new(i64::MAX));
    let read = sub_agents.wait(actor, workspace_id, &sub_agent_id, None, Duration::ZERO);
    assert_eq!(read.await.unwrap().state, TurnState::Interrupted);

    let no_result = "No result: the Turn stopped before this call returned. A Run that the call started may still be running.";
    let closed = calls
        .iter()
        .map(|call| call.result(no_result.to_owned(), true, None));
    let thread = lead.thread(&sub_agent_id).await;
    let after_the_brief = [asked, Message::tool_results(closed.collect())];
    assert_eq!(thread.get(1..), Some(&after_the_brief[..]));
}

mod search;
mod steps_and_steering;
