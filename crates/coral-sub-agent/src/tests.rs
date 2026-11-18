use std::sync::Arc;
use std::time::Duration;

use coral_app::{McpInterfaceDependencies, McpInterfaceFactory as _};
use coral_db::{Database, SystemClock, UnixNanos};
use coral_mcp::{HumanInterfaceUrl, McpCaller, McpInterface, McpModules, ToolCallError};
use coral_outbound::{DestinationPolicy, NetworkAccess};
use coral_provider::ProviderModule;
use coral_run::{CoralInstanceId, RunInbox, RunModule, RuntimeLimits};
use coral_search::UniversalSearchModule;
use coral_task::{TaskCommands, TaskIntent, TaskModule};
use coral_test_support::{
    EVENTUALLY, TEST_COMPILATION_TIMEOUT, TestDatabase, change, provision_user,
};
use coral_workspace::{
    AuthenticatedPrincipal, AuthenticatedUser, OrganizationRole, WorkspaceDisplayName, WorkspaceId,
    WorkspaceModule,
};
use rmcp::model::{CallToolRequestParams, CallToolResponse, Tool};
use serde_json::{Value, json};
use wiremock::matchers::{any, body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::model::ModelClient;
use crate::sub_agents::{Error, SubAgents, TurnState};
use crate::{MODEL_CALL_TIMEOUT, SubAgentFactory, repository};

const POLICY: DestinationPolicy =
    DestinationPolicy::new(NetworkAccess::HttpAndHttps, NetworkAccess::Deny);
const MODEL: &str = "sailfish-1";
const MESSAGE: &str = "How many invoices are open?";

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
            clock: Arc::new(SystemClock),
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
        let tasks = self.modules.tasks.clone();
        SubAgents::new(self.database.clone(), tasks, model, Arc::new(SystemClock))
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
    ResponseTemplate::new(200).set_body_json(json!({
        "choices": [{"message": {"role": "assistant", "content": text}}],
    }))
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

// Catches: an answer that is not the model's reply to the system prompt and
// the lead's message, and a Turn that hides why the model gave no reply.
#[tokio::test]
async fn a_turn_ends_with_the_models_reply_or_the_reason_it_gave_none() {
    let lead = Lead::new().await;
    let (tools, caller) = (lead.tools(), lead.caller());
    let conversation = json!({
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "You are a Sub-agent that Coral runs for another agent, your lead. The lead sent you one message. Answer it in one reply. You have no tools, so say what you cannot know instead of guessing."},
            {"role": "user", "content": MESSAGE},
        ],
    });
    let refused = "the model server refused the request with status 400 Bad Request";
    for (answer, state, text) in [
        (reply("Twelve."), "completed", ("answer", "Twelve.")),
        (
            ResponseTemplate::new(400).set_body_string("the server's own detail"),
            "failed",
            ("failure", refused),
        ),
    ] {
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .and(body_json(&conversation))
            .respond_with(answer)
            .mount(&lead.model)
            .await;
        let agent = json!({"goal": "Count the invoices", "message": MESSAGE});
        let started = result(call(&*tools, &caller, "agent", agent).await);

        let sub_agent_id = started.get("sub_agent_id").unwrap();
        let wait = json!({"sub_agent_id": sub_agent_id, "wait_ms": 30_000});
        let waited = result(call(&*tools, &caller, "wait", wait).await);

        let ended = json!({"sub_agent_id": sub_agent_id, "state": state, text.0: text.1});
        assert_eq!(waited, ended);
        lead.model.reset().await;
    }
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
