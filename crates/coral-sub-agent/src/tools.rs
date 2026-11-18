//! What the lead is served: core's tools, the `agent` tool, and a `wait` and
//! a `cancel` that also take a `sub_agent_id`.

use std::sync::Arc;
use std::time::Duration;

use coral_mcp::{
    HumanInterfaceUrl, MAX_WAIT_MS, McpCaller, McpInterface, Progress, ToolCallError, ToolFailure,
};
use coral_task::{TaskId, TaskIntent, TaskModule};
use rmcp::ErrorData;
use rmcp::handler::server::tool::schema_for_type;
use rmcp::model::{CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject, Tool};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Map, Value, json};

use crate::lead_text::{self, AGENT_DESCRIPTION};
use crate::run_tools::{run_failure, task_failure};
use crate::sub_agents::{Error, SubAgents, TurnPage};

mod page;

/// Core's tool surface with one more tool, `agent`, and a `wait` and a
/// `cancel` that also take a `sub_agent_id`.
pub(crate) struct CoreWithAgent {
    core: Arc<dyn McpInterface>,
    tasks: TaskModule,
    sub_agents: SubAgents,
    /// Where a required action's page is.
    human_interface: HumanInterfaceUrl,
}

impl CoreWithAgent {
    pub(crate) fn new(
        core: Arc<dyn McpInterface>,
        tasks: TaskModule,
        sub_agents: SubAgents,
        human_interface: HumanInterfaceUrl,
    ) -> Self {
        Self {
            core,
            tasks,
            sub_agents,
            human_interface,
        }
    }
}

#[async_trait::async_trait]
impl McpInterface for CoreWithAgent {
    fn instructions(&self) -> &str {
        lead_text::INSTRUCTIONS
    }

    async fn tools(&self, caller: &McpCaller) -> Result<Vec<Tool>, ErrorData> {
        let mut tools = self.core.tools(caller).await?;
        for tool in &mut tools {
            match tool.name.as_ref() {
                "wait" => accept_sub_agent_id(tool, WAIT_BY_SUB_AGENT_ID),
                "cancel" => accept_sub_agent_id(tool, CANCEL_BY_SUB_AGENT_ID),
                _ => {}
            }
        }
        let agent_arguments = schema_for_type::<AgentArguments>();
        tools.push(Tool::new("agent", AGENT_DESCRIPTION, agent_arguments));
        Ok(tools)
    }

    async fn call(
        &self,
        caller: &McpCaller,
        request: CallToolRequestParams,
        progress: Option<Progress>,
    ) -> Result<CallToolResponse, ToolCallError> {
        let names_sub_agent = request
            .arguments
            .as_ref()
            .is_some_and(|arguments| arguments.contains_key("sub_agent_id"));
        match request.name.as_ref() {
            "agent" => {
                let arguments = parse(request.arguments, AGENT_TAKES)?;
                self.agent(caller, arguments).await
            }
            "wait" if names_sub_agent => {
                let arguments = parse(request.arguments, WAIT_TAKES)?;
                self.wait(caller, arguments).await
            }
            "cancel" if names_sub_agent => {
                let arguments = parse(request.arguments, CANCEL_TAKES)?;
                self.cancel(caller, arguments).await
            }
            _ => self.core.call(caller, request, progress).await,
        }
    }
}

impl CoreWithAgent {
    async fn agent(
        &self,
        caller: &McpCaller,
        arguments: AgentArguments,
    ) -> Result<CallToolResponse, ToolCallError> {
        if arguments.message.trim().is_empty() {
            return Err(invalid("`message` must not be blank"));
        }
        let (actor, workspace_id) = (caller.principal(), caller.workspace_id());
        let (sub_agent_id, task_id) = match (arguments.sub_agent_id, arguments.task_id) {
            (Some(sub_agent_id), None) if arguments.goal.is_none() => {
                let messaged =
                    self.sub_agents
                        .message(actor, workspace_id, &sub_agent_id, &arguments.message);
                messaged.await?;
                (sub_agent_id, None)
            }
            (Some(_), _) => return Err(invalid(AGENT_TAKES)),
            (None, task_id) => {
                let task_id = self.task(caller, task_id, arguments.goal).await?;
                let started =
                    self.sub_agents
                        .start(actor, workspace_id, &task_id, arguments.message);
                (started.await?, Some(task_id))
            }
        };
        let page = self.sub_agents.page(actor, workspace_id, &sub_agent_id);
        let mut fields = self.page_fields(&sub_agent_id, &page.await?);
        if let Some(task_id) = task_id {
            fields.insert("task_id".to_owned(), json!(task_id.as_str()));
        }
        Ok(result(Value::Object(fields)))
    }

    /// The Task that a new Sub-agent works in. As in core's `exec`, a
    /// `task_id` wins over a `goal`, which starts a new Task.
    async fn task(
        &self,
        caller: &McpCaller,
        task_id: Option<String>,
        goal: Option<String>,
    ) -> Result<TaskId, ToolCallError> {
        let (actor, workspace_id) = (caller.principal(), caller.workspace_id());
        match (task_id, goal) {
            (Some(task_id), _) => TaskId::parse(&task_id).map_err(invalid),
            (None, Some(goal)) => {
                let goal = TaskIntent::parse(&goal).map_err(invalid)?;
                let started = self.tasks.start(actor, workspace_id, goal, caller.now());
                let task = started.await.map_err(Error::from)?;
                Ok(task.id().clone())
            }
            (None, None) => Err(invalid(AGENT_TAKES)),
        }
    }

    async fn wait(
        &self,
        caller: &McpCaller,
        arguments: WaitArguments,
    ) -> Result<CallToolResponse, ToolCallError> {
        let wait_ms = arguments.wait_ms.unwrap_or_default();
        if wait_ms > MAX_WAIT_MS {
            return Err(invalid(format!("wait_ms must be at most {MAX_WAIT_MS}")));
        }
        let sub_agent_id = &arguments.sub_agent_id;
        let waited = self.sub_agents.wait(
            caller.principal(),
            caller.workspace_id(),
            sub_agent_id,
            arguments.cursor.as_deref(),
            Duration::from_millis(wait_ms),
        );
        let fields = self.page_fields(sub_agent_id, &waited.await?);
        Ok(result(Value::Object(fields)))
    }

    async fn cancel(
        &self,
        caller: &McpCaller,
        arguments: CancelArguments,
    ) -> Result<CallToolResponse, ToolCallError> {
        let sub_agent_id = &arguments.sub_agent_id;
        let canceled =
            self.sub_agents
                .cancel(caller.principal(), caller.workspace_id(), sub_agent_id);
        let fields = self.page_fields(sub_agent_id, &canceled.await?);
        Ok(result(Value::Object(fields)))
    }

    /// What `agent`, `wait` and `cancel` return for a Sub-agent.
    fn page_fields(&self, sub_agent_id: &str, page: &TurnPage) -> Map<String, Value> {
        let mut fields = Map::new();
        fields.insert("sub_agent_id".to_owned(), json!(sub_agent_id));
        fields.extend(page::turn_fields(page, &self.human_interface));
        fields
    }
}

const AGENT_TAKES: &str = "agent takes `message`, and one of `sub_agent_id`, `task_id` or `goal`: pass `sub_agent_id` to reply to a Sub-agent, `task_id` to start one in a Task you started, or `goal` to start one in a new Task";

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AgentArguments {
    /// The `sub_agent_id` of a Sub-agent you started, to reply to it. It
    /// reads `message` before its next step, or in a new turn once its turn
    /// has ended.
    sub_agent_id: Option<String>,
    /// What the user asked for, in one short phrase. Starts a new Task for
    /// this unit of user work, unless `task_id` is given.
    goal: Option<String>,
    /// The `task_id` of the Task that the Sub-agent works in. Given with
    /// `goal`, it wins and `goal` is ignored.
    task_id: Option<String>,
    /// Everything the Sub-agent is to do. It reads nothing else.
    message: String,
}

const SUB_AGENT_ID_DESCRIPTION: &str = "The sub_agent_id from agent, in place of run_id.";
const WAIT_BY_SUB_AGENT_ID: &str = "With `sub_agent_id` in place of `run_id`, observe that Sub-agent; pass the latest `next_cursor` from `agent` or `wait` on it as `cursor`. The call returns when its turn ends, it newly needs a person, or `wait_ms` runs out. It returns its `state`, its `steps`, its `answer` once `completed` and its `failure` once `failed`. `needs_action` means that the run of a step waits for a person: handle its `required_actions` as a run's. `interrupted` means that Coral stopped before the Sub-agent answered: call `agent` again with its `sub_agent_id`.";
const WAIT_TAKES: &str = "with `sub_agent_id`, wait takes only `cursor` and `wait_ms` beside it";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArguments {
    sub_agent_id: String,
    cursor: Option<String>,
    wait_ms: Option<u64>,
}

const CANCEL_BY_SUB_AGENT_ID: &str = "With `sub_agent_id` in place of `run_id`, stop that Sub-agent and return its `state` and `steps`.";
const CANCEL_TAKES: &str = "with `sub_agent_id`, cancel takes nothing beside it";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelArguments {
    sub_agent_id: String,
}

/// Lets core's `wait` or `cancel` take a `sub_agent_id` in place of its
/// `run_id`. `effect` says what the tool then does.
fn accept_sub_agent_id(tool: &mut Tool, effect: &str) {
    let mut arguments = tool.input_schema.as_ref().clone();
    if let Some(Value::Object(properties)) = arguments.get_mut("properties") {
        let sub_agent_id = json!({"type": "string", "description": SUB_AGENT_ID_DESCRIPTION});
        properties.insert("sub_agent_id".to_owned(), sub_agent_id);
    }
    if let Some(Value::Array(required)) = arguments.get_mut("required") {
        required.retain(|argument| argument.as_str() != Some("run_id"));
    }
    tool.input_schema = Arc::new(arguments);
    let for_runs = tool.description.as_deref().unwrap_or_default();
    tool.description = Some(format!("{for_runs} {effect}").into());
    // Core's schema describes a Run Update, and the result may now be a Turn.
    tool.output_schema = None;
}

/// Reads the arguments of a tool. `takes` says what the tool takes, and a
/// refusal repeats no value that the caller sent.
fn parse<Arguments: DeserializeOwned>(
    arguments: Option<JsonObject>,
    takes: &'static str,
) -> Result<Arguments, ToolCallError> {
    serde_json::from_value(Value::Object(arguments.unwrap_or_default()))
        .map_err(|_quotes_the_value| invalid(takes))
}

fn invalid(problem: impl std::fmt::Display) -> ToolCallError {
    ToolCallError::Protocol(ErrorData::invalid_params(problem.to_string(), None))
}

/// The failure that the lead reads when a Sub-agent call did nothing: a Task's
/// or a Run's as the model reads it.
impl From<Error> for ToolCallError {
    fn from(error: Error) -> Self {
        Self::Tool(match error {
            Error::CursorOfAnotherTurn => return invalid(error),
            Error::SubAgentNotFound => {
                ToolFailure::stable("sub_agent_not_found", "Sub-agent was not found")
            }
            // The lead can start again, which the model cannot.
            Error::Task(coral_task::Error::TaskClosed) => ToolFailure::stable(
                "task_closed",
                "Task is closed. Call `agent` with `goal` to start a new Task.",
            ),
            Error::Task(error) => task_failure(error).into(),
            Error::Run(error) => run_failure(error).into(),
            Error::IdentifierIssuance | Error::CorruptData | Error::Database(_) => {
                ToolFailure::internal()
            }
        })
    }
}

fn result(structured: Value) -> CallToolResponse {
    CallToolResult::structured(structured).into()
}
