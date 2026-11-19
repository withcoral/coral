//! What the lead is served: core's tools, the `agent` tool, and a `wait` and
//! a `cancel` that also take a `sub_agent_id`.

use std::sync::Arc;
use std::time::Duration;

use coral_mcp::{MAX_WAIT_MS, McpCaller, McpInterface, Progress, ToolCallError, ToolFailure};
use coral_task::{TaskId, TaskIntent, TaskModule};
use rmcp::ErrorData;
use rmcp::handler::server::tool::schema_for_type;
use rmcp::model::{CallToolRequestParams, CallToolResponse, CallToolResult, JsonObject, Tool};
use schemars::JsonSchema;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::{Value, json};

use crate::sub_agents::{Error, SubAgents, TurnState};

/// Core's tool surface with one more tool, `agent`, and a `wait` and a
/// `cancel` that also take a `sub_agent_id`.
pub(crate) struct CoreWithAgent {
    core: Arc<dyn McpInterface>,
    tasks: TaskModule,
    sub_agents: SubAgents,
}

impl CoreWithAgent {
    pub(crate) const fn new(
        core: Arc<dyn McpInterface>,
        tasks: TaskModule,
        sub_agents: SubAgents,
    ) -> Self {
        Self {
            core,
            tasks,
            sub_agents,
        }
    }
}

#[async_trait::async_trait]
impl McpInterface for CoreWithAgent {
    fn instructions(&self) -> &str {
        self.core.instructions()
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
        // As in core's `exec`, a `task_id` wins over a `goal`.
        let task_id = match (&arguments.task_id, &arguments.goal) {
            (Some(task_id), _) => TaskId::parse(task_id).map_err(invalid)?,
            (None, Some(goal)) => {
                let goal = TaskIntent::parse(goal).map_err(invalid)?;
                let started = self.tasks.start(actor, workspace_id, goal, caller.now());
                let task = started.await.map_err(Error::from)?;
                task.id().clone()
            }
            (None, None) => return Err(invalid(AGENT_TAKES)),
        };
        let message = arguments.message;
        let started = self
            .sub_agents
            .start(actor, workspace_id, &task_id, message);
        let sub_agent_id = started.await?;
        Ok(result(json!({
            "sub_agent_id": sub_agent_id,
            "task_id": task_id.as_str(),
        })))
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
            Duration::from_millis(wait_ms),
        );
        Ok(turn_result(sub_agent_id, waited.await?))
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
        Ok(turn_result(sub_agent_id, canceled.await?))
    }
}

const AGENT_DESCRIPTION: &str = "Give one message to a Sub-agent: a model that Coral runs for you, inside your Task. The call starts the Sub-agent and returns its `sub_agent_id` at once. Read the answer with `wait`, and stop the Sub-agent with `cancel`: pass each the `sub_agent_id` in place of `run_id`. A Sub-agent reads only `message`. It calls no tools: it answers from the message alone.";
const AGENT_TAKES: &str = "agent takes `message`, and `task_id` or `goal`: pass `task_id` to work in a Task you started, or `goal` to start a new one";

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct AgentArguments {
    /// What the user asked for, in one short phrase. Starts a new Task for
    /// this unit of user work, unless `task_id` is given.
    goal: Option<String>,
    /// The `task_id` of the Task that the Sub-agent works in. Given with
    /// `goal`, it wins and `goal` is ignored.
    task_id: Option<String>,
    /// Everything the Sub-agent is to do. It reads nothing else.
    message: String,
}

const SUB_AGENT_ID_DESCRIPTION: &str =
    "The sub_agent_id from agent. Pass it in place of run_id, without a cursor.";
const WAIT_BY_SUB_AGENT_ID: &str = "With `sub_agent_id` in place of `run_id`, observe that Sub-agent: the call returns its `state` when it stops running or `wait_ms` runs out, with its `answer` once `completed` or its `failure` once `failed`.";
const WAIT_TAKES: &str = "with `sub_agent_id`, wait takes only `wait_ms` beside it";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArguments {
    sub_agent_id: String,
    wait_ms: Option<u64>,
}

const CANCEL_BY_SUB_AGENT_ID: &str =
    "With `sub_agent_id` in place of `run_id`, stop that Sub-agent and return its `state`.";
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

/// The failure that the lead reads when a Sub-agent call did nothing.
impl From<Error> for ToolCallError {
    fn from(error: Error) -> Self {
        Self::Tool(match error {
            Error::SubAgentNotFound => {
                ToolFailure::stable("sub_agent_not_found", "Sub-agent was not found")
            }
            Error::Task(coral_task::Error::TaskNotFound) => {
                ToolFailure::stable("task_not_found", "Task was not found")
            }
            Error::Task(coral_task::Error::TaskClosed) => ToolFailure::stable(
                "task_closed",
                "Task is closed. Call `agent` with `goal` to start a new Task.",
            ),
            Error::Task(_)
            | Error::IdentifierIssuance
            | Error::CorruptData
            | Error::Database(_) => ToolFailure::internal(),
        })
    }
}

/// What `wait` and `cancel` return for a Sub-agent: its Turn as it stands.
fn turn_result(sub_agent_id: &str, turn: TurnState) -> CallToolResponse {
    result(match turn {
        TurnState::Running => json!({"sub_agent_id": sub_agent_id, "state": "running"}),
        TurnState::Completed { answer } => {
            json!({"sub_agent_id": sub_agent_id, "state": "completed", "answer": answer})
        }
        TurnState::Failed { failure } => {
            json!({"sub_agent_id": sub_agent_id, "state": "failed", "failure": failure})
        }
        TurnState::Canceled => json!({"sub_agent_id": sub_agent_id, "state": "canceled"}),
    })
}

fn result(structured: Value) -> CallToolResponse {
    CallToolResult::structured(structured).into()
}
