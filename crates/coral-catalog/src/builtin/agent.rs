//! `coral.agent` and `coral.cancelAgent` (ADR 0040): their schemas, the
//! arguments Coral reads, and the result the Sub-agent writes.

use coral_workspace::WorkspaceAction;
use schemars::JsonSchema;
use schemars::generate::Contract;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::{ArgumentProblem, BuiltinTool, Declared, Definition, derived_schema, tool_result};
use crate::{ToolHints, ToolPresentation};

/// Any Principal that can inspect the Workspace hands work to a Sub-agent,
/// which acts with that Principal's permissions.
pub(super) const NEEDS: WorkspaceAction = WorkspaceAction::Inspect;

/// How long a call waits unless the Program says. `exec` itself waits 30
/// seconds, so a Program of one call ends within one `exec`.
const DEFAULT_WAIT_MS: u64 = 20_000;
/// The longest wait a call may ask for: as long as a Turn lasts.
const MAX_WAIT_MS: u64 = 600_000;

/// When and how to hand work over. `exec`'s description shows it to a lead
/// whose Workspace enables the Tool, so it stays short.
const AGENT_GUIDANCE: &str = "\
Delegate data work, reads and writes, to one of Coral's agents: \
`return await coral.agent({ message })`. It sees only your `message`: send the user's question \
verbatim, today's date, and where you would look first. One question per agent. Then wait for its \
answer: do not search or read the same data yourself.

A call returns when the turn ends, or after `wait_ms`, 20 seconds by default. While `state` is \
`running`, call again with `sub_agent_id`. On `needs_action`, write each `url` in \
`required_actions` to the user, then call again. To reply, pass `sub_agent_id` and `message`. A \
turn ends after 10 minutes: reply to continue it. To stop an agent, call `coral.cancelAgent`, or \
cancel the run that waits on it. To check a step, call `wait` with its `run_id`.";

pub(super) fn agent_definition() -> Definition {
    Definition {
        key: BuiltinTool::Agent.key(),
        summary: "Delegate data work to one of Coral's agents",
        presentation: ToolPresentation::new(
            Some("Agent".to_string()),
            "Delegate data work, reads and writes, to one of Coral's agents. It runs programs \
             with your permissions, in the task of this run."
                .to_string(),
            false,
            vec!["Agents".to_string()],
            Some(AGENT_GUIDANCE.to_string()),
        ),
        // Not read-only: it starts work. That work is the caller's own, and
        // each Program of it meets the Tool policy itself, so it is enabled.
        hints: ToolHints::new(Some(false), Some(false), Some(false), Some(true)),
        input: derived_schema::<AgentArguments>(Contract::Deserialize),
        output: derived_schema::<AgentResult>(Contract::Serialize),
        declared: Declared::Once,
        enabled_by_default: true,
    }
}

pub(super) fn cancel_definition() -> Definition {
    Definition {
        key: BuiltinTool::CancelAgent.key(),
        summary: "Stop one of the agents you started",
        presentation: ToolPresentation::new(
            Some("Cancel agent".to_string()),
            "Stop one of the agents you started: its turn and its unfinished runs. It returns \
             the agent as coral.agent does."
                .to_string(),
            false,
            vec!["Agents".to_string()],
            None,
        ),
        hints: ToolHints::new(Some(false), Some(true), Some(true), Some(false)),
        input: derived_schema::<CancelAgentArguments>(Contract::Deserialize),
        output: derived_schema::<AgentResult>(Contract::Serialize),
        declared: Declared::Once,
        enabled_by_default: true,
    }
}

/// `agent`'s arguments, checked by every rule that needs no Workspace state.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(untagged, deny_unknown_fields)]
#[schemars(description = "")]
pub enum AgentArguments {
    /// Briefs a new agent.
    Start {
        /// Everything the new agent is to do. It reads nothing else.
        #[schemars(length(min = 1))]
        message: String,
        /// The longest this call waits, in milliseconds. Default 20000. 0 returns at once.
        #[serde(default, deserialize_with = "super::present")]
        #[schemars(with = "u64", range(max = MAX_WAIT_MS))]
        wait_ms: Option<u64>,
    },
    /// Replies to an agent you started, or waits on it again.
    Continue {
        /// The `sub_agent_id` of an agent you started.
        sub_agent_id: String,
        /// Your reply. The agent reads it before its next step, or in a new
        /// turn once its turn has ended. Omit it to wait without a reply.
        #[serde(default, deserialize_with = "super::present")]
        #[schemars(with = "String", length(min = 1))]
        message: Option<String>,
        /// The latest `next_cursor` of this agent, so that the call returns
        /// only for a request you have not seen. A reply does not use it.
        #[serde(default, deserialize_with = "super::present")]
        #[schemars(with = "String")]
        cursor: Option<String>,
        /// The longest this call waits, in milliseconds. Default 20000. 0 returns at once.
        #[serde(default, deserialize_with = "super::present")]
        #[schemars(with = "u64", range(max = MAX_WAIT_MS))]
        wait_ms: Option<u64>,
    },
}

const AGENT_TAKES: &str = "pass `message` to brief a new agent, or `sub_agent_id` with an \
                           optional `message` and `cursor`; `wait_ms` is at most 600000";

impl AgentArguments {
    /// Reads the arguments: the shape first, then the message.
    /// # Errors
    /// Returns the argument to correct, and repeats no value the Program sent.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        // serde also reads a struct from a sequence, which the schema forbids.
        let read = arguments.is_object().then(|| Self::deserialize(arguments));
        let Some(Ok(read)) = read else {
            return Err(vec![ArgumentProblem::new("", AGENT_TAKES)]);
        };
        let (message, wait_ms) = match &read {
            Self::Start { message, wait_ms } => (Some(message), wait_ms),
            Self::Continue {
                message, wait_ms, ..
            } => (message.as_ref(), wait_ms),
        };
        if message.is_some_and(|message| message.trim().is_empty()) {
            return Err(vec![ArgumentProblem::new("message", "must not be blank")]);
        }
        if wait_ms.is_some_and(|wait_ms| wait_ms > MAX_WAIT_MS) {
            return Err(vec![ArgumentProblem::new("wait_ms", "is at most 600000")]);
        }
        Ok(read)
    }

    /// How long the call waits for the Turn.
    #[must_use]
    pub fn wait(&self) -> std::time::Duration {
        let (Self::Start { wait_ms, .. } | Self::Continue { wait_ms, .. }) = self;
        std::time::Duration::from_millis(wait_ms.unwrap_or(DEFAULT_WAIT_MS))
    }
}

/// `cancelAgent`'s arguments.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(description = "")]
pub struct CancelAgentArguments {
    /// The `sub_agent_id` of an agent you started.
    pub sub_agent_id: String,
}

impl CancelAgentArguments {
    /// Reads the arguments.
    /// # Errors
    /// Returns the argument to correct.
    pub fn parse(arguments: &Value) -> Result<Self, Vec<ArgumentProblem>> {
        if !arguments.is_object() {
            return Err(vec![ArgumentProblem::new("", "pass an object")]);
        }
        super::read(arguments).map_err(|problem| vec![problem])
    }
}

/// What `agent` and `cancelAgent` return: an agent's latest turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct AgentResult {
    /// Names the agent, for coral.agent and coral.cancelAgent.
    pub sub_agent_id: String,
    /// Where the turn stands. With `needs_action`, handle `required_actions`
    /// as a run's. After `interrupted`, send the agent a message again.
    #[schemars(extend("enum" = ["running", "needs_action", "completed", "failed", "canceled", "interrupted"]))]
    pub state: &'static str,
    /// The agent's answer, once state is `completed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub answer: Option<String>,
    /// Why the turn ended without an answer, once state is `failed`.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub failure: Option<String>,
    /// What the turn's runs wait for from a person.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub required_actions: Vec<AgentRequiredAction>,
    /// How the agent said its work went, if it said so in this turn.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String", extend("enum" = ["success", "failure"]))]
    pub outcome: Option<&'static str>,
    /// Each program and search of the turn, oldest first.
    pub steps: Vec<AgentStep>,
    /// Pass it as `cursor` when you call coral.agent on this agent again.
    pub next_cursor: String,
}

impl AgentResult {
    /// The Tool Result.
    #[must_use]
    pub fn into_tool_result(self) -> Value {
        tool_result(&self)
    }
}

/// One step of an agent's turn.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct AgentStep {
    /// What the agent meant to do, or what it searched for.
    pub intent: String,
    /// The run that the step started, if it started one.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub run_id: Option<String>,
}

/// One request that a run of an agent's turn waits on.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields, description = "")]
pub struct AgentRequiredAction {
    /// The run that waits.
    pub run_id: String,
    /// What the person does.
    #[schemars(extend("enum" = ["account_connection", "invocation_approval"]))]
    pub kind: &'static str,
    /// Names the request.
    pub request_id: String,
    /// Who acts.
    #[schemars(extend("enum" = ["invoking_user", "workspace_owner"]))]
    pub required_actor: &'static str,
    /// The call path of the tool that waits, if one tool does.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub tool: Option<String>,
    /// The provider to connect an account to, for an account connection.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    pub provider: Option<String>,
    /// When the request expires, as an RFC 3339 time.
    pub deadline_at: String,
    /// The page where the person acts. Write it to the user in full.
    pub url: String,
}

/// Arguments that use every part of each input schema, and results that use
/// every part of the output schema.
#[cfg(test)]
pub(super) mod tests {
    use serde_json::{Value, json};

    use super::{AgentRequiredAction, AgentResult, AgentStep};

    pub(in super::super) fn valid_agent_arguments() -> Vec<Value> {
        vec![
            json!({ "message": "Count the open invoices.", "wait_ms": 0 }),
            json!({ "sub_agent_id": "a1", "message": "Only this year.", "cursor": "t1", "wait_ms": 600_000 }),
        ]
    }

    pub(in super::super) fn valid_cancel_arguments() -> Vec<Value> {
        vec![json!({ "sub_agent_id": "a1" })]
    }

    pub(in super::super) fn results() -> Vec<Value> {
        let running = AgentResult {
            sub_agent_id: "a1".to_owned(),
            state: "running",
            answer: None,
            failure: None,
            required_actions: Vec::new(),
            outcome: None,
            steps: Vec::new(),
            next_cursor: "t1".to_owned(),
        };
        let waiting = AgentRequiredAction {
            run_id: "r1".to_owned(),
            kind: "invocation_approval",
            request_id: "q1".to_owned(),
            required_actor: "workspace_owner",
            tool: Some("coral.providers.mail.send".to_owned()),
            provider: Some("Mail".to_owned()),
            deadline_at: "2026-11-18T12:00:00.000000000Z".to_owned(),
            url: "https://coral.example/approvals/q1".to_owned(),
        };
        let steps = [Some("r1".to_owned()), None].map(|run_id| AgentStep {
            intent: "Read the invoices".to_owned(),
            run_id,
        });
        let full = AgentResult {
            state: "needs_action",
            answer: Some("Twelve.".to_owned()),
            failure: Some("the model gave no answer".to_owned()),
            required_actions: vec![waiting],
            outcome: Some("success"),
            steps: steps.to_vec(),
            ..running.clone()
        };
        [running, full].map(AgentResult::into_tool_result).to_vec()
    }
}
