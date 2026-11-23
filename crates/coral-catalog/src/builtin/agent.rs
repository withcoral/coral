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

const AGENT_GUIDANCE: &str = "\
Add providers, change policy and save custom tools yourself. You remain responsible for the answer.

Pass message alone to brief a new agent: one brief per question. An agent observes only your \
messages. Include the user's question verbatim, today's date, and, if you have one, a guess at \
where to look first, stated as a guess.

The call returns when the agent's turn ends, when one of its runs newly needs a person, when this \
run is asked to stop, or after 10 minutes. A turn ends when the agent sends a message without a \
tool call: an answer, or a report of where it is blocked. The turn goes on when this run ends. \
While state is running or needs_action, call again with sub_agent_id and the latest next_cursor \
as cursor. To reply, pass sub_agent_id and a message of one or two sentences; a message sent \
during a turn is read before the agent's next step. A step that started a run holds the agent's \
intent and that run's run_id; to check the step, call wait with that run_id. A program that an \
agent started cannot call this tool.";

pub(super) fn agent_definition() -> Definition {
    Definition {
        key: BuiltinTool::Agent.key(),
        summary: "Delegate data retrieval to one of Coral's agents",
        presentation: ToolPresentation::new(
            Some("Agent".to_string()),
            "Delegate data retrieval to one of Coral's agents, and wait for it. An agent reads \
             the user's connected providers and can make one change you specify exactly. It \
             runs programs with your permissions, in the task of this run."
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
        /// only for a request you have not seen.
        #[serde(default, deserialize_with = "super::present")]
        #[schemars(with = "String")]
        cursor: Option<String>,
    },
}

const AGENT_TAKES: &str = "pass `message` to brief a new agent, or `sub_agent_id` with an \
                           optional `message` and `cursor`";

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
        let message = match &read {
            Self::Start { message } => Some(message),
            Self::Continue { message, .. } => message.as_ref(),
        };
        if message.is_some_and(|message| message.trim().is_empty()) {
            return Err(vec![ArgumentProblem::new("message", "must not be blank")]);
        }
        Ok(read)
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
            json!({ "message": "Count the open invoices." }),
            json!({ "sub_agent_id": "a1", "message": "Only this year.", "cursor": "t1" }),
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
