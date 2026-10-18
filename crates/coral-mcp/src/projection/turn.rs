use coral_agent::{TurnEventKind, TurnState, TurnUpdate};
use rmcp::model::CallToolResult;
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;

use super::run::{timestamp, timestamp_schema};
use super::{ToolFailure, success};

/// Current observation of one agent turn, with its result only on the last page.
#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("allOf" = [
    {"if":{"properties":{"final":true},"required":["final"]},"then":{"properties":{"state":{"const":"completed"},"has_more":{"const":false}}}},
    {"if":{"properties":{"failure":true},"required":["failure"]},"then":{"properties":{"state":{"enum":["failed","interrupted"]},"has_more":{"const":false}}}}
]))]
pub(crate) struct TurnUpdateResult<'a> {
    #[schemars(schema_with = "crate::arguments::uuid_schema")]
    task_id: String,
    #[schemars(schema_with = "crate::arguments::uuid_schema")]
    turn_id: String,
    state: State,
    events: Vec<Event<'a>>,
    next_cursor: String,
    has_more: bool,
    /// The agent's last message: its answer, or one question for you.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "final")]
    final_message: Option<&'a str>,
    /// The answering Program and its Run. Check it before you rely on the answer.
    #[serde(skip_serializing_if = "Option::is_none")]
    evidence: Option<Evidence<'a>>,
    /// Why the turn ended without an answer, and what to send next.
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<&'a str>,
    /// A Run of this turn that waits for a person. Act on it as its own page says.
    #[serde(skip_serializing_if = "Option::is_none")]
    required_action: Option<RequiredAction<'a>>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum State {
    Running,
    NeedsAction,
    Completed,
    Failed,
    Canceled,
    Interrupted,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Event<'a> {
    /// The agent called one Coral tool.
    #[serde(rename = "agent.step")]
    Step {
        #[schemars(regex(pattern = "^[1-9][0-9]*$"))]
        sequence: String,
        #[schemars(schema_with = "timestamp_schema")]
        occurred_at: String,
        tool: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        intent: Option<&'a str>,
    },
    /// A Run the agent started began waiting for a person.
    #[serde(rename = "agent.needs_action")]
    NeedsAction {
        #[schemars(regex(pattern = "^[1-9][0-9]*$"))]
        sequence: String,
        #[schemars(schema_with = "timestamp_schema")]
        occurred_at: String,
        #[schemars(schema_with = "crate::arguments::uuid_schema")]
        run_id: &'a str,
    },
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Evidence<'a> {
    #[schemars(schema_with = "crate::arguments::uuid_schema")]
    run_id: &'a str,
    program: &'a str,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct RequiredAction<'a> {
    #[schemars(schema_with = "crate::arguments::uuid_schema")]
    run_id: &'a str,
    /// The Run's `required_actions`, as `wait` on that Run returns them.
    required_actions: &'a Value,
}

pub(crate) fn turn_update(update: &TurnUpdate) -> Result<CallToolResult, ToolFailure> {
    let events = update
        .events()
        .iter()
        .map(|event| {
            let sequence = event.sequence().to_string();
            let occurred_at = timestamp(event.occurred_at_unix_nanos());
            match event.kind() {
                TurnEventKind::Step { tool, intent } => Event::Step {
                    sequence,
                    occurred_at,
                    tool,
                    intent: intent.as_deref(),
                },
                TurnEventKind::NeedsAction(action) => Event::NeedsAction {
                    sequence,
                    occurred_at,
                    run_id: &action.run_id,
                },
            }
        })
        .collect();
    success(TurnUpdateResult {
        task_id: update.task_id().to_string(),
        turn_id: update.turn_id().to_string(),
        state: match update.state() {
            TurnState::Running => State::Running,
            TurnState::NeedsAction => State::NeedsAction,
            TurnState::Completed => State::Completed,
            TurnState::Failed => State::Failed,
            TurnState::Canceled => State::Canceled,
            TurnState::Interrupted => State::Interrupted,
        },
        events,
        next_cursor: update.next_cursor().to_string(),
        has_more: update.has_more(),
        final_message: update.final_message(),
        evidence: update.evidence().map(|evidence| Evidence {
            run_id: &evidence.run_id,
            program: &evidence.program,
        }),
        failure: update.failure(),
        required_action: update.required_action().map(|action| RequiredAction {
            run_id: &action.run_id,
            required_actions: &action.required_actions,
        }),
    })
}
