//! What `agent`, `wait` and `cancel` return for a Sub-agent: its latest Turn,
//! the steps it took, the outcome it recorded, and what its Runs wait for from
//! a person.

use coral_mcp::HumanInterfaceUrl;
use coral_run::{RequiredActionRequest, RequiredActor, RunId};
use serde::Serialize;
use serde_json::{Map, Value, json};

use crate::sub_agents::{TurnPage, TurnState, WaitingAction};

/// The Turn's fields, after the fields that name the Sub-agent. A running
/// Turn whose Runs wait for a person is `needs_action`. `outcome` appears once
/// the model has recorded one.
pub(super) fn turn_fields(
    page: &TurnPage,
    human_interface: &HumanInterfaceUrl,
) -> Map<String, Value> {
    let (state, detail) = match &page.state {
        TurnState::Running if page.required_actions.is_empty() => ("running", None),
        TurnState::Running => {
            let actions = page.required_actions.iter();
            let actions = actions.map(|waiting| Action::new(waiting, human_interface));
            let actions = json!(actions.collect::<Vec<_>>());
            ("needs_action", Some(("required_actions", actions)))
        }
        TurnState::Completed { answer } => ("completed", Some(("answer", json!(answer)))),
        TurnState::Failed { failure } => ("failed", Some(("failure", json!(failure)))),
        TurnState::Canceled => ("canceled", None),
        TurnState::Interrupted => ("interrupted", None),
    };
    let steps = page.steps.iter().map(|step| Step {
        intent: &step.intent,
        run_id: step.run_id.as_ref().map(RunId::as_str),
    });
    let mut fields = Map::new();
    fields.insert("state".to_owned(), json!(state));
    fields.extend(detail.map(|(name, value)| (name.to_owned(), value)));
    if let Some(outcome) = page.outcome {
        fields.insert("outcome".to_owned(), json!(outcome));
    }
    fields.insert("steps".to_owned(), json!(steps.collect::<Vec<_>>()));
    fields.insert("next_cursor".to_owned(), json!(page.cursor()));
    fields
}

/// One step as the lead reads it: the intent, and the Run it started.
#[derive(Serialize)]
struct Step<'a> {
    intent: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    run_id: Option<&'a str>,
}

/// One request a Run waits on: the fields of core's `required_actions`, and
/// the Run's `run_id`.
#[derive(Serialize)]
struct Action<'a> {
    run_id: &'a str,
    kind: &'static str,
    request_id: String,
    required_actor: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<&'a str>,
    deadline_at: String,
    url: String,
}

impl<'a> Action<'a> {
    fn new(waiting: &'a WaitingAction, human_interface: &HumanInterfaceUrl) -> Self {
        let action = &waiting.action;
        let (kind, request_id) = match &action.request {
            RequiredActionRequest::AccountConnection(id) => ("account_connection", id.to_string()),
            RequiredActionRequest::InvocationApproval(id) => {
                ("invocation_approval", id.to_string())
            }
        };
        let deadline_at = chrono::DateTime::from_timestamp_nanos(action.deadline_at_unix_nanos);
        Self {
            run_id: waiting.run_id.as_str(),
            kind,
            request_id,
            required_actor: match action.required_actor {
                RequiredActor::InvokingUser => "invoking_user",
                RequiredActor::WorkspaceOwner => "workspace_owner",
            },
            tool: action.call_path.as_deref(),
            provider: action.provider_label.as_deref(),
            deadline_at: deadline_at.to_rfc3339_opts(chrono::SecondsFormat::Nanos, true),
            url: human_interface.page_url(&action.action_path),
        }
    }
}
