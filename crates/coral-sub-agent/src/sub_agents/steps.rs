//! What the lead reads of a Sub-agent: its latest Turn, the steps the Turn
//! took, and what the Turn's Runs wait for from a person.

use std::collections::HashMap;

use coral_db::UnixNanos;
use coral_run::{RequiredAction, RunId, RunObservation};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use serde::Deserialize;

use super::{Error, SubAgents, TurnState};
use crate::model::{Block, Message};
use crate::repository;

impl SubAgents {
    /// The latest Turn of the caller's own Sub-agent, as the lead reads it.
    pub(crate) async fn page(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        sub_agent_id: &str,
    ) -> Result<TurnPage, Error> {
        let turn = self.turn(actor, workspace_id, sub_agent_id).await?;
        let messages =
            repository::turn_messages(&*self.database, workspace_id, sub_agent_id, &turn.id);
        let steps = steps(&messages.await?);
        let required_actions = if turn.state == TurnState::Running {
            self.required_actions(actor, workspace_id, &steps).await?
        } else {
            Vec::new()
        };
        Ok(TurnPage {
            turn_id: turn.id,
            state: turn.state,
            lease_expires_at: turn.lease_expires_at,
            steps,
            required_actions,
        })
    }

    /// What the Runs of `steps` wait for from a person now.
    async fn required_actions(
        &self,
        actor: &AuthenticatedPrincipal,
        workspace_id: &WorkspaceId,
        steps: &[Step],
    ) -> Result<Vec<WaitingAction>, Error> {
        let observation = RunObservation::immediate(None, 1)?.without_trace_link();
        let mut waiting = Vec::new();
        for run_id in steps.iter().filter_map(|step| step.run_id.as_ref()) {
            let run = self.runs.observe(actor, workspace_id, run_id, &observation);
            let actions = run.await?.required_actions().to_vec();
            waiting.extend(actions.into_iter().map(|action| WaitingAction {
                run_id: run_id.clone(),
                action,
            }));
        }
        Ok(waiting)
    }
}

/// A Sub-agent's latest Turn as the lead reads it.
#[derive(Debug)]
pub(crate) struct TurnPage {
    pub(crate) turn_id: String,
    pub(crate) state: TurnState,
    pub(crate) lease_expires_at: UnixNanos,
    pub(crate) steps: Vec<Step>,
    /// What the Turn's Runs wait for from a person, while the Turn runs.
    pub(crate) required_actions: Vec<WaitingAction>,
}

impl TurnPage {
    /// Names this Turn and the requests the page shows, so that a later
    /// `wait` returns once another request appears.
    pub(crate) fn cursor(&self) -> String {
        let requests = self.requests();
        [vec![self.turn_id.clone()], requests].concat().join(",")
    }

    /// The requests that a `wait` from `cursor` has seen: those it names, or
    /// with no cursor those of this first page. A cursor of another Turn is
    /// refused.
    pub(crate) fn seen(&self, cursor: Option<&str>) -> Result<Vec<String>, Error> {
        let Some(cursor) = cursor else {
            return Ok(self.requests());
        };
        let mut parts = cursor.split(',');
        if parts.next() != Some(self.turn_id.as_str()) {
            return Err(Error::CursorOfAnotherTurn);
        }
        Ok(parts.map(str::to_owned).collect())
    }

    /// Whether a Run waits on a request that `seen` does not hold.
    pub(crate) fn newly_needs_a_person(&self, seen: &[String]) -> bool {
        self.requests()
            .iter()
            .any(|request| !seen.contains(request))
    }

    fn requests(&self) -> Vec<String> {
        let requests = self.required_actions.iter();
        requests
            .map(|waiting| waiting.action.request.to_string())
            .collect()
    }
}

/// One `exec` or `search` call of a Turn: the intent the model gave or the
/// search's query, and the Run that an `exec` started once its result names
/// one.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Step {
    pub(crate) intent: String,
    pub(crate) run_id: Option<RunId>,
}

/// A request that a Run of the Turn waits on.
#[derive(Debug)]
pub(crate) struct WaitingAction {
    pub(crate) run_id: RunId,
    pub(crate) action: RequiredAction,
}

/// The steps of a Turn: each `exec` and `search` call in its messages, an
/// `exec` with the Run that its result names.
fn steps(messages: &[Message]) -> Vec<Step> {
    let blocks = messages.iter().flat_map(|message| &message.content);
    let started: HashMap<&str, &str> = blocks
        .filter_map(|block| match block {
            Block::ToolResult {
                tool_use_id,
                run_id,
                ..
            } => Some((tool_use_id.as_str(), run_id.as_deref()?)),
            Block::Text { .. }
            | Block::Reasoning { .. }
            | Block::ToolUse(_)
            | Block::Summary { .. } => None,
        })
        .collect();
    let calls = messages.iter().flat_map(Message::tool_calls);
    calls
        .filter_map(|call| match call.name.as_str() {
            "exec" => {
                let ExecArguments { intent } = serde_json::from_str(&call.arguments).ok()?;
                let run_id = started.get(call.id.as_str());
                let run_id = run_id.and_then(|run_id| RunId::parse(run_id).ok());
                Some(Step { intent, run_id })
            }
            "search" => {
                let search: SearchArguments = serde_json::from_str(&call.arguments).ok()?;
                let intent = search.intent();
                Some(Step {
                    intent,
                    run_id: None,
                })
            }
            _ => None,
        })
        .collect()
}

#[derive(Deserialize)]
struct ExecArguments {
    intent: String,
}

#[derive(Deserialize)]
struct SearchArguments {
    query: Option<String>,
    paths: Option<Vec<String>>,
}

impl SearchArguments {
    /// The query, or a phrase that names what the search reads.
    fn intent(self) -> String {
        match (self.query, self.paths) {
            (Some(query), _) => query,
            (None, Some(paths)) => format!("Read the declarations of {}", paths.join(", ")),
            (None, None) => "List the tools you can call".to_owned(),
        }
    }
}
