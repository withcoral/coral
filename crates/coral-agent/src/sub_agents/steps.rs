//! What the lead reads of a Sub-agent: its latest Turn, the steps the Turn
//! took, the outcome it recorded, and what the Turn's Runs wait for from a
//! person.

use coral_db::UnixNanos;
use coral_run::{RequiredAction, RunId, RunObservation};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use serde::Deserialize;

use super::{Error, SubAgents, TurnState};
use crate::model::{Block, Message, ToolCall};
use crate::repository;
use crate::run_tools::{Outcome, first_chars, recorded_outcome, shortened_intent};

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
        let messages = messages.await?;
        let (steps, outcome) = (steps(&messages), outcome(&messages));
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
            outcome,
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
    /// How the model said its work went, if it recorded that in this Turn.
    pub(crate) outcome: Option<Outcome>,
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
pub(super) fn steps(messages: &[Message]) -> Vec<Step> {
    let calls = calls_with_results(messages);
    let steps = calls.filter_map(|(call, result)| {
        let started = match result {
            Some(Block::ToolResult { run_id, .. }) => run_id.as_deref(),
            _ => None,
        };
        step(call, started)
    });
    steps.collect()
}

/// The outcome of the Turn's last `end_task` that recorded one.
pub(super) fn outcome(messages: &[Message]) -> Option<Outcome> {
    let calls = calls_with_results(messages);
    let ended = calls.filter(|(call, _result)| call.name == "end_task");
    let recorded = ended.filter_map(|(_call, result)| match result {
        Some(Block::ToolResult { content, .. }) => recorded_outcome(content),
        _ => None,
    });
    recorded.last()
}

/// Each tool call in the messages, with its result once stored. A reply's
/// results are the next message, in the order of its calls, so a call is
/// matched to its result by its place: a model may give two calls the same
/// id.
fn calls_with_results(messages: &[Message]) -> impl Iterator<Item = (&ToolCall, Option<&Block>)> {
    messages.iter().enumerate().flat_map(move |(index, reply)| {
        let results = messages
            .get(index + 1)
            .map_or(&[][..], |next| &next.content);
        let calls = reply.tool_calls().enumerate();
        calls.map(move |(place, call)| (call, results.get(place)))
    })
}

/// The step that `call` is, if it is an `exec` or a `search`. An `exec` shows
/// its intent as its Run keeps it, and a search the start of what it looked
/// for: the model sets no length on either.
fn step(call: &ToolCall, started: Option<&str>) -> Option<Step> {
    match call.name.as_str() {
        "exec" => {
            let ExecArguments { intent } = serde_json::from_str(&call.arguments).ok()?;
            Some(Step {
                intent: shortened_intent(&intent).to_owned(),
                run_id: started.and_then(|run_id| RunId::parse(run_id).ok()),
            })
        }
        "search" => {
            let search: SearchArguments = serde_json::from_str(&call.arguments).ok()?;
            Some(Step {
                intent: first_chars(&search.intent(), SEARCH_STEP_MAX_CHARS).to_owned(),
                run_id: None,
            })
        }
        _ => None,
    }
}

/// The most of a search's query or paths that a step shows.
const SEARCH_STEP_MAX_CHARS: usize = 200;

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::Role;

    // Catches: a step that shows another call's Run because the model gave
    // both calls one id, and a step as long as the model made its intent or
    // its search.
    #[test]
    fn a_step_shows_the_run_of_its_own_call_and_a_bounded_intent() {
        let runs = ["first-run-of-the-turn", "second-run-of-the-turn"];
        let mut thread = vec![Message::user("brief".to_owned())];
        for run_id in runs {
            let call = ToolCall {
                id: "call_0".to_owned(),
                name: "exec".to_owned(),
                arguments: format!(r#"{{"intent":"{}"}}"#, "é".repeat(500)),
            };
            let started = RunId::parse(run_id).ok();
            let result = call.result("{}".to_owned(), started);
            thread.push(Message {
                role: Role::Assistant,
                content: vec![Block::ToolUse(call)],
            });
            thread.push(Message::tool_results(vec![result]));
        }

        let search = ToolCall {
            id: "call_0".to_owned(),
            name: "search".to_owned(),
            arguments: format!(r#"{{"query":"{}"}}"#, "é".repeat(500)),
        };
        thread.push(Message {
            role: Role::Assistant,
            content: vec![Block::ToolUse(search)],
        });

        let steps = steps(&thread);

        let shown: Vec<_> = steps.iter().map(|step| step.run_id.as_ref()).collect();
        let started = runs.iter().map(|run_id| RunId::parse(run_id).ok());
        let started: Vec<_> = started.chain([None]).collect();
        let started: Vec<_> = started.iter().map(Option::as_ref).collect();
        assert_eq!(shown, started, "two execs, then the search");
        let lengths = steps.iter().map(|step| step.intent.chars().count());
        assert_eq!(lengths.collect::<Vec<_>>(), [80, 80, 200]);
    }

    // Catches: an outcome read from a refused `end_task`, from another call's
    // result, or from an earlier call than the Turn's last.
    #[test]
    fn the_outcome_is_the_last_that_end_task_recorded() {
        let end_task = |outcome: &str| ToolCall {
            id: "call_0".to_owned(),
            name: "end_task".to_owned(),
            arguments: format!(r#"{{"task_id":"lead-task-id","outcome":"{outcome}"}}"#),
        };
        let recorded =
            |outcome: &str| format!(r#"{{"task_id":"lead-task-id","outcome":"{outcome}"}}"#);
        let refused = r#"{"error":{"code":"wrong_task","message":"Nothing was recorded."}}"#;
        let exec = ToolCall {
            id: "call_0".to_owned(),
            name: "exec".to_owned(),
            arguments: r#"{"intent":"Read"}"#.to_owned(),
        };
        let reply = |calls: Vec<ToolCall>, results: Vec<String>| {
            let results = calls
                .iter()
                .zip(results)
                .map(|(call, content)| call.result(content, None));
            let results = Message::tool_results(results.collect());
            let calls = calls.into_iter().map(Block::ToolUse).collect();
            [
                Message {
                    role: Role::Assistant,
                    content: calls,
                },
                results,
            ]
        };
        let mut thread = vec![Message::user("brief".to_owned())];

        assert_eq!(outcome(&thread), None, "no end_task");
        thread.extend(reply(vec![end_task("failure")], vec![recorded("failure")]));
        assert_eq!(outcome(&thread), Some(Outcome::Failure));
        thread.extend(reply(vec![end_task("success")], vec![refused.to_owned()]));
        assert_eq!(outcome(&thread), Some(Outcome::Failure), "a refused call");
        let both = vec![end_task("success"), exec];
        thread.extend(reply(both, vec![recorded("success"), recorded("failure")]));
        assert_eq!(outcome(&thread), Some(Outcome::Success), "by its place");
    }
}
