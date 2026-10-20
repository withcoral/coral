//! One turn's loop: the model reads the thread, calls tools, and ends with a
//! final message. Every step commits before the next, so observers see progress
//! and a canceled turn stops writing at its next step.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Instant;

use coral_db::Database;
use coral_task::TaskId;
use coral_workspace::WorkspaceId;
use serde_json::{Value, json};

use crate::model::{Message, Model, ModelRequest, Role, StopReason};
use crate::repository::{self, Ending, ModelCall};
use crate::toolbox::{RunObserved, Toolbox};
use crate::{Error, Evidence, RequiredAction, TurnEventKind, TurnId, TurnState, now};

/// The most model calls one turn makes before it fails.
pub(crate) const MAX_MODEL_CALLS: usize = 60;

/// How the agent works and answers. The Coral tool instructions follow it.
pub(crate) const AGENT_INSTRUCTIONS: &str = "\
You are Coral's data agent. Another agent sent you one message on behalf of a person. \
You cannot see their conversation, only the messages in this thread. \
Answer with Coral's tools, then end your turn with your final message as plain text, without a tool call.

## How to work
- Use the task_id given in the thread for every tool call. The Task is already started.
- Skip the workflow steps for `start_task`, `save_function` and `end_task` below: the caller owns the Task and decides what to save.
- The thread's first message lists the Provider namespaces `search` accepts. Do not search or run Programs to find out which Providers exist.
- Explore with `search`, `describe` and small `exec` Programs until you know the Operations and data shapes. \
Make each exploring Program try every candidate call you are considering at once, each in its own try/catch. \
Return only the fields and text you need, never whole records.
- Then write one answering Program that makes every call the answer needs and passes values between them. \
Put values a person might change, such as a repository, a time window or a label, in constants at the top. \
Return the values your answer states, including the source text behind each key fact, so the caller can check them.
- In a loop over many records, catch each call's error and count failures by error code, so one failure does not end the Run.
- When the message says \"me\", \"my\" or \"I\", call the Provider's Operation for the signed-in account. \
Each Provider has its own identity.
- Do not change anything in a Provider unless the message asks for that exact change.
- If a Run returns `required_actions`, call `wait` on it with `wait_ms` 30000 until it ends.

## Your final message
- Lead with the answer. Keep it short, and summarize long lists with a count and the top items.
- Quote the source text behind each key fact, with its title, link or ID.
- If you could not read everything, say how much you covered, and count what failed by error code, such as \"115 channels: not_in_channel\".
- State each assumption you made for the person as a short clause: who \"me\" is, which repository, what a vague term means.
- If the message is ambiguous and nothing in the thread resolves it, ask one specific question instead of guessing.
- If you cannot answer, say what blocked you and what message would let you go on.
- Do not describe your tool calls. The caller receives the answering Program and its result separately.
- When a succeeded Run of this turn answers the message, make the last line `run_id: ` and that Run's run_id. \
Leave the line out when no Run answers, such as when you ask a question.

";

/// The tool result a later turn records for a call its turn never answered,
/// so the thread stays a valid sequence of calls and results.
pub(crate) fn closing_result(id: &str) -> Value {
    json!({"type": "tool_result", "tool_use_id": id, "content": "The turn ended before this call returned."})
}

/// What one turn needs to run.
pub(crate) struct TurnContext {
    pub(crate) database: Arc<Database>,
    pub(crate) model: Arc<dyn Model>,
    pub(crate) toolbox: Arc<dyn Toolbox>,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) task_id: TaskId,
    pub(crate) turn_id: TurnId,
}

/// Runs the turn to its end and records the ending. A turn that already ended,
/// such as a canceled one, keeps its first ending.
pub(crate) async fn run(context: TurnContext) {
    let ending = match drive(&context).await {
        Ok(Some(outcome)) => outcome,
        Ok(None) => return,
        Err(error) => {
            tracing::warn!(%error, turn = %context.turn_id, "Agent Turn failed");
            Outcome::Failed("The agent hit an internal error. Send the message again.".into())
        }
    };
    if let Err(error) = record(&context, &ending).await {
        tracing::warn!(%error, turn = %context.turn_id, "Agent Turn ending was not recorded");
    }
}

enum Outcome {
    Completed {
        message: String,
        evidence: Option<Evidence>,
    },
    Failed(String),
}

async fn record(context: &TurnContext, outcome: &Outcome) -> Result<(), Error> {
    let mut transaction = context.database.begin().await?;
    let ending = match outcome {
        Outcome::Completed { message, evidence } => Ending {
            state: TurnState::Completed,
            final_message: Some(message),
            evidence: evidence.as_ref(),
            failure: None,
            ended_at_unix_nanos: now()?,
        },
        Outcome::Failed(failure) => Ending {
            state: TurnState::Failed,
            final_message: None,
            evidence: None,
            failure: Some(failure),
            ended_at_unix_nanos: now()?,
        },
    };
    repository::finish_turn(
        &mut transaction,
        &context.workspace_id,
        &context.turn_id,
        &ending,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
}

/// What one turn has learned about its Runs so far.
#[derive(Default)]
struct Progress {
    /// Program source by the Run that `exec` started for it.
    programs: HashMap<String, String>,
    /// Runs of this turn that succeeded.
    succeeded: HashSet<String>,
    /// Final Values by the succeeded Run that returned them.
    final_values: HashMap<String, Value>,
    /// Whether the turn is recorded as waiting for a person.
    needs_action: bool,
}

impl Progress {
    /// The evidence for a Run the agent names, if this turn ran it to success.
    fn evidence(&self, run_id: &str) -> Option<Evidence> {
        if !self.succeeded.contains(run_id) {
            return None;
        }
        self.programs.get(run_id).map(|program| {
            Evidence::new(
                run_id.to_owned(),
                program.clone(),
                self.final_values.get(run_id),
            )
        })
    }
}

/// Returns the outcome, or `None` when the turn ended elsewhere first.
async fn drive(context: &TurnContext) -> Result<Option<Outcome>, Error> {
    let system = format!("{AGENT_INSTRUCTIONS}{}", context.toolbox.instructions());
    let tools = context.toolbox.tools();
    let mut progress = Progress::default();
    for _call in 0..MAX_MODEL_CALLS {
        let messages = {
            let mut transaction = context.database.begin_read_snapshot().await?;
            let messages =
                repository::thread(&mut transaction, &context.workspace_id, &context.task_id)
                    .await?;
            transaction.commit().await?;
            messages
        };
        let request = ModelRequest {
            system: system.clone(),
            tools: tools.clone(),
            messages,
        };
        let started = Instant::now();
        let response = match context.model.respond(&request).await {
            Ok(response) => response,
            Err(error) => return Ok(Some(Outcome::Failed(error.0))),
        };
        let call = ModelCall {
            latency_millis: i64::try_from(started.elapsed().as_millis()).unwrap_or(i64::MAX),
            usage: response.usage.as_ref(),
        };
        let assistant = Message {
            role: Role::Assistant,
            content: response.content,
        };
        if !append(context, &assistant, Some(&call)).await? {
            return Ok(None);
        }
        let failure = match response.stop_reason {
            StopReason::ToolUse => None,
            StopReason::EndTurn => {
                let text = final_text(&assistant.content);
                let (message, run_id) = split_run_id(&text);
                if message.trim().is_empty() {
                    Some("The agent ended without a message. Send the message again.")
                } else {
                    let evidence = run_id.and_then(|run_id| {
                        let evidence = progress.evidence(run_id);
                        if evidence.is_none() {
                            tracing::warn!(turn = %context.turn_id, "Agent named a Run that did not succeed in its turn");
                        }
                        evidence
                    });
                    return Ok(Some(Outcome::Completed {
                        message: message.to_owned(),
                        evidence,
                    }));
                }
            }
            StopReason::MaxTokens => {
                Some("The agent's reply was cut off. Send a narrower message.")
            }
            StopReason::Refusal => Some("The model declined this request."),
            StopReason::Other => Some("The model stopped unexpectedly. Send the message again."),
        };
        if let Some(failure) = failure {
            return Ok(Some(Outcome::Failed(failure.into())));
        }
        let calls = tool_uses(&assistant.content);
        if calls.is_empty() {
            return Ok(Some(Outcome::Failed(
                "The agent asked for a tool without naming one. Send the message again.".into(),
            )));
        }
        let Some(results) = call_tools(context, calls, &mut progress).await? else {
            return Ok(None);
        };
        // All results of one assistant message return in one user message.
        let user = Message {
            role: Role::User,
            content: results,
        };
        if !append(context, &user, None).await? {
            return Ok(None);
        }
    }
    Ok(Some(Outcome::Failed(format!(
        "The agent used its limit of {MAX_MODEL_CALLS} steps. Send a narrower message."
    ))))
}

/// Splits a last line `run_id: <id>` off the final message. Markdown emphasis
/// around the line, and `none` as the id, are accepted.
fn split_run_id(text: &str) -> (&str, Option<&str>) {
    let text = text.trim_end();
    let (body, last) = text.rsplit_once('\n').unwrap_or(("", text));
    let line = last.trim().trim_matches(['`', '*', '_']).trim();
    let Some(rest) = line
        .get(..7)
        .filter(|prefix| prefix.eq_ignore_ascii_case("run_id:"))
        .and_then(|_prefix| line.get(7..))
    else {
        return (text, None);
    };
    let id = rest.trim().trim_matches(['`', '*', '_']).trim();
    if id.is_empty() || id.contains(char::is_whitespace) {
        return (text, None);
    }
    let named = (!id.eq_ignore_ascii_case("none")).then_some(id);
    (body.trim_end(), named)
}

/// Calls each requested tool in order, or `None` when the turn ended elsewhere meanwhile.
async fn call_tools(
    context: &TurnContext,
    calls: Vec<(String, String, Value)>,
    progress: &mut Progress,
) -> Result<Option<Vec<Value>>, Error> {
    let mut results = Vec::with_capacity(calls.len());
    for (id, name, input) in calls {
        let intent = input
            .get("intent")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let step = TurnEventKind::Step {
            tool: name.clone(),
            intent,
        };
        if !event(context, &step).await? {
            return Ok(None);
        }
        let program = input
            .get("program")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let outcome = context.toolbox.call(&name, input).await;
        if let Some(run) = &outcome.run {
            if let Some(program) = program {
                progress.programs.insert(run.run_id.clone(), program);
            }
            if run.succeeded {
                progress.succeeded.insert(run.run_id.clone());
                if let Some(value) = &run.final_value {
                    progress
                        .final_values
                        .insert(run.run_id.clone(), value.clone());
                }
            }
            if !track_actions(context, run, &mut progress.needs_action).await? {
                return Ok(None);
            }
        }
        results.push(json!({
            "type": "tool_result",
            "tool_use_id": id,
            "content": outcome.text,
            "is_error": outcome.is_error,
        }));
    }
    Ok(Some(results))
}

/// Records a Run that waits for a person, or that stopped waiting.
async fn track_actions(
    context: &TurnContext,
    run: &RunObserved,
    needs_action: &mut bool,
) -> Result<bool, Error> {
    let waiting = !run.required_actions.is_empty();
    if !waiting && !*needs_action {
        return Ok(true);
    }
    let mut transaction = context.database.begin().await?;
    let action = waiting.then(|| RequiredAction {
        run_id: run.run_id.clone(),
        required_actions: Value::Array(run.required_actions.clone()),
    });
    let open = repository::set_open_state(
        &mut transaction,
        &context.workspace_id,
        &context.turn_id,
        action.as_ref(),
    )
    .await?;
    let recorded = match (open, action) {
        (true, Some(action)) => {
            repository::append_event(
                &mut transaction,
                &context.workspace_id,
                &context.turn_id,
                &TurnEventKind::NeedsAction(action),
                now()?,
            )
            .await?
        }
        (open, _) => open,
    };
    transaction.commit().await?;
    *needs_action = waiting;
    Ok(recorded)
}

async fn append(
    context: &TurnContext,
    message: &Message,
    call: Option<&ModelCall<'_>>,
) -> Result<bool, Error> {
    let mut transaction = context.database.begin().await?;
    let appended = repository::append_message(
        &mut transaction,
        &context.workspace_id,
        &context.task_id,
        &context.turn_id,
        message,
        call,
    )
    .await?;
    transaction.commit().await?;
    Ok(appended)
}

async fn event(context: &TurnContext, kind: &TurnEventKind) -> Result<bool, Error> {
    let mut transaction = context.database.begin().await?;
    let appended = repository::append_event(
        &mut transaction,
        &context.workspace_id,
        &context.turn_id,
        kind,
        now()?,
    )
    .await?;
    transaction.commit().await?;
    Ok(appended)
}

pub(crate) fn tool_uses(content: &[Value]) -> Vec<(String, String, Value)> {
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("tool_use"))
        .filter_map(|block| {
            Some((
                block.get("id")?.as_str()?.to_owned(),
                block.get("name")?.as_str()?.to_owned(),
                block.get("input").cloned().unwrap_or(Value::Null),
            ))
        })
        .collect()
}

fn final_text(content: &[Value]) -> String {
    content
        .iter()
        .filter(|block| block.get("type").and_then(Value::as_str) == Some("text"))
        .filter_map(|block| block.get("text").and_then(Value::as_str))
        .collect::<Vec<_>>()
        .join("\n\n")
}
