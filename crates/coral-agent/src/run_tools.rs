//! What the Sub-agent's tools `exec`, `wait`, `cancel` and `end_task` do.
//!
//! Each tool acts as the lead's caller, in the lead's Task, and answers with
//! the JSON text the model reads: a Run as it stands, the outcome that
//! `end_task` records, or a failure.

#[cfg(test)]
mod tests;

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use coral_db::Database;
use coral_run::{
    FinalValue, ProgramLogLevel, RUN_EVENT_PAGE_MAX, RequiredAction, RequiredActionRequest,
    RequiredActor, RunCursor, RunEvent, RunEventKind, RunFailure, RunFailureCode, RunId, RunModule,
    RunObservation, RunState, RunTerminalOutcome, RunUpdate,
};
use coral_task::{AttributedCommand, TASK_INTENT_MAX_CHARS, TaskCommands, TaskId, TaskIntent};
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedPrincipal, AuthorityRefusal, WorkspaceId};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::repository;

/// The longest wait that `exec` and `wait` take: 30 seconds.
const MAX_WAIT_MS: u64 = 30_000;

/// The time limit of a Run whose `exec` names none: 15 minutes.
const EXEC_DEFAULT_RUN_TIMEOUT_MS: u64 = 15 * 60 * 1000;

/// How long an `exec` that names no `wait_ms` waits: the longest wait.
const EXEC_DEFAULT_WAIT_MS: u64 = MAX_WAIT_MS;

/// How long a `wait` that names no `wait_ms` waits: it returns at once.
const WAIT_DEFAULT_WAIT_MS: u64 = 0;

/// A result longer than this keeps only its start and its end.
const RESULT_MAX_BYTES: usize = 20 * 1024;

/// How much of each end a shortened result keeps.
const RESULT_END_BYTES: usize = 10 * 1024;

/// Who a Sub-agent's tools act as: the lead's caller, in the lead's Task.
pub(crate) struct Caller {
    pub(crate) principal: AuthenticatedPrincipal,
    pub(crate) workspace_id: WorkspaceId,
    pub(crate) task_id: TaskId,
    /// The Runs that this caller's `exec` calls started, each from its
    /// acceptance on.
    pub(crate) started_runs: Mutex<Vec<RunId>>,
}

/// What one Sub-agent tool call returns to the thread.
pub(crate) struct ToolOutput {
    pub(crate) content: String,
    pub(crate) is_error: bool,
    pub(crate) run_id: Option<RunId>,
}

pub(crate) struct RunTools {
    database: Arc<Database>,
    commands: TaskCommands,
    runs: RunModule,
}

impl RunTools {
    pub(crate) const fn new(
        database: Arc<Database>,
        commands: TaskCommands,
        runs: RunModule,
    ) -> Self {
        Self {
            database,
            commands,
            runs,
        }
    }

    /// Runs the tool `name` with the arguments the model wrote, and returns the JSON text the model
    /// reads, whether the tool returned an error, and the Run that an `exec` started.
    pub(crate) async fn call(&self, caller: &Caller, name: &str, arguments: &str) -> ToolOutput {
        let outcome = match name {
            "exec" => self.exec(caller, arguments).await,
            "wait" => self.wait(caller, arguments).await,
            "cancel" => self.cancel(caller, arguments).await,
            // The one tool that answers without a Run.
            "end_task" => {
                let (content, is_error) = match end_task(caller, arguments) {
                    Ok(content) => (content, false),
                    Err(failure) => (failed(failure), true),
                };
                return ToolOutput {
                    content,
                    is_error,
                    run_id: None,
                };
            }
            _ => Err(Failure::unknown_tool(name)),
        };
        let started = outcome.as_ref().ok().filter(|_| name == "exec");
        let run_id = started.map(|update| update.run_id().clone());
        let (content, is_error) = match outcome {
            Ok(update) => (run_result(&update), false),
            Err(failure) => (failed(failure), true),
        };
        ToolOutput {
            content,
            is_error,
            run_id,
        }
    }

    /// Runs a Program in the lead's Task, then waits for its Run.
    async fn exec(&self, caller: &Caller, arguments: &str) -> Result<RunUpdate, Failure> {
        let exec: ExecArguments = parse_arguments("exec", arguments)?;
        if exec.task_id != caller.task_id.as_str() {
            return Err(Failure::other_task(&caller.task_id));
        }
        // Before the Run starts, so a wait that is refused starts no Run.
        let first_wait = first_wait(wait_time(exec.wait_ms, EXEC_DEFAULT_WAIT_MS)?)?;
        let intent = TaskIntent::parse(shortened_intent(&exec.intent)).map_err(task_failure)?;
        let call = AttributedCommand::new(
            caller.principal.clone(),
            caller.workspace_id.clone(),
            caller.task_id.clone(),
            intent,
        );
        let timeout_ms = exec.run_timeout_ms.unwrap_or(EXEC_DEFAULT_RUN_TIMEOUT_MS);
        let accepted = self
            .commands
            .submit(&call, &exec.program, timeout_ms)
            .await
            .map_err(task_failure)?;
        tracing::Span::current().record("coral.run.id", safe::id(&accepted.telemetry_id()));
        // The record that keeps this Run's Program from calling the agent.
        let (workspace_id, task_id) = (&caller.workspace_id, &caller.task_id);
        let run = repository::insert_sub_agent_run(
            &self.database,
            workspace_id,
            task_id,
            accepted.run_id(),
        );
        if let Err(error) = run.await {
            tracing::warn!(
                error.type = safe::kind(error.code()),
                "the Run that a Sub-agent's exec started was not recorded as its own"
            );
        }
        let started = caller.started_runs.lock();
        started
            .unwrap_or_else(PoisonError::into_inner)
            .push(accepted.run_id().clone());
        let Some(first_wait) = first_wait else {
            return Ok(accepted);
        };
        let observed = self
            .runs
            .observe(
                &caller.principal,
                &caller.workspace_id,
                accepted.run_id(),
                &first_wait,
            )
            .await;
        match observed {
            Ok(observed) => Ok(observed),
            // A started Run is always reported: without its ID the model can
            // neither read nor cancel it. A later `wait` reads it.
            Err(error) => {
                let run_id = accepted.telemetry_id();
                tracing::warn!(
                    coral.workspace.id = safe::id(&caller.workspace_id),
                    coral.run.id = safe::id(&run_id),
                    error.type = safe::kind(error.code()),
                    "failed to observe the Run that a Sub-agent's exec started"
                );
                Ok(accepted)
            }
        }
    }

    /// Reads a Run after the cursor, waiting up to `wait_ms` for it to end
    /// or its Required Actions to change.
    async fn wait(&self, caller: &Caller, arguments: &str) -> Result<RunUpdate, Failure> {
        let wait: WaitArguments = parse_arguments("wait", arguments)?;
        let run_id = RunId::parse(&wait.run_id).map_err(run_failure)?;
        let wait_time = wait_time(wait.wait_ms, WAIT_DEFAULT_WAIT_MS)?;
        // New output does not end the wait. The call returns it at the end.
        let observation = read_after(wait.cursor.as_deref())?
            .batching_output()
            .with_wait(wait_time)
            .map_err(run_failure)?;
        self.runs
            .observe(
                &caller.principal,
                &caller.workspace_id,
                &run_id,
                &observation,
            )
            .await
            .map_err(run_failure)
    }

    /// Cancels a Run, then reads it after the cursor.
    async fn cancel(&self, caller: &Caller, arguments: &str) -> Result<RunUpdate, Failure> {
        let cancel: CancelArguments = parse_arguments("cancel", arguments)?;
        let run_id = RunId::parse(&cancel.run_id).map_err(run_failure)?;
        let observation = read_after(cancel.cursor.as_deref())?;
        self.runs
            .cancel(
                &caller.principal,
                &caller.workspace_id,
                &run_id,
                &observation,
            )
            .await
            .map_err(run_failure)
    }
}

/// What `end_task` answers: the lead's Task and the outcome, in the fields of
/// core's `end_task`. The thread keeps that answer, and the lead reads the
/// outcome beside the Turn's answer. It ends nothing: the lead owns its Task,
/// and the Turn ends on the model's next message without a tool call. A model
/// trained on core's tools calls `end_task` before it answers (ADR 0040).
fn end_task(caller: &Caller, arguments: &str) -> Result<String, Failure> {
    let end: EndTaskArguments = parse_arguments("end_task", arguments)?;
    if end.task_id != caller.task_id.as_str() {
        return Err(Failure::other_task_to_end(&caller.task_id));
    }
    Ok(json!({"task_id": end.task_id, "outcome": end.outcome}).to_string())
}

/// The outcome that `end_task` recorded, read from what it answered. A refused
/// call answers with an error, and recorded nothing.
pub(crate) fn recorded_outcome(answered: &str) -> Option<Outcome> {
    #[derive(Deserialize)]
    struct Recorded {
        outcome: Outcome,
    }
    let recorded: Recorded = serde_json::from_str(answered).ok()?;
    Some(recorded.outcome)
}

/// A failure as the model reads it, recorded on the call's span.
#[expect(
    clippy::needless_pass_by_value,
    reason = "it is the `unwrap_or_else` of every tool's outcome"
)]
fn failed(failure: Failure) -> String {
    coral_telemetry::record_failure(&tracing::Span::current(), failure.code);
    failure.text()
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndTaskArguments {
    task_id: String,
    outcome: Outcome,
    /// Accepted as core accepts it. Nothing reads it but a reader of the
    /// thread.
    #[serde(rename = "feedback")]
    _feedback: Option<String>,
}

/// How the model says its work went.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub(crate) enum Outcome {
    Success,
    Failure,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecArguments {
    task_id: String,
    intent: String,
    program: String,
    run_timeout_ms: Option<u64>,
    wait_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitArguments {
    run_id: String,
    cursor: Option<String>,
    wait_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelArguments {
    run_id: String,
    cursor: Option<String>,
}

fn parse_arguments<T: DeserializeOwned>(tool: &str, arguments: &str) -> Result<T, Failure> {
    // The error describes only the JSON the model wrote.
    serde_json::from_str(arguments).map_err(|error| {
        Failure::shown(
            "invalid_arguments",
            format!("The arguments of {tool} are not valid: {error}"),
        )
    })
}

/// The wait that a tool names, or its default, at most [`MAX_WAIT_MS`].
fn wait_time(wait_ms: Option<u64>, default_ms: u64) -> Result<Duration, Failure> {
    let wait_ms = wait_ms.unwrap_or(default_ms);
    if wait_ms > MAX_WAIT_MS {
        return Err(Failure::shown(
            "invalid_arguments",
            format!("wait_ms must be at most {MAX_WAIT_MS}."),
        ));
    }
    Ok(Duration::from_millis(wait_ms))
}

/// How `exec` waits for the Run it starts, or `None` for a zero wait.
fn first_wait(wait: Duration) -> Result<Option<RunObservation>, Failure> {
    if wait.is_zero() {
        return Ok(None);
    }
    let observation = RunObservation::initial(wait).map_err(run_failure)?;
    // Acceptance links the request to the Run Trace, so the wait does not.
    Ok(Some(observation.without_trace_link()))
}

/// A read of the Run's output after `cursor`, or from its start.
fn read_after(cursor: Option<&str>) -> Result<RunObservation, Failure> {
    let cursor = cursor
        .map(RunCursor::parse)
        .transpose()
        .map_err(run_failure)?;
    RunObservation::immediate(cursor, RUN_EVENT_PAGE_MAX).map_err(run_failure)
}

/// The first [`TASK_INTENT_MAX_CHARS`] characters of `intent`.
pub(crate) fn shortened_intent(intent: &str) -> &str {
    first_chars(intent, TASK_INTENT_MAX_CHARS)
}

/// The first `count` characters of `text`.
pub(crate) fn first_chars(text: &str, count: usize) -> &str {
    match text.char_indices().nth(count) {
        Some((end, _)) => text.split_at(end).0,
        None => text,
    }
}

/// `text` whole, or its first and last [`RESULT_END_BYTES`] around a marker
/// that says how much was cut.
pub(crate) fn shortened(text: String) -> String {
    if text.len() <= RESULT_MAX_BYTES {
        return text;
    }
    let head_end = text.floor_char_boundary(RESULT_END_BYTES);
    let tail_start = text.ceil_char_boundary(text.len() - RESULT_END_BYTES);
    let (head, rest) = text.split_at(head_end);
    let (middle, tail) = rest.split_at(tail_start - head_end);
    let cut = middle.len();
    format!("{head}\n[... {cut} bytes cut from the middle of this result ...]\n{tail}")
}

/// Why a tool call failed, as the model and the lead read it.
#[derive(Debug)]
pub(crate) struct Failure {
    code: &'static str,
    message: String,
}

impl Failure {
    /// A failure whose message is safe to show the model.
    fn shown(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }

    /// The failure of a call that Coral could not complete, with no detail.
    fn internal() -> Self {
        Self::shown("internal_error", "Coral could not complete the tool call.")
    }

    fn unknown_tool(name: &str) -> Self {
        Self::shown(
            "unknown_tool",
            format!(
                "There is no tool named {name:?}. Call exec, wait, cancel, search or end_task."
            ),
        )
    }

    fn other_task(lead_task: &TaskId) -> Self {
        let task_id = lead_task.as_str();
        Self::shown(
            "wrong_task",
            format!("Nothing ran. Every exec runs in your Task: pass task_id \"{task_id}\"."),
        )
    }

    fn other_task_to_end(lead_task: &TaskId) -> Self {
        let task_id = lead_task.as_str();
        Self::shown(
            "wrong_task",
            format!("Nothing was recorded. Your Task is \"{task_id}\": pass that task_id."),
        )
    }

    fn text(&self) -> String {
        json!({"error": {"code": self.code, "message": self.message}}).to_string()
    }
}

/// Core's safe text where core marks it safe, our own wording for the few
/// failures a model can act on, and one general message for the rest.
pub(crate) fn task_failure(error: coral_task::Error) -> Failure {
    use coral_task::Error;
    match error {
        Error::InvalidInput(message) => Failure::shown("invalid_input", message),
        Error::TaskNotFound => Failure::shown("task_not_found", "Task was not found."),
        Error::TaskClosed => Failure::shown(
            "task_closed",
            "The Task has ended and runs no more Programs.",
        ),
        Error::WorkspaceNotFound => refusal_failure(AuthorityRefusal::WorkspaceNotFound),
        Error::PermissionDenied(_) => refusal_failure(AuthorityRefusal::PermissionDenied),
        Error::Workspace(error) => error
            .refusal()
            .map_or_else(Failure::internal, refusal_failure),
        Error::Search(error) => safe_or_internal(error.safe_rejection()),
        Error::Tools(error) => safe_or_internal(error.safe_rejection()),
        Error::Run(error) => run_failure(error),
        _ => Failure::internal(),
    }
}

#[expect(
    clippy::needless_pass_by_value,
    reason = "it is the `map_err` of every Run call"
)]
pub(crate) fn run_failure(error: coral_run::Error) -> Failure {
    use coral_run::Error;
    if let Some(safe) = error.safe_rejection() {
        return safe_or_internal(Some(safe));
    }
    match error {
        Error::RunNotFound => Failure::shown("run_not_found", "Run was not found."),
        Error::OwnerLost => Failure::shown(
            "run_owner_lost",
            "The Run's outcome is unknown. Call wait to read the Run again.",
        ),
        Error::RunRequestNotYetRecorded => Failure::shown(
            "run_request_not_yet_recorded",
            "The request is not yet recorded. Call again.",
        ),
        _ => Failure::internal(),
    }
}

/// The text a core module marks safe to show, or the general message.
fn safe_or_internal(safe: Option<(&'static str, String)>) -> Failure {
    safe.map_or_else(Failure::internal, |(code, message)| {
        Failure::shown(code, message)
    })
}

/// A refusal reads the same whichever module's error wrapped it.
fn refusal_failure(refusal: AuthorityRefusal) -> Failure {
    Failure::shown(refusal.code(), refusal.message())
}

/// A Run as the model reads it, with the field names of Coral's MCP tools.
/// The short fields come first, so a shortened result keeps them.
#[derive(Serialize)]
struct RunResult<'a> {
    run_id: &'a str,
    state: &'static str,
    next_cursor: String,
    has_more: bool,
    required_actions: Vec<Action<'a>>,
    events: Vec<Event<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    final_value: Option<&'a Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<ProgramFailure<'a>>,
    /// Given only once the Run has nothing more to read.
    #[serde(skip_serializing_if = "Option::is_none")]
    trace_id: Option<&'a str>,
}

#[derive(Serialize)]
struct Action<'a> {
    kind: &'static str,
    request_id: String,
    required_actor: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<&'a str>,
    deadline_at: String,
    /// The page where the person acts, relative to Coral's address.
    url: &'a str,
}

#[derive(Serialize)]
#[serde(tag = "kind")]
enum Event<'a> {
    #[serde(rename = "program.emitted")]
    Emitted {
        sequence: u64,
        occurred_at: String,
        value: &'a Value,
    },
    #[serde(rename = "program.logged")]
    Logged {
        sequence: u64,
        occurred_at: String,
        level: &'static str,
        message: &'a str,
    },
}

#[derive(Serialize)]
struct ProgramFailure<'a> {
    code: &'static str,
    message: &'a str,
    error: Option<&'a coral_run::ProgramError>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_result_shapes: Vec<ToolResultShape<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unsketched_tool_results: Option<u64>,
}

#[derive(Serialize)]
struct ToolResultShape<'a> {
    call_path: &'a str,
    invocations: u64,
    data: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    parsed_text: Option<&'a str>,
}

/// The JSON text of `update`.
fn run_result(update: &RunUpdate) -> String {
    let (final_value, failure) = match update.terminal_outcome() {
        Some(RunTerminalOutcome::Succeeded { final_value }) => match final_value {
            FinalValue::Present(value) => (Some(value.as_json()), None),
            FinalValue::Absent => (None, None),
        },
        Some(RunTerminalOutcome::Failed(failure)) => (None, Some(program_failure(update, failure))),
        Some(
            RunTerminalOutcome::Canceled
            | RunTerminalOutcome::TimedOut
            | RunTerminalOutcome::Interrupted,
        )
        | None => (None, None),
    };
    let result = RunResult {
        run_id: update.run_id().as_str(),
        state: state_name(update.state()),
        next_cursor: update.next_cursor().to_string(),
        has_more: update.has_more(),
        required_actions: update.required_actions().iter().map(action).collect(),
        events: update.events().iter().filter_map(event).collect(),
        final_value,
        failure,
        trace_id: update
            .trace_id()
            .filter(|_| !update.has_more())
            .map(coral_telemetry::TraceId::as_str),
    };
    serde_json::to_string(&result).unwrap_or_else(|_error| Failure::internal().text())
}

const fn state_name(state: RunState) -> &'static str {
    match state {
        RunState::Running => "running",
        RunState::Succeeded => "succeeded",
        RunState::Failed => "failed",
        RunState::Canceled => "canceled",
        RunState::TimedOut => "timed_out",
        RunState::Interrupted => "interrupted",
    }
}

fn action(action: &RequiredAction) -> Action<'_> {
    let (kind, request_id) = match &action.request {
        RequiredActionRequest::AccountConnection(id) => ("account_connection", id.to_string()),
        RequiredActionRequest::InvocationApproval(id) => ("invocation_approval", id.to_string()),
    };
    Action {
        kind,
        request_id,
        required_actor: match action.required_actor {
            RequiredActor::InvokingUser => "invoking_user",
            RequiredActor::WorkspaceOwner => "workspace_owner",
        },
        tool: action.call_path.as_deref(),
        provider: action.provider_label.as_deref(),
        deadline_at: timestamp(action.deadline_at_unix_nanos),
        url: &action.action_path,
    }
}

/// The Event as the model reads it, or `None` for an Event that a Run
/// Update does not carry: an update carries the Program's output only.
#[expect(
    clippy::match_same_arms,
    reason = "the named arm lists every kind there is; `RunEventKind` is non-exhaustive, so the compiler also requires the wildcard"
)]
fn event(event: &RunEvent) -> Option<Event<'_>> {
    let sequence = event.sequence();
    let occurred_at = timestamp(event.occurred_at_unix_nanos());
    match event.kind() {
        RunEventKind::ProgramEmitted { value } => Some(Event::Emitted {
            sequence,
            occurred_at,
            value: value.as_json(),
        }),
        RunEventKind::ProgramLogged { level, message } => Some(Event::Logged {
            sequence,
            occurred_at,
            level: match level {
                ProgramLogLevel::Debug => "debug",
                ProgramLogLevel::Info => "info",
                ProgramLogLevel::Warn => "warn",
                ProgramLogLevel::Error => "error",
            },
            message,
        }),
        RunEventKind::Started
        | RunEventKind::InvocationStarted { .. }
        | RunEventKind::InvocationEnded { .. }
        | RunEventKind::CancellationRequested { .. }
        | RunEventKind::Ended { .. } => None,
        // `RunEventKind` is non-exhaustive, so the compiler requires this arm.
        _ => None,
    }
}

fn program_failure<'a>(update: &'a RunUpdate, failure: &'a RunFailure) -> ProgramFailure<'a> {
    let shapes = update.tool_result_shapes();
    ProgramFailure {
        code: failure_code(failure.code()),
        message: failure.message(),
        error: failure.error(),
        tool_result_shapes: shapes.map_or_default(|shapes| {
            shapes
                .shapes()
                .iter()
                .map(|shape| ToolResultShape {
                    call_path: shape.call_path(),
                    invocations: shape.invocations(),
                    data: shape.data(),
                    parsed_text: shape.parsed_text(),
                })
                .collect()
        }),
        unsketched_tool_results: shapes
            .map(coral_run::ToolResultShapes::unsketched)
            .filter(|count| *count > 0),
    }
}

const fn failure_code(code: RunFailureCode) -> &'static str {
    match code {
        RunFailureCode::ResourceLimitExceeded => "resource_limit_exceeded",
        RunFailureCode::MemoryLimitExceeded => "memory_limit_exceeded",
        RunFailureCode::CpuLimitExceeded => "cpu_limit_exceeded",
        RunFailureCode::ProgramRejected => "program_rejected",
        RunFailureCode::InvalidProgramValue => "invalid_program_value",
        RunFailureCode::ProgramExecutionFailed => "program_execution_failed",
        RunFailureCode::RunAuthorityLost => "run_authority_lost",
    }
}

fn timestamp(unix_nanos: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(unix_nanos)
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}
