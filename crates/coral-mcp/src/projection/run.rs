use coral_run::{
    FinalValue, ProgramLogLevel, RequiredAction, RequiredActionRequest, RequiredActor, RunEvent,
    RunEventKind, RunFailureCode, RunState, RunTerminalOutcome, RunUpdate,
};
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Value, json};

use super::diagnostics::{ProgramError, SafeInteger};
use super::{ToolFailure, success};

/// Current observation, with terminal data only on the last output page.
#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("allOf" = [
    {"if":{"properties":{"final_value":true},"required":["final_value"]},"then":{"properties":{"state":{"const":"succeeded"},"has_more":{"const":false}}}},
    {"if":{"properties":{"failure":true},"required":["failure"]},"then":{"properties":{"state":{"const":"failed"},"has_more":{"const":false}}}},
    {"if":{"properties":{"state":{"const":"failed"},"has_more":{"const":false}},"required":["state","has_more"]},"then":{"properties":{"failure":true},"required":["failure"]}},
    {"if":{"properties":{"state":{"not":{"const":"running"}}},"required":["state"]},"then":{"properties":{"required_actions":{"type":"array","maxItems":0}}}}
]))]
pub(crate) struct RunUpdateResult<'a, Task> {
    #[serde(flatten)]
    task: Task,
    #[schemars(schema_with = "crate::arguments::identifier_schema")]
    run_id: String,
    state: State,
    required_actions: Vec<Action>,
    events: Vec<Event<'a>>,
    next_cursor: String,
    has_more: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "OutputValue<'a>")]
    final_value: Option<OutputValue<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "Failure<'a>")]
    failure: Option<Failure<'a>>,
    /// The W3C trace identifier of a Run that failed, timed out or was
    /// interrupted. Give it to the operator to find the Run in Coral's
    /// telemetry. It grants no authority.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(with = "String")]
    trace_id: Option<String>,
}

/// What `exec` reports: the Run, and the Task when this call started it.
pub(crate) type ExecResult<'a> = RunUpdateResult<'a, StartedTask<'a>>;

/// What `wait` and `cancel` report. They take a `run_id`, so they never name
/// the Task.
pub(crate) type ObservedRun<'a> = RunUpdateResult<'a, NoTask>;

// Flattened into the result, so only the field carries a description.
#[derive(Clone, Copy, Serialize, JsonSchema)]
pub(crate) struct StartedTask<'a> {
    /// The Task this call started from `goal`, only on that call's result.
    /// Pass it as `task_id` to every later call for the same unit of work.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "crate::arguments::identifier_schema")]
    task_id: Option<&'a str>,
}

#[derive(Clone, Copy, Serialize, JsonSchema)]
pub(crate) struct NoTask {}

/// The tool that reports a Run Update. Only `exec` can name a Task.
#[derive(Clone, Copy)]
pub(crate) enum Reporter<'a> {
    /// `exec`, with the Task it started from `goal`, if it started one.
    Exec(Option<&'a coral_task::TaskId>),
    /// `wait` or `cancel`.
    Observer,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum State {
    Running,
    Succeeded,
    Failed,
    Canceled,
    TimedOut,
    Interrupted,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend(
    "if" = {"properties":{"kind":{"const":"account_connection"}}},
    "then" = {"properties":{"required_actor":{"const":"invoking_user"}}}
))]
struct Action {
    /// What the person must do: approve one Tool call, or connect their account with the Provider.
    kind: ActionKind,
    /// Identifies the request on the page `url` opens. It grants no authority.
    #[schemars(schema_with = "crate::arguments::uuid_schema")]
    request_id: String,
    /// Who can act: the user who started the Run, or a workspace owner.
    required_actor: Actor,
    /// The Call Path of the Tool that waits. Absent when one connection request is shared by calls to different Tools.
    #[serde(skip_serializing_if = "Option::is_none")]
    tool: Option<String>,
    /// The display name of the Provider whose Tool waits.
    #[serde(skip_serializing_if = "Option::is_none")]
    provider: Option<String>,
    /// When the request expires and its waiting call fails; the Run ends `timed_out` at its own deadline if that comes first.
    #[schemars(schema_with = "timestamp_schema")]
    deadline_at: String,
    /// Absolute URL of the page where the person decides this entry, signed in to Coral: the approval request in review, or the person's account page for the Provider for a connection. Pass it on exactly as given. It grants no approval or access authority.
    #[schemars(extend("format" = "uri"))]
    url: String,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ActionKind {
    AccountConnection,
    InvocationApproval,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Actor {
    InvokingUser,
    WorkspaceOwner,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Event<'a> {
    #[serde(rename = "program.emitted")]
    Emitted {
        #[schemars(regex(pattern = "^[1-9][0-9]*$"))]
        sequence: String,
        #[schemars(schema_with = "timestamp_schema")]
        occurred_at: String,
        value: OutputValue<'a>,
    },
    #[serde(rename = "program.logged")]
    Logged {
        #[schemars(regex(pattern = "^[1-9][0-9]*$"))]
        sequence: String,
        #[schemars(schema_with = "timestamp_schema")]
        occurred_at: String,
        level: LogLevel,
        message: &'a str,
    },
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum OutputValue<'a> {
    /// Arbitrary Program Value, including explicit null, scalars, arrays and nested rich values.
    Value { value: &'a Value },
    /// Index of a direct rich block in this `CallToolResult`'s top-level
    /// `content` array. Index 0 is always the JSON text, so this is at least 1.
    Content {
        #[schemars(extend("minimum" = 1))]
        content_index: SafeInteger,
    },
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Failure<'a> {
    code: FailureCode,
    message: &'a str,
    error: Option<ProgramError<'a>>,
    /// The shapes of the Tool Results the Program received before it threw,
    /// in call order: keys and value types, never values.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_result_shapes: Vec<ToolResultShape<'a>>,
    /// Tool Results the Program received that no entry in
    /// `tool_result_shapes` describes.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schemars(extend("minimum" = 1))]
    unsketched_tool_results: Option<SafeInteger>,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct ToolResultShape<'a> {
    call_path: &'a str,
    /// Tool Invocations of this Call Path whose results had this shape.
    #[schemars(extend("minimum" = 1))]
    invocations: SafeInteger,
    /// TypeScript-like shape of the `data` the Program received, as a Tool
    /// Result or a Tool Error. A key some array elements lack is optional.
    data: &'a str,
    /// Shape of the JSON that the result's MCP text content parses to.
    #[serde(skip_serializing_if = "Option::is_none")]
    parsed_text: Option<&'a str>,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum FailureCode {
    ResourceLimitExceeded,
    MemoryLimitExceeded,
    CpuLimitExceeded,
    ProgramRejected,
    InvalidProgramValue,
    ProgramExecutionFailed,
    RunAuthorityLost,
}

impl From<RunFailureCode> for FailureCode {
    fn from(code: RunFailureCode) -> Self {
        match code {
            RunFailureCode::ResourceLimitExceeded => Self::ResourceLimitExceeded,
            RunFailureCode::MemoryLimitExceeded => Self::MemoryLimitExceeded,
            RunFailureCode::CpuLimitExceeded => Self::CpuLimitExceeded,
            RunFailureCode::ProgramRejected => Self::ProgramRejected,
            RunFailureCode::InvalidProgramValue => Self::InvalidProgramValue,
            RunFailureCode::ProgramExecutionFailed => Self::ProgramExecutionFailed,
            RunFailureCode::RunAuthorityLost => Self::RunAuthorityLost,
        }
    }
}

/// Projects the largest prefix of `update` that fits `page_bytes`, or one
/// item, in the result shape of the tool that reports it.
pub(crate) fn run_update(
    update: &RunUpdate,
    reporter: Reporter<'_>,
    human_interface: &crate::HumanInterfaceUrl,
    page_bytes: usize,
) -> Result<CallToolResult, ToolFailure> {
    match reporter {
        Reporter::Exec(started) => {
            let started = started.map(ToString::to_string);
            let task = StartedTask {
                task_id: started.as_deref(),
            };
            paged(update, task, human_interface, page_bytes)
        }
        Reporter::Observer => paged(update, NoTask {}, human_interface, page_bytes),
    }
}

fn paged<Task: Serialize + Copy>(
    update: &RunUpdate,
    task: Task,
    human_interface: &crate::HumanInterfaceUrl,
    page_bytes: usize,
) -> Result<CallToolResult, ToolFailure> {
    let mut count = update.events().len();
    loop {
        let page = update.event_prefix(count);
        let result = project_run_update(&page, task, human_interface)?;
        if fits(&result, page_bytes) {
            return Ok(result);
        }
        // Preserve the complete outcome for the final page. If only the
        // combination is too large, deliver these Events and advance first.
        if !page.events().is_empty() && page.terminal_outcome().is_some() {
            let deferred =
                project_run_update(&page.defer_terminal_outcome(), task, human_interface)?;
            if count <= 1 || fits(&deferred, page_bytes) {
                return Ok(deferred);
            }
        }
        // The limit is a paging target. Paging never splits a value, so one
        // item above it arrives whole on its own page; domain limits bound it.
        if count <= 1 {
            return Ok(result);
        }
        // Drop whole Events only. The cursor replays everything not delivered.
        count /= 2;
    }
}

fn fits(result: &CallToolResult, page_bytes: usize) -> bool {
    serde_json::to_vec(result).is_ok_and(|bytes| bytes.len() <= page_bytes)
}

fn project_run_update<Task: Serialize>(
    update: &RunUpdate,
    task: Task,
    human_interface: &crate::HumanInterfaceUrl,
) -> Result<CallToolResult, ToolFailure> {
    let mut rich = Vec::new();
    let events = update
        .events()
        .iter()
        .map(|event| run_event(event, update, &mut rich))
        .collect::<Result<_, _>>()?;
    let (final_value, failure) = match update.terminal_outcome() {
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        }) => (
            Some(output_value(
                value.as_json(),
                &format!("urn:coral:output:{}:final", update.run_id()),
                &mut rich,
            )?),
            None,
        ),
        Some(RunTerminalOutcome::Failed(failure)) => (
            None,
            Some(Failure {
                code: failure.code().into(),
                message: failure.message(),
                error: failure.error().map(ProgramError::try_from).transpose()?,
                tool_result_shapes: update
                    .tool_result_shapes()
                    .map(|shapes| {
                        shapes
                            .shapes()
                            .iter()
                            .map(|shape| {
                                Ok(ToolResultShape {
                                    call_path: shape.call_path(),
                                    invocations: SafeInteger::new(shape.invocations())?,
                                    data: shape.data(),
                                    parsed_text: shape.parsed_text(),
                                })
                            })
                            .collect::<Result<Vec<_>, ToolFailure>>()
                    })
                    .transpose()?
                    .unwrap_or_default(),
                unsketched_tool_results: update
                    .tool_result_shapes()
                    .map(coral_run::ToolResultShapes::unsketched)
                    .filter(|count| *count > 0)
                    .map(SafeInteger::new)
                    .transpose()?,
            }),
        ),
        _ => (None, None),
    };
    let mut result = success(RunUpdateResult {
        task,
        run_id: update.run_id().to_string(),
        state: match update.state() {
            RunState::Running => State::Running,
            RunState::Succeeded => State::Succeeded,
            RunState::Failed => State::Failed,
            RunState::Canceled => State::Canceled,
            RunState::TimedOut => State::TimedOut,
            RunState::Interrupted => State::Interrupted,
        },
        required_actions: update
            .required_actions()
            .iter()
            .map(|action| required_action(action, human_interface))
            .collect(),
        events,
        next_cursor: update.next_cursor().to_string(),
        has_more: update.has_more(),
        final_value,
        failure,
        trace_id: update
            .trace_id()
            .filter(|_| !update.has_more())
            .map(|trace| trace.as_str().to_string()),
    })?;
    result.content.extend(rich);
    Ok(result)
}

/// One line for the person while a call holds: what the Run waits for from
/// them and where to act. None when nothing waits on the invoking User.
pub(crate) fn handoff_progress(
    update: &RunUpdate,
    human_interface: &crate::HumanInterfaceUrl,
) -> Option<String> {
    let action = update
        .required_actions()
        .iter()
        .find(|action| action.required_actor == RequiredActor::InvokingUser)?;
    let url = human_interface.page_url(&action.action_path);
    let label = action.provider_label.as_deref();
    let provider = label.map(|label| format!(" ({label})")).unwrap_or_default();
    // The link comes first: a client that cuts the line keeps it whole.
    Some(match (&action.request, action.call_path.as_deref()) {
        (RequiredActionRequest::InvocationApproval(_), Some(tool)) => {
            format!("{url} · approve {tool}{provider}")
        }
        (RequiredActionRequest::InvocationApproval(_), None) => {
            format!("{url} · approve a tool call{provider}")
        }
        (RequiredActionRequest::AccountConnection(_), _) => match label {
            Some(label) => format!("{url} · connect your {label} account"),
            None => format!("{url} · connect your account"),
        },
    })
}

fn required_action(action: &RequiredAction, human_interface: &crate::HumanInterfaceUrl) -> Action {
    let (kind, request_id) = match &action.request {
        RequiredActionRequest::AccountConnection(id) => {
            (ActionKind::AccountConnection, id.to_string())
        }
        RequiredActionRequest::InvocationApproval(id) => {
            (ActionKind::InvocationApproval, id.to_string())
        }
    };
    Action {
        kind,
        request_id,
        required_actor: match action.required_actor {
            RequiredActor::InvokingUser => Actor::InvokingUser,
            RequiredActor::WorkspaceOwner => Actor::WorkspaceOwner,
        },
        tool: action.call_path.clone(),
        provider: action.provider_label.clone(),
        deadline_at: timestamp(action.deadline_at_unix_nanos),
        url: human_interface.page_url(&action.action_path),
    }
}

// Every i64 Unix-nanosecond value fits chrono's range. Keep conversion integral.
fn timestamp(unix_nanos: i64) -> String {
    chrono::DateTime::from_timestamp_nanos(unix_nanos)
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn timestamp_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string", "format": "date-time",
        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"
    })
}

/// Moves a root rich value into `rich`, the blocks that follow the JSON text.
/// Nested values stay structured Program data.
fn output_value<'a>(
    value: &'a Value,
    uri: &str,
    rich: &mut Vec<ContentBlock>,
) -> Result<OutputValue<'a>, ToolFailure> {
    let block = if value.get("_tag").and_then(Value::as_str) == Some("BinaryValue") {
        let mime = value
            .get("mimeType")
            .and_then(Value::as_str)
            .unwrap_or("application/octet-stream");
        serde_json::from_value::<ContentBlock>(if mime.starts_with("image/") {
            json!({"type":"image", "data":value["data"], "mimeType":mime})
        } else if mime.starts_with("audio/") {
            json!({"type":"audio", "data":value["data"], "mimeType":mime})
        } else {
            json!({"type":"resource", "resource":{"uri":uri, "mimeType":mime, "blob":value["data"]}})
        }).ok()
    } else {
        let mut candidate = value.clone();
        // These labels identify only this retained occurrence, never resources/read capabilities.
        match candidate.get("type").and_then(Value::as_str) {
            Some("resource_link")
                if candidate
                    .pointer("/resource/$coralMcpResource")
                    .is_some_and(Value::is_string) =>
            {
                if let Some(fields) = candidate.as_object_mut() {
                    fields.insert("uri".into(), json!(uri));
                }
            }
            Some("resource")
                if candidate
                    .pointer("/resource/uri")
                    .is_some_and(Value::is_string)
                    || candidate
                        .pointer("/resource/reference/$coralMcpResource")
                        .is_some_and(Value::is_string) =>
            {
                if let Some(fields) = candidate.get_mut("resource").and_then(Value::as_object_mut) {
                    fields.insert("uri".into(), json!(uri));
                }
            }
            _ => {}
        }
        // Only SDK-typed blocks are rich content; other JSON stays a plain value.
        serde_json::from_value::<ContentBlock>(candidate).ok()
    };
    if let Some(block) = block {
        // content[0] is the JSON text of structuredContent.
        let index = u64::try_from(rich.len() + 1).map_err(|_error| ToolFailure::internal())?;
        rich.push(block);
        Ok(OutputValue::Content {
            content_index: SafeInteger::new(index)?,
        })
    } else {
        Ok(OutputValue::Value { value })
    }
}

fn run_event<'a>(
    event: &'a RunEvent,
    update: &RunUpdate,
    rich: &mut Vec<ContentBlock>,
) -> Result<Event<'a>, ToolFailure> {
    let sequence = event.sequence().to_string();
    let occurred_at = timestamp(event.occurred_at_unix_nanos());
    Ok(match event.kind() {
        RunEventKind::ProgramEmitted { value } => Event::Emitted {
            sequence,
            occurred_at,
            value: output_value(
                value.as_json(),
                &format!("urn:coral:output:{}:{}", update.run_id(), event.sequence()),
                rich,
            )?,
        },
        RunEventKind::ProgramLogged { level, message } => Event::Logged {
            sequence,
            occurred_at,
            message,
            level: match level {
                ProgramLogLevel::Debug => LogLevel::Debug,
                ProgramLogLevel::Info => LogLevel::Info,
                ProgramLogLevel::Warn => LogLevel::Warn,
                ProgramLogLevel::Error => LogLevel::Error,
            },
        },
        // Run Updates contain authored output only, not other durable Event kinds.
        _ => return Err(ToolFailure::internal()),
    })
}

#[cfg(test)]
mod tests;
