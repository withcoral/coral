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
pub(crate) struct RunUpdateResult<'a> {
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
    kind: ActionKind,
    #[schemars(schema_with = "crate::arguments::uuid_schema")]
    request_id: String,
    required_actor: Actor,
    #[schemars(schema_with = "timestamp_schema")]
    deadline_at: String,
    /// Absolute URL that opens authenticated human review. It grants no approval or access authority.
    #[schemars(extend("format" = "uri"))]
    run_status_url: String,
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
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum FailureCode {
    ResourceLimitExceeded,
    MemoryLimitExceeded,
    ProgramRejected,
    InvalidProgramValue,
    ProgramExecutionFailed,
    RunAuthorityLost,
}

/// Projects the largest prefix of `update` that fits `page_bytes`, or one item.
pub(crate) fn run_update(
    update: &RunUpdate,
    human_interface: &crate::HumanInterfaceUrl,
    page_bytes: usize,
) -> Result<CallToolResult, ToolFailure> {
    let mut count = update.events().len();
    loop {
        let page = update.event_prefix(count);
        let result = project_run_update(&page, human_interface)?;
        if fits(&result, page_bytes) {
            return Ok(result);
        }
        // Preserve the complete outcome for the final page. If only the
        // combination is too large, deliver these Events and advance first.
        if !page.events().is_empty() && page.terminal_outcome().is_some() {
            let deferred = project_run_update(&page.defer_terminal_outcome(), human_interface)?;
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

fn project_run_update(
    update: &RunUpdate,
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
                code: match failure.code() {
                    RunFailureCode::ResourceLimitExceeded => FailureCode::ResourceLimitExceeded,
                    RunFailureCode::MemoryLimitExceeded => FailureCode::MemoryLimitExceeded,
                    RunFailureCode::ProgramRejected => FailureCode::ProgramRejected,
                    RunFailureCode::InvalidProgramValue => FailureCode::InvalidProgramValue,
                    RunFailureCode::ProgramExecutionFailed => FailureCode::ProgramExecutionFailed,
                    RunFailureCode::RunAuthorityLost => FailureCode::RunAuthorityLost,
                },
                message: failure.message(),
                error: failure.error().map(ProgramError::try_from).transpose()?,
            }),
        ),
        _ => (None, None),
    };
    let mut result = success(RunUpdateResult {
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
        deadline_at: timestamp(action.deadline_at_unix_nanos),
        run_status_url: human_interface.page_url(&action.run_status_path),
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
