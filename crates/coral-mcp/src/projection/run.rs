use coral_db::UnixNanos;
use coral_run::{
    FinalValue, ProgramLogLevel, RequiredAction, RequiredActionRequest, RequiredActor, RunEvent,
    RunEventKind, RunFailureCode, RunId, RunState, RunTerminalOutcome, RunUpdate, RunUpdatePage,
};
use rmcp::model::{CallToolResult, ContentBlock};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::{Value, json};

use super::diagnostics::{ProgramError, SafeInteger};
use super::{ToolFailure, success};
use crate::client_prompt::{Shown, WaitCursor};

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

/// What `show_required_actions` reports: what a Run waits for from a
/// person, and nothing of its output. The app reads `required_actions`.
#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShownRequiredActions {
    #[schemars(schema_with = "crate::arguments::identifier_schema")]
    run_id: String,
    state: State,
    /// Empty when the Run waits for no person or has ended.
    required_actions: Vec<Action>,
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

impl From<RunState> for State {
    fn from(state: RunState) -> Self {
        match state {
            RunState::Running => Self::Running,
            RunState::Succeeded => Self::Succeeded,
            RunState::Failed => Self::Failed,
            RunState::Canceled => Self::Canceled,
            RunState::TimedOut => Self::TimedOut,
            RunState::Interrupted => Self::Interrupted,
        }
    }
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
    shown: &Shown,
) -> Result<CallToolResult, ToolFailure> {
    match reporter {
        Reporter::Exec(started) => {
            let started = started.map(ToString::to_string);
            let task = StartedTask {
                task_id: started.as_deref(),
            };
            paged(update, task, human_interface, page_bytes, shown)
        }
        Reporter::Observer => paged(update, NoTask {}, human_interface, page_bytes, shown),
    }
}

/// Projects each Event once and builds one result. A candidate page's size
/// comes from the counted sizes of its parts, so choosing a page copies no
/// output. What a result holds besides its Events is counted again for each
/// candidate, and the page chosen is then encoded once.
fn paged<Task: Serialize + Copy>(
    update: &RunUpdate,
    task: Task,
    human_interface: &crate::HumanInterfaceUrl,
    page_bytes: usize,
    shown: &Shown,
) -> Result<CallToolResult, ToolFailure> {
    let mut events = PageEvents::default();
    for event in update.events() {
        let (event, block) = run_event(event, update.run_id(), events.rich.len() + 1)?;
        events.push(event, block)?;
        // No page carries the Events after the one that passes the target.
        if events.end(events.len()).bytes > page_bytes {
            break;
        }
    }
    largest_page(update, &events, task, human_interface, page_bytes, shown)?.into_result(events)
}

/// Chooses the page of `update` to deliver from the Events projected so far.
fn largest_page<'a, Task: Serialize + Copy>(
    update: &'a RunUpdate,
    events: &PageEvents<'a>,
    task: Task,
    human_interface: &crate::HumanInterfaceUrl,
    page_bytes: usize,
    shown: &Shown,
) -> Result<Page<'a, Task>, ToolFailure> {
    let all = update.events().len();
    let project = |page| project_page(page, task, human_interface, events, shown);
    // The whole update is a candidate only when every Event was projected.
    if events.len() == all {
        let whole = update.event_prefix(all);
        let complete = project(whole)?;
        if complete.bytes <= page_bytes {
            return Ok(complete);
        }
        // Preserve the complete outcome for the final page. If only the
        // combination is too large, deliver these Events and advance first.
        if all > 0 && whole.terminal_outcome().is_some() {
            let deferred = project(whole.defer_terminal_outcome())?;
            if all == 1 || deferred.bytes <= page_bytes {
                return Ok(deferred);
            }
        }
        // The limit is a paging target. Paging never splits a value, so one
        // item above it arrives whole on its own page; domain limits bound it.
        if all <= 1 {
            return Ok(complete);
        }
    }
    // Drop whole Events only, and no more than the target asks. The cursor
    // replays everything not delivered.
    let mut count = events.len().min(all.saturating_sub(1));
    loop {
        let prefix = project(update.event_prefix(count))?;
        if count <= 1 || prefix.bytes <= page_bytes {
            return Ok(prefix);
        }
        count -= 1;
    }
}

/// The size of a compact JSON encoding, counted without keeping its bytes.
#[derive(Default)]
struct Encoded {
    bytes: usize,
    /// The `"` and `\` among them: a JSON string that holds this text spends
    /// one more byte on each.
    escaped: usize,
}

impl Encoded {
    fn of(value: &impl Serialize) -> Result<Self, ToolFailure> {
        let mut size = Self::default();
        serde_json::to_writer(&mut size, value).map_err(|_error| ToolFailure::internal())?;
        Ok(size)
    }

    /// The bytes a part of `structuredContent` adds to an encoded result,
    /// which carries it twice: as JSON, and in the JSON text of `content[0]`.
    const fn in_result(&self) -> usize {
        2 * self.bytes + self.escaped
    }

    /// The bytes a rich block adds to an encoded result's `content`, with
    /// the comma before it.
    fn of_block(block: &ContentBlock) -> Result<usize, ToolFailure> {
        Ok(1 + Self::of(block)?.bytes)
    }

    /// The bytes of an encoded result around its structured content and the
    /// JSON text of it.
    fn of_envelope() -> Result<usize, ToolFailure> {
        let empty = Value::Null;
        Ok(Self::of(&success(&empty)?)?.bytes - Self::of(&empty)?.in_result())
    }
}

impl std::io::Write for Encoded {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes += bytes.len();
        self.escaped += bytes
            .iter()
            .filter(|byte| matches!(byte, b'"' | b'\\'))
            .count();
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// The Events of a Run Update, projected in order from its first, with the
/// rich blocks they moved out of their values.
#[derive(Default)]
struct PageEvents<'a> {
    events: Vec<Event<'a>>,
    rich: Vec<ContentBlock>,
    /// Where the prefix that ends with each Event ends.
    ends: Vec<PrefixEnd>,
}

/// What a prefix of Events adds to a result.
#[derive(Clone, Copy, Default)]
struct PrefixEnd {
    /// Encoded bytes: the Events in both copies and their rich blocks.
    bytes: usize,
    rich: usize,
}

impl<'a> PageEvents<'a> {
    fn len(&self) -> usize {
        self.events.len()
    }

    fn push(&mut self, event: Event<'a>, block: Option<ContentBlock>) -> Result<(), ToolFailure> {
        let before = self.end(self.len());
        // A comma separates it from the Event before it, in both copies.
        let mut bytes =
            Encoded::of(&event)?.in_result() + if self.events.is_empty() { 0 } else { 2 };
        if let Some(block) = block {
            bytes += Encoded::of_block(&block)?;
            self.rich.push(block);
        }
        self.events.push(event);
        self.ends.push(PrefixEnd {
            bytes: before.bytes + bytes,
            rich: self.rich.len(),
        });
        Ok(())
    }

    /// Where the prefix of the first `count` Events ends.
    fn end(&self, count: usize) -> PrefixEnd {
        count
            .checked_sub(1)
            .and_then(|last| self.ends.get(last))
            .copied()
            .unwrap_or_default()
    }
}

/// One output page: its result without the Events, and how many it carries.
struct Page<'a, Task> {
    result: RunUpdateResult<'a, Task>,
    /// The rich block of the Final Value, after those of the Events.
    final_block: Option<ContentBlock>,
    count: usize,
    /// The size of the encoded result, with the Events.
    bytes: usize,
}

impl<'a, Task: Serialize> Page<'a, Task> {
    /// A page of the first `count` of `events`, around a `result` that has none.
    fn new(
        result: RunUpdateResult<'a, Task>,
        final_block: Option<ContentBlock>,
        events: &PageEvents<'a>,
        count: usize,
    ) -> Result<Self, ToolFailure> {
        let mut bytes =
            Encoded::of_envelope()? + Encoded::of(&result)?.in_result() + events.end(count).bytes;
        if let Some(block) = &final_block {
            bytes += Encoded::of_block(block)?;
        }
        Ok(Self {
            result,
            final_block,
            count,
            bytes,
        })
    }

    fn into_result(mut self, events: PageEvents<'a>) -> Result<CallToolResult, ToolFailure> {
        let blocks = events.end(self.count).rich;
        self.result.events = events.events;
        self.result.events.truncate(self.count);
        let mut result = success(self.result)?;
        result.content.extend(events.rich.into_iter().take(blocks));
        result.content.extend(self.final_block);
        Ok(result)
    }
}

/// Projects everything of `page` but its Events, which `events` holds.
fn project_page<'a, Task: Serialize>(
    page: RunUpdatePage<'a>,
    task: Task,
    human_interface: &crate::HumanInterfaceUrl,
    events: &PageEvents<'a>,
    shown: &Shown,
) -> Result<Page<'a, Task>, ToolFailure> {
    let count = page.events().len();
    let (final_value, final_block, failure) = match page.terminal_outcome() {
        Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value),
        }) => {
            let (value, block) = output_value(
                value.as_json(),
                &format!("urn:coral:output:{}:final", page.run_id()),
                events.end(count).rich + 1,
            )?;
            (Some(value), block, None)
        }
        Some(RunTerminalOutcome::Failed(failure)) => (
            None,
            None,
            Some(Failure {
                code: failure.code().into(),
                message: failure.message(),
                error: failure.error().map(ProgramError::try_from).transpose()?,
                tool_result_shapes: page
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
                unsketched_tool_results: page
                    .tool_result_shapes()
                    .map(coral_run::ToolResultShapes::unsketched)
                    .filter(|count| *count > 0)
                    .map(SafeInteger::new)
                    .transpose()?,
            }),
        ),
        _ => (None, None, None),
    };
    let result = RunUpdateResult {
        task,
        run_id: page.run_id().to_string(),
        state: page.state().into(),
        required_actions: required_actions(page.required_actions(), human_interface),
        events: Vec::new(),
        next_cursor: WaitCursor::format(&page.next_cursor(), shown, page.required_actions()),
        has_more: page.has_more(),
        final_value,
        failure,
        trace_id: page
            .trace_id()
            .filter(|_| !page.has_more())
            .map(|trace| trace.as_str().to_string()),
    };
    Page::new(result, final_block, events, count)
}

/// Projects what `update`'s Run waits for from a person, for the app that
/// shows it.
pub(crate) fn shown_required_actions(
    update: &RunUpdate,
    human_interface: &crate::HumanInterfaceUrl,
) -> Result<CallToolResult, ToolFailure> {
    success(ShownRequiredActions {
        run_id: update.run_id().to_string(),
        state: update.state().into(),
        required_actions: required_actions(update.required_actions(), human_interface),
    })
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
    let provider = label.map_or_default(|label| format!(" ({label})"));
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

fn required_actions(
    actions: &[RequiredAction],
    human_interface: &crate::HumanInterfaceUrl,
) -> Vec<Action> {
    actions
        .iter()
        .map(|action| required_action(action, human_interface))
        .collect()
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
        deadline_at: timestamp(UnixNanos::new(action.deadline_at_unix_nanos)),
        url: human_interface.page_url(&action.action_path),
    }
}

// The Adapter's one conversion from an instant to its wire text. Every
// instant fits chrono's range. Keep conversion integral.
fn timestamp(instant: UnixNanos) -> String {
    chrono::DateTime::from_timestamp_nanos(instant.get())
        .to_rfc3339_opts(chrono::SecondsFormat::Nanos, true)
}

fn timestamp_schema(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "string", "format": "date-time",
        "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{9}Z$"
    })
}

/// Projects a root rich value as the block at `index` of the result's
/// `content`, where rich blocks follow the JSON text, and borrows any other
/// value. Nested values stay structured Program data.
fn output_value<'a>(
    value: &'a Value,
    uri: &str,
    index: usize,
) -> Result<(OutputValue<'a>, Option<ContentBlock>), ToolFailure> {
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
    } else if !names_content_block(value) {
        None
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
    let Some(block) = block else {
        return Ok((OutputValue::Value { value }, None));
    };
    let index = u64::try_from(index).map_err(|_error| ToolFailure::internal())?;
    let content_index = SafeInteger::new(index)?;
    Ok((OutputValue::Content { content_index }, Some(block)))
}

/// Whether the SDK may read `value` as a content block. It reads the block's
/// type from an object's `type`, or from the first item of an array. Any
/// other value stays plain, and is never copied to find that out.
fn names_content_block(value: &Value) -> bool {
    let block_type = match value {
        Value::Object(fields) => fields.get("type"),
        Value::Array(items) => items.first(),
        _ => None,
    };
    matches!(
        block_type.and_then(Value::as_str),
        Some("text" | "image" | "audio" | "resource" | "resource_link")
    )
}

/// Projects an Event. Its root rich value becomes the block at `index` of
/// the result's `content`.
fn run_event<'a>(
    event: &'a RunEvent,
    run_id: &RunId,
    index: usize,
) -> Result<(Event<'a>, Option<ContentBlock>), ToolFailure> {
    let sequence = event.sequence().to_string();
    let occurred_at = timestamp(UnixNanos::new(event.occurred_at_unix_nanos()));
    Ok(match event.kind() {
        RunEventKind::ProgramEmitted { value } => {
            let uri = format!("urn:coral:output:{run_id}:{}", event.sequence());
            let (value, block) = output_value(value.as_json(), &uri, index)?;
            let event = Event::Emitted {
                sequence,
                occurred_at,
                value,
            };
            (event, block)
        }
        RunEventKind::ProgramLogged { level, message } => (
            Event::Logged {
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
            None,
        ),
        // Run Updates contain authored output only, not other durable Event kinds.
        _ => return Err(ToolFailure::internal()),
    })
}

#[cfg(test)]
mod tests;
