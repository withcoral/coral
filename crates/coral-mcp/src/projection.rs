//! MCP-owned structured results. Serialization and advertisements use the same types.
mod diagnostics;
mod discovery;
mod run;

use coral_task::TaskOutcome;
use coral_workspace::TraceId;
use rmcp::model::CallToolResult;
use schemars::{
    JsonSchema, Schema,
    generate::SchemaSettings,
    transform::{RestrictFormats, Transform as _},
};
use serde::Serialize;
use serde_json::{Map, Value};

pub(crate) use discovery::{
    Description, SavedDetail, SavedFunctionResult, SearchResult, exact_description, saved_function,
    search,
};
pub(crate) use run::{RunUpdateResult, run_update};

/// Every Coral-owned result keeps the SDK's compact JSON text of
/// `structuredContent` as `content[0]`, as both served MCP versions advise.
/// Content-only clients, and clients that show tool errors from `content`,
/// read the same result there. Direct rich blocks follow it.
pub(crate) fn success(value: impl Serialize) -> Result<CallToolResult, ToolFailure> {
    let value = serde_json::to_value(value).map_err(|_error| ToolFailure::internal())?;
    Ok(CallToolResult::structured(value))
}

/// A Coral-owned tool failure, named by the trace of the call that failed.
pub(crate) fn failure(error: &ToolFailure, trace: &TraceId) -> CallToolResult {
    CallToolResult::structured_error(
        serde_json::to_value(TracedFailure {
            code: error.code,
            message: &error.message,
            trace_id: trace.as_str(),
        })
        .expect("string-only tool failures serialize"),
    )
}

/// Why Coral could not complete a tool call. Its result names the call's
/// trace beside it, never inside caller-influenced text.
pub(crate) struct ToolFailure {
    code: &'static str,
    message: String,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(rename = "ToolFailure")]
struct TracedFailure<'a> {
    code: &'static str,
    message: &'a str,
    /// The W3C trace identifier of this tool call. Give it to the operator
    /// to find the call in Coral's telemetry. It grants no authority.
    trace_id: &'a str,
}

impl ToolFailure {
    pub(crate) const fn code(&self) -> &'static str {
        self.code
    }

    pub(crate) fn stable(code: &'static str, message: &'static str) -> Self {
        Self {
            code,
            message: message.to_string(),
        }
    }

    pub(crate) fn safe(code: &'static str, message: impl AsRef<str>) -> Self {
        Self {
            code,
            message: bounded_message(message.as_ref()),
        }
    }

    pub(crate) fn internal() -> Self {
        Self::stable("internal_error", "Coral could not complete the tool call")
    }
}

fn bounded_message(message: &str) -> String {
    const MAXIMUM_BYTES: usize = 4 * 1024;
    message
        .get(..message.floor_char_boundary(MAXIMUM_BYTES.min(message.len())))
        .expect("floor_char_boundary returns a valid UTF-8 boundary")
        .to_string()
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct StartedTask {
    #[schemars(schema_with = "crate::arguments::identifier_schema")]
    pub(crate) task_id: String,
    /// The namespaces of the Providers you can search, and coral.workspace for
    /// the Built-in Provider's tools: the values `search` accepts as `provider`.
    pub(crate) providers: Vec<String>,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeletedFunction {
    pub(crate) call_path: String,
}

#[derive(Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(crate) struct EndedTask {
    #[schemars(schema_with = "crate::arguments::identifier_schema")]
    pub(crate) task_id: String,
    pub(crate) outcome: Outcome,
}

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
    Success,
    Failure,
}

impl From<TaskOutcome> for Outcome {
    fn from(outcome: TaskOutcome) -> Self {
        match outcome {
            TaskOutcome::Success => Self::Success,
            TaskOutcome::Failure => Self::Failure,
        }
    }
}

/// The MCP envelope's isError remains authoritative. Terminal Run failures
/// are successful observations, not `ToolFailure` values.
#[derive(JsonSchema)]
#[serde(untagged)]
#[expect(
    dead_code,
    reason = "schema union; the SDK constructs the tool envelope"
)]
enum ToolOutput<T> {
    Success(T),
    Failure(TracedFailure<'static>),
}

pub(crate) fn output_schema<T: JsonSchema>() -> Map<String, Value> {
    let mut schema = SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator()
        .into_root_schema_for::<ToolOutput<T>>();
    // MCP requires an explicit object root even when the result is a union.
    schema.insert("type".into(), Value::String("object".into()));
    RestrictFormats::default().transform(&mut schema);
    Schema::ensure_object(&mut schema).clone()
}
