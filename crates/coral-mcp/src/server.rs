//! RMCP server implementation for Coral's stdio MCP surface.

use std::collections::BTreeMap;

use coral_api::v1::{
    CodeModeRunError, CodeModeRunErrorCause, CodeModeRunEvent, CodeModeRunStatus,
    DescribeExportCandidate, DescribeExportRequest, DescribeExportResponse, ExecCodeModeRequest,
    ExecCodeModeResponse, ExportBindingKind, ExportDescription, ExportDiagnosticDescription,
    InitializeCodeModeRequest, ListSourcesRequest, PaginationRequest, SearchExportItem,
    SearchExportsRequest, SearchExportsResponse, SubmitFeedbackRequest, TerminateCodeModeRequest,
    WaitCodeModeRequest, WaitCodeModeResponse, code_mode_run_event,
};
use coral_capabilities::{Capability, code_mode_tool_input_schema, generated_tool_output_schema};
use coral_client::{
    AppClient, CodeModeClient, DiscoveryClient, FeedbackClient, SourceClient, default_workspace,
};
use rmcp::{
    ErrorData, ServerHandler,
    model::{
        CallToolRequestParams, CallToolResult, Implementation, ListToolsResult,
        PaginatedRequestParams, ServerCapabilities, ServerInfo,
    },
    service::{RequestContext, RoleServer},
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use tonic::Request;

use crate::{
    McpOptions, McpRuntimeExposure,
    surface::{
        DescribeView, build_tool_result, describe_arguments, describe_tool, exec_arguments,
        exec_tool, feedback_tool, initial_instructions, required_string_argument, search_arguments,
        search_tool, status_to_error_data, tool_error_from_status, tool_error_result,
        wait_arguments, wait_tool,
    },
    telemetry,
};

enum ToolCallOutcome {
    Success(Value),
    ToolError {
        operation: &'static str,
        status: tonic::Status,
    },
}

const COMPACT_SQL_COLUMN_LIMIT: usize = 24;

#[derive(Serialize)]
struct FeedbackStoredValue {
    feedback_id: String,
    created_at: String,
    message: &'static str,
}

fn serialize_tool_value(value: impl Serialize) -> Result<Value, tonic::Status> {
    serde_json::to_value(value).map_err(|error| tonic::Status::internal(error.to_string()))
}

fn exec_code_mode_tool_value(response: ExecCodeModeResponse) -> Value {
    let ExecCodeModeResponse {
        run_id,
        cell_id,
        status,
        events,
    } = response;
    code_mode_response_tool_value(run_id.as_str(), cell_id.as_str(), status, events, 0)
}

fn wait_code_mode_tool_value(response: WaitCodeModeResponse, initial_cursor: u64) -> Value {
    let WaitCodeModeResponse {
        run_id,
        cell_id,
        status,
        events,
    } = response;
    code_mode_response_tool_value(
        run_id.as_str(),
        cell_id.as_str(),
        status,
        events,
        initial_cursor,
    )
}

fn code_mode_response_tool_value(
    run_id: &str,
    cell_id: &str,
    status: i32,
    events: Vec<CodeModeRunEvent>,
    initial_cursor: u64,
) -> Value {
    let mut summary = CodeModeSummary {
        last_event_id: initial_cursor,
        ..CodeModeSummary::default()
    };
    for event in events {
        summarize_code_mode_event(event, &mut summary);
    }

    let status_label = code_mode_status_label(status);
    let include_calls = status_label == "running"
        || summary.error.is_some()
        || summary
            .calls
            .values()
            .any(|call| call.get("status").and_then(Value::as_str) == Some("failed"));
    let mut run = Map::from_iter([
        ("id".to_string(), json!(run_id)),
        ("status".to_string(), json!(status_label)),
    ]);
    if !cell_id.is_empty() {
        run.insert("cell_id".to_string(), json!(cell_id));
    }

    let mut events = Map::from_iter([
        ("last_event_id".to_string(), json!(summary.last_event_id)),
        (
            "next_after_event_id".to_string(),
            json!(summary.last_event_id),
        ),
        ("has_more".to_string(), json!(status_label == "running")),
        ("items".to_string(), Value::Array(summary.events)),
    ]);
    if include_calls && !summary.calls.is_empty() {
        events.insert(
            "tool_calls".to_string(),
            Value::Array(summary.calls.into_values().map(Value::Object).collect()),
        );
    }

    let mut value = Map::from_iter([
        ("run".to_string(), Value::Object(run)),
        ("result".to_string(), code_mode_result_value(summary.result)),
        ("events".to_string(), Value::Object(events)),
    ]);
    if let Some(error) = summary.error {
        value.insert("error".to_string(), error);
    }
    Value::Object(value)
}

#[derive(Default)]
struct CodeModeSummary {
    last_event_id: u64,
    events: Vec<Value>,
    result: Option<Value>,
    error: Option<Value>,
    calls: BTreeMap<String, Map<String, Value>>,
}

fn code_mode_status_label(status: i32) -> &'static str {
    let status = CodeModeRunStatus::try_from(status).unwrap_or(CodeModeRunStatus::Unspecified);
    match status {
        CodeModeRunStatus::Running => "running",
        CodeModeRunStatus::Completed => "completed",
        CodeModeRunStatus::Failed => "failed",
        CodeModeRunStatus::Terminated => "terminated",
        CodeModeRunStatus::Unspecified => "unknown",
    }
}

fn code_mode_error_cause_label(cause: i32) -> &'static str {
    let cause =
        CodeModeRunErrorCause::try_from(cause).unwrap_or(CodeModeRunErrorCause::Unspecified);
    match cause {
        CodeModeRunErrorCause::UserException => "user_exception",
        CodeModeRunErrorCause::OutputBudgetExceeded => "output_budget_exceeded",
        CodeModeRunErrorCause::HeapLimitExceeded => "heap_limit_exceeded",
        CodeModeRunErrorCause::ToolUnavailable => "tool_unavailable",
        CodeModeRunErrorCause::NestedToolFailed => "nested_tool_failed",
        CodeModeRunErrorCause::SqlError => "sql_error",
        CodeModeRunErrorCause::Internal => "internal",
        CodeModeRunErrorCause::Unspecified => "unknown",
    }
}

fn summarize_code_mode_event(event: CodeModeRunEvent, summary: &mut CodeModeSummary) {
    let event_id = event.id;
    summary.last_event_id = summary.last_event_id.max(event_id);
    let Some(payload) = event.event else {
        return;
    };
    match payload {
        code_mode_run_event::Event::ContentItem(payload) => {
            let value = payload.item.map_or(Value::Null, Value::from);
            summary
                .events
                .push(code_mode_content_event(event_id, value));
        }
        code_mode_run_event::Event::ResultItem(payload) => {
            summary.result = Some(payload.item.map_or(Value::Null, Value::from));
        }
        code_mode_run_event::Event::RunFailed(payload) => {
            summary.error = Some(
                payload
                    .error
                    .as_ref()
                    .map_or(Value::Null, code_mode_error_value),
            );
        }
        code_mode_run_event::Event::ToolStarted(payload) => {
            summarize_tool_call(
                &mut summary.calls,
                event_id,
                &payload.tool_call_id,
                &payload.tool_name,
                "started",
                None,
            );
        }
        code_mode_run_event::Event::ToolCompleted(payload) => {
            summarize_tool_call(
                &mut summary.calls,
                event_id,
                &payload.tool_call_id,
                &payload.tool_name,
                "completed",
                None,
            );
        }
        code_mode_run_event::Event::ToolFailed(payload) => {
            summarize_tool_call(
                &mut summary.calls,
                event_id,
                &payload.tool_call_id,
                &payload.tool_name,
                "failed",
                payload.error.as_ref().map(code_mode_error_value),
            );
        }
        code_mode_run_event::Event::RunStarted(_)
        | code_mode_run_event::Event::CellStarted(_)
        | code_mode_run_event::Event::RunCompleted(_) => {}
    }
}

fn code_mode_content_event(event_id: u64, value: Value) -> Value {
    match value {
        Value::String(text) => json!({
            "id": event_id,
            "type": "stdout",
            "text": text,
        }),
        Value::Object(object)
            if object.get("type").and_then(Value::as_str) == Some("output_shaping") =>
        {
            json!({
                "id": event_id,
                "type": "output_shaping",
                "metadata": object,
            })
        }
        value => json!({
            "id": event_id,
            "type": "content",
            "value": value,
        }),
    }
}

fn code_mode_result_value(result: Option<Value>) -> Value {
    let Some(result) = result else {
        return Value::Null;
    };
    if let Some(value) = truncated_code_mode_result_value(&result) {
        return value;
    }
    json!({
        "status": "available",
        "format": "json",
        "truncated": false,
        "preview": result,
    })
}

fn truncated_code_mode_result_value(result: &Value) -> Option<Value> {
    let object = result.as_object()?;
    if object.get("type").and_then(Value::as_str) != Some("code_mode_truncated_result") {
        return None;
    }
    let preview = object.get("preview").cloned().unwrap_or(Value::Null);
    let preview = preview
        .as_str()
        .and_then(|text| serde_json::from_str::<Value>(text).ok())
        .unwrap_or(preview);
    let truncation = object.get("truncation").cloned().unwrap_or(Value::Null);
    let mut value = Map::from_iter([
        ("status".to_string(), json!("available")),
        ("format".to_string(), json!("json")),
        ("truncated".to_string(), json!(true)),
        ("preview".to_string(), preview),
        ("truncation".to_string(), truncation.clone()),
    ]);
    if let Some(path) = truncation
        .pointer("/full_output_path")
        .and_then(Value::as_str)
        .filter(|path| !path.is_empty())
    {
        let mut artifact = Map::from_iter([
            ("kind".to_string(), json!("json")),
            ("path".to_string(), json!(path)),
        ]);
        if let Some(bytes) = truncation
            .pointer("/original_bytes")
            .and_then(Value::as_u64)
        {
            artifact.insert("bytes".to_string(), json!(bytes));
        }
        value.insert("artifact".to_string(), Value::Object(artifact));
    }
    if let Some(output_shaping) = object.get("output_shaping") {
        value.insert("output_shaping".to_string(), output_shaping.clone());
    }
    Some(Value::Object(value))
}

fn summarize_tool_call(
    calls: &mut BTreeMap<String, Map<String, Value>>,
    event_id: u64,
    tool_call_id: &str,
    tool_name: &str,
    status: &str,
    error: Option<Value>,
) {
    let id = if tool_call_id.is_empty() {
        format!("event:{event_id}")
    } else {
        tool_call_id.to_string()
    };
    let call = calls.entry(id.clone()).or_insert_with(|| {
        Map::from_iter([
            ("id".to_string(), json!(id)),
            ("name".to_string(), json!(tool_name)),
        ])
    });
    call.insert("name".to_string(), json!(tool_name));
    call.insert("status".to_string(), json!(status));
    if let Some(error) = error {
        call.insert("error".to_string(), error);
    }
}

fn code_mode_error_value(error: &CodeModeRunError) -> Value {
    let mut value = Map::from_iter([
        (
            "cause".to_string(),
            json!(code_mode_error_cause_label(error.cause)),
        ),
        ("message".to_string(), json!(&error.message)),
    ]);
    if !error.correlation_id.is_empty() {
        value.insert("correlation_id".to_string(), json!(&error.correlation_id));
    }
    Value::Object(value)
}

impl ToolCallOutcome {
    fn from_value_result(operation: &'static str, result: Result<Value, tonic::Status>) -> Self {
        match result {
            Ok(value) => Self::Success(value),
            Err(status) => Self::ToolError { operation, status },
        }
    }
}

#[derive(Clone)]
pub(crate) struct CoralMcpServer {
    discovery: DiscoveryClient,
    code_mode: CodeModeClient,
    source: SourceClient,
    feedback: FeedbackClient,
    options: McpOptions,
}

impl CoralMcpServer {
    pub(crate) fn new(app: &AppClient, options: McpOptions) -> Self {
        Self {
            discovery: app.discovery_client(),
            code_mode: app.code_mode_client(),
            source: app.source_client(),
            feedback: app.feedback_client(),
            options,
        }
    }

    async fn runtime_metadata_value(&self) -> Value {
        let mut source_client = self.source.clone();
        let installed_source_count = match source_client
            .list_sources(Request::new(ListSourcesRequest {
                workspace: Some(default_workspace()),
            }))
            .await
        {
            Ok(response) => json!(response.into_inner().sources.len()),
            Err(status) => json!({
                "error": status.message(),
            }),
        };
        let typescript_count = if self.options.runtime_exposure.typescript_enabled {
            self.visible_binding_count(ExportBindingKind::Typescript)
                .await
        } else {
            json!(0)
        };
        let sql_table_count = if self.options.runtime_exposure.sql_enabled {
            self.visible_binding_count(ExportBindingKind::SqlTable)
                .await
        } else {
            json!(0)
        };
        let sql_function_count = if self.options.runtime_exposure.sql_enabled {
            self.visible_binding_count(ExportBindingKind::SqlFunction)
                .await
        } else {
            json!(0)
        };
        json!({
            "exposure": self.options.runtime_exposure.label(),
            "typescript_enabled": self.options.runtime_exposure.typescript_enabled,
            "sql_enabled": self.options.runtime_exposure.sql_enabled,
            "installed_source_count": installed_source_count,
            "visible_bindings": {
                "typescript": typescript_count,
                "sql_table": sql_table_count,
                "sql_function": sql_function_count,
            },
        })
    }

    async fn visible_binding_count(&self, kind: ExportBindingKind) -> Value {
        let mut discovery_client = self.discovery.clone();
        match discovery_client
            .search(Request::new(SearchExportsRequest {
                workspace: Some(default_workspace()),
                query: String::new(),
                source_key: String::new(),
                display_name: String::new(),
                kind: kind as i32,
                allowed_kinds: Vec::new(),
                capability_kind: String::new(),
                effect: String::new(),
                pagination: Some(PaginationRequest {
                    limit: 1,
                    offset: 0,
                }),
            }))
            .await
        {
            Ok(response) => json!(response.into_inner().total),
            Err(status) => json!({
                "error": status.message(),
            }),
        }
    }

    async fn submit_feedback_value(
        &self,
        trying_to_do: &str,
        tried: &str,
        stuck: &str,
    ) -> Result<Value, tonic::Status> {
        let mut feedback_client = self.feedback.clone();
        let response = feedback_client
            .submit_feedback(Request::new(SubmitFeedbackRequest {
                workspace: Some(default_workspace()),
                trying_to_do: trying_to_do.to_string(),
                tried: tried.to_string(),
                stuck: stuck.to_string(),
            }))
            .await?
            .into_inner();
        let report = response
            .report
            .ok_or_else(|| tonic::Status::internal("feedback response missing report"))?;
        serialize_tool_value(FeedbackStoredValue {
            feedback_id: report.id,
            created_at: report.created_at,
            message: "Feedback report stored.",
        })
    }

    async fn search_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = search_arguments(request_arguments)?;
        if !arguments.kind.is_empty()
            && !self
                .options
                .runtime_exposure
                .exposes_tool_kind(&arguments.kind)
        {
            return Err(ErrorData::invalid_params(
                format!(
                    "search kind '{}' is hidden by runtime exposure",
                    arguments.kind
                ),
                None,
            ));
        }
        let mut discovery_client = self.discovery.clone();
        let allowed_kinds = if arguments.kind.is_empty()
            && self.options.runtime_exposure != McpRuntimeExposure::both()
        {
            let visible_kinds = visible_binding_kinds(self.options.runtime_exposure);
            if visible_kinds.is_empty() {
                let response =
                    empty_search_response(arguments.pagination.limit, arguments.pagination.offset);
                let result = search_tool_value(&response);
                return Ok(ToolCallOutcome::from_value_result("Search", result));
            }
            visible_kinds.into_iter().map(|kind| kind as i32).collect()
        } else {
            Vec::new()
        };
        let result = discovery_client
            .search(Request::new(SearchExportsRequest {
                workspace: Some(default_workspace()),
                query: arguments.query,
                source_key: arguments.source_key,
                display_name: arguments.display_name,
                kind: binding_kind_from_tool(&arguments.kind) as i32,
                allowed_kinds,
                capability_kind: arguments.capability_kind,
                effect: arguments.effect,
                pagination: Some(PaginationRequest {
                    limit: arguments.pagination.limit,
                    offset: arguments.pagination.offset,
                }),
            }))
            .await
            .map(|response| {
                let mut response = response.into_inner();
                prune_search_response_for_runtime(&mut response, self.options.runtime_exposure);
                search_tool_value(&response)
            })
            .and_then(std::convert::identity);
        Ok(ToolCallOutcome::from_value_result("Search", result))
    }

    async fn describe_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = describe_arguments(request_arguments)?;
        if reference_is_hidden_by_runtime(&arguments.reference, self.options.runtime_exposure) {
            return Err(ErrorData::invalid_params(
                format!(
                    "describe reference '{}' is hidden by runtime exposure",
                    arguments.reference
                ),
                None,
            ));
        }
        let mut discovery_client = self.discovery.clone();
        // Runtime metadata costs several sequential discovery RPCs and is only
        // rendered by the detailed view; the default compact view ignores it.
        let runtime = if arguments.view == DescribeView::Detailed {
            self.runtime_metadata_value().await
        } else {
            Value::Null
        };
        let result = discovery_client
            .describe(Request::new(DescribeExportRequest {
                workspace: Some(default_workspace()),
                reference: arguments.reference,
            }))
            .await
            .map(|response| {
                let mut response = response.into_inner();
                prune_describe_response_for_runtime(&mut response, self.options.runtime_exposure);
                describe_tool_value(&response, arguments.view, &runtime)
            })
            .and_then(std::convert::identity);
        Ok(ToolCallOutcome::from_value_result("Describe", result))
    }

    async fn exec_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = exec_arguments(request_arguments)?;
        let mut code_mode_client = self.code_mode.clone();
        code_mode_client
            .initialize(Request::new(InitializeCodeModeRequest {
                workspace: Some(default_workspace()),
            }))
            .await
            .map_err(|status| status_to_error_data(&status))?;
        let result = code_mode_client
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(default_workspace()),
                source: arguments.source,
            }))
            .await
            .map(|response| exec_code_mode_tool_value(response.into_inner()));
        Ok(ToolCallOutcome::from_value_result("Code Mode exec", result))
    }

    async fn wait_tool_result(
        &self,
        request_arguments: Option<&Map<String, Value>>,
    ) -> Result<ToolCallOutcome, ErrorData> {
        let arguments = wait_arguments(request_arguments)?;
        let mut code_mode_client = self.code_mode.clone();
        let workspace = Some(default_workspace());
        let result = if arguments.terminate {
            code_mode_client
                .terminate(Request::new(TerminateCodeModeRequest {
                    workspace,
                    run_id: arguments.run_id,
                }))
                .await
                .map(|response| wait_code_mode_tool_value(response.into_inner(), 0))
        } else {
            code_mode_client
                .wait(Request::new(WaitCodeModeRequest {
                    workspace,
                    run_id: arguments.run_id,
                    after_event_id: arguments.after_event_id,
                }))
                .await
                .map(|response| {
                    wait_code_mode_tool_value(response.into_inner(), arguments.after_event_id)
                })
        };
        Ok(ToolCallOutcome::from_value_result("Code Mode wait", result))
    }

    async fn dispatch_tool(
        &self,
        request: CallToolRequestParams,
    ) -> Result<ToolCallOutcome, ErrorData> {
        match request.name.as_ref() {
            "search" => self.search_tool_result(request.arguments.as_ref()).await,
            "describe" => self.describe_tool_result(request.arguments.as_ref()).await,
            "exec" => self.exec_tool_result(request.arguments.as_ref()).await,
            "wait" => self.wait_tool_result(request.arguments.as_ref()).await,
            "feedback" => {
                let trying_to_do =
                    required_string_argument(request.arguments.as_ref(), "trying_to_do")?;
                let tried = required_string_argument(request.arguments.as_ref(), "tried")?;
                let stuck = required_string_argument(request.arguments.as_ref(), "stuck")?;
                Ok(ToolCallOutcome::from_value_result(
                    "Feedback submission",
                    self.submit_feedback_value(&trying_to_do, &tried, &stuck)
                        .await,
                ))
            }
            _ => Err(ErrorData::invalid_params(
                format!("tool '{}' not found", request.name),
                None,
            )),
        }
    }
}

fn empty_search_response(limit: u32, offset: u32) -> SearchExportsResponse {
    SearchExportsResponse {
        items: Vec::new(),
        total: 0,
        has_more: false,
        next_offset: 0,
        limit,
        offset,
        diagnostics: Vec::new(),
    }
}

fn search_tool_value(response: &SearchExportsResponse) -> Result<Value, tonic::Status> {
    let max_score = response
        .items
        .iter()
        .map(|item| item.score)
        .max()
        .unwrap_or(0);
    let items = response
        .items
        .iter()
        .map(|item| compact_search_item_value(item, max_score))
        .collect::<Vec<_>>();
    let mut value = Map::from_iter([
        ("items".to_string(), Value::Array(items)),
        (
            "pagination".to_string(),
            json!({
                "limit": response.limit,
                "offset": response.offset,
                "total": response.total,
                "next_offset": response.has_more.then_some(response.next_offset),
            }),
        ),
    ]);
    if !response.diagnostics.is_empty() {
        value.insert(
            "diagnostics".to_string(),
            diagnostics_value(&response.diagnostics)?,
        );
    }
    Ok(Value::Object(value))
}

fn compact_search_item_value(item: &SearchExportItem, max_score: u32) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&item.refs));
    insert_nonempty(&mut value, "call", &item.full_path);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&item.refs));
    insert_nonempty(&mut value, "source_key", &item.source_key);
    insert_nonempty(&mut value, "capability_kind", &item.capability_kind);
    insert_effect_fields(&mut value, &item.effects);
    insert_nonempty(&mut value, "title", &item.title);
    insert_nonempty(&mut value, "description", &item.description);
    value.insert("score".to_string(), normalized_score(item.score, max_score));
    // Absolute, cross-page-comparable magnitude. `score` is normalized to the
    // current page's max (a weak top hit still reads 1.0), so `raw_score` lets
    // the LLM reranker judge true confidence and compare across pages.
    value.insert("raw_score".to_string(), json!(item.score));
    if !item.matched_terms.is_empty() {
        value.insert("matched_terms".to_string(), json!(&item.matched_terms));
    }
    if !item.matched_fields.is_empty() {
        value.insert("matched_fields".to_string(), json!(&item.matched_fields));
    }
    insert_nonempty(&mut value, "rank_reason", &item.rank_reason);
    value.insert(
        "input_schema_available".to_string(),
        json!(search_item_input_schema_available(item)),
    );
    if item.deprecated {
        value.insert("deprecated".to_string(), Value::Bool(true));
    }
    if item.support_status != "generated" && !item.support_status.is_empty() {
        value.insert("support".to_string(), json!(&item.support_status));
    }
    if item.diagnostic_count > 0 {
        value.insert("diagnostics".to_string(), json!(item.diagnostic_count));
    }
    Value::Object(value)
}

fn normalized_score(score: u32, max_score: u32) -> Value {
    if max_score == 0 {
        return json!(0.0);
    }
    let value = (f64::from(score) / f64::from(max_score) * 100.0).round() / 100.0;
    json!(value)
}

fn search_item_input_schema_available(item: &SearchExportItem) -> bool {
    item.available_bindings.iter().any(|kind| {
        *kind == ExportBindingKind::Typescript as i32
            || *kind == ExportBindingKind::SqlFunction as i32
    })
}

fn insert_effect_fields(value: &mut Map<String, Value>, effects: &[String]) {
    match effects {
        [] => {}
        [effect] => {
            value.insert("effect".to_string(), json!(effect));
        }
        effects => {
            value.insert("effects".to_string(), json!(effects));
        }
    }
}

fn preferred_ref(refs: &[String]) -> &str {
    refs.iter()
        .find(|ref_| ref_.starts_with("typescript:"))
        .or_else(|| refs.iter().find(|ref_| ref_.starts_with("sql_table:")))
        .or_else(|| refs.iter().find(|ref_| ref_.starts_with("sql_function:")))
        .or_else(|| refs.first())
        .map_or("", String::as_str)
}

fn preferred_sql_ref(refs: &[String]) -> &str {
    refs.iter()
        .find(|ref_| ref_.starts_with("sql_table:") || ref_.starts_with("sql_function:"))
        .map_or("", String::as_str)
}

fn insert_nonempty(value: &mut Map<String, Value>, key: &str, entry: impl AsRef<str>) {
    let entry = entry.as_ref();
    if !entry.is_empty() {
        value.insert(key.to_string(), json!(entry));
    }
}

fn visible_binding_kinds(exposure: McpRuntimeExposure) -> Vec<ExportBindingKind> {
    exposure
        .visible_search_kinds()
        .map(binding_kind_from_tool)
        .collect()
}

fn prune_search_response_for_runtime(
    response: &mut SearchExportsResponse,
    exposure: McpRuntimeExposure,
) {
    let original_len = response.items.len();
    for item in &mut response.items {
        prune_search_item_for_runtime(item, exposure);
    }
    response.items.retain(search_item_has_visible_binding);
    let dropped_from_page = original_len.saturating_sub(response.items.len());
    if dropped_from_page > 0 {
        // Entries are already exposure-filtered before pagination in
        // coral-app's `filter_runtime_exposure`, so this is a defensive
        // second pass. Adjust the reported total, but never force
        // `has_more = false` / `next_offset = 0`: doing so silently hid every
        // later page whenever a single in-page item was pruned.
        response.total = response
            .total
            .saturating_sub(u32::try_from(dropped_from_page).unwrap_or(u32::MAX));
    }
}

fn prune_search_item_for_runtime(item: &mut SearchExportItem, exposure: McpRuntimeExposure) {
    item.refs.retain(|ref_| ref_is_visible(ref_, exposure));
    item.available_bindings
        .retain(|kind| binding_kind_i32_is_visible(*kind, exposure));
    if !exposure.typescript_enabled {
        item.full_path.clear();
        item.alias.clear();
    }
}

fn search_item_has_visible_binding(item: &SearchExportItem) -> bool {
    !item.refs.is_empty() || !item.available_bindings.is_empty() || !item.full_path.is_empty()
}

fn prune_describe_response_for_runtime(
    response: &mut DescribeExportResponse,
    exposure: McpRuntimeExposure,
) {
    if let Some(entry) = &mut response.entry {
        prune_description_for_runtime(entry, exposure);
        if !description_has_visible_binding(entry) {
            response.found = false;
            response.entry = None;
        }
    }
    for candidate in &mut response.candidates {
        prune_candidate_for_runtime(candidate, exposure);
    }
    response.candidates.retain(candidate_has_visible_binding);
    if response.candidates.is_empty() {
        response.ambiguous = false;
    }
}

fn prune_description_for_runtime(
    description: &mut ExportDescription,
    exposure: McpRuntimeExposure,
) {
    description
        .refs
        .retain(|ref_| ref_is_visible(ref_, exposure));
    if !exposure.typescript_enabled {
        description.alias.clear();
        description.typescript_path.clear();
        description.typescript_binding = None;
        description.full_path.clear();
    }
    if exposure.sql_enabled {
        description
            .sql_bindings
            .retain(|binding| binding_kind_i32_is_visible(binding.kind, exposure));
    } else {
        description.sql_bindings.clear();
    }
}

fn description_has_visible_binding(description: &ExportDescription) -> bool {
    !description.refs.is_empty()
        || description.typescript_binding.is_some()
        || !description.sql_bindings.is_empty()
        || !description.full_path.is_empty()
}

fn prune_candidate_for_runtime(
    candidate: &mut DescribeExportCandidate,
    exposure: McpRuntimeExposure,
) {
    candidate.refs.retain(|ref_| ref_is_visible(ref_, exposure));
    candidate
        .binding_kinds
        .retain(|kind| binding_kind_i32_is_visible(*kind, exposure));
    if !exposure.typescript_enabled {
        candidate.full_path.clear();
        candidate.alias.clear();
    }
}

fn candidate_has_visible_binding(candidate: &DescribeExportCandidate) -> bool {
    !candidate.refs.is_empty()
        || !candidate.binding_kinds.is_empty()
        || !candidate.full_path.is_empty()
}

fn ref_is_visible(ref_: &str, exposure: McpRuntimeExposure) -> bool {
    if ref_.starts_with("typescript:") {
        return exposure.typescript_enabled;
    }
    if ref_.starts_with("sql_table:") || ref_.starts_with("sql_function:") {
        return exposure.sql_enabled;
    }
    true
}

fn reference_is_hidden_by_runtime(reference: &str, exposure: McpRuntimeExposure) -> bool {
    let reference = reference.trim();
    (reference.starts_with("typescript:") && !exposure.typescript_enabled)
        || ((reference.starts_with("sql_table:") || reference.starts_with("sql_function:"))
            && !exposure.sql_enabled)
        || (reference.starts_with("tools.") && !exposure.typescript_enabled)
}

fn binding_kind_i32_is_visible(kind: i32, exposure: McpRuntimeExposure) -> bool {
    ExportBindingKind::try_from(kind).is_ok_and(|kind| binding_kind_is_visible(kind, exposure))
}

fn binding_kind_is_visible(kind: ExportBindingKind, exposure: McpRuntimeExposure) -> bool {
    match kind {
        ExportBindingKind::Unspecified => true,
        ExportBindingKind::Typescript => exposure.typescript_enabled,
        ExportBindingKind::SqlTable | ExportBindingKind::SqlFunction => exposure.sql_enabled,
    }
}

fn describe_tool_value(
    response: &DescribeExportResponse,
    view: DescribeView,
    runtime: &Value,
) -> Result<Value, tonic::Status> {
    if view == DescribeView::Compact {
        return compact_describe_tool_value(response, runtime);
    }
    let mut value = serialize_tool_value(response)?;
    normalize_diagnostics_field(&mut value, &response.diagnostics);
    normalize_describe_tool_value(&mut value, response);
    insert_runtime_metadata(&mut value, runtime);
    Ok(value)
}

fn insert_runtime_metadata(value: &mut Value, runtime: &Value) {
    if let Some(object) = value.as_object_mut() {
        object.insert("runtime".to_string(), runtime.clone());
    }
}

fn compact_describe_tool_value(
    response: &DescribeExportResponse,
    _runtime: &Value,
) -> Result<Value, tonic::Status> {
    if response.found {
        if let Some(entry) = response.entry.as_ref() {
            return compact_entry_value(entry);
        }
        return Ok(json!({ "status": "not_found" }));
    }

    let mut value = Map::new();
    if response.ambiguous {
        value.insert("status".to_string(), json!("ambiguous"));
        value.insert(
            "candidates".to_string(),
            Value::Array(
                response
                    .candidates
                    .iter()
                    .map(compact_candidate_value)
                    .collect(),
            ),
        );
    } else {
        value.insert("status".to_string(), json!("not_found"));
    }
    if !response.diagnostics.is_empty() {
        value.insert(
            "diagnostics".to_string(),
            diagnostics_value(&response.diagnostics)?,
        );
    }
    Ok(Value::Object(value))
}

fn compact_entry_value(description: &ExportDescription) -> Result<Value, tonic::Status> {
    let capability_value = description
        .capability
        .clone()
        .map_or(Value::Null, Value::from);
    let code_mode_input_schema = serde_json::from_value::<Capability>(capability_value.clone())
        .map(|capability| code_mode_tool_input_schema(&capability))
        .or_else(|_| {
            Ok::<_, tonic::Status>(
                capability_value
                    .pointer("/input_schema/schema")
                    .cloned()
                    .unwrap_or(Value::Null),
            )
        })?;
    let code_mode_output_schema = serde_json::from_value::<Capability>(capability_value)
        .map_or(Value::Null, |capability| {
            generated_tool_output_schema(&capability)
        });
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&description.refs));
    insert_nonempty(&mut value, "call", &description.full_path);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&description.refs));
    insert_nonempty(&mut value, "source_key", &description.source_key);
    insert_nonempty(&mut value, "capability_kind", &description.capability_kind);
    insert_effect_fields(&mut value, &description.effects);
    insert_nonempty(&mut value, "title", &description.title);
    insert_nonempty(&mut value, "description", &description.description);
    value.insert("input_schema".to_string(), code_mode_input_schema.clone());
    if !code_mode_output_schema.is_null() {
        value.insert("output_schema".to_string(), code_mode_output_schema);
    }
    if let Some(example) = compact_call_example(description, &code_mode_input_schema) {
        value.insert("examples".to_string(), Value::Array(vec![example]));
    }
    let sql = description
        .sql_bindings
        .iter()
        .map(compact_sql_binding_value)
        .collect::<Vec<_>>();
    if !sql.is_empty() {
        value.insert("sql_bindings".to_string(), Value::Array(sql));
    }
    if description.deprecated {
        value.insert("deprecated".to_string(), Value::Bool(true));
    }
    if description.support_status != "generated" && !description.support_status.is_empty() {
        value.insert("support".to_string(), json!(&description.support_status));
    }
    if !description.diagnostics.is_empty() {
        value.insert(
            "diagnostics".to_string(),
            diagnostics_value(&description.diagnostics)?,
        );
    }
    Ok(Value::Object(value))
}

fn compact_call_example(description: &ExportDescription, input_schema: &Value) -> Option<Value> {
    let call = (!description.full_path.is_empty()).then_some(description.full_path.as_str())?;
    let args = example_args_from_schema(input_schema);
    let args_text = serde_json::to_string(&args).ok()?;
    Some(json!({
        "call": call,
        "args": args,
        "javascript": format!("await {call}({args_text});"),
    }))
}

fn example_args_from_schema(schema: &Value) -> Value {
    let required = schema
        .as_object()
        .and_then(|object| object.get("required"))
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(|name| (name.to_string(), example_value_for_property(schema, name)))
        .collect::<Map<_, _>>();
    Value::Object(required)
}

fn example_value_for_property(schema: &Value, name: &str) -> Value {
    let Some(property_schema) = schema.pointer(&format!("/properties/{name}")) else {
        return Value::String(format!("<{name}>"));
    };
    if let Some(default) = property_schema.get("default") {
        return default.clone();
    }
    if let Some(values) = property_schema.get("enum").and_then(Value::as_array)
        && let Some(value) = values.first()
    {
        return value.clone();
    }
    match property_schema.get("type").and_then(Value::as_str) {
        Some("integer" | "number") => json!(0),
        Some("boolean") => json!(false),
        Some("array") => Value::Array(Vec::new()),
        Some("object") => Value::Object(Map::new()),
        _ => Value::String(format!("<{name}>")),
    }
}

fn compact_candidate_value(candidate: &DescribeExportCandidate) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&candidate.refs));
    insert_nonempty(&mut value, "call", &candidate.full_path);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&candidate.refs));
    if candidate.deprecated {
        value.insert("deprecated".to_string(), Value::Bool(true));
    }
    if candidate.support_status != "generated" && !candidate.support_status.is_empty() {
        value.insert("support".to_string(), json!(&candidate.support_status));
    }
    Value::Object(value)
}

fn compact_sql_binding_value(binding: &coral_api::v1::SqlBindingDescription) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", &binding.r#ref);
    insert_nonempty(&mut value, "sql", &binding.sql_reference);
    insert_nonempty(&mut value, "shape", &binding.row_shape);
    if binding.columns.len() > COMPACT_SQL_COLUMN_LIMIT {
        value.insert("column_count".to_string(), json!(binding.columns.len()));
    }
    let columns = binding
        .columns
        .iter()
        .take(COMPACT_SQL_COLUMN_LIMIT)
        .map(|column| format!("{}:{}", column.name, column.data_type))
        .collect::<Vec<_>>();
    if !columns.is_empty() {
        value.insert("columns".to_string(), json!(columns));
    }
    let inputs = binding
        .inputs
        .iter()
        .map(|input| {
            let required = if input.required { "*" } else { "" };
            format!("{}{required}:{}", input.name, input.data_type)
        })
        .collect::<Vec<_>>();
    if !inputs.is_empty() {
        value.insert("inputs".to_string(), json!(inputs));
    }
    Value::Object(value)
}

/// Hydrates each serialized diagnostic object with its provider `details`,
/// pairing the rendered values with their proto diagnostics by position.
fn inject_diagnostic_details(values: &mut [Value], diagnostics: &[ExportDiagnosticDescription]) {
    for (value, proto_diagnostic) in values.iter_mut().zip(diagnostics.iter()) {
        if let Some(object) = value.as_object_mut()
            && let Some(details) = proto_diagnostic.details.clone()
        {
            object.insert("details".to_string(), Value::from(details));
        }
    }
}

fn diagnostics_value(diagnostics: &[ExportDiagnosticDescription]) -> Result<Value, tonic::Status> {
    let mut value = serialize_tool_value(diagnostics)?;
    if let Value::Array(diagnostic_values) = &mut value {
        inject_diagnostic_details(diagnostic_values, diagnostics);
    }
    Ok(value)
}

fn normalize_diagnostics_field(value: &mut Value, diagnostics: &[ExportDiagnosticDescription]) {
    let Some(Value::Array(diagnostic_values)) = value.get_mut("diagnostics") else {
        return;
    };
    inject_diagnostic_details(diagnostic_values, diagnostics);
}

fn normalize_describe_tool_value(value: &mut Value, response: &DescribeExportResponse) {
    let found = value.get("found").and_then(Value::as_bool) == Some(true);
    if !found {
        return;
    }
    let Some(description) = response.entry.as_ref() else {
        return;
    };
    if let Some(entry) = value.get_mut("entry").and_then(Value::as_object_mut) {
        if let Some(capability) = description.capability.clone() {
            entry.insert("capability".to_string(), Value::from(capability));
        }
        insert_code_mode_output_schema(entry);
        if let Some(Value::Array(diagnostics)) = entry.get_mut("diagnostics") {
            inject_diagnostic_details(diagnostics, &description.diagnostics);
        }
    }
}

fn insert_code_mode_output_schema(description: &mut Map<String, Value>) {
    let Some(capability_value) = description.get("capability").cloned() else {
        return;
    };
    let Ok(capability) = serde_json::from_value::<Capability>(capability_value) else {
        return;
    };
    description.insert(
        "code_mode_output_schema".to_string(),
        generated_tool_output_schema(&capability),
    );
    description.insert(
        "code_mode_input_schema".to_string(),
        code_mode_tool_input_schema(&capability),
    );
}

impl ServerHandler for CoralMcpServer {
    fn get_info(&self) -> ServerInfo {
        ServerInfo::new(ServerCapabilities::builder().enable_tools().build())
            .with_server_info(Implementation::new("coral", env!("CARGO_PKG_VERSION")))
            .with_instructions(initial_instructions(self.options.runtime_exposure))
    }

    async fn list_tools(
        &self,
        _request: Option<PaginatedRequestParams>,
        _context: RequestContext<RoleServer>,
    ) -> Result<ListToolsResult, ErrorData> {
        let span = telemetry::list_tools_span(self.options.trace_parent.as_deref());
        telemetry::instrument_protocol(span, async {
            let tools = vec![
                search_tool(self.options.runtime_exposure),
                describe_tool(self.options.runtime_exposure),
                exec_tool(),
                wait_tool(),
                feedback_tool(),
            ];
            Ok(ListToolsResult::with_all_items(tools))
        })
        .await
    }

    async fn call_tool(
        &self,
        request: CallToolRequestParams,
        _context: RequestContext<RoleServer>,
    ) -> Result<CallToolResult, ErrorData> {
        let span =
            telemetry::call_tool_span(request.name.as_ref(), self.options.trace_parent.as_deref());
        let outcome = telemetry::instrument(span.clone(), self.dispatch_tool(request)).await;
        finish_tool_call(&span, outcome)
    }
}

fn finish_tool_call(
    span: &tracing::Span,
    outcome: Result<ToolCallOutcome, ErrorData>,
) -> Result<CallToolResult, ErrorData> {
    match outcome {
        Ok(ToolCallOutcome::Success(value)) => {
            let result = Ok(build_tool_result(value));
            telemetry::record_protocol_result(span, &result);
            result
        }
        Ok(ToolCallOutcome::ToolError { operation, status }) => {
            telemetry::record_tonic_status(span, &status);
            Ok(tool_error_result(tool_error_from_status(
                operation, &status,
            )))
        }
        Err(error) => {
            telemetry::record_protocol_error(span, &error);
            Err(error)
        }
    }
}

fn binding_kind_from_tool(kind: &str) -> ExportBindingKind {
    match kind {
        "typescript" => ExportBindingKind::Typescript,
        "sql_table" => ExportBindingKind::SqlTable,
        "sql_function" => ExportBindingKind::SqlFunction,
        _ => ExportBindingKind::Unspecified,
    }
}
