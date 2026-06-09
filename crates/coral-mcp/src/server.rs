//! RMCP server implementation for Coral's stdio MCP surface.

use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
};

use coral_api::v1::{
    CodeModeJsonValue, CodeModeRunError, CodeModeRunErrorCause, CodeModeRunEvent,
    CodeModeRunStatus, DescribeExportCandidate, DescribeExportRequest, DescribeExportResponse,
    ExecCodeModeRequest, ExecCodeModeResponse, ExportBindingKind, ExportDescription,
    ExportDiagnosticDescription, InitializeCodeModeRequest, JsonValue as ProtoJsonValue,
    ListSourcesRequest, PaginationRequest, SearchExportItem, SearchExportsRequest,
    SearchExportsResponse, SubmitFeedbackRequest, TerminateCodeModeRequest, WaitCodeModeRequest,
    WaitCodeModeResponse, code_mode_json_value, code_mode_run_event,
    json_value as proto_json_value,
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

const COMPACT_ARG_PROPERTY_LIMIT: usize = 48;
const COMPACT_ARG_DESCRIPTION_CHARS: usize = 120;
const COMPACT_NESTED_PROPERTY_LIMIT: usize = 8;
const COMPACT_ENUM_VALUE_LIMIT: usize = 12;
const COMPACT_SQL_COLUMN_LIMIT: usize = 24;
const REST_PARAMETER_LOCATIONS: &[&str] = &["path", "query", "header", "cookie"];

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
    _cell_id: &str,
    status: i32,
    events: Vec<CodeModeRunEvent>,
    initial_cursor: u64,
) -> Value {
    let mut cursor = initial_cursor;
    let mut output = Vec::new();
    let mut result = None;
    let mut error = None;
    let mut calls = BTreeMap::new();
    for event in events {
        cursor = cursor.max(event.id);
        summarize_code_mode_event(event, &mut output, &mut result, &mut error, &mut calls);
    }

    let status_label = code_mode_status_label(status);
    let include_calls = status_label == "running"
        || error.is_some()
        || calls
            .values()
            .any(|call| call.get("status").and_then(Value::as_str) == Some("failed"));
    let mut value = Map::from_iter([
        ("run_id".to_string(), json!(run_id)),
        ("status".to_string(), json!(status_label)),
        ("cursor".to_string(), json!(cursor)),
    ]);
    if status_label == "running" {
        value.insert(
            "wait".to_string(),
            json!({
                "run_id": run_id,
                "after_event_id": cursor,
            }),
        );
    }
    if !output.is_empty() {
        value.insert("output".to_string(), Value::Array(output));
    }
    if let Some(result) = result {
        value.insert("result".to_string(), result);
    }
    if let Some(error) = error {
        value.insert("error".to_string(), error);
    }
    if include_calls && !calls.is_empty() {
        value.insert(
            "calls".to_string(),
            Value::Array(calls.into_values().map(Value::Object).collect()),
        );
    }
    Value::Object(value)
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

fn summarize_code_mode_event(
    event: CodeModeRunEvent,
    output: &mut Vec<Value>,
    result: &mut Option<Value>,
    error: &mut Option<Value>,
    calls: &mut BTreeMap<String, Map<String, Value>>,
) {
    let event_id = event.id;
    let Some(payload) = event.event else {
        return;
    };
    match payload {
        code_mode_run_event::Event::ContentItem(payload) => {
            output.push(payload.item.map_or(Value::Null, json_value_from_code_mode));
        }
        code_mode_run_event::Event::ResultItem(payload) => {
            *result = Some(payload.item.map_or(Value::Null, json_value_from_code_mode));
        }
        code_mode_run_event::Event::RunFailed(payload) => {
            *error = Some(
                payload
                    .error
                    .as_ref()
                    .map_or(Value::Null, code_mode_error_value),
            );
        }
        code_mode_run_event::Event::ToolStarted(payload) => {
            summarize_tool_call(
                calls,
                event_id,
                &payload.tool_call_id,
                &payload.tool_name,
                "started",
                None,
            );
        }
        code_mode_run_event::Event::ToolCompleted(payload) => {
            summarize_tool_call(
                calls,
                event_id,
                &payload.tool_call_id,
                &payload.tool_name,
                "completed",
                None,
            );
        }
        code_mode_run_event::Event::ToolFailed(payload) => {
            summarize_tool_call(
                calls,
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

fn json_value_from_code_mode(value: CodeModeJsonValue) -> Value {
    match value.kind {
        Some(code_mode_json_value::Kind::NullValue(_)) | None => Value::Null,
        Some(code_mode_json_value::Kind::BoolValue(value)) => Value::Bool(value),
        Some(code_mode_json_value::Kind::IntegerValue(value)) => json!(value),
        Some(code_mode_json_value::Kind::UnsignedIntegerValue(value)) => json!(value),
        Some(code_mode_json_value::Kind::DoubleValue(value)) => json!(value),
        Some(code_mode_json_value::Kind::StringValue(value)) => Value::String(value),
        Some(code_mode_json_value::Kind::ObjectValue(object)) => Value::Object(
            object
                .fields
                .into_iter()
                .map(|(key, value)| (key, json_value_from_code_mode(value)))
                .collect(),
        ),
        Some(code_mode_json_value::Kind::ArrayValue(array)) => Value::Array(
            array
                .values
                .into_iter()
                .map(json_value_from_code_mode)
                .collect(),
        ),
    }
}

fn json_value_from_proto(value: ProtoJsonValue) -> Value {
    match value.kind {
        Some(proto_json_value::Kind::NullValue(_)) | None => Value::Null,
        Some(proto_json_value::Kind::BoolValue(value)) => Value::Bool(value),
        Some(proto_json_value::Kind::IntegerValue(value)) => json!(value),
        Some(proto_json_value::Kind::UnsignedIntegerValue(value)) => json!(value),
        Some(proto_json_value::Kind::DoubleValue(value)) => json!(value),
        Some(proto_json_value::Kind::StringValue(value)) => Value::String(value),
        Some(proto_json_value::Kind::ObjectValue(object)) => Value::Object(
            object
                .fields
                .into_iter()
                .map(|(key, value)| (key, json_value_from_proto(value)))
                .collect(),
        ),
        Some(proto_json_value::Kind::ArrayValue(array)) => Value::Array(
            array
                .values
                .into_iter()
                .map(json_value_from_proto)
                .collect(),
        ),
    }
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
                source_id: String::new(),
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
        let runtime = self.runtime_metadata_value().await;
        let allowed_kinds = if arguments.kind.is_empty()
            && self.options.runtime_exposure != McpRuntimeExposure::both()
        {
            let visible_kinds = visible_binding_kinds(self.options.runtime_exposure);
            if visible_kinds.is_empty() {
                let response =
                    empty_search_response(arguments.pagination.limit, arguments.pagination.offset);
                let result = search_tool_value(&response, &runtime);
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
                source_id: arguments.source_id,
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
                search_tool_value(&response, &runtime)
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
        let runtime = self.runtime_metadata_value().await;
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

fn search_tool_value(
    response: &SearchExportsResponse,
    _runtime: &Value,
) -> Result<Value, tonic::Status> {
    let items = response
        .items
        .iter()
        .map(compact_search_item_value)
        .collect::<Vec<_>>();
    let mut value = Map::from_iter([
        ("items".to_string(), Value::Array(items)),
        ("total".to_string(), json!(response.total)),
    ]);
    if response.has_more {
        value.insert(
            "next".to_string(),
            json!({
                "limit": response.limit,
                "offset": response.next_offset,
            }),
        );
    }
    if !response.diagnostics.is_empty() {
        value.insert(
            "diagnostics".to_string(),
            diagnostics_value(&response.diagnostics)?,
        );
    }
    Ok(Value::Object(value))
}

fn compact_search_item_value(item: &SearchExportItem) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&item.refs));
    insert_nonempty(&mut value, "call", &item.full_path);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&item.refs));
    insert_nonempty(&mut value, "name", preferred_name(item));
    insert_nonempty(&mut value, "source", &item.source_key);
    insert_nonempty(&mut value, "title", &item.title);
    insert_nonempty(&mut value, "description", &item.description);
    insert_nonempty(
        &mut value,
        "kind",
        kind_effect_label(&item.capability_kind, &item.effects),
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

fn preferred_name(item: &SearchExportItem) -> &str {
    if item.alias.is_empty() {
        &item.capability_id
    } else {
        &item.alias
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

fn kind_effect_label(kind: &str, effects: &[String]) -> String {
    if effects.is_empty() {
        return kind.to_string();
    }
    format!("{kind}/{}", effects.join("+"))
}

fn insert_nonempty(value: &mut Map<String, Value>, key: &str, entry: impl AsRef<str>) {
    let entry = entry.as_ref();
    if !entry.is_empty() {
        value.insert(key.to_string(), json!(entry));
    }
}

fn visible_binding_kinds(exposure: McpRuntimeExposure) -> Vec<ExportBindingKind> {
    let mut kinds = Vec::new();
    if exposure.typescript_enabled {
        kinds.push(ExportBindingKind::Typescript);
    }
    if exposure.sql_enabled {
        kinds.push(ExportBindingKind::SqlTable);
        kinds.push(ExportBindingKind::SqlFunction);
    }
    kinds
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
        response.total = response
            .total
            .saturating_sub(u32::try_from(dropped_from_page).unwrap_or(u32::MAX));
        response.has_more = false;
        response.next_offset = 0;
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
    let mut value = Map::new();
    if response.found {
        value.insert("status".to_string(), json!("found"));
        if let Some(entry) = response.entry.as_ref() {
            value.insert("entry".to_string(), compact_entry_value(entry)?);
        }
    } else if response.ambiguous {
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
        .map_or(Value::Null, json_value_from_proto);
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
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&description.refs));
    insert_nonempty(&mut value, "call", &description.full_path);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&description.refs));
    insert_nonempty(&mut value, "name", preferred_description_name(description));
    insert_nonempty(
        &mut value,
        "kind",
        kind_effect_label(&description.capability_kind, &description.effects),
    );
    value.insert(
        "args".to_string(),
        argument_summary(&code_mode_input_schema),
    );
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

fn argument_summary(schema: &Value) -> Value {
    let Some(root) = schema.as_object() else {
        return Value::Object(Map::new());
    };
    let Some(raw_properties) = root.get("properties").and_then(Value::as_object) else {
        return Value::Object(Map::new());
    };
    let visible_properties = visible_argument_properties(raw_properties);
    let visible_names = visible_properties
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<BTreeSet<_>>();
    let (required, require_one_of) = argument_required_groups(root, &visible_names);
    let mut value = Map::new();
    if !required.is_empty() {
        value.insert(
            "required".to_string(),
            Value::Array(required.into_iter().map(Value::String).collect()),
        );
    }
    if !require_one_of.is_empty() {
        value.insert("require_one_of".to_string(), json!(require_one_of));
    }
    if !visible_properties.is_empty() {
        if visible_properties.len() > COMPACT_ARG_PROPERTY_LIMIT {
            value.insert(
                "property_count".to_string(),
                json!(visible_properties.len()),
            );
        }
        value.insert(
            "properties".to_string(),
            Value::Object(
                visible_properties
                    .iter()
                    .take(COMPACT_ARG_PROPERTY_LIMIT)
                    .map(|(name, schema)| {
                        (
                            name.clone(),
                            Value::String(argument_property_summary(schema)),
                        )
                    })
                    .collect(),
            ),
        );
    }
    Value::Object(value)
}

fn argument_property_summary(schema: &Value) -> String {
    let mut summary = schema_summary(schema, 0);
    let Some(description) = schema
        .as_object()
        .and_then(|object| object.get("description"))
        .and_then(Value::as_str)
        .map(compact_description)
    else {
        return summary;
    };
    if !description.is_empty() {
        summary.push_str(" - ");
        summary.push_str(&description);
    }
    summary
}

fn compact_description(description: &str) -> String {
    let description = description.split_whitespace().collect::<Vec<_>>().join(" ");
    truncate_chars(&description, COMPACT_ARG_DESCRIPTION_CHARS)
}

fn truncate_chars(value: &str, limit: usize) -> String {
    if value.chars().count() <= limit {
        return value.to_string();
    }
    let keep = limit.saturating_sub(3);
    let mut truncated = value.chars().take(keep).collect::<String>();
    truncated.push_str("...");
    truncated
}

fn visible_argument_properties(properties: &Map<String, Value>) -> Vec<(String, &Value)> {
    properties
        .iter()
        .filter(|(name, _)| !hidden_rest_location_property(name, properties))
        .map(|(name, schema)| (name.clone(), schema))
        .collect()
}

fn hidden_rest_location_property(name: &str, properties: &Map<String, Value>) -> bool {
    if !REST_PARAMETER_LOCATIONS.contains(&name) {
        return false;
    }
    properties
        .get(name)
        .and_then(Value::as_object)
        .and_then(|schema| schema.get("properties"))
        .and_then(Value::as_object)
        .is_some_and(|location_properties| {
            location_properties
                .keys()
                .any(|property_name| properties.contains_key(property_name))
        })
}

fn argument_required_groups(
    root: &Map<String, Value>,
    visible_names: &BTreeSet<&str>,
) -> (Vec<String>, Vec<Vec<String>>) {
    let mut required = root
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .filter(|name| visible_names.contains(name))
        .map(ToString::to_string)
        .collect::<BTreeSet<_>>();
    let mut require_one_of = BTreeSet::new();

    if let Some(all_of) = root.get("allOf").and_then(Value::as_array) {
        for constraint in all_of {
            if let Some(alternatives) = required_alternatives(constraint, visible_names) {
                match alternatives.as_slice() {
                    [single] if single.len() == 1 => {
                        if let Some(name) = single.first() {
                            required.insert(name.clone());
                        }
                    }
                    [] => {}
                    _ => {
                        let group = alternatives
                            .into_iter()
                            .flatten()
                            .collect::<BTreeSet<_>>()
                            .into_iter()
                            .collect::<Vec<_>>();
                        if !group.is_empty() {
                            require_one_of.insert(group);
                        }
                    }
                }
            }
        }
    }

    (
        required.into_iter().collect(),
        require_one_of.into_iter().collect(),
    )
}

fn required_alternatives(
    constraint: &Value,
    visible_names: &BTreeSet<&str>,
) -> Option<Vec<Vec<String>>> {
    let alternatives = constraint
        .get("anyOf")
        .and_then(Value::as_array)?
        .iter()
        .filter_map(|alternative| {
            let required = alternative
                .get("required")
                .and_then(Value::as_array)?
                .iter()
                .filter_map(Value::as_str)
                .filter(|name| visible_names.contains(name))
                .map(ToString::to_string)
                .collect::<Vec<_>>();
            if required.is_empty() {
                None
            } else {
                Some(required)
            }
        })
        .collect::<Vec<_>>();
    if alternatives.is_empty() {
        None
    } else {
        Some(alternatives)
    }
}

fn schema_summary(schema: &Value, depth: usize) -> String {
    let Some(object) = schema.as_object() else {
        return "unknown".to_string();
    };
    if let Some(reference) = object.get("$ref").and_then(Value::as_str) {
        return format!("ref({})", reference.rsplit('/').next().unwrap_or(reference));
    }
    if let Some(value) = object.get("const") {
        return format!("const({})", compact_json_value(value));
    }
    if let Some(values) = object.get("enum").and_then(Value::as_array) {
        let rendered = values
            .iter()
            .take(COMPACT_ENUM_VALUE_LIMIT)
            .map(compact_json_value)
            .collect::<Vec<_>>();
        let mut summary = format!("enum[{}]", rendered.join("|"));
        if values.len() > COMPACT_ENUM_VALUE_LIMIT {
            write!(summary, "+{}", values.len() - COMPACT_ENUM_VALUE_LIMIT)
                .expect("write enum truncation marker");
        }
        return add_default(summary, object);
    }
    if let Some(variants) = object.get("anyOf").and_then(Value::as_array) {
        let rendered = variants
            .iter()
            .map(|variant| schema_summary(variant, depth))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !rendered.is_empty() {
            return add_default(rendered.join("|"), object);
        }
    }
    if let Some(variants) = object.get("oneOf").and_then(Value::as_array) {
        let rendered = variants
            .iter()
            .map(|variant| schema_summary(variant, depth))
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect::<Vec<_>>();
        if !rendered.is_empty() {
            return add_default(rendered.join("|"), object);
        }
    }
    if object.contains_key("properties") {
        return add_default(object_summary(object, depth), object);
    }
    if let Some(schema_type) = object.get("type") {
        let summary = if let Some(types) = schema_type.as_array() {
            types
                .iter()
                .filter_map(Value::as_str)
                .map(|schema_type| schema_type_summary(schema_type, object, depth))
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect::<Vec<_>>()
                .join("|")
        } else if let Some(schema_type) = schema_type.as_str() {
            schema_type_summary(schema_type, object, depth)
        } else {
            "unknown".to_string()
        };
        return add_default(summary, object);
    }
    if let Some(variants) = object.get("allOf").and_then(Value::as_array) {
        let rendered = variants
            .iter()
            .map(|variant| schema_summary(variant, depth))
            .collect::<Vec<_>>();
        if !rendered.is_empty() {
            return add_default(rendered.join("&"), object);
        }
    }
    add_default("unknown".to_string(), object)
}

fn schema_type_summary(schema_type: &str, object: &Map<String, Value>, depth: usize) -> String {
    match schema_type {
        "array" => {
            let item = object.get("items").map_or_else(
                || "unknown".to_string(),
                |items| schema_summary(items, depth + 1),
            );
            format!("array<{item}>")
        }
        "object" => object_summary(object, depth),
        _ => schema_type.to_string(),
    }
}

fn object_summary(object: &Map<String, Value>, depth: usize) -> String {
    let Some(properties) = object.get("properties").and_then(Value::as_object) else {
        if let Some(additional) = object.get("additionalProperties") {
            return match additional {
                Value::Object(_) => {
                    format!("object<string,{}>", schema_summary(additional, depth + 1))
                }
                Value::Bool(true) => "object<string,unknown>".to_string(),
                _ => "object".to_string(),
            };
        }
        return "object".to_string();
    };
    if depth >= 1 {
        return "object".to_string();
    }
    let required = object
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<BTreeSet<_>>();
    let mut fields = properties
        .iter()
        .take(COMPACT_NESTED_PROPERTY_LIMIT)
        .map(|(name, schema)| {
            let required_marker = if required.contains(name.as_str()) {
                "*"
            } else {
                ""
            };
            format!(
                "{name}{required_marker}:{}",
                schema_summary(schema, depth + 1)
            )
        })
        .collect::<Vec<_>>();
    if properties.len() > COMPACT_NESTED_PROPERTY_LIMIT {
        fields.push(format!(
            "+{}",
            properties.len() - COMPACT_NESTED_PROPERTY_LIMIT
        ));
    }
    format!("object{{{}}}", fields.join(","))
}

fn add_default(mut summary: String, object: &Map<String, Value>) -> String {
    if let Some(value) = object.get("default") {
        summary.push('=');
        summary.push_str(&compact_json_value(value));
    }
    summary
}

fn compact_json_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "value".to_string()),
    }
}

fn compact_candidate_value(candidate: &DescribeExportCandidate) -> Value {
    let mut value = Map::new();
    insert_nonempty(&mut value, "ref", preferred_ref(&candidate.refs));
    insert_nonempty(&mut value, "call", &candidate.full_path);
    insert_nonempty(&mut value, "sql", preferred_sql_ref(&candidate.refs));
    insert_nonempty(&mut value, "name", candidate_name(candidate));
    if candidate.deprecated {
        value.insert("deprecated".to_string(), Value::Bool(true));
    }
    if candidate.support_status != "generated" && !candidate.support_status.is_empty() {
        value.insert("support".to_string(), json!(&candidate.support_status));
    }
    Value::Object(value)
}

fn candidate_name(candidate: &DescribeExportCandidate) -> &str {
    if candidate.alias.is_empty() {
        &candidate.capability_id
    } else {
        &candidate.alias
    }
}

fn preferred_description_name(description: &ExportDescription) -> &str {
    if description.alias.is_empty() {
        &description.operation_id
    } else {
        &description.alias
    }
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

fn diagnostics_value(diagnostics: &[ExportDiagnosticDescription]) -> Result<Value, tonic::Status> {
    let mut value = serialize_tool_value(diagnostics)?;
    if let Value::Array(diagnostic_values) = &mut value {
        for (diagnostic, proto_diagnostic) in diagnostic_values.iter_mut().zip(diagnostics.iter()) {
            if let Some(diagnostic) = diagnostic.as_object_mut()
                && let Some(details) = proto_diagnostic.details.clone()
            {
                diagnostic.insert("details".to_string(), json_value_from_proto(details));
            }
        }
    }
    Ok(value)
}

fn normalize_diagnostics_field(value: &mut Value, diagnostics: &[ExportDiagnosticDescription]) {
    let Some(Value::Array(diagnostic_values)) = value.get_mut("diagnostics") else {
        return;
    };
    for (diagnostic, proto_diagnostic) in diagnostic_values.iter_mut().zip(diagnostics.iter()) {
        if let Some(diagnostic) = diagnostic.as_object_mut()
            && let Some(details) = proto_diagnostic.details.clone()
        {
            diagnostic.insert("details".to_string(), json_value_from_proto(details));
        }
    }
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
            entry.insert("capability".to_string(), json_value_from_proto(capability));
        }
        insert_code_mode_output_schema(entry);
        if let Some(Value::Array(diagnostics)) = entry.get_mut("diagnostics") {
            for (diagnostic, proto_diagnostic) in
                diagnostics.iter_mut().zip(description.diagnostics.iter())
            {
                if let Some(diagnostic) = diagnostic.as_object_mut()
                    && let Some(details) = proto_diagnostic.details.clone()
                {
                    diagnostic.insert("details".to_string(), json_value_from_proto(details));
                }
            }
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
            let result = build_tool_result(value);
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

#[cfg(test)]
mod tests {
    #![allow(
        clippy::indexing_slicing,
        reason = "test code: assertion-style JSON indexing is idiomatic in tests"
    )]

    use serde_json::json;

    use super::argument_summary;

    #[test]
    fn argument_summary_reports_flat_rest_required_aliases() {
        let summary = argument_summary(&json!({
            "type": "object",
            "properties": {
                "path": {
                    "type": "object",
                    "required": ["owner"],
                    "properties": {
                        "owner": { "type": "string" }
                    }
                },
                "owner": { "type": "string" },
                "body": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    }
                },
                "json": {
                    "type": "object",
                    "required": ["title"],
                    "properties": {
                        "title": { "type": "string" }
                    }
                }
            },
            "allOf": [
                {
                    "anyOf": [
                        { "required": ["owner"] },
                        {
                            "required": ["path"],
                            "properties": {
                                "path": { "required": ["owner"] }
                            }
                        }
                    ]
                },
                {
                    "anyOf": [
                        { "required": ["body"] },
                        { "required": ["json"] }
                    ]
                }
            ]
        }));

        assert_eq!(summary["required"], json!(["owner"]));
        assert_eq!(summary["require_one_of"], json!([["body", "json"]]));
        assert_eq!(summary["properties"]["owner"], "string");
        assert_eq!(summary["properties"]["body"], "object{title*:string}");
        assert!(summary["properties"].get("path").is_none());
    }

    #[test]
    fn argument_summary_includes_bounded_top_level_property_descriptions() {
        let summary = argument_summary(&json!({
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of results to return, up to a max of 20. Defaults to 20."
                }
            }
        }));

        assert_eq!(
            summary["properties"]["limit"],
            "integer - Number of results to return, up to a max of 20. Defaults to 20."
        );
    }
}
