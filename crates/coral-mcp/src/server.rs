//! RMCP server implementation for Coral's stdio MCP surface.

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
    code_mode_response_tool_value(run_id.as_str(), cell_id.as_str(), status, events)
}

fn wait_code_mode_tool_value(response: WaitCodeModeResponse) -> Value {
    let WaitCodeModeResponse {
        run_id,
        cell_id,
        status,
        events,
    } = response;
    code_mode_response_tool_value(run_id.as_str(), cell_id.as_str(), status, events)
}

fn code_mode_response_tool_value(
    run_id: &str,
    cell_id: &str,
    status: i32,
    events: Vec<CodeModeRunEvent>,
) -> Value {
    let events = events
        .into_iter()
        .map(code_mode_event_tool_value)
        .collect::<Vec<_>>();
    json!({
        "run_id": run_id,
        "cell_id": cell_id,
        "status": code_mode_status_value(status),
        "events": events,
    })
}

fn code_mode_status_value(status: i32) -> Value {
    let status = CodeModeRunStatus::try_from(status).unwrap_or(CodeModeRunStatus::Unspecified);
    json!({
        "code": status as i32,
        "name": status.as_str_name(),
    })
}

fn code_mode_error_cause_value(cause: i32) -> Value {
    let cause =
        CodeModeRunErrorCause::try_from(cause).unwrap_or(CodeModeRunErrorCause::Unspecified);
    json!({
        "code": cause as i32,
        "name": cause.as_str_name(),
    })
}

fn code_mode_event_tool_value(event: CodeModeRunEvent) -> Value {
    let Some(payload) = event.event else {
        return json!({
            "id": event.id,
            "type": "unspecified",
        });
    };
    match payload {
        code_mode_run_event::Event::RunStarted(payload) => json!({
            "id": event.id,
            "type": "run_started",
            "run_started": { "run_id": payload.run_id },
        }),
        code_mode_run_event::Event::CellStarted(payload) => json!({
            "id": event.id,
            "type": "cell_started",
            "cell_started": {
                "run_id": payload.run_id,
                "cell_id": payload.cell_id,
            },
        }),
        code_mode_run_event::Event::ContentItem(payload) => json!({
            "id": event.id,
            "type": "content_item",
            "content_item": {
                "cell_id": payload.cell_id,
                "item": payload.item.map_or(Value::Null, json_value_from_code_mode),
            },
        }),
        code_mode_run_event::Event::ResultItem(payload) => json!({
            "id": event.id,
            "type": "result_item",
            "result_item": {
                "cell_id": payload.cell_id,
                "item": payload.item.map_or(Value::Null, json_value_from_code_mode),
            },
        }),
        code_mode_run_event::Event::RunCompleted(payload) => json!({
            "id": event.id,
            "type": "run_completed",
            "run_completed": { "run_id": payload.run_id },
        }),
        code_mode_run_event::Event::RunFailed(payload) => json!({
            "id": event.id,
            "type": "run_failed",
            "run_failed": {
                "run_id": payload.run_id,
                "error": payload.error.as_ref().map_or(Value::Null, code_mode_error_value),
            },
        }),
        code_mode_run_event::Event::ToolStarted(payload) => json!({
            "id": event.id,
            "type": "tool_started",
            "tool_started": {
                "cell_id": payload.cell_id,
                "tool_call_id": payload.tool_call_id,
                "tool_name": payload.tool_name,
            },
        }),
        code_mode_run_event::Event::ToolCompleted(payload) => json!({
            "id": event.id,
            "type": "tool_completed",
            "tool_completed": {
                "cell_id": payload.cell_id,
                "tool_call_id": payload.tool_call_id,
                "tool_name": payload.tool_name,
            },
        }),
        code_mode_run_event::Event::ToolFailed(payload) => json!({
            "id": event.id,
            "type": "tool_failed",
            "tool_failed": {
                "cell_id": payload.cell_id,
                "tool_call_id": payload.tool_call_id,
                "tool_name": payload.tool_name,
                "error": payload.error.as_ref().map_or(Value::Null, code_mode_error_value),
            },
        }),
    }
}

fn code_mode_error_value(error: &CodeModeRunError) -> Value {
    let mut value = Map::from_iter([
        (
            "cause".to_string(),
            code_mode_error_cause_value(error.cause),
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
                experimental_mutations: false,
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
                .map(|response| wait_code_mode_tool_value(response.into_inner()))
        } else {
            code_mode_client
                .wait(Request::new(WaitCodeModeRequest {
                    workspace,
                    run_id: arguments.run_id,
                    after_event_id: arguments.after_event_id,
                }))
                .await
                .map(|response| wait_code_mode_tool_value(response.into_inner()))
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
    runtime: &Value,
) -> Result<Value, tonic::Status> {
    let mut value = serialize_tool_value(response)?;
    normalize_diagnostics_field(&mut value, &response.diagnostics);
    insert_runtime_metadata(&mut value, runtime);
    Ok(value)
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
    runtime: &Value,
) -> Result<Value, tonic::Status> {
    let diagnostics = diagnostics_value(&response.diagnostics)?;
    let entry = response
        .entry
        .as_ref()
        .map(compact_entry_value)
        .transpose()?;
    Ok(json!({
        "found": response.found,
        "ambiguous": response.ambiguous,
        "entry": entry.unwrap_or(Value::Null),
        "candidates": response.candidates,
        "diagnostics": diagnostics,
        "runtime": runtime,
    }))
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
    let input_summary = input_schema_summary(&code_mode_input_schema);
    let effect_profile = capability_value
        .get("effect_profile")
        .cloned()
        .unwrap_or_else(|| {
            json!({
                "capability_kind": description.capability_kind,
                "effects": description.effects,
            })
        });
    Ok(json!({
        "capability_id": description.capability_id,
        "alias": description.alias,
        "refs": description.refs,
        "source_id": description.source_id,
        "display_name": description.display_name,
        "source_key": description.source_key,
        "interface_id": description.interface_id,
        "operation_id": description.operation_id,
        "title": description.title,
        "description": description.description,
        "deprecated": description.deprecated,
        "support_status": description.support_status,
        "capability_kind": description.capability_kind,
        "effects": description.effects,
        "effect_profile": effect_profile,
        "full_path": description.full_path,
        "typescript_binding": description.typescript_binding,
        "input": input_summary,
        "result_wrapper": {
            "shape": "{ ok, complete, partial, errors, source_status, value, error, envelope }",
            "success_value_path": "value",
            "fail_closed": "Generated tools reject by default on provider or transport failure; try/catch can recover, uncaught rejections fail the run. Use allowErrorResult only for explicit raw-error collection."
        },
        "credential_requirements": capability_value
            .get("credential_requirements")
            .cloned()
            .unwrap_or(Value::Null),
        "sql_bindings": description.sql_bindings,
        "diagnostics": diagnostics_value(&description.diagnostics)?,
    }))
}

fn input_schema_summary(schema: &Value) -> Value {
    let required = schema
        .get("required")
        .and_then(Value::as_array)
        .map(|values| {
            values
                .iter()
                .filter_map(Value::as_str)
                .map(ToString::to_string)
                .collect::<std::collections::BTreeSet<_>>()
        })
        .unwrap_or_default();
    let optional = schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .keys()
                .filter(|key| !required.contains(*key))
                .cloned()
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    json!({
        "schema": schema,
        "required": required.into_iter().collect::<Vec<_>>(),
        "optional": optional,
    })
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
