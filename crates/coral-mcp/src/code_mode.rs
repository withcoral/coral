//! MCP Code Mode projection over Coral's finite function bridge.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use coral_api::v1::Table;
use coral_code_mode::{
    CodeModeNestedToolCall, CodeModeSchemaColumn, CodeModeSchemaTable, CodeModeService,
    CodeModeToolKind, CodeModeTurnHost, CodeModeTurnWorker, ExecuteRequest,
    FunctionCallOutputContentItem, RuntimeResponse, ToolDefinition, ToolName, WaitOutcome,
    WaitRequest, build_coral_exec_tool_description, build_wait_tool_description,
    failed_structured_result, normalize_code_mode_identifier, normalize_nested_tool_input,
    parse_exec_source,
};
use rmcp::model::{Content, Tool};
use serde_json::Value;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tracing::Instrument as _;

use crate::bridge::{BridgeCallOutcome, CoralToolBridge, bridge_outcome_result};
use crate::surface::{ExecArguments, MAX_CODE_MODE_YIELD_TIME_MS, WaitArguments};
use crate::telemetry;

pub(crate) struct CodeModeState {
    service: CodeModeService,
    host: Arc<CoralCodeModeHost>,
    _worker: CodeModeTurnWorker,
}

impl CodeModeState {
    pub(crate) fn new(bridge: CoralToolBridge) -> Self {
        let service = CodeModeService::new();
        let host = Arc::new(CoralCodeModeHost::new(bridge));
        let worker = service.start_turn_worker(host.clone());
        Self {
            service,
            host,
            _worker: worker,
        }
    }

    pub(crate) fn exec_description(
        nested_tools: &[Tool],
        schema_tables: &[CodeModeSchemaTable],
    ) -> String {
        let definitions = tool_definitions(nested_tools);
        build_coral_exec_tool_description(&definitions, schema_tables)
    }

    pub(crate) async fn execute(
        &self,
        arguments: ExecArguments,
        nested_tools: &[Tool],
    ) -> BridgeCallOutcome {
        let parsed = match parse_exec_source(&arguments.source) {
            Ok(parsed) => parsed,
            Err(error) => return failed_code_mode_result(error),
        };
        if let Some(yield_time_ms) = parsed.yield_time_ms
            && yield_time_ms > MAX_CODE_MODE_YIELD_TIME_MS
        {
            return failed_code_mode_result(format!(
                "exec pragma yield_time_ms must be at most {MAX_CODE_MODE_YIELD_TIME_MS}"
            ));
        }
        let cell_id = self.service.allocate_cell_id();
        telemetry::record_code_mode_cell_id(&tracing::Span::current(), &cell_id);
        self.host
            .set_cell_span(&cell_id, tracing::Span::current())
            .await;
        let request = ExecuteRequest {
            cell_id: cell_id.clone(),
            enabled_tools: tool_definitions(nested_tools),
            source: parsed.code,
            stored_values: self.service.stored_values().await,
            yield_time_ms: arguments.yield_time_ms.or(parsed.yield_time_ms),
            max_output_tokens: arguments.max_output_tokens.or(parsed.max_output_tokens),
        };

        match self.service.execute(request).await {
            Ok(response) => self.runtime_response_result(response).await,
            Err(error) => {
                self.host.clear_cell(&cell_id).await;
                failed_code_mode_result(error)
            }
        }
    }

    pub(crate) async fn wait(&self, arguments: WaitArguments) -> BridgeCallOutcome {
        let cell_id = arguments.cell_id.clone();
        telemetry::record_code_mode_cell_id(&tracing::Span::current(), &cell_id);
        self.host
            .set_cell_span(&cell_id, tracing::Span::current())
            .await;
        let response = self
            .service
            .wait(WaitRequest {
                cell_id: arguments.cell_id,
                yield_time_ms: arguments
                    .yield_time_ms
                    .unwrap_or(coral_code_mode::DEFAULT_WAIT_YIELD_TIME_MS),
                terminate: arguments.terminate,
            })
            .await;

        match response {
            Ok(WaitOutcome::LiveCell(response) | WaitOutcome::MissingCell(response)) => {
                self.runtime_response_result(response).await
            }
            Err(error) => {
                self.host.clear_cell(&cell_id).await;
                failed_code_mode_result(error)
            }
        }
    }

    async fn runtime_response_result(&self, response: RuntimeResponse) -> BridgeCallOutcome {
        let cell_id = response.cell_id().to_string();
        let terminal = !response.is_yielded();
        let (value, content_items) = response.into_structured_result();
        let result = code_mode_result(value, content_items);
        if terminal {
            self.host.clear_cell(&cell_id).await;
        }
        result
    }
}

struct CoralCodeModeHost {
    bridge: CoralToolBridge,
    cell_spans: Mutex<HashMap<String, tracing::Span>>,
}

impl CoralCodeModeHost {
    fn new(bridge: CoralToolBridge) -> Self {
        Self {
            bridge,
            cell_spans: Mutex::new(HashMap::new()),
        }
    }

    async fn clear_cell(&self, cell_id: &str) {
        self.cell_spans.lock().await.remove(cell_id);
    }

    async fn set_cell_span(&self, cell_id: &str, span: tracing::Span) {
        self.cell_spans
            .lock()
            .await
            .insert(cell_id.to_string(), span);
    }

    async fn cell_span(&self, cell_id: &str) -> Option<tracing::Span> {
        self.cell_spans.lock().await.get(cell_id).cloned()
    }
}

#[async_trait]
impl CodeModeTurnHost for CoralCodeModeHost {
    async fn invoke_tool(
        &self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> Result<Value, String> {
        if invocation.tool_kind != CodeModeToolKind::Function {
            return Err("Coral Code Mode only supports function tools".to_string());
        }
        let cell_id = invocation.cell_id;
        let tool_name = invocation.tool_name.name;
        let input =
            normalize_nested_tool_input(&tool_name, invocation.tool_kind, invocation.input)?;
        let arguments = input.as_object().ok_or_else(|| {
            format!("tools.{tool_name} expects an object argument in Coral Code Mode")
        })?;
        let parent_span = self.cell_span(&cell_id).await;
        if cancellation_token.is_cancelled() {
            return Err(format!(
                "code mode cell {cell_id} was terminated before tools.{tool_name} completed"
            ));
        }
        tokio::select! {
            biased;
            () = cancellation_token.cancelled() => Err(format!(
                "code mode cell {cell_id} was terminated before tools.{tool_name} completed"
            )),
            outcome = async {
                if cancellation_token.is_cancelled() {
                    return Err(format!(
                        "code mode cell {cell_id} was terminated before tools.{tool_name} completed"
                    ));
                }
                let call = self.bridge.call(&tool_name, Some(arguments));
                let outcome = if let Some(parent_span) = parent_span {
                    call.instrument(parent_span).await
                } else {
                    call.await
                };
                bridge_outcome_result(outcome)
            } => outcome,
        }
    }
}

pub(crate) fn tool_definitions(tools: &[Tool]) -> Vec<ToolDefinition> {
    tools
        .iter()
        .map(|tool| {
            let name = tool.name.to_string();
            ToolDefinition {
                name: normalize_code_mode_identifier(&name),
                tool_name: ToolName::plain(name),
                description: tool.description.as_deref().unwrap_or_default().to_string(),
                kind: CodeModeToolKind::Function,
                input_schema: Some(Value::Object((*tool.input_schema).clone())),
                output_schema: tool
                    .output_schema
                    .as_ref()
                    .map(|schema| Value::Object((**schema).clone())),
            }
        })
        .collect()
}

pub(crate) fn wait_description() -> &'static str {
    build_wait_tool_description()
}

pub(crate) fn schema_tables(tables: &[Table]) -> Vec<CodeModeSchemaTable> {
    tables
        .iter()
        .map(|table| CodeModeSchemaTable {
            schema_name: table.schema_name.clone(),
            name: table.name.clone(),
            columns: table
                .columns
                .iter()
                .map(|column| CodeModeSchemaColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                })
                .collect(),
        })
        .collect()
}

fn failed_code_mode_result(message: impl Into<String>) -> BridgeCallOutcome {
    BridgeCallOutcome::Success(failed_structured_result(message))
}

fn code_mode_result(
    value: Value,
    content_items: Vec<FunctionCallOutputContentItem>,
) -> BridgeCallOutcome {
    let content = code_mode_content(content_items);
    if content.is_empty() {
        BridgeCallOutcome::Success(value)
    } else {
        BridgeCallOutcome::SuccessWithContent { value, content }
    }
}

fn code_mode_content(items: Vec<FunctionCallOutputContentItem>) -> Vec<Content> {
    items
        .into_iter()
        .map(|item| match item {
            FunctionCallOutputContentItem::InputImage { image_url, .. } => {
                data_url_image_content(&image_url)
                    .unwrap_or_else(|| Content::text(format!("[image] {image_url}")))
            }
        })
        .collect()
}

fn data_url_image_content(image_url: &str) -> Option<Content> {
    let data_url = image_url.strip_prefix("data:")?;
    let (metadata, data) = data_url.split_once(',')?;
    let mime_type = metadata.strip_suffix(";base64")?;
    if mime_type.is_empty() || data.is_empty() {
        return None;
    }
    Some(Content::image(data.to_string(), mime_type.to_string()))
}
