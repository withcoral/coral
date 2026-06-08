//! Implements the gRPC `CodeModeService`.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use coral_api::v1::code_mode_json_value;
use coral_api::v1::code_mode_run_event;
use coral_api::v1::code_mode_service_server::CodeModeService as CodeModeServiceApi;
use coral_api::v1::{
    CodeModeCellStarted, CodeModeContentItem, CodeModeJsonArray, CodeModeJsonNull,
    CodeModeJsonObject, CodeModeJsonValue, CodeModeResultItem, CodeModeRunCompleted,
    CodeModeRunError as ProtoCodeModeRunError, CodeModeRunErrorCause as ProtoCodeModeRunErrorCause,
    CodeModeRunEvent as ProtoCodeModeRunEvent, CodeModeRunFailed, CodeModeRunStarted,
    CodeModeRunStatus as ProtoCodeModeRunStatus, CodeModeToolCompleted, CodeModeToolFailed,
    CodeModeToolStarted, ExecCodeModeRequest, ExecCodeModeResponse, InitializeCodeModeRequest,
    InitializeCodeModeResponse, JsonValue as ProtoJsonValue, TerminateCodeModeRequest,
    WaitCodeModeRequest, WaitCodeModeResponse, json_value as proto_json_value,
};
use coral_capabilities::{Capability, code_mode_tool_input_schema, generated_tool_output_schema};
use coral_client::batches_to_json_rows_json_safe_numbers;
use coral_code_mode::{
    CodeModeNestedToolCall, CodeModeService as V8CodeModeService, CodeModeToolKind,
    CodeModeTurnHost, CodeModeTurnWorker, ExecuteRequest, RuntimeResponse, ToolDefinition,
    ToolName, WaitOutcome, WaitRequest, parse_exec_source,
};
use coral_exports::{Binding, CapabilityExport};
use coral_sql::ColumnInfo;
use serde_json::{Map as JsonMap, Value as JsonValue, json};
use tokio::sync::{Mutex, Notify, oneshot};
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Response, Status};
use uuid::Uuid;

use crate::RuntimeExposureMode;
use crate::capability::service::{CapabilityInvocationRequest, CapabilityInvoker};
use crate::discovery::manager::{DiscoveryManager, DiscoveryPagination, DiscoverySearchFilter};
use crate::query::manager::{QueryManager, QueryManagerError};
use crate::transport::{grpc_span, instrument_grpc, workspace_name_from_proto};
use crate::workspaces::WorkspaceName;

const SEARCH_TOOL_NAME: &str = "coral.search";
const DESCRIBE_TOOL_NAME: &str = "coral.describe";
const SQL_QUERY_TOOL_NAME: &str = "coral.sql.query";
const MAX_LIVE_RUNS_PER_WORKSPACE: usize = 8;
const MAX_LIVE_RUNS_GLOBAL: usize = 64;
const MAX_TERMINAL_RUN_HISTORY: usize = 64;

#[derive(Clone)]
pub(crate) struct CodeModeService {
    runtime: Arc<AppCodeModeRuntime>,
    runtime_exposure: RuntimeExposureMode,
}

impl CodeModeService {
    pub(crate) fn new(
        discovery: DiscoveryManager,
        queries: QueryManager,
        capability_invoker: CapabilityInvoker,
        runtime_exposure: RuntimeExposureMode,
    ) -> Self {
        let engine = Arc::new(V8CodeModeService::new());
        let runs = Arc::new(Mutex::new(HashMap::new()));
        let notify = Arc::new(Notify::new());
        let host = Arc::new(AppCodeModeHost {
            discovery,
            queries,
            capability_invoker,
            runtime_exposure,
            runs: runs.clone(),
            notify: notify.clone(),
            cell_contexts: Mutex::new(HashMap::new()),
        });
        let worker = engine.start_turn_worker(host.clone());
        Self {
            runtime: Arc::new(AppCodeModeRuntime {
                engine,
                host,
                runs,
                terminal_run_sequence: AtomicU64::new(1),
                stored_values_by_workspace: Mutex::new(HashMap::new()),
                initialized_by_workspace: Mutex::new(HashMap::new()),
                _worker: worker,
                notify,
            }),
            runtime_exposure,
        }
    }
}

#[tonic::async_trait]
impl CodeModeServiceApi for CodeModeService {
    async fn initialize(
        &self,
        request: Request<InitializeCodeModeRequest>,
    ) -> Result<Response<InitializeCodeModeResponse>, Status> {
        let span = grpc_span(&request);
        let runtime = self.runtime.clone();
        let runtime_exposure = self.runtime_exposure;
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let initialized = InitializedSession {
                workspace_name,
                experimental_mutations: request.experimental_mutations,
            };
            runtime
                .initialized_by_workspace
                .lock()
                .await
                .insert(initialized.workspace_name.clone(), initialized.clone());
            Ok(Response::new(InitializeCodeModeResponse {
                protocol_version: 1,
                workspace_id: initialized.workspace_name.as_str().to_string(),
                experimental_mutations: initialized.experimental_mutations,
                supports_search: true,
                supports_describe: true,
                supports_sql: runtime_exposure.exposes_sql(),
                supports_invoke: runtime_exposure.exposes_typescript(),
            }))
        })
        .await
    }

    async fn exec(
        &self,
        request: Request<ExecCodeModeRequest>,
    ) -> Result<Response<ExecCodeModeResponse>, Status> {
        let span = grpc_span(&request);
        let runtime = self.runtime.clone();
        let runtime_exposure = self.runtime_exposure;
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let initialized = runtime.initialized_for_workspace(&workspace_name).await?;
            let parsed = parse_exec_source(&request.source).map_err(Status::invalid_argument)?;
            let run_id = format!("run_{}", Uuid::new_v4());
            let cell_id = format!("cell_{}", runtime.engine.allocate_cell_id());
            let enabled_tools = runtime.enabled_tools(&workspace_name, runtime_exposure)?;
            let stored_values = runtime.stored_values_for_workspace(&workspace_name).await;
            let cancellation_token = CancellationToken::new();
            let record = RunRecord::new(
                workspace_name.clone(),
                run_id.clone(),
                cell_id.clone(),
                cancellation_token.clone(),
                stored_values.clone(),
            );
            runtime.register_run(&workspace_name, &run_id, record).await?;
            runtime
                .host
                .set_cell_context(
                    &cell_id,
                    CellContext {
                        run_id: run_id.clone(),
                        workspace_name: workspace_name.clone(),
                        experimental_mutations: initialized.experimental_mutations,
                    },
                )
                .await;

            let (initial_response_tx, initial_response_rx) = oneshot::channel();
            let task_runtime = runtime.clone();
            let task_workspace_name = workspace_name.clone();
            let task_run_id = run_id.clone();
            let task_cell_id = cell_id.clone();
            let cell_id_for_error = cell_id.clone();
            let cell_id_for_cancel = cell_id;
            tokio::spawn(async move {
                let execute_request = ExecuteRequest {
                    cell_id: task_cell_id,
                    enabled_tools,
                    source: parsed.code,
                    stored_values,
                    yield_time_ms: parsed.yield_time_ms,
                    max_output_tokens: parsed.max_output_tokens,
                };
                let response = tokio::select! {
                    response = task_runtime.engine.execute(execute_request) => {
                        response.unwrap_or_else(|error| runtime_error_response(cell_id_for_error, error))
                    }
                    () = cancellation_token.cancelled() => {
                        task_runtime.terminate_cell_response(cell_id_for_cancel).await
                    }
                };
                task_runtime
                    .record_runtime_response(&task_run_id, response)
                    .await;
                let response = {
                    let runs = task_runtime.runs.lock().await;
                    run_response(&runs, &task_workspace_name, &task_run_id, None)
                        .map(exec_response_to_proto)
                };
                let _send_result = initial_response_tx.send(response);
            });
            let response = initial_response_rx
                .await
                .map_err(|_recv_error| {
                    Status::internal("Code Mode exec ended before initial response")
                })??;
            Ok(Response::new(response))
        })
        .await
    }

    async fn wait(
        &self,
        request: Request<WaitCodeModeRequest>,
    ) -> Result<Response<WaitCodeModeResponse>, Status> {
        let span = grpc_span(&request);
        let runtime = self.runtime.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            let cursor = (request.after_event_id != 0).then_some(request.after_event_id);
            runtime
                .poll_run_if_ready(&workspace_name, &request.run_id, false)
                .await?;
            let runs = runtime.runs.lock().await;
            let response = run_response(&runs, &workspace_name, &request.run_id, cursor)?;
            Ok(Response::new(wait_response_to_proto(response)))
        })
        .await
    }

    async fn terminate(
        &self,
        request: Request<TerminateCodeModeRequest>,
    ) -> Result<Response<WaitCodeModeResponse>, Status> {
        let span = grpc_span(&request);
        let runtime = self.runtime.clone();
        instrument_grpc(span, async move {
            let request = request.into_inner();
            let workspace_name = workspace_name_from_proto(request.workspace.as_ref())?;
            runtime
                .poll_run_if_ready(&workspace_name, &request.run_id, true)
                .await?;
            let runs = runtime.runs.lock().await;
            let response = run_response(&runs, &workspace_name, &request.run_id, None)?;
            Ok(Response::new(wait_response_to_proto(response)))
        })
        .await
    }
}

struct AppCodeModeRuntime {
    engine: Arc<V8CodeModeService>,
    host: Arc<AppCodeModeHost>,
    runs: Arc<Mutex<HashMap<String, RunRecord>>>,
    terminal_run_sequence: AtomicU64,
    stored_values_by_workspace: Mutex<HashMap<WorkspaceName, HashMap<String, JsonValue>>>,
    initialized_by_workspace: Mutex<HashMap<WorkspaceName, InitializedSession>>,
    _worker: CodeModeTurnWorker,
    notify: Arc<Notify>,
}

impl AppCodeModeRuntime {
    async fn initialized_for_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<InitializedSession, Status> {
        self.initialized_by_workspace
            .lock()
            .await
            .get(workspace_name)
            .cloned()
            .ok_or_else(|| Status::failed_precondition("Code Mode session is not initialized"))
    }

    fn enabled_tools(
        &self,
        workspace_name: &WorkspaceName,
        runtime_exposure: RuntimeExposureMode,
    ) -> Result<Vec<ToolDefinition>, Status> {
        let mut tools = vec![search_tool_definition(), describe_tool_definition()];
        if runtime_exposure.exposes_sql() {
            tools.push(sql_tool_definition());
        }
        if runtime_exposure.exposes_typescript() {
            tools.extend(self.host.generated_tool_definitions(workspace_name)?);
        }
        Ok(tools)
    }

    async fn stored_values_for_workspace(
        &self,
        workspace_name: &WorkspaceName,
    ) -> HashMap<String, JsonValue> {
        self.stored_values_by_workspace
            .lock()
            .await
            .get(workspace_name)
            .cloned()
            .unwrap_or_default()
    }

    async fn register_run(
        &self,
        workspace_name: &WorkspaceName,
        run_id: &str,
        record: RunRecord,
    ) -> Result<(), Status> {
        let mut runs = self.runs.lock().await;
        check_live_run_limits(&runs, workspace_name)?;
        runs.insert(run_id.to_string(), record);
        Ok(())
    }

    async fn poll_run_if_ready(
        &self,
        workspace_name: &WorkspaceName,
        run_id: &str,
        terminate: bool,
    ) -> Result<(), Status> {
        let cancelled_cell_id = if terminate {
            self.request_termination(workspace_name, run_id).await?
        } else {
            None
        };
        let poll = self.wait_until_pollable(workspace_name, run_id).await?;
        let Some(cell_id) = poll else {
            if let Some(cell_id) = cancelled_cell_id {
                self.record_runtime_response(
                    run_id,
                    RuntimeResponse::Terminated {
                        cell_id,
                        content_items: Vec::new(),
                    },
                )
                .await;
            }
            return Ok(());
        };
        let response = self
            .engine
            .wait(WaitRequest {
                cell_id,
                yield_time_ms: coral_code_mode::DEFAULT_WAIT_YIELD_TIME_MS,
                terminate,
            })
            .await
            .map_err(Status::internal)?;
        let response = match response {
            WaitOutcome::LiveCell(response) | WaitOutcome::MissingCell(response) => response,
        };
        self.record_runtime_response(run_id, response).await;
        Ok(())
    }

    async fn request_termination(
        &self,
        workspace_name: &WorkspaceName,
        run_id: &str,
    ) -> Result<Option<String>, Status> {
        let cell_id = {
            let runs = self.runs.lock().await;
            let record = runs
                .get(run_id)
                .ok_or_else(|| Status::not_found("Code Mode run was not found"))?;
            if record.workspace_name != *workspace_name {
                return Err(Status::not_found("Code Mode run was not found"));
            }
            if record.terminal {
                return Ok(None);
            }
            record.cancellation_token.cancel();
            record.cell_id.clone()
        };
        self.notify.notify_waiters();
        Ok(Some(cell_id))
    }

    async fn wait_until_pollable(
        &self,
        workspace_name: &WorkspaceName,
        run_id: &str,
    ) -> Result<Option<String>, Status> {
        let timeout = tokio::time::sleep(Duration::from_millis(
            coral_code_mode::DEFAULT_WAIT_YIELD_TIME_MS,
        ));
        tokio::pin!(timeout);
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            {
                let runs = self.runs.lock().await;
                let record = runs
                    .get(run_id)
                    .ok_or_else(|| Status::not_found("Code Mode run was not found"))?;
                if record.workspace_name != *workspace_name {
                    return Err(Status::not_found("Code Mode run was not found"));
                }
                if record.terminal {
                    return Ok(None);
                }
                if !record.initial_execution_pending {
                    return Ok(Some(record.cell_id.clone()));
                }
            }
            tokio::select! {
                () = &mut notified => {}
                () = &mut timeout => return Ok(None),
            }
        }
    }

    async fn terminate_cell_response(&self, cell_id: String) -> RuntimeResponse {
        match self
            .engine
            .wait(WaitRequest {
                cell_id: cell_id.clone(),
                yield_time_ms: coral_code_mode::DEFAULT_WAIT_YIELD_TIME_MS,
                terminate: true,
            })
            .await
        {
            Ok(WaitOutcome::LiveCell(response)) => response,
            Ok(WaitOutcome::MissingCell(_)) => RuntimeResponse::Terminated {
                cell_id,
                content_items: Vec::new(),
            },
            Err(error) => runtime_error_response(cell_id, error),
        }
    }

    async fn record_runtime_response(&self, run_id: &str, response: RuntimeResponse) {
        let terminal = !response.is_yielded();
        let cell_id = response.cell_id().to_string();
        let record_update = {
            let mut runs = self.runs.lock().await;
            if let Some(record) = runs.get_mut(run_id) {
                if record.terminal {
                    return;
                }
                let workspace_name = record.workspace_name.clone();
                let stored_value_updates = stored_value_updates(&response);
                record.initial_execution_pending = false;
                record.apply_runtime_response(response);
                if terminal {
                    record.terminal_sequence.get_or_insert_with(|| {
                        self.terminal_run_sequence.fetch_add(1, Ordering::Relaxed)
                    });
                    prune_terminal_runs(&mut runs);
                }
                Some(RecordRuntimeUpdate {
                    workspace_name,
                    stored_value_updates,
                    clear_cell_context: terminal.then_some(cell_id),
                })
            } else {
                None
            }
        };
        if let Some(update) = record_update {
            if !update.stored_value_updates.is_empty() {
                self.stored_values_by_workspace
                    .lock()
                    .await
                    .entry(update.workspace_name)
                    .or_default()
                    .extend(update.stored_value_updates);
            }
            if let Some(cell_id) = update.clear_cell_context {
                self.host.clear_cell_context(&cell_id).await;
            }
        }
        self.notify.notify_waiters();
    }
}

struct RecordRuntimeUpdate {
    workspace_name: WorkspaceName,
    stored_value_updates: HashMap<String, JsonValue>,
    clear_cell_context: Option<String>,
}

fn stored_value_updates(response: &RuntimeResponse) -> HashMap<String, JsonValue> {
    let RuntimeResponse::Result {
        stored_value_updates,
        ..
    } = response
    else {
        return HashMap::new();
    };
    stored_value_updates.clone()
}

#[derive(Clone)]
struct InitializedSession {
    workspace_name: WorkspaceName,
    experimental_mutations: bool,
}

#[derive(Clone)]
struct CellContext {
    run_id: String,
    workspace_name: WorkspaceName,
    experimental_mutations: bool,
}

struct AppCodeModeHost {
    discovery: DiscoveryManager,
    queries: QueryManager,
    capability_invoker: CapabilityInvoker,
    runtime_exposure: RuntimeExposureMode,
    runs: Arc<Mutex<HashMap<String, RunRecord>>>,
    notify: Arc<Notify>,
    cell_contexts: Mutex<HashMap<String, CellContext>>,
}

impl AppCodeModeHost {
    async fn set_cell_context(&self, cell_id: &str, context: CellContext) {
        self.cell_contexts
            .lock()
            .await
            .insert(cell_id.to_string(), context);
    }

    async fn clear_cell_context(&self, cell_id: &str) {
        self.cell_contexts.lock().await.remove(cell_id);
    }

    async fn cell_context(&self, cell_id: &str) -> Result<CellContext, String> {
        self.cell_contexts
            .lock()
            .await
            .get(cell_id)
            .cloned()
            .ok_or_else(|| format!("Code Mode cell {cell_id} is not registered"))
    }

    async fn push_run_event(&self, context: &CellContext, kind: CodeModeRunEventKind) {
        let mut runs = self.runs.lock().await;
        let Some(record) = runs.get_mut(&context.run_id) else {
            return;
        };
        if record.terminal {
            return;
        }
        record.push(kind);
        self.notify.notify_waiters();
    }

    fn generated_tool_definitions(
        &self,
        workspace_name: &WorkspaceName,
    ) -> Result<Vec<ToolDefinition>, Status> {
        let loaded = self
            .discovery
            .load_workspace_exports_best_effort(workspace_name)
            .map_err(|error| Status::internal(error.to_string()))?;
        let mut definitions = Vec::new();
        for entry in &loaded.exports.entries {
            let Some(binding) = entry.bindings.iter().find_map(|binding| match binding {
                Binding::Typescript(binding) => Some(binding),
                Binding::Sql(_) => None,
            }) else {
                continue;
            };
            let Some(full_path) = Binding::Typescript(binding.clone()).full_path() else {
                continue;
            };
            let capability = loaded.capability_by_id.get(&entry.capability_id);
            let input_schema = capability.map(code_mode_tool_input_schema);
            definitions.push(ToolDefinition {
                name: full_path.clone(),
                tool_name: ToolName::plain(full_path),
                description: entry.description.clone(),
                kind: CodeModeToolKind::Function,
                input_schema,
                output_schema: capability.map(generated_tool_output_schema),
            });
        }
        Ok(definitions)
    }

    fn invoke_search(&self, context: &CellContext, input: &JsonValue) -> Result<JsonValue, String> {
        let query = input
            .get("query")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "coral.search requires a string query".to_string())?;
        let limit = optional_u32_argument(input, "limit")?.unwrap_or(0);
        let offset = optional_u32_argument(input, "offset")?.unwrap_or(0);
        let page = self
            .discovery
            .search(
                &context.workspace_name,
                query,
                &DiscoverySearchFilter::default(),
                DiscoveryPagination::new(limit, offset),
            )
            .map_err(|error| error.to_string())?;
        serde_json::to_value(json!({
            "items": page.items,
            "total": page.total,
            "has_more": page.has_more,
            "next_offset": page.next_offset,
            "limit": page.limit,
            "offset": page.offset,
            "diagnostics": page.diagnostics,
        }))
        .map_err(|error| error.to_string())
    }

    fn invoke_describe(
        &self,
        context: &CellContext,
        input: &JsonValue,
    ) -> Result<JsonValue, String> {
        let reference = input
            .get("reference")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "coral.describe requires a string reference".to_string())?;
        match self
            .discovery
            .describe(&context.workspace_name, reference)
            .map_err(|error| error.to_string())?
        {
            crate::discovery::manager::DiscoveryDescribeResult::Found(description) => {
                let entry =
                    describe_entry_value(&description.entry, description.capability.as_ref())?;
                Ok(json!({
                    "status": "found",
                    "found": true,
                    "ambiguous": false,
                    "entry": entry,
                    "diagnostics": [],
                }))
            }
            crate::discovery::manager::DiscoveryDescribeResult::Ambiguous(candidates) => {
                let candidates = candidates
                    .iter()
                    .map(|candidate| describe_entry_value(candidate, None))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(json!({
                    "status": "ambiguous",
                    "found": false,
                    "ambiguous": true,
                    "candidates": candidates,
                    "diagnostics": [],
                }))
            }
            crate::discovery::manager::DiscoveryDescribeResult::NotFound { diagnostics } => {
                Ok(json!({
                    "status": "not_found",
                    "found": false,
                    "ambiguous": false,
                    "diagnostics": diagnostics,
                }))
            }
        }
    }

    async fn invoke_sql(
        &self,
        context: &CellContext,
        input: &JsonValue,
    ) -> Result<JsonValue, String> {
        if !self.runtime_exposure.exposes_sql() {
            return Err("SQL is disabled by the active runtime exposure".to_string());
        }
        if input.get("params").is_some() {
            return Err(
                "coral.sql.query does not support SQL params yet; pass a SQL string or { sql }"
                    .to_string(),
            );
        }
        let sql = input
            .get("sql")
            .and_then(JsonValue::as_str)
            .ok_or_else(|| "coral.sql.query requires a string sql field".to_string())?;
        let execution = self
            .queries
            .execute_sql(&context.workspace_name, sql)
            .await
            .map_err(code_mode_query_error)?;
        let rows = batches_to_json_rows_json_safe_numbers(execution.batches())
            .map_err(|error| error.to_string())?;
        let columns = code_mode_sql_columns(execution.schema());
        Ok(json!({
            "columns": columns,
            "rows": rows,
            "row_count": execution.row_count(),
        }))
    }

    async fn invoke_generated_tool(
        &self,
        context: &CellContext,
        full_path: &str,
        args: JsonValue,
        _allow_error_result: bool,
    ) -> Result<JsonValue, String> {
        if !self.runtime_exposure.exposes_typescript() {
            return Err(
                "TypeScript invocation is disabled by the active runtime exposure".to_string(),
            );
        }
        let loaded = self
            .discovery
            .load_workspace_exports_best_effort(&context.workspace_name)
            .map_err(|error| error.to_string())?;
        let entry = match coral_exports::describe_export(&loaded.exports, full_path) {
            coral_exports::DescribeResolution::Found { entry } => entry,
            coral_exports::DescribeResolution::Ambiguous { .. } => {
                return Err("generated tool path resolved ambiguously".to_string());
            }
            coral_exports::DescribeResolution::NotFound => {
                return Err(format!("generated tool path '{full_path}' was not found"));
            }
        };
        let binding_ref = typescript_ref(&entry)?;
        let response = Box::pin(self.capability_invoker.invoke(
            &context.workspace_name,
            CapabilityInvocationRequest {
                capability_id: entry.capability_id.to_string(),
                binding_ref: binding_ref.clone(),
                binding_path: Vec::new(),
                args_json: serde_json::to_string(&args).map_err(|error| error.to_string())?,
                experimental_mutations: context.experimental_mutations,
            },
        ))
        .await
        .map_err(|error| error.to_string())?;
        let ok = response.ok;
        let error_value = response.error.map_or(JsonValue::Null, |error| {
            json!({
                "kind": error.kind,
                "message": error.message,
                "details": error.details.map_or(JsonValue::Null, proto_json_value_to_json),
            })
        });
        let partial = !ok && provider_error_has_partial_data(&error_value);
        let errors = if ok {
            Vec::new()
        } else {
            vec![error_value.clone()]
        };
        let value = json!({
            "ok": ok,
            "complete": ok && !partial,
            "partial": partial,
            "errors": errors,
            "source_status": [{
                "source_id": entry.source_id.as_str(),
                "capability_id": entry.capability_id.as_str(),
                "binding_ref": binding_ref,
                "full_path": full_path,
                "ok": ok,
                "complete": ok && !partial,
                "partial": partial,
                "error": if ok { JsonValue::Null } else { error_value.clone() },
            }],
            "value": response.value.map_or(JsonValue::Null, proto_json_value_to_json),
            "error": error_value,
            "envelope": response.envelope.map_or(JsonValue::Null, proto_json_value_to_json),
        });
        if !ok && !generated_tool_provider_error_result(&value) {
            return Err(generated_tool_failure_text(full_path, &value));
        }
        Ok(value)
    }
}

#[async_trait::async_trait]
impl CodeModeTurnHost for AppCodeModeHost {
    async fn invoke_tool(
        &self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> Result<JsonValue, String> {
        if invocation.tool_kind != CodeModeToolKind::Function {
            return Err("Coral Code Mode only supports function tools".to_string());
        }
        let context = self.cell_context(&invocation.cell_id).await?;
        let cell_id = invocation.cell_id;
        let tool_call_id = invocation.runtime_tool_call_id;
        let tool_name = invocation.tool_name.name;
        let allow_error_result = invocation.allow_error_result;
        let input = invocation
            .input
            .unwrap_or_else(|| JsonValue::Object(JsonMap::new()));
        self.push_run_event(
            &context,
            CodeModeRunEventKind::ToolStarted {
                cell_id: cell_id.clone(),
                tool_call_id: tool_call_id.clone(),
                tool_name: tool_name.clone(),
            },
        )
        .await;
        let result = tokio::select! {
            biased;
            () = cancellation_token.cancelled() => Err(format!(
                "code mode cell {cell_id} was terminated before {tool_name} completed"
            )),
            result = async {
                match tool_name.as_str() {
                    SEARCH_TOOL_NAME => self.invoke_search(&context, &input),
                    DESCRIBE_TOOL_NAME => self.invoke_describe(&context, &input),
                    SQL_QUERY_TOOL_NAME => self.invoke_sql(&context, &input).await,
                    full_path if full_path.starts_with("tools.") => {
                        Box::pin(self.invoke_generated_tool(
                            &context,
                            full_path,
                            input,
                            allow_error_result,
                        ))
                        .await
                    }
                    other => Err(format!("Code Mode tool '{other}' is not available")),
                }
            } => result,
        };
        match result {
            Ok(value) => {
                if !allow_error_result && generated_tool_error_result(&value) {
                    self.push_run_event(
                        &context,
                        CodeModeRunEventKind::ToolFailed {
                            cell_id,
                            tool_call_id,
                            tool_name: tool_name.clone(),
                            error: tool_call_error(
                                &tool_name,
                                generated_tool_failure_text(&tool_name, &value),
                            ),
                        },
                    )
                    .await;
                } else {
                    self.push_run_event(
                        &context,
                        CodeModeRunEventKind::ToolCompleted {
                            cell_id,
                            tool_call_id,
                            tool_name,
                        },
                    )
                    .await;
                }
                Ok(value)
            }
            Err(error) => {
                self.push_run_event(
                    &context,
                    CodeModeRunEventKind::ToolFailed {
                        cell_id,
                        tool_call_id,
                        tool_name: tool_name.clone(),
                        error: tool_call_error(&tool_name, error.clone()),
                    },
                )
                .await;
                Err(error)
            }
        }
    }
}

fn search_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: SEARCH_TOOL_NAME.to_string(),
        tool_name: ToolName::plain(SEARCH_TOOL_NAME),
        description: "Search Coral workspace exports.".to_string(),
        kind: CodeModeToolKind::Function,
        input_schema: Some(json!({
            "type": "object",
            "properties": {
                "query": { "type": "string" },
                "limit": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Maximum results to return. 0 or omitted uses Coral's default page size."
                },
                "offset": {
                    "type": "integer",
                    "minimum": 0,
                    "description": "Zero-based result offset for pagination."
                }
            },
            "required": ["query"],
            "additionalProperties": false
        })),
        output_schema: None,
    }
}

fn describe_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: DESCRIBE_TOOL_NAME.to_string(),
        tool_name: ToolName::plain(DESCRIBE_TOOL_NAME),
        description: "Describe one Coral export by full path, typed ref, capability id, or alias."
            .to_string(),
        kind: CodeModeToolKind::Function,
        input_schema: Some(json!({
            "type": "object",
            "properties": {
                "reference": { "type": "string" }
            },
            "required": ["reference"],
            "additionalProperties": false
        })),
        output_schema: None,
    }
}

fn sql_tool_definition() -> ToolDefinition {
    ToolDefinition {
        name: SQL_QUERY_TOOL_NAME.to_string(),
        tool_name: ToolName::plain(SQL_QUERY_TOOL_NAME),
        description: "Run a SQL query through Coral.".to_string(),
        kind: CodeModeToolKind::Function,
        input_schema: Some(json!({
            "type": "object",
            "properties": {
                "sql": { "type": "string" }
            },
            "required": ["sql"],
            "additionalProperties": false
        })),
        output_schema: None,
    }
}

fn typescript_ref(entry: &CapabilityExport) -> Result<String, String> {
    entry
        .bindings
        .iter()
        .find_map(|binding| match binding {
            Binding::Typescript(binding) => Some(binding.ref_.value.clone()),
            Binding::Sql(_) => None,
        })
        .ok_or_else(|| {
            format!(
                "capability '{}' has no TypeScript binding",
                entry.capability_id
            )
        })
}

fn generated_tool_failure_text(full_path: &str, value: &JsonValue) -> String {
    let message = value
        .get("error")
        .and_then(JsonValue::as_object)
        .and_then(|error| error.get("message"))
        .and_then(JsonValue::as_str)
        .filter(|message| !message.trim().is_empty())
        .unwrap_or("capability invocation returned ok=false");
    let Some(details) = value.pointer("/error/details") else {
        return format!("generated tool `{full_path}` failed: {message}");
    };
    if details.is_null() {
        return format!("generated tool `{full_path}` failed: {message}");
    }
    let details = serde_json::to_string(details).map_or_else(
        |_| "<unserializable error details>".to_string(),
        |details| truncate_error_detail_text(&details),
    );
    format!("generated tool `{full_path}` failed: {message}; details: {details}")
}

fn generated_tool_error_result(value: &JsonValue) -> bool {
    let Some(object) = value.as_object() else {
        return false;
    };
    object.get("ok").and_then(JsonValue::as_bool) == Some(false)
        && object.get("error").is_some_and(JsonValue::is_object)
        && ((object.contains_key("value") && object.contains_key("envelope"))
            || (object.contains_key("complete")
                && object.contains_key("partial")
                && object.contains_key("source_status")))
}

fn generated_tool_provider_error_result(value: &JsonValue) -> bool {
    generated_tool_error_result(value)
        && value
            .pointer("/error/kind")
            .and_then(JsonValue::as_str)
            .is_some_and(|kind| kind == "provider_error")
}

const MAX_GENERATED_TOOL_ERROR_DETAIL_CHARS: usize = 8192;

fn truncate_error_detail_text(value: &str) -> String {
    if value.len() <= MAX_GENERATED_TOOL_ERROR_DETAIL_CHARS {
        return value.to_string();
    }
    let mut end = 0;
    for (index, _) in value.char_indices() {
        if index > MAX_GENERATED_TOOL_ERROR_DETAIL_CHARS {
            break;
        }
        end = index;
    }
    format!(
        "{}... [truncated {} bytes]",
        value.get(..end).unwrap_or_default(),
        value.len().saturating_sub(end)
    )
}

fn provider_error_has_partial_data(error_value: &JsonValue) -> bool {
    if error_value
        .pointer("/details/provider_error/partial_data")
        .is_some_and(|value| !value.is_null())
    {
        return true;
    }
    let Some(detail) = error_value.pointer("/details/provider_error/detail") else {
        return false;
    };
    if detail
        .pointer("/partial_data")
        .is_some_and(|value| !value.is_null())
    {
        return true;
    }
    let Some(detail) = detail.as_str() else {
        return false;
    };
    serde_json::from_str::<JsonValue>(detail)
        .ok()
        .and_then(|value| value.get("partial_data").cloned())
        .is_some_and(|value| !value.is_null())
}

fn optional_u32_argument(input: &JsonValue, name: &str) -> Result<Option<u32>, String> {
    let Some(value) = input.get(name) else {
        return Ok(None);
    };
    let Some(number) = value.as_u64() else {
        return Err(format!(
            "coral.search {name} must be a non-negative integer"
        ));
    };
    u32::try_from(number).map(Some).map_err(|error| {
        format!("coral.search {name} must fit in a 32-bit unsigned integer: {error}")
    })
}

fn code_mode_query_error(error: QueryManagerError) -> String {
    match error {
        QueryManagerError::App(error) => error.to_string(),
        QueryManagerError::Core(error) => error.to_string(),
    }
}

fn run_error(message: String) -> CodeModeRunError {
    CodeModeRunError {
        cause: classify_run_error(&message),
        message,
        correlation_id: None,
    }
}

fn tool_call_error(tool_name: &str, message: String) -> CodeModeRunError {
    CodeModeRunError {
        cause: classify_tool_call_error(tool_name, &message),
        message,
        correlation_id: None,
    }
}

fn classify_run_error(message: &str) -> CodeModeRunErrorCause {
    let normalized = message.to_ascii_lowercase();
    if normalized.contains("max_output_tokens")
        || normalized.contains("output budget")
        || normalized.contains("output token")
    {
        CodeModeRunErrorCause::OutputBudgetExceeded
    } else if normalized.contains("code mode budget exceeded")
        || normalized.contains("limit_name=max_nested_tool_calls")
        || normalized.contains("limit_name=max_parallel_tool_calls")
        || normalized.contains("limit_name=max_total_invocations")
    {
        CodeModeRunErrorCause::NestedToolFailed
    } else if normalized.contains("heap")
        || normalized.contains("out of memory")
        || normalized.contains("memory limit")
    {
        CodeModeRunErrorCause::HeapLimitExceeded
    } else if normalized.contains("tool") && tool_unavailable_text(&normalized) {
        CodeModeRunErrorCause::ToolUnavailable
    } else if normalized.contains("tool") && nested_tool_failure_text(&normalized) {
        CodeModeRunErrorCause::NestedToolFailed
    } else if normalized.contains("sql") {
        CodeModeRunErrorCause::SqlError
    } else if internal_runtime_error_text(&normalized) {
        CodeModeRunErrorCause::Internal
    } else {
        CodeModeRunErrorCause::UserException
    }
}

fn classify_tool_call_error(tool_name: &str, message: &str) -> CodeModeRunErrorCause {
    let normalized = message.to_ascii_lowercase();
    if tool_name == SQL_QUERY_TOOL_NAME {
        CodeModeRunErrorCause::SqlError
    } else if tool_unavailable_text(&normalized) {
        CodeModeRunErrorCause::ToolUnavailable
    } else {
        CodeModeRunErrorCause::NestedToolFailed
    }
}

fn tool_unavailable_text(normalized: &str) -> bool {
    normalized.contains("not available")
        || normalized.contains("disabled by the active runtime exposure")
        || normalized.contains("was not found")
        || normalized.contains("resolved ambiguously")
}

fn nested_tool_failure_text(normalized: &str) -> bool {
    normalized.contains("toolerror")
        || normalized.contains("tool error")
        || normalized.contains("tool call")
        || normalized.contains("generated tool")
        || normalized.contains("nested tool")
}

fn internal_runtime_error_text(normalized: &str) -> bool {
    normalized.contains("runtime ended")
        || normalized.contains("code mode cell")
        || normalized.contains("code mode exec ended")
        || normalized.contains("internal")
}

fn runtime_error_response(cell_id: String, error: String) -> RuntimeResponse {
    RuntimeResponse::Result {
        cell_id,
        content_items: Vec::new(),
        stored_values: HashMap::new(),
        stored_value_updates: HashMap::new(),
        result: None,
        error_text: Some(error),
    }
}

#[derive(Debug, Clone)]
struct RunRecord {
    workspace_name: WorkspaceName,
    run_id: String,
    cell_id: String,
    cancellation_token: CancellationToken,
    initial_stored_values: HashMap<String, JsonValue>,
    status: RunStatus,
    events: Vec<CodeModeRunEvent>,
    terminal: bool,
    terminal_sequence: Option<u64>,
    initial_execution_pending: bool,
}

impl RunRecord {
    fn new(
        workspace_name: WorkspaceName,
        run_id: String,
        cell_id: String,
        cancellation_token: CancellationToken,
        initial_stored_values: HashMap<String, JsonValue>,
    ) -> Self {
        let mut record = Self {
            workspace_name,
            run_id: run_id.clone(),
            cell_id: cell_id.clone(),
            cancellation_token,
            initial_stored_values,
            status: RunStatus::Running,
            events: Vec::new(),
            terminal: false,
            terminal_sequence: None,
            initial_execution_pending: true,
        };
        record.push(CodeModeRunEventKind::RunStarted {
            run_id: run_id.clone(),
        });
        record.push(CodeModeRunEventKind::CellStarted { run_id, cell_id });
        record
    }

    fn apply_runtime_response(&mut self, response: RuntimeResponse) {
        match response {
            RuntimeResponse::Yielded { content_items, .. } => {
                self.push_content_items(content_items);
                self.status = RunStatus::Running;
            }
            RuntimeResponse::Terminated { content_items, .. } => {
                self.push_content_items(content_items);
                self.status = RunStatus::Terminated;
                self.terminal = true;
            }
            RuntimeResponse::Result {
                content_items,
                result,
                error_text,
                ..
            } => {
                self.push_content_items(content_items);
                if let Some(result) = result {
                    self.push(CodeModeRunEventKind::ResultItem {
                        cell_id: self.cell_id.clone(),
                        item: result,
                    });
                }
                self.terminal = true;
                if let Some(error_text) = error_text {
                    self.status = RunStatus::Failed;
                    self.push(CodeModeRunEventKind::RunFailed {
                        run_id: self.run_id.clone(),
                        error: run_error(error_text),
                    });
                } else {
                    self.status = RunStatus::Completed;
                    self.push(CodeModeRunEventKind::RunCompleted {
                        run_id: self.run_id.clone(),
                    });
                }
            }
        }
    }

    fn push_content_items(
        &mut self,
        content_items: Vec<coral_code_mode::FunctionCallOutputContentItem>,
    ) {
        for item in content_items {
            let value = serde_json::to_value(item).unwrap_or_else(|error| {
                json!({
                    "error": format!("failed to serialize content item: {error}")
                })
            });
            self.push(CodeModeRunEventKind::ContentItem {
                cell_id: self.cell_id.clone(),
                item: value,
            });
        }
    }

    fn push(&mut self, kind: CodeModeRunEventKind) {
        let id = u64::try_from(self.events.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
        self.events.push(CodeModeRunEvent { id, kind });
    }
}

fn prune_terminal_runs(runs: &mut HashMap<String, RunRecord>) {
    let terminal_count = runs.values().filter(|record| record.terminal).count();
    if terminal_count <= MAX_TERMINAL_RUN_HISTORY {
        return;
    }
    let mut terminal_runs = runs
        .iter()
        .filter(|(_, record)| record.terminal)
        .map(|(run_id, record)| (record.terminal_sequence.unwrap_or(0), run_id.clone()))
        .collect::<Vec<_>>();
    terminal_runs.sort_by_key(|(sequence, _)| *sequence);
    for (_, run_id) in terminal_runs
        .into_iter()
        .take(terminal_count.saturating_sub(MAX_TERMINAL_RUN_HISTORY))
    {
        runs.remove(&run_id);
    }
}

fn check_live_run_limits(
    runs: &HashMap<String, RunRecord>,
    workspace_name: &WorkspaceName,
) -> Result<(), Status> {
    let workspace_live_runs = runs
        .values()
        .filter(|record| !record.terminal && record.workspace_name == *workspace_name)
        .count();
    if workspace_live_runs >= MAX_LIVE_RUNS_PER_WORKSPACE {
        return Err(Status::resource_exhausted(format!(
            "Code Mode already has {workspace_live_runs} live runs in workspace '{}' (limit {MAX_LIVE_RUNS_PER_WORKSPACE}); wait for a run to finish or terminate one before starting another.",
            workspace_name.as_str()
        )));
    }
    let global_live_runs = runs.values().filter(|record| !record.terminal).count();
    if global_live_runs >= MAX_LIVE_RUNS_GLOBAL {
        return Err(Status::resource_exhausted(format!(
            "Code Mode already has {global_live_runs} live runs across all workspaces (limit {MAX_LIVE_RUNS_GLOBAL}); wait for a run to finish or terminate one before starting another."
        )));
    }
    Ok(())
}

#[derive(Debug, Clone)]
struct RunResponse {
    run_id: String,
    cell_id: String,
    status: RunStatus,
    events: Vec<CodeModeRunEvent>,
}

fn run_response(
    runs: &HashMap<String, RunRecord>,
    workspace_name: &WorkspaceName,
    run_id: &str,
    after_event_id: Option<u64>,
) -> Result<RunResponse, Status> {
    let record = runs
        .get(run_id)
        .ok_or_else(|| Status::not_found("Code Mode run was not found"))?;
    if record.workspace_name != *workspace_name {
        return Err(Status::not_found("Code Mode run was not found"));
    }
    Ok(RunResponse {
        run_id: record.run_id.clone(),
        cell_id: record.cell_id.clone(),
        status: record.status,
        events: record
            .events
            .iter()
            .filter(|event| after_event_id.is_none_or(|cursor| event.id > cursor))
            .cloned()
            .collect(),
    })
}

fn wait_response_to_proto(response: RunResponse) -> WaitCodeModeResponse {
    WaitCodeModeResponse {
        run_id: response.run_id,
        cell_id: response.cell_id,
        status: run_status_to_proto(response.status) as i32,
        events: events_to_proto(response.events),
    }
}

fn exec_response_to_proto(response: RunResponse) -> ExecCodeModeResponse {
    ExecCodeModeResponse {
        run_id: response.run_id,
        cell_id: response.cell_id,
        status: run_status_to_proto(response.status) as i32,
        events: events_to_proto(response.events),
    }
}

fn run_status_to_proto(status: RunStatus) -> ProtoCodeModeRunStatus {
    match status {
        RunStatus::Running => ProtoCodeModeRunStatus::Running,
        RunStatus::Completed => ProtoCodeModeRunStatus::Completed,
        RunStatus::Failed => ProtoCodeModeRunStatus::Failed,
        RunStatus::Terminated => ProtoCodeModeRunStatus::Terminated,
    }
}

fn events_to_proto(events: Vec<CodeModeRunEvent>) -> Vec<ProtoCodeModeRunEvent> {
    events.into_iter().map(event_to_proto).collect()
}

fn event_to_proto(event: CodeModeRunEvent) -> ProtoCodeModeRunEvent {
    ProtoCodeModeRunEvent {
        id: event.id,
        event: Some(match event.kind {
            CodeModeRunEventKind::RunStarted { run_id } => {
                code_mode_run_event::Event::RunStarted(CodeModeRunStarted { run_id })
            }
            CodeModeRunEventKind::CellStarted { run_id, cell_id } => {
                code_mode_run_event::Event::CellStarted(CodeModeCellStarted { run_id, cell_id })
            }
            CodeModeRunEventKind::ContentItem { cell_id, item } => {
                code_mode_run_event::Event::ContentItem(CodeModeContentItem {
                    cell_id,
                    item: Some(json_value_to_proto(item)),
                })
            }
            CodeModeRunEventKind::ResultItem { cell_id, item } => {
                code_mode_run_event::Event::ResultItem(CodeModeResultItem {
                    cell_id,
                    item: Some(json_value_to_proto(item)),
                })
            }
            CodeModeRunEventKind::RunCompleted { run_id } => {
                code_mode_run_event::Event::RunCompleted(CodeModeRunCompleted { run_id })
            }
            CodeModeRunEventKind::RunFailed { run_id, error } => {
                code_mode_run_event::Event::RunFailed(CodeModeRunFailed {
                    run_id,
                    error: Some(run_error_to_proto(error)),
                })
            }
            CodeModeRunEventKind::ToolStarted {
                cell_id,
                tool_call_id,
                tool_name,
            } => code_mode_run_event::Event::ToolStarted(CodeModeToolStarted {
                cell_id,
                tool_call_id,
                tool_name,
            }),
            CodeModeRunEventKind::ToolCompleted {
                cell_id,
                tool_call_id,
                tool_name,
            } => code_mode_run_event::Event::ToolCompleted(CodeModeToolCompleted {
                cell_id,
                tool_call_id,
                tool_name,
            }),
            CodeModeRunEventKind::ToolFailed {
                cell_id,
                tool_call_id,
                tool_name,
                error,
            } => code_mode_run_event::Event::ToolFailed(CodeModeToolFailed {
                cell_id,
                tool_call_id,
                tool_name,
                error: Some(run_error_to_proto(error)),
            }),
        }),
    }
}

fn run_error_to_proto(error: CodeModeRunError) -> ProtoCodeModeRunError {
    ProtoCodeModeRunError {
        cause: run_error_cause_to_proto(error.cause) as i32,
        message: error.message,
        correlation_id: error.correlation_id.unwrap_or_default(),
    }
}

fn run_error_cause_to_proto(cause: CodeModeRunErrorCause) -> ProtoCodeModeRunErrorCause {
    match cause {
        CodeModeRunErrorCause::UserException => ProtoCodeModeRunErrorCause::UserException,
        CodeModeRunErrorCause::OutputBudgetExceeded => {
            ProtoCodeModeRunErrorCause::OutputBudgetExceeded
        }
        CodeModeRunErrorCause::HeapLimitExceeded => ProtoCodeModeRunErrorCause::HeapLimitExceeded,
        CodeModeRunErrorCause::ToolUnavailable => ProtoCodeModeRunErrorCause::ToolUnavailable,
        CodeModeRunErrorCause::NestedToolFailed => ProtoCodeModeRunErrorCause::NestedToolFailed,
        CodeModeRunErrorCause::SqlError => ProtoCodeModeRunErrorCause::SqlError,
        CodeModeRunErrorCause::Internal => ProtoCodeModeRunErrorCause::Internal,
    }
}

fn proto_json_value_to_json(value: ProtoJsonValue) -> JsonValue {
    match value.kind {
        Some(proto_json_value::Kind::NullValue(_)) | None => JsonValue::Null,
        Some(proto_json_value::Kind::BoolValue(value)) => JsonValue::Bool(value),
        Some(proto_json_value::Kind::IntegerValue(value)) => json!(value),
        Some(proto_json_value::Kind::UnsignedIntegerValue(value)) => json!(value),
        Some(proto_json_value::Kind::DoubleValue(value)) => json!(value),
        Some(proto_json_value::Kind::StringValue(value)) => JsonValue::String(value),
        Some(proto_json_value::Kind::ArrayValue(array)) => JsonValue::Array(
            array
                .values
                .into_iter()
                .map(proto_json_value_to_json)
                .collect(),
        ),
        Some(proto_json_value::Kind::ObjectValue(object)) => JsonValue::Object(
            object
                .fields
                .into_iter()
                .map(|(key, value)| (key, proto_json_value_to_json(value)))
                .collect(),
        ),
    }
}

fn json_value_to_proto(value: JsonValue) -> CodeModeJsonValue {
    let kind = match value {
        JsonValue::Null => code_mode_json_value::Kind::NullValue(CodeModeJsonNull {}),
        JsonValue::Bool(value) => code_mode_json_value::Kind::BoolValue(value),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_i64() {
                code_mode_json_value::Kind::IntegerValue(value)
            } else if let Some(value) = value.as_u64() {
                code_mode_json_value::Kind::UnsignedIntegerValue(value)
            } else {
                code_mode_json_value::Kind::DoubleValue(value.as_f64().unwrap_or_default())
            }
        }
        JsonValue::String(value) => code_mode_json_value::Kind::StringValue(value),
        JsonValue::Array(values) => code_mode_json_value::Kind::ArrayValue(CodeModeJsonArray {
            values: values.into_iter().map(json_value_to_proto).collect(),
        }),
        JsonValue::Object(fields) => code_mode_json_value::Kind::ObjectValue(CodeModeJsonObject {
            fields: fields
                .into_iter()
                .map(|(key, value)| (key, json_value_to_proto(value)))
                .collect(),
        }),
    };
    CodeModeJsonValue { kind: Some(kind) }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunStatus {
    Running,
    Completed,
    Failed,
    Terminated,
}

#[derive(Debug, Clone)]
struct CodeModeRunEvent {
    id: u64,
    kind: CodeModeRunEventKind,
}

#[derive(Debug, Clone)]
enum CodeModeRunEventKind {
    RunStarted {
        run_id: String,
    },
    CellStarted {
        run_id: String,
        cell_id: String,
    },
    ContentItem {
        cell_id: String,
        item: JsonValue,
    },
    ResultItem {
        cell_id: String,
        item: JsonValue,
    },
    RunCompleted {
        run_id: String,
    },
    RunFailed {
        run_id: String,
        error: CodeModeRunError,
    },
    ToolStarted {
        cell_id: String,
        tool_call_id: String,
        tool_name: String,
    },
    ToolCompleted {
        cell_id: String,
        tool_call_id: String,
        tool_name: String,
    },
    ToolFailed {
        cell_id: String,
        tool_call_id: String,
        tool_name: String,
        error: CodeModeRunError,
    },
}

#[derive(Debug, Clone)]
struct CodeModeRunError {
    cause: CodeModeRunErrorCause,
    message: String,
    correlation_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CodeModeRunErrorCause {
    UserException,
    OutputBudgetExceeded,
    HeapLimitExceeded,
    ToolUnavailable,
    NestedToolFailed,
    SqlError,
    Internal,
}

fn code_mode_status(error: &str) -> Status {
    Status::new(Code::Internal, error.to_string())
}

fn code_mode_sql_columns(schema: &[ColumnInfo]) -> Vec<JsonValue> {
    schema
        .iter()
        .map(|column| {
            json!({
                "name": &column.name,
                "data_type": { "kind": &column.data_type },
                "nullable": column.nullable,
            })
        })
        .collect()
}

fn describe_entry_value(
    entry: &CapabilityExport,
    capability: Option<&Capability>,
) -> Result<JsonValue, String> {
    let mut value = serde_json::to_value(entry).map_err(|error| error.to_string())?;
    let object = value
        .as_object_mut()
        .ok_or_else(|| "serialized capability export was not an object".to_string())?;
    if let Some(full_path) = entry.bindings.iter().find_map(Binding::full_path) {
        object.insert("full_path".to_string(), JsonValue::String(full_path));
    }
    object.insert(
        "refs".to_string(),
        JsonValue::Array(
            entry
                .bindings
                .iter()
                .map(|binding| JsonValue::String(binding.ref_().value.clone()))
                .collect(),
        ),
    );
    if let Some(capability) = capability {
        object.insert(
            "input_schema".to_string(),
            capability.input_schema.schema.clone(),
        );
        object.insert(
            "code_mode_input_schema".to_string(),
            code_mode_tool_input_schema(capability),
        );
        object.insert(
            "output_contract".to_string(),
            serde_json::to_value(&capability.output_contract).map_err(|error| error.to_string())?,
        );
        object.insert(
            "code_mode_output_schema".to_string(),
            generated_tool_output_schema(capability),
        );
        object.insert(
            "capability".to_string(),
            serde_json::to_value(capability).map_err(|error| error.to_string())?,
        );
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coral_api::v1::code_mode_json_value;
    use coral_api::v1::code_mode_run_event;
    use coral_api::v1::code_mode_service_server::CodeModeService as _;
    use coral_api::v1::{
        CodeModeJsonValue, CodeModeRunErrorCause, CodeModeRunEvent, CodeModeRunStatus,
        ExecCodeModeRequest, InitializeCodeModeRequest, TerminateCodeModeRequest,
        WaitCodeModeRequest, WaitCodeModeResponse, Workspace,
    };
    use coral_sql::ColumnInfo;
    use coral_sql::QueryRuntimeContext;
    use serde_json::json;
    use tempfile::TempDir;
    use tonic::{Code, Request};

    use coral_capabilities::{
        Capability, EffectProfile, FileArtifactRef, FileFormatDescriptor, FileScanBinding,
        HttpMethod, InvocationSchema, OutputContract, ProviderOrigin, ProviderOriginKind,
        RestOutputVariant, RestUpstreamBinding, SourceCapabilitySet, SourceId, StatusRange,
        UpstreamBinding,
    };
    use coral_exports::{
        BindingBuildContext, SourceKey, TypescriptBindingContributor, build_source_exports,
    };

    use super::{
        CodeModeRunErrorCause as InternalRunErrorCause, CodeModeService,
        MAX_LIVE_RUNS_PER_WORKSPACE, MAX_TERMINAL_RUN_HISTORY, classify_run_error,
        code_mode_sql_columns, code_mode_tool_input_schema, describe_entry_value,
        generated_tool_failure_text, generated_tool_output_schema, provider_error_has_partial_data,
        sql_tool_definition,
    };
    use crate::RuntimeExposureMode;
    use crate::capability::service::CapabilityInvoker;
    use crate::credentials::{CredentialManager, CredentialStore};
    use crate::discovery::manager::DiscoveryManager;
    use crate::query::manager::QueryManager;
    use crate::sources::SourceName;
    use crate::sources::model::{InstalledSource, SourceOrigin};
    use crate::state::{AppStateLayout, ConfigStore};

    #[test]
    fn sql_tool_definition_does_not_advertise_params() {
        let definition = sql_tool_definition();
        let schema = definition.input_schema.expect("input schema");

        assert_eq!(
            schema
                .pointer("/properties/sql/type")
                .and_then(|value| value.as_str()),
            Some("string")
        );
        assert!(schema.pointer("/properties/params").is_none());
    }

    #[test]
    fn generated_tool_failure_classifies_as_nested_tool_failure() {
        let message = generated_tool_failure_text(
            "tools.example.sql_query",
            &json!({ "error": { "message": "provider SQL rejected the request" } }),
        );

        assert_eq!(
            classify_run_error(&message),
            InternalRunErrorCause::NestedToolFailed
        );
    }

    #[test]
    fn sql_columns_use_advertised_shape() {
        let columns = code_mode_sql_columns(&[ColumnInfo {
            name: "answer".to_string(),
            data_type: "Int64".to_string(),
            nullable: false,
            is_virtual: false,
            is_required_filter: false,
            description: String::new(),
            ordinal_position: 0,
        }]);

        let column = columns.first().expect("one SQL result column");
        assert_eq!(
            column.pointer("/name").and_then(serde_json::Value::as_str),
            Some("answer")
        );
        assert_eq!(
            column
                .pointer("/data_type/kind")
                .and_then(serde_json::Value::as_str),
            Some("Int64")
        );
        assert_eq!(
            column
                .pointer("/nullable")
                .and_then(serde_json::Value::as_bool),
            Some(false)
        );
    }

    #[test]
    fn generated_tool_output_schema_wraps_provider_result() {
        let mut capability = test_capability();
        capability.output_contract = OutputContract::RestResponseVariants {
            variants: vec![RestOutputVariant {
                status: StatusRange::Code { code: 200 },
                media_type: "application/json".to_string(),
                schema: InvocationSchema::new(json!({
                    "type": "object",
                    "properties": {
                        "items": { "type": "array" }
                    }
                })),
                provider_origin: "application/json".to_string(),
            }],
        };

        let schema = generated_tool_output_schema(&capability);

        assert_eq!(
            schema
                .pointer("/properties/ok/type")
                .and_then(|value| value.as_str()),
            Some("boolean")
        );
        assert_eq!(
            schema
                .pointer("/properties/value/anyOf/0/properties/items/type")
                .and_then(|value| value.as_str()),
            Some("array")
        );
        assert_eq!(
            schema
                .pointer("/properties/error/anyOf/0/properties/message/type")
                .and_then(|value| value.as_str()),
            Some("string")
        );
        assert_eq!(
            schema
                .pointer("/properties/complete/type")
                .and_then(|value| value.as_str()),
            Some("boolean")
        );
        assert_eq!(
            schema
                .pointer("/properties/source_status/items/properties/source_id/type")
                .and_then(|value| value.as_str()),
            Some("string")
        );
        assert_eq!(
            schema
                .pointer(
                    "/properties/envelope/anyOf/0/properties/provider/properties/headers/additionalProperties/type"
                )
                .and_then(|value| value.as_str()),
            Some("string")
        );
    }

    #[test]
    fn provider_error_partial_detection_parses_graphql_detail() {
        let error = json!({
            "kind": "provider_error",
            "message": "graphql failed",
            "details": {
                "provider_error": {
                    "kind": "graphql_error",
                    "detail": json!({
                        "errors": [{ "message": "field failed" }],
                        "partial_data": { "viewer": null }
                    })
                    .to_string()
                }
            }
        });

        assert!(provider_error_has_partial_data(&error));

        let missing_data_error = json!({
            "kind": "provider_error",
            "message": "graphql failed",
            "details": {
                "provider_error": {
                    "kind": "graphql_error",
                    "detail": json!({
                        "errors": [{ "message": "field failed" }],
                        "partial_data": null
                    })
                    .to_string()
                }
            }
        });
        assert!(!provider_error_has_partial_data(&missing_data_error));
    }

    #[test]
    fn describe_entry_value_includes_runtime_schema_and_full_path() {
        let mut capability = test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["path"],
            "properties": {
                "path": {
                    "type": "object",
                    "required": ["owner", "repo", "pull_number"],
                    "properties": {
                        "owner": { "type": "string" },
                        "repo": { "type": "string" },
                        "pull_number": { "type": "integer" }
                    }
                }
            }
        }));
        let ctx = coral_exports::BindingBuildContext {
            source_id: capability.source_id.clone(),
            display_name: "GitHub".to_string(),
            source_key: coral_exports::SourceKey("github".to_string()),
        };
        let mut entry = coral_exports::CapabilityExport::from_capability(&capability, &ctx);
        entry.bindings.push(coral_exports::Binding::Typescript(
            coral_exports::TypescriptBinding {
                ref_: coral_exports::ExportRef::typescript(&[
                    "github".to_string(),
                    "rest".to_string(),
                    "pulls".to_string(),
                    "listReviews".to_string(),
                ]),
                path: vec![
                    "github".to_string(),
                    "rest".to_string(),
                    "pulls".to_string(),
                    "listReviews".to_string(),
                ],
                args_type_name: "GithubRestPullsListReviewsArgs".to_string(),
                result_type_name: "GithubRestPullsListReviewsResult".to_string(),
            },
        ));

        let value = describe_entry_value(&entry, Some(&capability)).expect("describe value");

        assert_eq!(
            value.pointer("/full_path").and_then(|value| value.as_str()),
            Some("tools.github.rest.pulls.listReviews")
        );
        assert_eq!(
            value
                .pointer("/input_schema/properties/path/properties/pull_number/type")
                .and_then(|value| value.as_str()),
            Some("integer")
        );
        assert_eq!(
            value
                .pointer("/code_mode_input_schema/properties/pull_number/type")
                .and_then(|value| value.as_str()),
            Some("integer")
        );
        let compiled = jsonschema::JSONSchema::compile(
            value
                .get("code_mode_input_schema")
                .expect("code mode input schema"),
        )
        .expect("code mode schema compiles");
        assert!(compiled.is_valid(&json!({
            "path": {
                "owner": "withcoral",
                "repo": "coral"
            },
            "pull_number": 42
        })));
        assert!(
            value
                .pointer("/code_mode_output_schema/properties/value")
                .is_some()
        );
    }

    #[test]
    fn code_mode_tool_input_schema_allows_grouped_and_flat_rest_args() {
        let mut capability = test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "required": ["path", "query"],
            "properties": {
                "path": {
                    "type": "object",
                    "required": ["owner", "repo"],
                    "properties": {
                        "owner": { "type": "string" },
                        "repo": { "type": "string" }
                    },
                    "additionalProperties": false
                },
                "query": {
                    "type": "object",
                    "required": ["q"],
                    "properties": {
                        "q": { "type": "string" },
                        "page": { "type": "integer" }
                    },
                    "additionalProperties": false
                }
            },
            "additionalProperties": false
        }));

        let schema = code_mode_tool_input_schema(&capability);
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");

        assert_eq!(
            schema
                .pointer("/properties/path/properties/owner/type")
                .and_then(|value| value.as_str()),
            Some("string")
        );
        assert_eq!(
            schema
                .pointer("/properties/owner/type")
                .and_then(|value| value.as_str()),
            Some("string")
        );
        assert!(compiled.is_valid(&json!({
            "path": {
                "owner": "withcoral",
                "repo": "coral",
                "pull_number": 42
            },
            "q": "reviewed"
        })));
        assert!(compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": 42,
            "query": {
                "q": "reviewed"
            }
        })));
        assert!(compiled.is_valid(&json!({
            "owner": "withcoral",
            "repo": "coral",
            "pull_number": 42,
            "q": "reviewed"
        })));
    }

    #[test]
    fn code_mode_tool_input_schema_infers_stale_rest_path_args() {
        let mut capability = test_capability();
        capability.input_schema = InvocationSchema::new(json!({
            "type": "object",
            "properties": {},
            "additionalProperties": false
        }));

        let schema = code_mode_tool_input_schema(&capability);

        assert_eq!(
            schema
                .pointer("/properties/path/properties/owner/type")
                .and_then(|value| value.as_array())
                .map(Vec::len),
            Some(3)
        );
        assert!(
            schema
                .pointer("/properties/pull_number/type")
                .and_then(|value| value.as_array())
                .is_some_and(|types| {
                    ["string", "number", "boolean"]
                        .into_iter()
                        .all(|kind| types.iter().any(|value| value.as_str() == Some(kind)))
                })
        );
        let compiled = jsonschema::JSONSchema::compile(&schema).expect("schema compiles");
        assert!(compiled.is_valid(&json!({
            "path": {
                "owner": "withcoral",
                "repo": "coral"
            },
            "pull_number": 42
        })));
    }

    fn test_capability() -> Capability {
        Capability::new(
            SourceId("src_github".to_string()),
            "rest",
            "pulls_list_reviews",
            ProviderOrigin {
                kind: ProviderOriginKind::RestOperation,
                snapshot_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews"
                        .to_string(),
                provider_name: "pulls/list-reviews".to_string(),
                tags: vec!["Pulls".to_string()],
            },
            UpstreamBinding::Rest(RestUpstreamBinding {
                operation_ref:
                    "interfaces/rest/provider-snapshot.yaml#/operations/pulls_list_reviews"
                        .to_string(),
                method: HttpMethod::Get,
                path_template: "/repos/{owner}/{repo}/pulls/{pull_number}/reviews".to_string(),
                parameter_bindings: Vec::new(),
                request_bodies: Vec::new(),
                responses: Vec::new(),
                pagination: None,
            }),
        )
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exec_returns_completed_for_fast_script_without_wait() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;

        let run = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: "return 1;".to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(run.status, status(CodeModeRunStatus::Completed));
        assert_eq!(last_output_item_from_events(&run.events), Some(json!(1)));

        let wait = service
            .wait(Request::new(WaitCodeModeRequest {
                workspace: Some(workspace("default")),
                run_id: run.run_id,
                after_event_id: 0,
            }))
            .await
            .expect("wait")
            .into_inner();
        assert_eq!(wait.status, status(CodeModeRunStatus::Completed));
        assert_eq!(last_output_item(&wait), Some(json!(1)));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn console_output_shaping_preserves_event_cursor_order() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;

        let run = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
for (let i = 0; i < 100; i += 1) {
  console.log("line", i);
}
return "done";
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(run.status, status(CodeModeRunStatus::Completed));
        assert!(run.events.windows(2).all(|events| {
            let [previous, next] = events else {
                return true;
            };
            previous.id < next.id
        }));

        let content_items = content_item_values_from_events(&run.events);
        assert_eq!(content_items.len(), 33);
        let (shaping_event_id, shaping_item) = content_items
            .iter()
            .find(|(_, item)| {
                item.get("type").and_then(serde_json::Value::as_str) == Some("output_shaping")
            })
            .expect("output shaping event");
        assert_eq!(
            shaping_item
                .get("limit_name")
                .and_then(serde_json::Value::as_str),
            Some("max_output_content_items")
        );
        assert_eq!(
            shaping_item
                .get("observed_items")
                .and_then(serde_json::Value::as_u64),
            Some(100)
        );
        assert_eq!(
            shaping_item
                .get("dropped_items")
                .and_then(serde_json::Value::as_u64),
            Some(68)
        );

        let result_event_id = run
            .events
            .iter()
            .find_map(|event| {
                matches!(
                    event.event.as_ref(),
                    Some(code_mode_run_event::Event::ResultItem(_))
                )
                .then_some(event.id)
            })
            .expect("result event");
        assert!(*shaping_event_id < result_event_id);

        let wait = service
            .wait(Request::new(WaitCodeModeRequest {
                workspace: Some(workspace("default")),
                run_id: run.run_id.clone(),
                after_event_id: *shaping_event_id,
            }))
            .await
            .expect("wait")
            .into_inner();
        assert_eq!(wait.status, status(CodeModeRunStatus::Completed));
        assert_eq!(last_output_item(&wait), Some(json!("done")));
        assert!(
            content_item_values_from_events(&wait.events)
                .iter()
                .all(
                    |(_, item)| item.get("type").and_then(serde_json::Value::as_str)
                        != Some("output_shaping")
                )
        );

        let wait_after_result = service
            .wait(Request::new(WaitCodeModeRequest {
                workspace: Some(workspace("default")),
                run_id: run.run_id,
                after_event_id: result_event_id,
            }))
            .await
            .expect("wait after result")
            .into_inner();
        assert_eq!(
            content_item_values_from_events(&wait_after_result.events),
            Vec::<(u64, serde_json::Value)>::new()
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn exec_records_nested_tool_lifecycle_events() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;

        let completed = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
await coral.search({ query: "" });
return "done";
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(completed.status, status(CodeModeRunStatus::Completed));
        assert!(has_tool_started(&completed.events, "coral.search"));
        assert!(has_tool_completed(&completed.events, "coral.search"));

        let failed_tool = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
try {
  await coral.sql.query({ sql: "select * from missing_table" });
} catch (_error) {
  return "caught";
}
return "unexpected";
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(failed_tool.status, status(CodeModeRunStatus::Failed));
        assert_eq!(
            tool_failed_cause(&failed_tool.events, "coral.sql.query"),
            Some(CodeModeRunErrorCause::SqlError)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn generated_tool_errors_are_catchable_without_failing_run() {
        let (temp, service) = test_service();
        install_search_fixture_source(temp.path());
        initialize_workspace(&service, "default").await;

        let completed = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
try {
  await tools.searchFixture.files.alpha({ file_id: "bad" });
  return "unexpected";
} catch (error) {
  return {
    caught: true,
    text: String(error),
  };
}
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(completed.status, status(CodeModeRunStatus::Completed));
        assert_eq!(
            last_output_item_from_events(&completed.events)
                .and_then(|value| { value.get("caught").and_then(serde_json::Value::as_bool) }),
            Some(true)
        );
        let text = last_output_item_from_events(&completed.events)
            .and_then(|value| {
                value
                    .get("text")
                    .and_then(serde_json::Value::as_str)
                    .map(str::to_string)
            })
            .expect("caught error text");
        assert!(text.contains("invalid_response"), "{text}");
        assert_eq!(
            tool_failed_cause(&completed.events, "tools.searchFixture.files.alpha"),
            Some(CodeModeRunErrorCause::NestedToolFailed)
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn generated_tool_non_provider_errors_remain_fatal_with_allow_error_result() {
        let (temp, service) = test_service();
        install_search_fixture_source(temp.path());
        initialize_workspace(&service, "default").await;

        let failed = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
try {
  await tools.searchFixture.files.alpha({ bogus: true }, { allowErrorResult: true });
  return "unexpected";
} catch (error) {
  return {
    caught: true,
    text: String(error),
  };
}
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(failed.status, status(CodeModeRunStatus::Failed));
        assert_eq!(
            tool_failed_cause(&failed.events, "tools.searchFixture.files.alpha"),
            Some(CodeModeRunErrorCause::NestedToolFailed)
        );
        assert!(last_output_item_from_events(&failed.events).is_none());
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn coral_search_honors_limit_and_offset() {
        let (temp, service) = test_service();
        install_search_fixture_source(temp.path());
        initialize_workspace(&service, "default").await;

        let completed = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
const page = await coral.search({ query: "pageable", limit: 1, offset: 1 });
return {
  titles: page.items.map((item) => item.title),
  total: page.total,
  limit: page.limit,
  offset: page.offset,
  has_more: page.has_more,
  next_offset: page.next_offset
};
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        assert_eq!(completed.status, status(CodeModeRunStatus::Completed));
        assert_eq!(
            last_output_item_from_events(&completed.events),
            Some(json!({
                "titles": ["Pageable beta"],
                "total": 2,
                "limit": 1,
                "offset": 1,
                "has_more": false,
                "next_offset": null,
            }))
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn stored_values_are_scoped_by_workspace() {
        let (_temp, service) = test_service();

        initialize_workspace(&service, "workspace_a").await;
        let stored = exec_and_wait(
            &service,
            "workspace_a",
            r#"store("key", "from_a"); return "stored";"#,
        )
        .await;
        assert_eq!(stored.status, status(CodeModeRunStatus::Completed));

        initialize_workspace(&service, "workspace_b").await;
        let missing = exec_and_wait(
            &service,
            "workspace_b",
            r#"return load("key") === undefined ? "missing" : load("key");"#,
        )
        .await;
        assert_eq!(last_output_item(&missing), Some(json!("missing")));

        initialize_workspace(&service, "workspace_a").await;
        let loaded = exec_and_wait(&service, "workspace_a", r#"return load("key");"#).await;
        assert_eq!(last_output_item(&loaded), Some(json!("from_a")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn initialized_sessions_are_scoped_by_workspace() {
        let (_temp, service) = test_service();

        initialize_workspace(&service, "workspace_a").await;
        let first_a = exec_and_wait(&service, "workspace_a", r#"return "a1";"#).await;
        assert_eq!(last_output_item(&first_a), Some(json!("a1")));

        initialize_workspace(&service, "workspace_b").await;
        let b = exec_and_wait(&service, "workspace_b", r#"return "b";"#).await;
        assert_eq!(last_output_item(&b), Some(json!("b")));

        let second_a = exec_and_wait(&service, "workspace_a", r#"return "a2";"#).await;
        assert_eq!(last_output_item(&second_a), Some(json!("a2")));

        let err = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("workspace_c")),
                source: "return 1;".to_string(),
            }))
            .await
            .expect_err("uninitialized workspace should still be rejected");
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn initialize_reports_generated_invocation_from_runtime_exposure() {
        let (_temp, service) = test_service_with_exposure(RuntimeExposureMode::Both);
        let response = service
            .initialize(Request::new(InitializeCodeModeRequest {
                workspace: Some(workspace("default")),
                experimental_mutations: false,
            }))
            .await
            .expect("initialize")
            .into_inner();
        assert!(response.supports_invoke);

        let (_temp, service) = test_service_with_exposure(RuntimeExposureMode::Sql);
        let response = service
            .initialize(Request::new(InitializeCodeModeRequest {
                workspace: Some(workspace("default")),
                experimental_mutations: false,
            }))
            .await
            .expect("initialize")
            .into_inner();
        assert!(!response.supports_invoke);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn reverted_store_update_after_yield_overwrites_concurrent_change() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;

        let seeded = exec_and_wait(&service, "default", r#"store("key", "initial");"#).await;
        assert_eq!(seeded.status, status(CodeModeRunStatus::Completed));

        let run = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"
store("key", "temp");
yield_control();
await new Promise((resolve) => setTimeout(resolve, 250));
store("key", "initial");
return load("key");
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();

        tokio::time::sleep(Duration::from_millis(50)).await;
        let concurrent = exec_and_wait(&service, "default", r#"store("key", "concurrent");"#).await;
        assert_eq!(concurrent.status, status(CodeModeRunStatus::Completed));

        let completed = service
            .wait(Request::new(WaitCodeModeRequest {
                workspace: Some(workspace("default")),
                run_id: run.run_id,
                after_event_id: 0,
            }))
            .await
            .expect("wait")
            .into_inner();
        assert_eq!(completed.status, status(CodeModeRunStatus::Completed));
        assert_eq!(last_output_item(&completed), Some(json!("initial")));

        let loaded = exec_and_wait(&service, "default", r#"return load("key");"#).await;
        assert_eq!(last_output_item(&loaded), Some(json!("initial")));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminal_run_history_is_bounded() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;
        let mut latest_run_id = String::new();

        for index in 0..(MAX_TERMINAL_RUN_HISTORY + 2) {
            let run = service
                .exec(Request::new(ExecCodeModeRequest {
                    workspace: Some(workspace("default")),
                    source: format!("return {index};"),
                }))
                .await
                .expect("exec")
                .into_inner();
            latest_run_id = run.run_id.clone();
            let completed = service
                .wait(Request::new(WaitCodeModeRequest {
                    workspace: Some(workspace("default")),
                    run_id: run.run_id,
                    after_event_id: 0,
                }))
                .await
                .expect("wait")
                .into_inner();
            assert_eq!(completed.status, status(CodeModeRunStatus::Completed));
        }

        let runs = service.runtime.runs.lock().await;
        let terminal_count = runs.values().filter(|record| record.terminal).count();
        assert_eq!(terminal_count, MAX_TERMINAL_RUN_HISTORY);
        assert!(
            runs.contains_key(&latest_run_id),
            "latest completed run should remain available"
        );
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn live_runs_are_bounded_per_workspace() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;
        let mut run_ids = Vec::new();

        for _ in 0..MAX_LIVE_RUNS_PER_WORKSPACE {
            let run = service
                .exec(Request::new(ExecCodeModeRequest {
                    workspace: Some(workspace("default")),
                    source: r"
yield_control();
await new Promise(() => {});
"
                    .to_string(),
                }))
                .await
                .expect("exec")
                .into_inner();
            assert_eq!(run.status, status(CodeModeRunStatus::Running));
            run_ids.push(run.run_id);
        }

        let err = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: "return 1;".to_string(),
            }))
            .await
            .expect_err("live run limit should reject another exec");
        assert_eq!(err.code(), Code::ResourceExhausted);

        service
            .terminate(Request::new(TerminateCodeModeRequest {
                workspace: Some(workspace("default")),
                run_id: run_ids.pop().expect("one live run"),
            }))
            .await
            .expect("terminate");

        let admitted = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: "return 1;".to_string(),
            }))
            .await
            .expect("exec after terminating a live run")
            .into_inner();
        assert_eq!(admitted.status, status(CodeModeRunStatus::Completed));

        for run_id in run_ids {
            service
                .terminate(Request::new(TerminateCodeModeRequest {
                    workspace: Some(workspace("default")),
                    run_id,
                }))
                .await
                .expect("cleanup live run");
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn terminate_before_delayed_execution_prevents_late_store_updates() {
        let (_temp, service) = test_service();
        initialize_workspace(&service, "default").await;

        let run = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace("default")),
                source: r#"// @exec: {"yield_time_ms": 1}
await new Promise((resolve) => setTimeout(resolve, 1000));
store("ran_after_terminate", true);
return "ran";
"#
                .to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();
        assert_eq!(run.status, status(CodeModeRunStatus::Running));

        let terminated = service
            .terminate(Request::new(TerminateCodeModeRequest {
                workspace: Some(workspace("default")),
                run_id: run.run_id,
            }))
            .await
            .expect("terminate")
            .into_inner();
        assert_eq!(terminated.status, status(CodeModeRunStatus::Terminated));

        let loaded = exec_and_wait(
            &service,
            "default",
            r#"return load("ran_after_terminate") === undefined ? "not_ran" : "ran";"#,
        )
        .await;
        assert_eq!(last_output_item(&loaded), Some(json!("not_ran")));
    }

    fn test_service() -> (TempDir, CodeModeService) {
        test_service_with_exposure(RuntimeExposureMode::Both)
    }

    fn test_service_with_exposure(
        runtime_exposure: RuntimeExposureMode,
    ) -> (TempDir, CodeModeService) {
        let temp = TempDir::new().expect("temp dir");
        let layout =
            AppStateLayout::discover(Some(temp.path().join("coral-config"))).expect("layout");
        let config_store = ConfigStore::new(layout.clone());
        let discovery = DiscoveryManager::new(config_store.clone(), layout.clone());
        let credential_manager = CredentialManager::new(CredentialStore::new(layout.clone()));
        let queries = QueryManager::new(
            config_store,
            credential_manager.clone(),
            QueryRuntimeContext::default(),
            layout,
            runtime_exposure,
        );
        let capability_invoker =
            CapabilityInvoker::new(discovery.clone(), credential_manager, runtime_exposure);
        let service =
            CodeModeService::new(discovery, queries, capability_invoker, runtime_exposure);
        (temp, service)
    }

    async fn initialize_workspace(service: &CodeModeService, workspace_id: &str) {
        service
            .initialize(Request::new(InitializeCodeModeRequest {
                workspace: Some(workspace(workspace_id)),
                experimental_mutations: false,
            }))
            .await
            .expect("initialize");
    }

    fn install_search_fixture_source(root: &std::path::Path) {
        let layout = AppStateLayout::discover(Some(root.join("coral-config"))).expect("layout");
        layout.ensure().expect("layout dirs");
        let config_store = ConfigStore::new(layout.clone());
        let workspace_name = crate::workspaces::WorkspaceName::default();
        let source = InstalledSource {
            name: SourceName::parse("search_fixture").expect("source name"),
            source_id: "src_search_fixture".to_string(),
            display_name: "Search Fixture".to_string(),
            source_key: "search_fixture".to_string(),
            version: None,
            variables: std::collections::BTreeMap::new(),
            secrets: Vec::new(),
            credential_storage: None,
            origin: SourceOrigin::Imported,
        };
        config_store
            .upsert_source(&workspace_name, source.clone())
            .expect("source config");

        let source_id = SourceId(source.source_id.clone());
        let capabilities = SourceCapabilitySet::new(
            source_id.clone(),
            ["alpha", "beta"]
                .into_iter()
                .map(|operation_id| {
                    let mut capability = Capability::new(
                        source_id.clone(),
                        "files",
                        operation_id,
                        ProviderOrigin {
                            kind: ProviderOriginKind::FileRelation,
                            snapshot_ref: format!(
                                "interfaces/files/provider-snapshot.yaml#/files/{operation_id}"
                            ),
                            provider_name: operation_id.to_string(),
                            tags: Vec::new(),
                        },
                        UpstreamBinding::FileRead(FileScanBinding {
                            file_refs: (operation_id == "alpha")
                                .then(|| FileArtifactRef {
                                    id: "bad".to_string(),
                                    source_local_path: "interfaces/files/files/bad.jsonl"
                                        .to_string(),
                                    display_name: Some("bad.jsonl".to_string()),
                                })
                                .into_iter()
                                .collect(),
                            format: FileFormatDescriptor::Jsonl,
                            schema_ref: None,
                        }),
                    );
                    capability.effect_profile = EffectProfile::read();
                    capability.display.title = format!("Pageable {operation_id}");
                    capability
                })
                .collect(),
        );
        let exports = build_source_exports(
            &capabilities,
            &BindingBuildContext {
                source_id,
                display_name: source.display_name.clone(),
                source_key: SourceKey(source.source_key.clone()),
            },
            &[&TypescriptBindingContributor::new()],
        )
        .expect("source exports");
        let materialized_dir = layout.source_materialized_dir(&workspace_name, &source.name);
        std::fs::create_dir_all(materialized_dir.join("exports")).expect("exports dir");
        std::fs::create_dir_all(materialized_dir.join("interfaces/files/files"))
            .expect("files dir");
        std::fs::write(
            materialized_dir.join("interfaces/files/files/bad.jsonl"),
            b"{not-json}\n",
        )
        .expect("write bad provider data");
        std::fs::write(
            materialized_dir.join("exports/source-exports.yaml"),
            serde_yaml::to_string(&exports).expect("exports yaml"),
        )
        .expect("write exports");
        std::fs::write(
            materialized_dir.join("capabilities.yaml"),
            serde_yaml::to_string(&capabilities).expect("capabilities yaml"),
        )
        .expect("write capabilities");
    }

    async fn exec_and_wait(
        service: &CodeModeService,
        workspace_id: &str,
        source: &str,
    ) -> WaitCodeModeResponse {
        let run = service
            .exec(Request::new(ExecCodeModeRequest {
                workspace: Some(workspace(workspace_id)),
                source: source.to_string(),
            }))
            .await
            .expect("exec")
            .into_inner();
        service
            .wait(Request::new(WaitCodeModeRequest {
                workspace: Some(workspace(workspace_id)),
                run_id: run.run_id,
                after_event_id: 0,
            }))
            .await
            .expect("wait")
            .into_inner()
    }

    fn workspace(id: &str) -> Workspace {
        Workspace {
            name: id.to_string(),
        }
    }

    const fn status(status: CodeModeRunStatus) -> i32 {
        status as i32
    }

    fn last_output_item(response: &WaitCodeModeResponse) -> Option<serde_json::Value> {
        last_output_item_from_events(&response.events)
    }

    fn last_output_item_from_events(events: &[CodeModeRunEvent]) -> Option<serde_json::Value> {
        events.iter().rev().find_map(|event| {
            let code_mode_run_event::Event::ResultItem(result_item) = event.event.as_ref()? else {
                return None;
            };
            result_item.item.as_ref().map(json_value_from_proto)
        })
    }

    fn content_item_values_from_events(
        events: &[CodeModeRunEvent],
    ) -> Vec<(u64, serde_json::Value)> {
        events
            .iter()
            .filter_map(|event| {
                let code_mode_run_event::Event::ContentItem(content_item) = event.event.as_ref()?
                else {
                    return None;
                };
                content_item
                    .item
                    .as_ref()
                    .map(|item| (event.id, json_value_from_proto(item)))
            })
            .collect()
    }

    fn has_tool_started(events: &[CodeModeRunEvent], tool_name: &str) -> bool {
        events.iter().any(|event| {
            matches!(
                event.event.as_ref(),
                Some(code_mode_run_event::Event::ToolStarted(started))
                    if started.tool_name == tool_name
            )
        })
    }

    fn has_tool_completed(events: &[CodeModeRunEvent], tool_name: &str) -> bool {
        events.iter().any(|event| {
            matches!(
                event.event.as_ref(),
                Some(code_mode_run_event::Event::ToolCompleted(completed))
                    if completed.tool_name == tool_name
            )
        })
    }

    fn tool_failed_cause(
        events: &[CodeModeRunEvent],
        tool_name: &str,
    ) -> Option<CodeModeRunErrorCause> {
        events.iter().find_map(|event| {
            let code_mode_run_event::Event::ToolFailed(failed) = event.event.as_ref()? else {
                return None;
            };
            if failed.tool_name != tool_name {
                return None;
            }
            failed
                .error
                .as_ref()
                .and_then(|error| CodeModeRunErrorCause::try_from(error.cause).ok())
        })
    }

    fn json_value_from_proto(value: &CodeModeJsonValue) -> serde_json::Value {
        match value.kind.as_ref() {
            Some(code_mode_json_value::Kind::NullValue(_)) | None => serde_json::Value::Null,
            Some(code_mode_json_value::Kind::BoolValue(value)) => json!(value),
            Some(code_mode_json_value::Kind::IntegerValue(value)) => json!(value),
            Some(code_mode_json_value::Kind::UnsignedIntegerValue(value)) => json!(value),
            Some(code_mode_json_value::Kind::DoubleValue(value)) => json!(value),
            Some(code_mode_json_value::Kind::StringValue(value)) => json!(value),
            Some(code_mode_json_value::Kind::ArrayValue(array)) => {
                serde_json::Value::Array(array.values.iter().map(json_value_from_proto).collect())
            }
            Some(code_mode_json_value::Kind::ObjectValue(object)) => serde_json::Value::Object(
                object
                    .fields
                    .iter()
                    .map(|(key, value)| (key.clone(), json_value_from_proto(value)))
                    .collect(),
            ),
        }
    }
}
