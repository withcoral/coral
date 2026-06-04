mod callbacks;
mod globals;
mod module_loader;
mod timers;
mod value;

use std::collections::HashMap;
use std::ffi::c_void;
use std::sync::OnceLock;
use std::sync::mpsc as std_mpsc;
use std::thread;

use serde::Serialize;
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

use crate::description::CodeModeToolKind;
use crate::description::EnabledToolMetadata;
use crate::description::ToolDefinition;
use crate::description::ToolName;
use crate::description::enabled_tool_metadata;
use crate::input::wrap_exec_source;
use crate::response::FunctionCallOutputContentItem;

pub const DEFAULT_EXEC_YIELD_TIME_MS: u64 = 10_000;
pub const DEFAULT_WAIT_YIELD_TIME_MS: u64 = 10_000;
pub const DEFAULT_MAX_OUTPUT_TOKENS_PER_EXEC_CALL: usize = 10_000;
const MAX_OUTPUT_TOKENS_PER_EXEC_CALL: usize = 100_000;
pub(crate) const MAX_NESTED_CALLS_PER_CELL: usize = 100;
pub(crate) const MAX_NESTED_TOOL_INPUT_BYTES: usize = 256 * 1024;
pub(crate) const MAX_NESTED_TOOL_INPUT_BYTES_PER_CELL: usize = 1024 * 1024;
pub(crate) const MAX_OUTPUT_CONTENT_ITEMS_PER_CELL: usize = 32;
pub(crate) const MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM: usize = 1024 * 1024;
pub(crate) const MAX_OUTPUT_IMAGE_URL_BYTES_PER_CELL: usize = 4 * 1024 * 1024;
pub(crate) const MAX_STORED_VALUE_KEY_BYTES: usize = 256;
pub(crate) const MAX_STORED_VALUES_PER_CELL: usize = 128;
pub(crate) const MAX_STORED_VALUE_BYTES: usize = 256 * 1024;
pub(crate) const MAX_STORED_VALUE_BYTES_PER_CELL: usize = 1024 * 1024;
const CODE_MODE_MAX_V8_HEAP_BYTES: usize = 128 * 1024 * 1024;
const CODE_MODE_V8_HEAP_LIMIT_GRACE_BYTES: usize = 16 * 1024 * 1024;
const EXIT_SENTINEL: &str = "__codex_code_mode_exit__";

#[derive(Clone, Debug)]
pub struct ExecuteRequest {
    /// Runtime cell id for this execution.
    ///
    /// Callers allocate this before execution so tracing, waits, and nested tool
    /// calls can refer to the cell as soon as JavaScript starts.
    pub cell_id: String,
    pub enabled_tools: Vec<ToolDefinition>,
    pub source: String,
    pub stored_values: HashMap<String, JsonValue>,
    pub yield_time_ms: Option<u64>,
    pub max_output_tokens: Option<usize>,
}

#[derive(Clone, Debug)]
pub struct WaitRequest {
    pub cell_id: String,
    pub yield_time_ms: u64,
    pub terminate: bool,
}

#[derive(Clone, Debug)]
pub struct WaitToPendingRequest {
    pub cell_id: String,
    pub yield_time_ms: Option<u64>,
}

/// Result of waiting on a code-mode cell.
///
/// The wrapped `RuntimeResponse` is the model-facing wait result. The enum
/// variant carries the extra lifecycle provenance that `RuntimeResponse` cannot:
/// a failed real cell and a missing-cell wait both use
/// `RuntimeResponse::Result { error_text: Some(..), .. }`, but only the former
/// should be treated as a code-cell lifecycle event.
#[derive(Debug, PartialEq)]
pub enum WaitOutcome {
    /// The requested code cell was live when the wait command was accepted.
    LiveCell(RuntimeResponse),
    /// The requested code cell was not live.
    MissingCell(RuntimeResponse),
}

/// Result of executing a code-mode cell until it either completes or reaches a
/// quiescent pending state.
#[derive(Debug, PartialEq)]
pub enum ExecuteToPendingOutcome {
    /// The cell is waiting for more runtime input after draining the runtime
    /// input queue that was ready at the pending boundary.
    Pending {
        cell_id: String,
        content_items: Vec<FunctionCallOutputContentItem>,
        /// Runtime tool-call ids emitted before this paused execution frontier
        /// sealed. Hosts can use these ids to drain their tool-call transport
        /// before surfacing the pending boundary to callers.
        pending_tool_call_ids: Vec<String>,
    },
    /// The cell reached a terminal runtime response before going pending.
    Completed(RuntimeResponse),
}

/// Result of resuming a live code-mode cell until it completes or becomes
/// quiescent again.
#[derive(Debug, PartialEq)]
pub enum WaitToPendingOutcome {
    /// The requested code cell was live when the wait command was accepted.
    LiveCell(ExecuteToPendingOutcome),
    /// The requested code cell was not live.
    MissingCell(RuntimeResponse),
}

impl From<WaitOutcome> for RuntimeResponse {
    fn from(outcome: WaitOutcome) -> Self {
        match outcome {
            WaitOutcome::LiveCell(response) | WaitOutcome::MissingCell(response) => response,
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub enum RuntimeResponse {
    Yielded {
        cell_id: String,
        content_items: Vec<FunctionCallOutputContentItem>,
    },
    Terminated {
        cell_id: String,
        content_items: Vec<FunctionCallOutputContentItem>,
    },
    Result {
        cell_id: String,
        content_items: Vec<FunctionCallOutputContentItem>,
        stored_values: HashMap<String, JsonValue>,
        result: Option<JsonValue>,
        error_text: Option<String>,
    },
}

impl RuntimeResponse {
    pub fn cell_id(&self) -> &str {
        match self {
            Self::Yielded { cell_id, .. }
            | Self::Terminated { cell_id, .. }
            | Self::Result { cell_id, .. } => cell_id,
        }
    }

    pub fn is_yielded(&self) -> bool {
        matches!(self, Self::Yielded { .. })
    }

    pub fn into_structured_result(self) -> (JsonValue, Vec<FunctionCallOutputContentItem>) {
        match self {
            Self::Yielded {
                cell_id,
                content_items,
            } => (
                serde_json::json!({
                    "status": "running",
                    "cell_id": cell_id,
                }),
                content_items,
            ),
            Self::Terminated {
                cell_id,
                content_items,
            } => (
                serde_json::json!({
                    "status": "terminated",
                    "cell_id": cell_id,
                }),
                content_items,
            ),
            Self::Result {
                content_items,
                result: _,
                error_text: Some(error_text),
                ..
            } => (failed_structured_result(error_text), content_items),
            Self::Result {
                content_items,
                result: Some(result),
                error_text: None,
                ..
            } => (
                serde_json::json!({
                    "status": "completed",
                    "result": result,
                }),
                content_items,
            ),
            Self::Result {
                content_items,
                result: None,
                error_text: None,
                ..
            } => (
                serde_json::json!({
                    "status": "completed",
                }),
                content_items,
            ),
        }
    }
}

pub fn failed_structured_result(message: impl Into<String>) -> JsonValue {
    serde_json::json!({
        "status": "failed",
        "error": {
            "message": message.into(),
        }
    })
}

/// Nested tool request emitted by one code-mode cell.
///
/// Code mode owns the per-cell runtime id. Hosts should preserve it for
/// provenance/debugging, but should still assign their own runtime tool call id
/// if their tool-call graph requires globally unique ids.
#[derive(Debug)]
pub struct CodeModeNestedToolCall {
    pub cell_id: String,
    pub runtime_tool_call_id: String,
    pub tool_name: ToolName,
    pub tool_kind: CodeModeToolKind,
    pub input: Option<JsonValue>,
}

#[derive(Debug)]
pub(crate) enum TurnMessage {
    ToolCall(CodeModeNestedToolCall),
}

#[derive(Debug)]
pub(crate) enum RuntimeCommand {
    ToolResponse { id: String, result: JsonValue },
    ToolError { id: String, error_text: String },
    TimeoutFired { id: u64 },
    Terminate,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum PendingRuntimeMode {
    Continue,
    PauseUntilResumed,
}

#[derive(Debug)]
pub(crate) enum RuntimeControlCommand {
    Resume,
    Terminate,
}

#[derive(Debug)]
pub(crate) enum RuntimeEvent {
    Started,
    Pending,
    ContentItem(FunctionCallOutputContentItem),
    YieldRequested,
    ToolCall {
        id: String,
        name: ToolName,
        kind: CodeModeToolKind,
        input: Option<JsonValue>,
    },
    Result {
        stored_values: HashMap<String, JsonValue>,
        stored_value_updates: HashMap<String, JsonValue>,
        result: Option<JsonValue>,
        error_text: Option<String>,
    },
}

pub(crate) fn spawn_runtime(
    request: ExecuteRequest,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_mode: PendingRuntimeMode,
) -> Result<
    (
        std_mpsc::Sender<RuntimeCommand>,
        std_mpsc::Sender<RuntimeControlCommand>,
        v8::IsolateHandle,
    ),
    String,
> {
    initialize_v8()?;

    let (command_tx, command_rx) = std_mpsc::channel();
    let (control_tx, control_rx) = std_mpsc::channel();
    let runtime_command_tx = command_tx.clone();
    let (isolate_handle_tx, isolate_handle_rx) = std_mpsc::sync_channel(1);
    let enabled_tools = request
        .enabled_tools
        .iter()
        .map(enabled_tool_metadata)
        .collect::<Vec<_>>();
    reject_duplicate_tool_global_names(&enabled_tools)?;
    let config = RuntimeConfig {
        enabled_tools,
        source: wrap_exec_source(&request.source),
        stored_values: request.stored_values,
        max_output_tokens: request
            .max_output_tokens
            .unwrap_or(DEFAULT_MAX_OUTPUT_TOKENS_PER_EXEC_CALL)
            .min(MAX_OUTPUT_TOKENS_PER_EXEC_CALL),
    };

    thread::spawn(move || {
        run_runtime(
            config,
            event_tx,
            command_rx,
            control_rx,
            pending_mode,
            isolate_handle_tx,
            runtime_command_tx,
        );
    });

    let isolate_handle = isolate_handle_rx
        .recv()
        .map_err(|_| "failed to initialize code mode runtime".to_string())?;
    Ok((command_tx, control_tx, isolate_handle))
}

#[derive(Clone)]
struct RuntimeConfig {
    enabled_tools: Vec<EnabledToolMetadata>,
    source: String,
    stored_values: HashMap<String, JsonValue>,
    max_output_tokens: usize,
}

pub(super) struct RuntimeState {
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    pending_tool_calls: HashMap<String, v8::Global<v8::PromiseResolver>>,
    pending_timeouts: HashMap<u64, timers::ScheduledTimeout>,
    stored_values: HashMap<String, JsonValue>,
    stored_value_updates: HashMap<String, JsonValue>,
    stored_value_bytes: usize,
    enabled_tools: Vec<EnabledToolMetadata>,
    next_tool_call_id: u64,
    next_timeout_id: u64,
    nested_tool_call_count: usize,
    nested_tool_input_bytes: usize,
    output_content_item_count: usize,
    output_image_url_bytes: usize,
    runtime_command_tx: std_mpsc::Sender<RuntimeCommand>,
    exit_requested: bool,
}

pub(super) enum CompletionState {
    Pending,
    Completed {
        stored_values: HashMap<String, JsonValue>,
        stored_value_updates: HashMap<String, JsonValue>,
        result: Option<JsonValue>,
        error_text: Option<String>,
    },
}

fn initialize_v8() -> Result<(), String> {
    static PLATFORM: OnceLock<Result<v8::SharedRef<v8::Platform>, String>> = OnceLock::new();

    match PLATFORM.get_or_init(|| {
        v8::icu::set_common_data_77(deno_core_icudata::ICU_DATA)
            .map_err(|error_code| format!("failed to initialize ICU data: {error_code}"))?;
        let platform = v8::new_default_platform(0, false).make_shared();
        v8::V8::initialize_platform(platform.clone());
        v8::V8::initialize();
        Ok(platform)
    }) {
        Ok(_) => Ok(()),
        Err(error_text) => Err(error_text.clone()),
    }
}

fn code_mode_create_params() -> v8::CreateParams {
    v8::CreateParams::default().heap_limits(0, CODE_MODE_MAX_V8_HEAP_BYTES)
}

struct RuntimeHeapLimitState {
    isolate_handle: Option<v8::IsolateHandle>,
    exceeded: bool,
}

impl RuntimeHeapLimitState {
    fn apply_result(
        &self,
        result: Option<JsonValue>,
        error_text: Option<String>,
    ) -> (Option<JsonValue>, Option<String>) {
        if self.exceeded {
            (None, Some(heap_limit_error_text()))
        } else {
            (result, error_text)
        }
    }

    fn apply_error(&self, error_text: Option<String>) -> Option<String> {
        if self.exceeded {
            Some(heap_limit_error_text())
        } else {
            error_text
        }
    }
}

unsafe extern "C" fn code_mode_near_heap_limit_callback(
    data: *mut c_void,
    current_heap_limit: usize,
    _initial_heap_limit: usize,
) -> usize {
    // SAFETY: `data` points to the stack-owned `RuntimeHeapLimitState` in
    // `run_runtime`. That state is declared before the isolate, so it outlives
    // every callback invocation from the isolate.
    let state = unsafe { &mut *data.cast::<RuntimeHeapLimitState>() };
    state.exceeded = true;
    if let Some(isolate_handle) = &state.isolate_handle {
        let _ = isolate_handle.terminate_execution();
    }
    current_heap_limit.saturating_add(CODE_MODE_V8_HEAP_LIMIT_GRACE_BYTES)
}

fn heap_limit_error_text() -> String {
    format!(
        "code mode cell exceeded the V8 heap limit of {} MiB",
        CODE_MODE_MAX_V8_HEAP_BYTES / (1024 * 1024)
    )
}

fn run_runtime(
    config: RuntimeConfig,
    event_tx: mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: std_mpsc::Receiver<RuntimeCommand>,
    control_rx: std_mpsc::Receiver<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
    isolate_handle_tx: std_mpsc::SyncSender<v8::IsolateHandle>,
    runtime_command_tx: std_mpsc::Sender<RuntimeCommand>,
) {
    let mut heap_limit_state = RuntimeHeapLimitState {
        isolate_handle: None,
        exceeded: false,
    };
    let isolate = &mut v8::Isolate::new(code_mode_create_params());
    let isolate_handle = isolate.thread_safe_handle();
    heap_limit_state.isolate_handle = Some(isolate_handle.clone());
    let heap_limit_state_ptr = (&raw mut heap_limit_state).cast::<c_void>();
    isolate.add_near_heap_limit_callback(code_mode_near_heap_limit_callback, heap_limit_state_ptr);
    if isolate_handle_tx.send(isolate_handle).is_err() {
        return;
    }
    isolate.set_host_import_module_dynamically_callback(module_loader::dynamic_import_callback);

    v8::scope!(let scope, isolate);
    let context = v8::Context::new(scope, Default::default());
    let scope = &mut v8::ContextScope::new(scope, context);

    scope.set_slot(RuntimeState {
        event_tx: event_tx.clone(),
        pending_tool_calls: HashMap::new(),
        pending_timeouts: HashMap::new(),
        stored_value_bytes: stored_values_serialized_len(&config.stored_values),
        stored_values: config.stored_values,
        stored_value_updates: HashMap::new(),
        enabled_tools: config.enabled_tools,
        next_tool_call_id: 1,
        next_timeout_id: 1,
        nested_tool_call_count: 0,
        nested_tool_input_bytes: 0,
        output_content_item_count: 0,
        output_image_url_bytes: 0,
        runtime_command_tx,
        exit_requested: false,
    });

    if let Err(error_text) = globals::install_globals(scope) {
        send_result(
            &event_tx,
            HashMap::new(),
            HashMap::new(),
            None,
            Some(error_text),
            config.max_output_tokens,
        );
        return;
    }

    let _ = event_tx.send(RuntimeEvent::Started);

    let pending_promise = match module_loader::evaluate_main_module(scope, &config.source) {
        Ok(pending_promise) => pending_promise,
        Err(error_text) => {
            capture_scope_send_error(
                scope,
                &event_tx,
                heap_limit_state.apply_error(Some(error_text)),
                config.max_output_tokens,
            );
            return;
        }
    };

    match module_loader::completion_state(scope, pending_promise.as_ref(), config.max_output_tokens)
    {
        CompletionState::Completed {
            stored_values,
            stored_value_updates,
            result,
            error_text,
        } => {
            let (result, error_text) = heap_limit_state.apply_result(result, error_text);
            send_result(
                &event_tx,
                stored_values,
                stored_value_updates,
                result,
                error_text,
                config.max_output_tokens,
            );
            return;
        }
        CompletionState::Pending => {}
    }

    let mut pending_promise = pending_promise;
    let mut pause_before_command = pending_mode == PendingRuntimeMode::PauseUntilResumed;
    loop {
        let Some(command) = next_runtime_command(
            &event_tx,
            &command_rx,
            &control_rx,
            pending_mode,
            &mut pause_before_command,
        ) else {
            break;
        };

        match command {
            RuntimeCommand::Terminate => break,
            RuntimeCommand::ToolResponse { id, result } => {
                if let Err(error_text) =
                    module_loader::resolve_tool_response(scope, &id, Ok(result))
                {
                    capture_scope_send_error(
                        scope,
                        &event_tx,
                        heap_limit_state.apply_error(Some(error_text)),
                        config.max_output_tokens,
                    );
                    return;
                }
            }
            RuntimeCommand::ToolError { id, error_text } => {
                if let Err(runtime_error) =
                    module_loader::resolve_tool_response(scope, &id, Err(error_text))
                {
                    capture_scope_send_error(
                        scope,
                        &event_tx,
                        heap_limit_state.apply_error(Some(runtime_error)),
                        config.max_output_tokens,
                    );
                    return;
                }
            }
            RuntimeCommand::TimeoutFired { id } => {
                if let Err(runtime_error) = timers::invoke_timeout_callback(scope, id) {
                    capture_scope_send_error(
                        scope,
                        &event_tx,
                        heap_limit_state.apply_error(Some(runtime_error)),
                        config.max_output_tokens,
                    );
                    return;
                }
            }
        }

        scope.perform_microtask_checkpoint();
        if let Some((stored_values, stored_value_updates)) = exit_requested_result(scope) {
            send_result(
                &event_tx,
                stored_values,
                stored_value_updates,
                None,
                None,
                config.max_output_tokens,
            );
            return;
        }

        match module_loader::completion_state(
            scope,
            pending_promise.as_ref(),
            config.max_output_tokens,
        ) {
            CompletionState::Completed {
                stored_values,
                stored_value_updates,
                result,
                error_text,
            } => {
                let (result, error_text) = heap_limit_state.apply_result(result, error_text);
                send_result(
                    &event_tx,
                    stored_values,
                    stored_value_updates,
                    result,
                    error_text,
                    config.max_output_tokens,
                );
                return;
            }
            CompletionState::Pending => {}
        }

        if let Some(promise) = pending_promise.as_ref() {
            let promise = v8::Local::new(scope, promise);
            if promise.state() != v8::PromiseState::Pending {
                pending_promise = None;
            }
        }
    }
}

fn next_runtime_command(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: &std_mpsc::Receiver<RuntimeCommand>,
    control_rx: &std_mpsc::Receiver<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
    pause_before_command: &mut bool,
) -> Option<RuntimeCommand> {
    match pending_mode {
        PendingRuntimeMode::Continue => next_continuing_runtime_command(event_tx, command_rx),
        PendingRuntimeMode::PauseUntilResumed => {
            next_paused_runtime_command(event_tx, command_rx, control_rx, pause_before_command)
        }
    }
}

fn next_continuing_runtime_command(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: &std_mpsc::Receiver<RuntimeCommand>,
) -> Option<RuntimeCommand> {
    match command_rx.try_recv() {
        Ok(command) => Some(command),
        Err(std_mpsc::TryRecvError::Disconnected) => None,
        Err(std_mpsc::TryRecvError::Empty) => {
            let _ = event_tx.send(RuntimeEvent::Pending);
            command_rx.recv().ok()
        }
    }
}

fn next_paused_runtime_command(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    command_rx: &std_mpsc::Receiver<RuntimeCommand>,
    control_rx: &std_mpsc::Receiver<RuntimeControlCommand>,
    pause_before_command: &mut bool,
) -> Option<RuntimeCommand> {
    loop {
        if *pause_before_command {
            let _ = event_tx.send(RuntimeEvent::Pending);
            match control_rx.recv().ok()? {
                RuntimeControlCommand::Resume => {
                    *pause_before_command = false;
                }
                RuntimeControlCommand::Terminate => return Some(RuntimeCommand::Terminate),
            }
        }

        match command_rx.try_recv() {
            Ok(command) => return Some(command),
            Err(std_mpsc::TryRecvError::Disconnected) => return None,
            Err(std_mpsc::TryRecvError::Empty) => {
                *pause_before_command = true;
            }
        }
    }
}

fn exit_requested_result(
    scope: &mut v8::PinScope<'_, '_>,
) -> Option<(HashMap<String, JsonValue>, HashMap<String, JsonValue>)> {
    let state = scope.get_slot::<RuntimeState>()?;
    if !state.exit_requested {
        return None;
    }

    Some((
        state.stored_values.clone(),
        state.stored_value_updates.clone(),
    ))
}

fn capture_scope_send_error(
    scope: &mut v8::PinScope<'_, '_>,
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    error_text: Option<String>,
    max_output_tokens: usize,
) {
    let (stored_values, stored_value_updates) = scope
        .get_slot::<RuntimeState>()
        .map(|state| {
            (
                state.stored_values.clone(),
                state.stored_value_updates.clone(),
            )
        })
        .unwrap_or_default();

    send_result(
        event_tx,
        stored_values,
        stored_value_updates,
        None,
        error_text,
        max_output_tokens,
    );
}

fn send_result(
    event_tx: &mpsc::UnboundedSender<RuntimeEvent>,
    stored_values: HashMap<String, JsonValue>,
    stored_value_updates: HashMap<String, JsonValue>,
    result: Option<JsonValue>,
    error_text: Option<String>,
    max_output_tokens: usize,
) {
    let (result, error_text) = enforce_result_budget(result, error_text, max_output_tokens);
    let _ = event_tx.send(RuntimeEvent::Result {
        stored_values,
        stored_value_updates,
        result,
        error_text,
    });
}

fn reject_duplicate_tool_global_names(enabled_tools: &[EnabledToolMetadata]) -> Result<(), String> {
    let mut global_names = HashMap::new();
    for tool in enabled_tools {
        if let Some(previous_tool_name) =
            global_names.insert(tool.global_name.clone(), tool.tool_name.name.clone())
        {
            return Err(format!(
                "multiple tools normalize to the same JavaScript tool name `{}`: `{previous_tool_name}` and `{}`",
                tool.global_name, tool.tool_name.name
            ));
        }
    }
    Ok(())
}

fn enforce_result_budget(
    result: Option<JsonValue>,
    error_text: Option<String>,
    max_output_tokens: usize,
) -> (Option<JsonValue>, Option<String>) {
    if error_text.is_some() {
        return (
            None,
            error_text.map(|error_text| truncate_error_text(error_text, max_output_tokens)),
        );
    }
    let Some(result_value) = result else {
        return (None, None);
    };
    let serialized = match serde_json::to_string(&result_value) {
        Ok(serialized) => serialized,
        Err(error) => {
            return (
                None,
                Some(format!(
                    "failed to serialize code mode result for output budget enforcement: {error}"
                )),
            );
        }
    };
    let estimated_tokens = estimated_json_tokens(serialized.len());
    if estimated_tokens > max_output_tokens {
        return (
            None,
            Some(format!(
                "code mode result exceeded max_output_tokens ({estimated_tokens} estimated tokens > {max_output_tokens})"
            )),
        );
    }
    (Some(result_value), None)
}

fn truncate_error_text(mut error_text: String, max_output_tokens: usize) -> String {
    let max_bytes = max_output_tokens.saturating_mul(4).max(1);
    if error_text.len() <= max_bytes {
        return error_text;
    }
    let suffix = "... [truncated]";
    let keep_bytes = max_bytes.saturating_sub(suffix.len()).max(1);
    let mut truncate_at = keep_bytes.min(error_text.len());
    while !error_text.is_char_boundary(truncate_at) {
        truncate_at = truncate_at.saturating_sub(1);
    }
    error_text.truncate(truncate_at);
    error_text.push_str(suffix);
    error_text
}

fn stored_values_serialized_len(values: &HashMap<String, JsonValue>) -> usize {
    values
        .values()
        .map(json_value_serialized_len)
        .fold(0, usize::saturating_add)
}

pub(super) fn json_value_serialized_len(value: &JsonValue) -> usize {
    serde_json::to_string(value).map_or(usize::MAX, |value| value.len())
}

fn estimated_json_tokens(byte_len: usize) -> usize {
    byte_len.div_ceil(4)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use pretty_assertions::assert_eq;
    use serde_json::json;
    use tokio::sync::mpsc;

    use crate::description::CodeModeToolKind;
    use crate::description::ToolDefinition;
    use crate::description::ToolName;

    use super::ExecuteRequest;
    use super::PendingRuntimeMode;
    use super::RuntimeCommand;
    use super::RuntimeControlCommand;
    use super::RuntimeEvent;
    use super::spawn_runtime;

    fn execute_request(source: &str) -> ExecuteRequest {
        ExecuteRequest {
            cell_id: "1".to_string(),
            enabled_tools: Vec::new(),
            source: source.to_string(),
            stored_values: HashMap::new(),
            yield_time_ms: Some(1),
            max_output_tokens: None,
        }
    }

    #[test]
    fn code_mode_create_params_sets_a_v8_heap_limit() {
        let params = super::code_mode_create_params();
        let max_old_generation_size = params.max_old_generation_size_in_bytes();

        assert!(max_old_generation_size > 0);
        assert!(max_old_generation_size <= super::CODE_MODE_MAX_V8_HEAP_BYTES);
    }

    #[test]
    fn heap_limit_state_overrides_successful_results() {
        let state = super::RuntimeHeapLimitState {
            isolate_handle: None,
            exceeded: true,
        };

        let (result, error_text) = state.apply_result(Some(json!("ok")), None);

        assert_eq!(result, None);
        assert_eq!(
            error_text,
            Some("code mode cell exceeded the V8 heap limit of 128 MiB".to_string())
        );
    }

    #[tokio::test]
    async fn terminate_execution_stops_cpu_bound_module() {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (_runtime_tx, _runtime_control_tx, runtime_terminate_handle) = spawn_runtime(
            execute_request("while (true) {}"),
            event_tx,
            PendingRuntimeMode::Continue,
        )
        .unwrap();

        let started_event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(started_event, RuntimeEvent::Started));

        assert!(runtime_terminate_handle.terminate_execution());

        let result_event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let RuntimeEvent::Result {
            stored_values,
            stored_value_updates,
            result,
            error_text,
        } = result_event
        else {
            panic!("expected runtime result after termination");
        };
        assert_eq!(stored_values, HashMap::new());
        assert_eq!(stored_value_updates, HashMap::new());
        assert_eq!(result, None);
        assert!(error_text.is_some());

        assert!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn pending_mode_freezes_runtime_commands_until_resume() {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let (runtime_tx, runtime_control_tx, _runtime_terminate_handle) = spawn_runtime(
            execute_request(
                r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
globalThis.__coral_code_mode_result = "after";
await new Promise(() => {});
"#,
            ),
            event_tx,
            PendingRuntimeMode::PauseUntilResumed,
        )
        .unwrap();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Started
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Pending
        ));

        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();
        tokio::time::timeout(Duration::from_millis(100), event_rx.recv())
            .await
            .unwrap_err();

        runtime_control_tx
            .send(RuntimeControlCommand::Resume)
            .unwrap();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Pending
        ));

        runtime_control_tx
            .send(RuntimeControlCommand::Terminate)
            .unwrap();
    }

    #[tokio::test]
    async fn pending_mode_surfaces_frontier_before_queued_tool_response() {
        let (event_tx, mut event_rx) = mpsc::unbounded_channel();
        let mut request =
            execute_request(r#"return await tools.echo({ value: "completed too early" });"#);
        request.enabled_tools = vec![ToolDefinition {
            name: "echo".to_string(),
            tool_name: ToolName::plain("echo"),
            description: String::new(),
            kind: CodeModeToolKind::Function,
            input_schema: None,
            output_schema: None,
        }];
        let (runtime_tx, runtime_control_tx, _runtime_terminate_handle) =
            spawn_runtime(request, event_tx, PendingRuntimeMode::PauseUntilResumed).unwrap();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Started
        ));
        let RuntimeEvent::ToolCall { id, .. } =
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap()
        else {
            panic!("expected tool call");
        };

        runtime_tx
            .send(RuntimeCommand::ToolResponse {
                id,
                result: json!({ "value": "completed too early" }),
            })
            .unwrap();

        assert!(matches!(
            tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
                .await
                .unwrap()
                .unwrap(),
            RuntimeEvent::Pending
        ));

        runtime_control_tx
            .send(RuntimeControlCommand::Resume)
            .unwrap();

        let RuntimeEvent::Result {
            result, error_text, ..
        } = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .unwrap()
            .unwrap()
        else {
            panic!("expected runtime result after resume");
        };
        assert_eq!(result, Some(json!({ "value": "completed too early" })));
        assert_eq!(error_text, None);
    }
}
