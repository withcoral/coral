use std::collections::{HashMap, HashSet};
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value as JsonValue;
use tokio::sync::Mutex;
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::FunctionCallOutputContentItem;
use crate::runtime::CodeModeNestedToolCall;
use crate::runtime::DEFAULT_EXEC_YIELD_TIME_MS;
use crate::runtime::DEFAULT_WAIT_YIELD_TIME_MS;
use crate::runtime::ExecuteRequest;
use crate::runtime::ExecuteToPendingOutcome;
use crate::runtime::MAX_NESTED_CALLS_PER_CELL;
use crate::runtime::PendingRuntimeMode;
use crate::runtime::RuntimeCommand;
use crate::runtime::RuntimeControlCommand;
use crate::runtime::RuntimeEvent;
use crate::runtime::RuntimeResponse;
use crate::runtime::TurnMessage;
use crate::runtime::WaitOutcome;
use crate::runtime::WaitRequest;
use crate::runtime::WaitToPendingOutcome;
use crate::runtime::WaitToPendingRequest;
use crate::runtime::nested_tool_budget_exceeded_error;
use crate::runtime::spawn_runtime;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CodeModeToolError {
    pub message: String,
    pub fatal: bool,
}

impl CodeModeToolError {
    #[must_use]
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            fatal: true,
        }
    }

    #[must_use]
    pub fn recoverable(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            fatal: false,
        }
    }
}

#[async_trait]
pub trait CodeModeTurnHost: Send + Sync {
    async fn invoke_tool(
        &self,
        invocation: CodeModeNestedToolCall,
        cancellation_token: CancellationToken,
    ) -> Result<JsonValue, CodeModeToolError>;
}

#[derive(Clone)]
struct SessionHandle {
    control_tx: mpsc::UnboundedSender<SessionControlCommand>,
    runtime_tx: std::sync::mpsc::Sender<RuntimeCommand>,
    cancellation_token: CancellationToken,
}

struct Inner {
    stored_values: Mutex<HashMap<String, JsonValue>>,
    sessions: Mutex<HashMap<String, SessionHandle>>,
    waiting_cells: StdMutex<HashSet<String>>,
    nested_call_counts: Mutex<HashMap<String, usize>>,
    turn_message_tx: async_channel::Sender<TurnMessage>,
    turn_message_rx: async_channel::Receiver<TurnMessage>,
    shutdown_token: CancellationToken,
    next_cell_id: AtomicU64,
}

pub struct CodeModeService {
    inner: Arc<Inner>,
}

impl CodeModeService {
    pub fn new() -> Self {
        let (turn_message_tx, turn_message_rx) = async_channel::unbounded();

        Self {
            inner: Arc::new(Inner {
                stored_values: Mutex::new(HashMap::new()),
                sessions: Mutex::new(HashMap::new()),
                waiting_cells: StdMutex::new(HashSet::new()),
                nested_call_counts: Mutex::new(HashMap::new()),
                turn_message_tx,
                turn_message_rx,
                shutdown_token: CancellationToken::new(),
                next_cell_id: AtomicU64::new(1),
            }),
        }
    }

    pub async fn stored_values(&self) -> HashMap<String, JsonValue> {
        self.inner.stored_values.lock().await.clone()
    }

    pub async fn replace_stored_values(&self, values: HashMap<String, JsonValue>) {
        *self.inner.stored_values.lock().await = values;
    }

    /// Reserves the runtime cell id for a future `execute` request.
    ///
    /// The runtime can issue nested tool calls before the first `execute`
    /// response is returned. Hosts that need a parent trace object for those
    /// nested calls should allocate the cell id up front and pass it back on the
    /// `ExecuteRequest`.
    pub fn allocate_cell_id(&self) -> String {
        self.inner
            .next_cell_id
            .fetch_add(1, Ordering::Relaxed)
            .to_string()
    }

    pub async fn execute(&self, request: ExecuteRequest) -> Result<RuntimeResponse, String> {
        let initial_yield_time_ms = request.yield_time_ms.unwrap_or(DEFAULT_EXEC_YIELD_TIME_MS);
        let (response_tx, response_rx) = oneshot::channel();
        self.start_session(
            request,
            SessionResponseSender::Runtime(response_tx),
            Some(initial_yield_time_ms),
            PendingRuntimeMode::Continue,
        )
        .await?;

        response_rx
            .await
            .map_err(|_| "exec runtime ended unexpectedly".to_string())
    }

    pub async fn execute_to_pending(
        &self,
        request: ExecuteRequest,
    ) -> Result<ExecuteToPendingOutcome, String> {
        let initial_yield_time_ms = request.yield_time_ms.unwrap_or(DEFAULT_EXEC_YIELD_TIME_MS);
        let (response_tx, response_rx) = oneshot::channel();
        self.start_session(
            request,
            SessionResponseSender::ExecuteToPending(response_tx),
            Some(initial_yield_time_ms),
            PendingRuntimeMode::PauseUntilResumed,
        )
        .await?;

        response_rx
            .await
            .map_err(|_| "exec runtime ended unexpectedly".to_string())
    }

    async fn start_session(
        &self,
        request: ExecuteRequest,
        initial_response_tx: SessionResponseSender,
        initial_yield_time_ms: Option<u64>,
        pending_mode: PendingRuntimeMode,
    ) -> Result<(), String> {
        let cell_id = request.cell_id.clone();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let cancellation_token = CancellationToken::new();
        let (runtime_tx, runtime_control_tx, runtime_terminate_handle) = {
            let mut sessions = self.inner.sessions.lock().await;
            if sessions.contains_key(&cell_id) {
                return Err(format!("exec cell {cell_id} already exists"));
            }

            let (runtime_tx, runtime_control_tx, runtime_terminate_handle) =
                spawn_runtime(request, event_tx, pending_mode)?;

            // Keep the session registry locked through insertion so a
            // caller-owned cell id cannot race with another execute and replace
            // a live runtime.
            sessions.insert(
                cell_id.clone(),
                SessionHandle {
                    control_tx,
                    runtime_tx: runtime_tx.clone(),
                    cancellation_token: cancellation_token.clone(),
                },
            );
            (runtime_tx, runtime_control_tx, runtime_terminate_handle)
        };

        tokio::spawn(run_session_control(
            Arc::clone(&self.inner),
            SessionControlContext {
                cell_id: cell_id.clone(),
                runtime_tx,
                runtime_control_tx,
                pending_mode,
                runtime_terminate_handle,
                cancellation_token,
                shutdown_token: self.inner.shutdown_token.child_token(),
            },
            event_rx,
            control_rx,
            initial_response_tx,
            initial_yield_time_ms,
        ));

        Ok(())
    }

    pub async fn wait(&self, request: WaitRequest) -> Result<WaitOutcome, String> {
        let cell_id = request.cell_id.clone();
        let _wait_guard = self.begin_wait(&cell_id)?;
        self.wait_inner(request).await
    }

    async fn wait_inner(&self, request: WaitRequest) -> Result<WaitOutcome, String> {
        let cell_id = request.cell_id.clone();
        let handle = self
            .inner
            .sessions
            .lock()
            .await
            .get(&request.cell_id)
            .cloned();
        let Some(handle) = handle else {
            return Ok(WaitOutcome::MissingCell(missing_cell_response(cell_id)));
        };
        let (response_tx, response_rx) = oneshot::channel();
        if request.terminate {
            handle.cancellation_token.cancel();
        }
        let control_message = if request.terminate {
            SessionControlCommand::Terminate { response_tx }
        } else {
            SessionControlCommand::Poll {
                yield_time_ms: request.yield_time_ms,
                response_tx,
            }
        };
        if handle.control_tx.send(control_message).is_err() {
            return Ok(WaitOutcome::MissingCell(missing_cell_response(cell_id)));
        }
        match response_rx.await {
            Ok(response) => Ok(WaitOutcome::LiveCell(response)),
            Err(_) => Ok(WaitOutcome::MissingCell(missing_cell_response(
                request.cell_id,
            ))),
        }
    }

    pub async fn wait_to_pending(
        &self,
        request: WaitToPendingRequest,
    ) -> Result<WaitToPendingOutcome, String> {
        let cell_id = request.cell_id.clone();
        let _wait_guard = self.begin_wait(&cell_id)?;
        self.wait_to_pending_inner(request).await
    }

    async fn wait_to_pending_inner(
        &self,
        request: WaitToPendingRequest,
    ) -> Result<WaitToPendingOutcome, String> {
        let cell_id = request.cell_id.clone();
        let yield_time_ms = request.yield_time_ms.unwrap_or(DEFAULT_WAIT_YIELD_TIME_MS);
        let handle = self
            .inner
            .sessions
            .lock()
            .await
            .get(&request.cell_id)
            .cloned();
        let Some(handle) = handle else {
            return Ok(WaitToPendingOutcome::MissingCell(missing_cell_response(
                cell_id,
            )));
        };
        let (response_tx, response_rx) = oneshot::channel();
        if handle
            .control_tx
            .send(SessionControlCommand::PollToPending {
                yield_time_ms,
                response_tx,
            })
            .is_err()
        {
            return Ok(WaitToPendingOutcome::MissingCell(missing_cell_response(
                cell_id,
            )));
        }
        match response_rx.await {
            Ok(response) => Ok(WaitToPendingOutcome::LiveCell(response)),
            Err(_) => Ok(WaitToPendingOutcome::MissingCell(missing_cell_response(
                request.cell_id,
            ))),
        }
    }

    fn begin_wait(&self, cell_id: &str) -> Result<WaitCellGuard, String> {
        let mut waiting_cells = self
            .inner
            .waiting_cells
            .lock()
            .map_err(|_| "code mode wait state is unavailable".to_string())?;
        if !waiting_cells.insert(cell_id.to_string()) {
            return Err(format!(
                "exec cell {cell_id} already has a wait in progress"
            ));
        }
        Ok(WaitCellGuard {
            inner: Arc::clone(&self.inner),
            cell_id: cell_id.to_string(),
        })
    }
}

struct WaitCellGuard {
    inner: Arc<Inner>,
    cell_id: String,
}

impl Drop for WaitCellGuard {
    fn drop(&mut self) {
        if let Ok(mut waiting_cells) = self.inner.waiting_cells.lock() {
            waiting_cells.remove(&self.cell_id);
        }
    }
}

impl CodeModeService {
    pub fn start_turn_worker(&self, host: Arc<dyn CodeModeTurnHost>) -> CodeModeTurnWorker {
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel();
        let inner = Arc::clone(&self.inner);
        let turn_message_rx = self.inner.turn_message_rx.clone();

        tokio::spawn(async move {
            loop {
                let next_message = tokio::select! {
                    _ = &mut shutdown_rx => break,
                    message = turn_message_rx.recv() => message.ok(),
                };
                let Some(next_message) = next_message else {
                    break;
                };
                match next_message {
                    TurnMessage::ToolCall(invocation) => {
                        let host = Arc::clone(&host);
                        let inner = Arc::clone(&inner);
                        tokio::spawn(async move {
                            let cell_id = invocation.cell_id.clone();
                            let runtime_tool_call_id = invocation.runtime_tool_call_id.clone();
                            let Some(handle) = inner.sessions.lock().await.get(&cell_id).cloned()
                            else {
                                return;
                            };
                            let cancellation_token = handle.cancellation_token.child_token();
                            if cancellation_token.is_cancelled() {
                                return;
                            }
                            let response = host
                                .invoke_tool(invocation, cancellation_token.clone())
                                .await;
                            if cancellation_token.is_cancelled() {
                                return;
                            }
                            let runtime_tx = inner
                                .sessions
                                .lock()
                                .await
                                .get(&cell_id)
                                .filter(|handle| !handle.cancellation_token.is_cancelled())
                                .map(|handle| handle.runtime_tx.clone());
                            let Some(runtime_tx) = runtime_tx else {
                                return;
                            };
                            let command = match response {
                                Ok(result) => RuntimeCommand::ToolResponse {
                                    id: runtime_tool_call_id,
                                    result,
                                },
                                Err(error) => RuntimeCommand::ToolError {
                                    id: runtime_tool_call_id,
                                    error_text: error.message,
                                    fatal: error.fatal,
                                },
                            };
                            let _ = runtime_tx.send(command);
                        });
                    }
                }
            }
        });

        CodeModeTurnWorker {
            shutdown_tx: Some(shutdown_tx),
        }
    }
}

impl Default for CodeModeService {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for CodeModeService {
    fn drop(&mut self) {
        self.inner.shutdown_token.cancel();
    }
}

pub struct CodeModeTurnWorker {
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl Drop for CodeModeTurnWorker {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
    }
}

enum SessionControlCommand {
    Poll {
        yield_time_ms: u64,
        response_tx: oneshot::Sender<RuntimeResponse>,
    },
    PollToPending {
        yield_time_ms: u64,
        response_tx: oneshot::Sender<ExecuteToPendingOutcome>,
    },
    Terminate {
        response_tx: oneshot::Sender<RuntimeResponse>,
    },
}

enum SessionResponseSender {
    Runtime(oneshot::Sender<RuntimeResponse>),
    ExecuteToPending(oneshot::Sender<ExecuteToPendingOutcome>),
}

struct PendingResult {
    content_items: Vec<FunctionCallOutputContentItem>,
    stored_values: HashMap<String, JsonValue>,
    stored_value_updates: HashMap<String, JsonValue>,
    result: Option<JsonValue>,
    error_text: Option<String>,
}

struct PendingFrontier {
    content_items: Vec<FunctionCallOutputContentItem>,
    pending_tool_call_ids: Vec<String>,
}

struct SessionControlContext {
    cell_id: String,
    runtime_tx: std::sync::mpsc::Sender<RuntimeCommand>,
    runtime_control_tx: std::sync::mpsc::Sender<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
    runtime_terminate_handle: v8::IsolateHandle,
    cancellation_token: CancellationToken,
    shutdown_token: CancellationToken,
}

fn missing_cell_response(cell_id: String) -> RuntimeResponse {
    RuntimeResponse::Result {
        error_text: Some(format!("exec cell {cell_id} not found")),
        cell_id,
        content_items: Vec::new(),
        stored_values: HashMap::new(),
        stored_value_updates: HashMap::new(),
        result: None,
    }
}

fn pending_result_response(cell_id: &str, result: PendingResult) -> RuntimeResponse {
    RuntimeResponse::Result {
        cell_id: cell_id.to_string(),
        content_items: result.content_items,
        stored_values: result.stored_values,
        stored_value_updates: result.stored_value_updates,
        result: result.result,
        error_text: result.error_text,
    }
}

fn send_terminal_response(
    response_tx: SessionResponseSender,
    response: RuntimeResponse,
) -> Result<(), Box<RuntimeResponse>> {
    match response_tx {
        SessionResponseSender::Runtime(response_tx) => response_tx.send(response).map_err(Box::new),
        SessionResponseSender::ExecuteToPending(response_tx) => {
            match response_tx.send(ExecuteToPendingOutcome::Completed(response)) {
                Ok(()) => Ok(()),
                Err(ExecuteToPendingOutcome::Completed(response)) => Err(Box::new(response)),
                Err(ExecuteToPendingOutcome::Pending { .. }) => unreachable!(
                    "terminal response sender cannot fail with a pending execute-to-pending outcome"
                ),
            }
        }
    }
}

fn pending_result_from_response(response: RuntimeResponse) -> Option<PendingResult> {
    match response {
        RuntimeResponse::Result {
            content_items,
            stored_values,
            stored_value_updates,
            result,
            error_text,
            ..
        } => Some(PendingResult {
            content_items,
            stored_values,
            stored_value_updates,
            result,
            error_text,
        }),
        RuntimeResponse::Yielded { .. } | RuntimeResponse::Terminated { .. } => None,
    }
}

fn send_or_buffer_result(
    cell_id: &str,
    result: PendingResult,
    response_tx: &mut Option<SessionResponseSender>,
    pending_result: &mut Option<PendingResult>,
) -> bool {
    if let Some(response_tx) = response_tx.take() {
        let response = pending_result_response(cell_id, result);
        if let Err(response) = send_terminal_response(response_tx, response) {
            *pending_result = pending_result_from_response(*response);
            return false;
        }
        return true;
    }

    *pending_result = Some(result);
    false
}

fn send_yield_response(
    cell_id: &str,
    content_items: &mut Vec<FunctionCallOutputContentItem>,
    response_tx: &mut Option<SessionResponseSender>,
) {
    let Some(current_response_tx) = response_tx.take() else {
        return;
    };
    match current_response_tx {
        SessionResponseSender::Runtime(response_tx) => {
            let response = RuntimeResponse::Yielded {
                cell_id: cell_id.to_string(),
                content_items: std::mem::take(content_items),
            };
            if let Err(RuntimeResponse::Yielded {
                content_items: returned_items,
                ..
            }) = response_tx.send(response)
            {
                *content_items = returned_items;
            }
        }
        SessionResponseSender::ExecuteToPending(execute_to_pending_tx) => {
            *response_tx = Some(SessionResponseSender::ExecuteToPending(
                execute_to_pending_tx,
            ));
        }
    }
}

fn send_execute_to_pending_watchdog_response(
    cell_id: &str,
    content_items: &mut Vec<FunctionCallOutputContentItem>,
    response_tx: &mut Option<SessionResponseSender>,
) -> bool {
    let Some(current_response_tx) = response_tx.take() else {
        return false;
    };
    match current_response_tx {
        SessionResponseSender::Runtime(runtime_response_tx) => {
            *response_tx = Some(SessionResponseSender::Runtime(runtime_response_tx));
            false
        }
        SessionResponseSender::ExecuteToPending(response_tx) => {
            let response = RuntimeResponse::Result {
                cell_id: cell_id.to_string(),
                content_items: std::mem::take(content_items),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: Some(format!(
                    "exec cell {cell_id} did not reach a pending state before the startup watchdog fired"
                )),
            };
            let _ = response_tx.send(ExecuteToPendingOutcome::Completed(response));
            true
        }
    }
}

fn send_or_buffer_pending_frontier(
    cell_id: &str,
    response_tx: oneshot::Sender<ExecuteToPendingOutcome>,
    pending_frontier: &mut Option<PendingFrontier>,
    content_items: Vec<FunctionCallOutputContentItem>,
    pending_tool_call_ids: Vec<String>,
) {
    let outcome = ExecuteToPendingOutcome::Pending {
        cell_id: cell_id.to_string(),
        content_items,
        pending_tool_call_ids,
    };
    if let Err(ExecuteToPendingOutcome::Pending {
        content_items,
        pending_tool_call_ids,
        ..
    }) = response_tx.send(outcome)
    {
        *pending_frontier = Some(PendingFrontier {
            content_items,
            pending_tool_call_ids,
        });
    }
}

async fn run_session_control(
    inner: Arc<Inner>,
    context: SessionControlContext,
    mut event_rx: mpsc::UnboundedReceiver<RuntimeEvent>,
    mut control_rx: mpsc::UnboundedReceiver<SessionControlCommand>,
    initial_response_tx: SessionResponseSender,
    initial_yield_time_ms: Option<u64>,
) {
    let SessionControlContext {
        cell_id,
        runtime_tx,
        runtime_control_tx,
        pending_mode,
        runtime_terminate_handle,
        cancellation_token,
        shutdown_token,
    } = context;
    let mut content_items = Vec::new();
    let mut pending_tool_call_ids = Vec::new();
    let mut pending_result: Option<PendingResult> = None;
    let mut pending_frontier: Option<PendingFrontier> = None;
    let mut response_tx = Some(initial_response_tx);
    let mut termination_requested = false;
    let mut runtime_closed = false;
    let mut yield_timer: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;

    loop {
        tokio::select! {
            maybe_event = async {
                if runtime_closed {
                    std::future::pending::<Option<RuntimeEvent>>().await
                } else {
                    event_rx.recv().await
                }
            } => {
                let Some(event) = maybe_event else {
                    runtime_closed = true;
                    if termination_requested {
                        if let Some(response_tx) = response_tx.take() {
                            let response = RuntimeResponse::Terminated {
                                cell_id: cell_id.clone(),
                                content_items: std::mem::take(&mut content_items),
                            };
                            let _ = send_terminal_response(response_tx, response);
                        }
                        break;
                    }
                    if pending_result.is_none() {
                        let result = PendingResult {
                            content_items: std::mem::take(&mut content_items),
                            stored_values: HashMap::new(),
                            stored_value_updates: HashMap::new(),
                            result: None,
                            error_text: Some("exec runtime ended unexpectedly".to_string()),
                        };
                        if send_or_buffer_result(
                            &cell_id,
                            result,
                            &mut response_tx,
                            &mut pending_result,
                        ) {
                            break;
                        }
                    }
                    continue;
                };
                match event {
                    RuntimeEvent::Started => {
                        yield_timer = initial_yield_time_ms.map(|initial_yield_time_ms| {
                            Box::pin(tokio::time::sleep(Duration::from_millis(initial_yield_time_ms)))
                        });
                    }
                    RuntimeEvent::Pending => {
                        if let Some(current_response_tx) = response_tx.take() {
                            match current_response_tx {
                                SessionResponseSender::Runtime(runtime_response_tx) => {
                                    response_tx =
                                        Some(SessionResponseSender::Runtime(runtime_response_tx));
                                }
                                SessionResponseSender::ExecuteToPending(response_tx) => {
                                    send_or_buffer_pending_frontier(
                                        &cell_id,
                                        response_tx,
                                        &mut pending_frontier,
                                        std::mem::take(&mut content_items),
                                        std::mem::take(&mut pending_tool_call_ids),
                                    );
                                }
                            }
                        }
                    }
                    RuntimeEvent::ContentItem(item) => {
                        content_items.push(item);
                    }
                    RuntimeEvent::YieldRequested => {
                        yield_timer = None;
                        send_yield_response(&cell_id, &mut content_items, &mut response_tx);
                    }
                    RuntimeEvent::ToolCall {
                        id,
                        name,
                        kind,
                        input,
                        allow_error_result,
                        envelope,
                    } => {
                        if let Err(error_text) =
                            increment_nested_call_count(&inner, &cell_id, &name.name).await
                        {
                            let _ = runtime_tx.send(RuntimeCommand::ToolError {
                                id,
                                error_text,
                                fatal: true,
                            });
                            continue;
                        }
                        if pending_mode == PendingRuntimeMode::PauseUntilResumed {
                            pending_tool_call_ids.push(id.clone());
                        }
                        let tool_call = CodeModeNestedToolCall {
                            cell_id: cell_id.clone(),
                            runtime_tool_call_id: id,
                            tool_name: name,
                            tool_kind: kind,
                            input,
                            allow_error_result,
                            envelope,
                        };
                        let _ = inner
                            .turn_message_tx
                            .send(TurnMessage::ToolCall(tool_call))
                            .await;
                    }
                    RuntimeEvent::Result {
                        stored_values,
                        stored_value_updates,
                        result: structured_result,
                        error_text,
                    } => {
                        yield_timer = None;
                        if termination_requested {
                            if let Some(response_tx) = response_tx.take() {
                                let response = RuntimeResponse::Terminated {
                                    cell_id: cell_id.clone(),
                                    content_items: std::mem::take(&mut content_items),
                                };
                                let _ = send_terminal_response(response_tx, response);
                            }
                            break;
                        }
                        let stored_values =
                            merge_stored_value_updates(&inner, stored_values, &stored_value_updates)
                                .await;
                        let result = PendingResult {
                            content_items: std::mem::take(&mut content_items),
                            stored_values,
                            stored_value_updates,
                            result: structured_result,
                            error_text,
                        };
                        if send_or_buffer_result(
                            &cell_id,
                            result,
                            &mut response_tx,
                            &mut pending_result,
                        ) {
                            break;
                        }
                    }
                }
            }
            maybe_command = control_rx.recv() => {
                let Some(command) = maybe_command else {
                    break;
                };
                match command {
                    SessionControlCommand::Poll {
                        yield_time_ms,
                        response_tx: next_response_tx,
                    } => {
                        if let Some(result) = pending_result.take() {
                            let response = pending_result_response(&cell_id, result);
                            if let Err(response) = next_response_tx.send(response) {
                                pending_result = pending_result_from_response(response);
                                continue;
                            }
                            break;
                        }
                        drain_pending_frontier_content(&mut content_items, &mut pending_frontier);
                        response_tx = Some(SessionResponseSender::Runtime(next_response_tx));
                        yield_timer = Some(Box::pin(tokio::time::sleep(Duration::from_millis(yield_time_ms))));
                        resume_paused_runtime(&runtime_control_tx, pending_mode);
                    }
                    SessionControlCommand::PollToPending {
                        yield_time_ms,
                        response_tx: next_response_tx,
                    } => {
                        if let Some(frontier) = pending_frontier.take() {
                            send_or_buffer_pending_frontier(
                                &cell_id,
                                next_response_tx,
                                &mut pending_frontier,
                                frontier.content_items,
                                frontier.pending_tool_call_ids,
                            );
                            continue;
                        }
                        if let Some(result) = pending_result.take() {
                            let response = pending_result_response(&cell_id, result);
                            match next_response_tx.send(ExecuteToPendingOutcome::Completed(response))
                            {
                                Ok(()) => break,
                                Err(ExecuteToPendingOutcome::Completed(response)) => {
                                    pending_result = pending_result_from_response(response);
                                    continue;
                                }
                                Err(ExecuteToPendingOutcome::Pending { .. }) => unreachable!(
                                    "poll-to-pending completion send cannot fail with a pending outcome"
                                ),
                            }
                        }
                        response_tx =
                            Some(SessionResponseSender::ExecuteToPending(next_response_tx));
                        yield_timer =
                            Some(Box::pin(tokio::time::sleep(Duration::from_millis(yield_time_ms))));
                        resume_paused_runtime(&runtime_control_tx, pending_mode);
                    }
                    SessionControlCommand::Terminate { response_tx: next_response_tx } => {
                        cancellation_token.cancel();
                        if let Some(result) = pending_result.take() {
                            let response = pending_result_response(&cell_id, result);
                            if let Err(response) = next_response_tx.send(response) {
                                pending_result = pending_result_from_response(response);
                                continue;
                            }
                            break;
                        }

                        drain_pending_frontier_content(&mut content_items, &mut pending_frontier);
                        response_tx = Some(SessionResponseSender::Runtime(next_response_tx));
                        termination_requested = true;
                        yield_timer = None;
                        let _ = runtime_tx.send(RuntimeCommand::Terminate);
                        terminate_paused_runtime(&runtime_control_tx, pending_mode);
                        let _ = runtime_terminate_handle.terminate_execution();
                        if runtime_closed {
                            if let Some(response_tx) = response_tx.take() {
                                let response = RuntimeResponse::Terminated {
                                    cell_id: cell_id.clone(),
                                    content_items: std::mem::take(&mut content_items),
                                };
                                let _ = send_terminal_response(response_tx, response);
                            }
                            break;
                        } else {
                            continue;
                        }
                    }
                }
            }
            () = shutdown_token.cancelled() => {
                cancellation_token.cancel();
                termination_requested = true;
                yield_timer = None;
                response_tx = None;
                let _ = runtime_tx.send(RuntimeCommand::Terminate);
                terminate_paused_runtime(&runtime_control_tx, pending_mode);
                let _ = runtime_terminate_handle.terminate_execution();
                if runtime_closed {
                    break;
                }
            }
            _ = async {
                if let Some(yield_timer) = yield_timer.as_mut() {
                    yield_timer.await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => {
                yield_timer = None;
                if send_execute_to_pending_watchdog_response(
                    &cell_id,
                    &mut content_items,
                    &mut response_tx,
                ) {
                    cancellation_token.cancel();
                    let _ = runtime_tx.send(RuntimeCommand::Terminate);
                    terminate_paused_runtime(&runtime_control_tx, pending_mode);
                    let _ = runtime_terminate_handle.terminate_execution();
                    break;
                }
                send_yield_response(&cell_id, &mut content_items, &mut response_tx);
            }
        }
    }

    let _ = runtime_tx.send(RuntimeCommand::Terminate);
    cancellation_token.cancel();
    terminate_paused_runtime(&runtime_control_tx, pending_mode);
    inner.sessions.lock().await.remove(&cell_id);
    inner.nested_call_counts.lock().await.remove(&cell_id);
}

async fn increment_nested_call_count(
    inner: &Inner,
    cell_id: &str,
    invocation_path: &str,
) -> Result<(), String> {
    let mut counts = inner.nested_call_counts.lock().await;
    let count = counts.entry(cell_id.to_string()).or_default();
    *count = count.saturating_add(1);
    if *count > MAX_NESTED_CALLS_PER_CELL {
        return Err(nested_tool_budget_exceeded_error(
            cell_id,
            invocation_path,
            MAX_NESTED_CALLS_PER_CELL,
            *count,
        ));
    }
    Ok(())
}

async fn merge_stored_value_updates(
    inner: &Inner,
    runtime_stored_values: HashMap<String, JsonValue>,
    stored_value_updates: &HashMap<String, JsonValue>,
) -> HashMap<String, JsonValue> {
    let mut stored_values = inner.stored_values.lock().await;
    for (key, value) in stored_value_updates {
        stored_values.insert(key.clone(), value.clone());
    }
    runtime_stored_values
}

fn drain_pending_frontier_content(
    content_items: &mut Vec<FunctionCallOutputContentItem>,
    pending_frontier: &mut Option<PendingFrontier>,
) {
    if let Some(frontier) = pending_frontier.take() {
        content_items.extend(frontier.content_items);
    }
}

fn resume_paused_runtime(
    runtime_control_tx: &std::sync::mpsc::Sender<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
) {
    if pending_mode == PendingRuntimeMode::PauseUntilResumed {
        let _ = runtime_control_tx.send(RuntimeControlCommand::Resume);
    }
}

fn terminate_paused_runtime(
    runtime_control_tx: &std::sync::mpsc::Sender<RuntimeControlCommand>,
    pending_mode: PendingRuntimeMode,
) {
    if pending_mode == PendingRuntimeMode::PauseUntilResumed {
        let _ = runtime_control_tx.send(RuntimeControlCommand::Terminate);
    }
}

#[cfg(test)]
#[expect(
    clippy::needless_raw_string_hashes,
    reason = "code-mode service tests preserve copied V8 source fixtures verbatim"
)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;
    use std::time::Duration;

    use crate::ToolName;
    use async_trait::async_trait;
    use pretty_assertions::assert_eq;
    use serde_json::Value as JsonValue;
    use serde_json::json;
    use tokio::sync::Mutex;
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;
    use tokio_util::sync::CancellationToken;

    use super::CodeModeService;
    use super::CodeModeToolError;
    use super::CodeModeTurnHost;
    use super::Inner;
    use super::PendingRuntimeMode;
    use super::RuntimeCommand;
    use super::RuntimeResponse;
    use super::SessionControlCommand;
    use super::SessionControlContext;
    use super::SessionResponseSender;
    use super::TurnMessage;
    use super::WaitOutcome;
    use super::WaitRequest;
    use super::WaitToPendingOutcome;
    use super::WaitToPendingRequest;
    use super::run_session_control;
    use crate::CodeModeToolKind;
    use crate::FunctionCallOutputContentItem;
    use crate::ToolDefinition;
    use crate::runtime::CodeModeNestedToolCall;
    use crate::runtime::ExecuteRequest;
    use crate::runtime::ExecuteToPendingOutcome;
    use crate::runtime::MAX_NESTED_CALLS_PER_CELL;
    use crate::runtime::MAX_NESTED_TOOL_INPUT_BYTES;
    use crate::runtime::MAX_OUTPUT_CONTENT_ITEMS_PER_CELL;
    use crate::runtime::MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM;
    use crate::runtime::MAX_OUTPUT_RESULT_SPILL_BYTES_PER_CELL;
    use crate::runtime::MAX_OUTPUT_TEXT_BYTES_PER_ITEM;
    use crate::runtime::MAX_OUTPUT_TEXT_SPILL_BYTES_PER_CELL;
    use crate::runtime::MAX_STORED_VALUES_PER_CELL;
    use crate::runtime::RuntimeEvent;
    use crate::runtime::spawn_runtime;

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

    fn test_inner() -> Arc<Inner> {
        let (turn_message_tx, turn_message_rx) = async_channel::unbounded();
        Arc::new(Inner {
            stored_values: Mutex::new(HashMap::new()),
            sessions: Mutex::new(HashMap::new()),
            waiting_cells: std::sync::Mutex::new(std::collections::HashSet::new()),
            nested_call_counts: Mutex::new(HashMap::new()),
            turn_message_tx,
            turn_message_rx,
            shutdown_token: CancellationToken::new(),
            next_cell_id: AtomicU64::new(1),
        })
    }

    struct CancellationRecordingHost {
        token_tx: Mutex<Option<oneshot::Sender<CancellationToken>>>,
        cancelled_tx: Mutex<Option<oneshot::Sender<()>>>,
    }

    #[async_trait]
    impl CodeModeTurnHost for CancellationRecordingHost {
        async fn invoke_tool(
            &self,
            _invocation: CodeModeNestedToolCall,
            cancellation_token: CancellationToken,
        ) -> Result<JsonValue, CodeModeToolError> {
            if let Some(token_tx) = self.token_tx.lock().await.take() {
                let _ = token_tx.send(cancellation_token.clone());
            }
            cancellation_token.cancelled().await;
            if let Some(cancelled_tx) = self.cancelled_tx.lock().await.take() {
                let _ = cancelled_tx.send(());
            }
            Err(CodeModeToolError::fatal("cancelled"))
        }
    }

    struct EchoHost;

    #[async_trait]
    impl CodeModeTurnHost for EchoHost {
        async fn invoke_tool(
            &self,
            invocation: CodeModeNestedToolCall,
            _cancellation_token: CancellationToken,
        ) -> Result<JsonValue, CodeModeToolError> {
            Ok(invocation.input.unwrap_or(JsonValue::Null))
        }
    }

    struct FailingHost;

    #[async_trait]
    impl CodeModeTurnHost for FailingHost {
        async fn invoke_tool(
            &self,
            _invocation: CodeModeNestedToolCall,
            _cancellation_token: CancellationToken,
        ) -> Result<JsonValue, CodeModeToolError> {
            Err(CodeModeToolError::fatal("host transport failed"))
        }
    }

    struct RecoverableFailingHost;

    #[async_trait]
    impl CodeModeTurnHost for RecoverableFailingHost {
        async fn invoke_tool(
            &self,
            _invocation: CodeModeNestedToolCall,
            _cancellation_token: CancellationToken,
        ) -> Result<JsonValue, CodeModeToolError> {
            Err(CodeModeToolError::recoverable(
                "provider failed; details: {\"provider_error\":{\"detail\":{\"http_status\":400}}}",
            ))
        }
    }

    struct DiscoveryInvokeHost;

    #[async_trait]
    impl CodeModeTurnHost for DiscoveryInvokeHost {
        async fn invoke_tool(
            &self,
            invocation: CodeModeNestedToolCall,
            _cancellation_token: CancellationToken,
        ) -> Result<JsonValue, CodeModeToolError> {
            match invocation.tool_name.name.as_str() {
                "coral.search" => Ok(json!({
                    "items": [{
                        "full_path": "tools.github.rest.search.issuesAndPullRequests",
                        "title": "Search issues and pull requests",
                        "score": 100,
                    }],
                    "total": 1,
                    "has_more": false,
                    "next_offset": null,
                })),
                "coral.describe" => Ok(json!({
                    "status": "found",
                    "entry": {
                        "capability_id": "github.rest.searchIssuesAndPullRequests",
                        "bindings": [{
                            "binding_type": "typescript",
                            "path": ["github", "rest", "search", "issuesAndPullRequests"],
                        }],
                    },
                })),
                _ => Ok(json!({
                    "called": invocation.tool_name.name,
                    "input": invocation.input.unwrap_or(JsonValue::Null),
                })),
            }
        }
    }

    fn function_tool(name: &str) -> ToolDefinition {
        ToolDefinition {
            name: name.to_string(),
            tool_name: ToolName::plain(name),
            description: String::new(),
            kind: CodeModeToolKind::Function,
            input_schema: None,
            output_schema: None,
        }
    }

    #[tokio::test]
    async fn generated_tool_paths_do_not_collide_with_object_prototype_properties() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(DiscoveryInvokeHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("tools.constructor.rest.ping")],
                source: r#"
const result = await tools.constructor.rest.ping({ ok: true });
return {
  called: result.called,
  input: result.input,
  toolsPrototypeIsNull: Object.getPrototypeOf(tools) === null,
  constructorPrototypeIsNull: Object.getPrototypeOf(tools.constructor) === null,
};
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "called": "tools.constructor.rest.ping",
                    "input": {
                        "ok": true,
                    },
                    "toolsPrototypeIsNull": true,
                    "constructorPrototypeIsNull": true,
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn search_items_do_not_get_invoke_handles() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(DiscoveryInvokeHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![
                    function_tool("coral.search"),
                    function_tool("tools.github.rest.search.issuesAndPullRequests"),
                ],
                source: r#"
const queries = [{ q: "repo:withcoral/coral is:pr is:open review:required draft:false" }];
const hits = await coral.search({ query: "github prs to review" });
const hit = hits.items[0];
const direct = await tools.github.rest.search.issuesAndPullRequests({
  query: { q: queries[0].q },
});
return {
  hasInvoke: typeof hit.invoke === "function",
  ownsInvoke: Object.prototype.hasOwnProperty.call(hit, "invoke"),
  enumerable: Object.keys(hit).includes("invoke"),
  called: direct.called,
  input: direct.input,
};
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "hasInvoke": false,
                    "ownsInvoke": false,
                    "enumerable": false,
                    "called": "tools.github.rest.search.issuesAndPullRequests",
                    "input": {
                        "query": {
                            "q": "repo:withcoral/coral is:pr is:open review:required draft:false",
                        },
                    },
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn describe_entries_do_not_get_invoke_handles_from_typescript_binding_paths() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(DiscoveryInvokeHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![
                    function_tool("coral.describe"),
                    function_tool("tools.github.rest.search.issuesAndPullRequests"),
                ],
                source: r#"
const described = await coral.describe({ reference: "tools.github.rest.search.issuesAndPullRequests" });
const direct = await tools.github.rest.search.issuesAndPullRequests({ query: { q: "repo:withcoral/coral is:pr" } });
return {
  hasInvoke: typeof described.entry.invoke === "function",
  ownsInvoke: Object.prototype.hasOwnProperty.call(described.entry, "invoke"),
  called: direct.called,
  input: direct.input,
};
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "hasInvoke": false,
                    "ownsInvoke": false,
                    "called": "tools.github.rest.search.issuesAndPullRequests",
                    "input": {
                        "query": {
                            "q": "repo:withcoral/coral is:pr",
                        },
                    },
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn bare_is_error_tool_output_is_returned_as_data() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
const result = await tools.echo({ isError: true, content: [{ type: "text", text: "domain data" }] });
return result;
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "isError": true,
                    "content": [{ "type": "text", "text": "domain data" }],
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn bare_coral_error_key_tool_output_is_returned_as_data() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
const result = await tools.echo({
  "$coral_error": true,
  error: { kind: "provider_error", message: "provider failed", details: null },
});
return result.error.kind;
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("provider_error")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn recoverable_tool_error_rejects_by_default() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(RecoverableFailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
await tools.fail({});
return "unexpected";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected nested tool error failure");
        };
        assert!(error_text.contains("provider failed"), "{error_text}");
    }

    #[tokio::test]
    async fn recoverable_tool_error_can_be_caught() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(RecoverableFailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
try {
  await tools.fail({});
  return "unexpected";
} catch (error) {
  return {
    caught: true,
    text: String(error),
  };
}
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: Some(result),
            error_text: None,
            ..
        } = response
        else {
            panic!("expected caught nested error to complete");
        };
        assert_eq!(
            result.get("caught").and_then(JsonValue::as_bool),
            Some(true)
        );
        let text = result
            .get("text")
            .and_then(JsonValue::as_str)
            .expect("error text");
        assert!(text.contains("provider failed"), "{text}");
        assert!(text.contains("http_status"), "{text}");
    }

    #[tokio::test]
    async fn unawaited_recoverable_tool_error_fails_closed() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(RecoverableFailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
tools.fail({});
return "unexpected";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected unawaited nested error to fail closed");
        };
        assert!(error_text.contains("provider failed"), "{error_text}");
        assert!(error_text.contains("http_status"), "{error_text}");
    }

    #[tokio::test]
    async fn unawaited_recoverable_tool_error_fails_while_main_promise_is_pending() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(RecoverableFailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
tools.fail({});
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected unawaited nested error to fail while main promise is pending");
        };
        assert!(error_text.contains("provider failed"), "{error_text}");
    }

    #[tokio::test]
    async fn propagated_recoverable_tool_error_fails_closed() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(RecoverableFailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
tools.fail({}).then(() => "ignored");
return "unexpected";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected propagated nested error to fail closed");
        };
        assert!(error_text.contains("provider failed"), "{error_text}");
    }

    #[tokio::test]
    async fn nested_error_result_can_be_returned_with_explicit_option() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
const result = await tools.echo({
  ok: false,
  value: null,
  partial: false,
  error: { kind: "provider_error", message: "provider failed", details: { id: 1 } },
}, { allowErrorResult: true });
return result;
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "ok": false,
                    "value": null,
                    "partial": false,
                    "error": {
                        "kind": "provider_error",
                        "message": "provider failed",
                        "details": { "id": 1 },
                    },
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn unmarked_error_shaped_tool_output_is_returned_as_data() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        // Provider data that happens to carry ok/error/value/envelope keys is
        // never shape-sniffed into a failure.
        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
const result = await tools.echo({
  ok: false,
  value: null,
  error: { kind: "provider_error", message: "domain data, not a coral error" },
  envelope: null,
});
return result.error.message;
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("domain data, not a coral error")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn envelope_option_is_plumbed_to_the_host() {
        struct OptionRecordingHost;

        #[async_trait]
        impl CodeModeTurnHost for OptionRecordingHost {
            async fn invoke_tool(
                &self,
                invocation: CodeModeNestedToolCall,
                _cancellation_token: CancellationToken,
            ) -> Result<JsonValue, CodeModeToolError> {
                Ok(json!({
                    "allow_error_result": invocation.allow_error_result,
                    "envelope": invocation.envelope,
                }))
            }
        }

        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(OptionRecordingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
const with_envelope = await tools.echo({}, { envelope: true });
const with_both = await tools.echo({}, { allowErrorResult: true, envelope: true });
const plain = await tools.echo({});
return { with_envelope, with_both, plain };
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "with_envelope": { "allow_error_result": false, "envelope": true },
                    "with_both": { "allow_error_result": true, "envelope": true },
                    "plain": { "allow_error_result": false, "envelope": false },
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn unsupported_tool_option_is_rejected() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
await tools.echo({}, { rawEnvelope: true });
return "unexpected";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected unsupported tool option failure");
        };
        assert!(
            error_text.contains("unsupported tool option `rawEnvelope`"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn explicit_error_result_option_does_not_downgrade_host_errors() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(FailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
try {
  await tools.fail({}, { allowErrorResult: true });
  return "unexpected";
} catch (error) {
  return "caught";
}
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected caught host error to remain fatal");
        };
        assert!(error_text.contains("host transport failed"), "{error_text}");
    }

    #[tokio::test]
    async fn exit_does_not_downgrade_caught_host_errors() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(FailingHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("fail")],
                source: r#"
try {
  await tools.fail({}, { allowErrorResult: true });
  return "unexpected";
} catch (_error) {
  exit();
}
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected exit after host error to remain fatal");
        };
        assert!(error_text.contains("host transport failed"), "{error_text}");
    }

    #[tokio::test]
    async fn plain_ok_false_tool_output_is_returned_as_data() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![function_tool("echo")],
                source: r#"
const result = await tools.echo({ ok: false, reason: "normal domain value" });
return result;
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "ok": false,
                    "reason": "normal domain value",
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn synchronous_exit_returns_successfully() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"globalThis.__coral_code_mode_result = "before"; exit(); globalThis.__coral_code_mode_result = "after";"#.to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn user_rejected_exit_sentinel_fails_closed() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
Promise.reject("__codex_code_mode_exit__");
return "unexpected";
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected user-created sentinel rejection to fail closed");
        };
        assert!(
            error_text.contains("__codex_code_mode_exit__"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn caught_exit_still_returns_successfully() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
try {
  exit();
} catch (_error) {
}
return "after";
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn delayed_exit_returns_successfully() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
setTimeout(() => exit(), 60_000);
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let response = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 60_000,
                terminate: false,
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            WaitOutcome::LiveCell(RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            })
        );
    }

    #[tokio::test]
    async fn delayed_callback_errors_respect_max_output_tokens() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
setTimeout(() => {
  throw "x".repeat(1000);
}, 60_000);
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(1),
                max_output_tokens: Some(2),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let response = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 60_000,
                terminate: false,
            })
            .await
            .unwrap();

        let WaitOutcome::LiveCell(RuntimeResponse::Result {
            error_text: Some(error_text),
            ..
        }) = response
        else {
            panic!("expected delayed callback error");
        };
        assert!(
            error_text.len() <= 2 * 4,
            "expected tiny output budget to apply, got {} bytes",
            error_text.len()
        );
    }

    #[tokio::test]
    async fn execute_to_pending_returns_completed_for_synchronous_results() {
        let service = CodeModeService::new();

        let response = service
            .execute_to_pending(ExecuteRequest {
                source: r#"globalThis.__coral_code_mode_result = "done";"#.to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Completed(RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("done")),
                error_text: None,
            })
        );
    }

    #[tokio::test]
    async fn execute_wraps_source_as_async_function_body() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: "return 7;".to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!(7)),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn execute_treats_leading_function_declaration_as_body_source() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
async function f() {
  return 7;
}
return await f();
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!(7)),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn execute_invokes_common_function_expression_sources() {
        for (source, expected) in [
            ("() => 1", 1),
            ("x => 2", 2),
            ("async x => 3", 3),
            ("function main() { return 4; }", 4),
            ("async function main() { return 5; }", 5),
        ] {
            let service = CodeModeService::new();

            let response = service
                .execute(ExecuteRequest {
                    source: source.to_string(),
                    yield_time_ms: None,
                    ..execute_request("")
                })
                .await
                .unwrap();

            assert_eq!(
                response,
                RuntimeResponse::Result {
                    cell_id: "1".to_string(),
                    content_items: Vec::new(),
                    stored_values: HashMap::new(),
                    stored_value_updates: HashMap::new(),
                    result: Some(json!(expected)),
                    error_text: None,
                },
                "source: {source}"
            );
        }
    }

    #[tokio::test]
    async fn execute_invokes_function_expression_with_string_delimiters() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"async function main() { return "}"; }"#.to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("}")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn execute_treats_parenthesized_iife_as_raw_javascript() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"(async () => { globalThis.__coral_code_mode_result = 9; })();"#
                    .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!(9)),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn result_slot_returns_structured_result_without_touching_outputs_or_store() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
store("kept", { nested: true });
globalThis.__coral_code_mode_result = {
  ok: true,
  items: [1, "two", null],
  nested: { value: 3 },
};
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let mut stored_values = HashMap::new();
        stored_values.insert("kept".to_string(), json!({ "nested": true }));
        let stored_value_updates = stored_values.clone();
        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values,
                stored_value_updates,
                result: Some(json!({
                    "ok": true,
                    "items": [1, "two", null],
                    "nested": { "value": 3 },
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn result_slot_missing_or_undefined_returns_no_structured_result() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"globalThis.__coral_code_mode_result = undefined;"#.to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn dropping_service_terminates_live_cells() {
        let service = CodeModeService::new();
        let inner = Arc::downgrade(&service.inner);

        let response = service
            .execute(ExecuteRequest {
                source: "await new Promise(() => {});".to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert!(matches!(response, RuntimeResponse::Yielded { .. }));
        drop(service);

        tokio::time::timeout(Duration::from_secs(1), async {
            while inner.upgrade().is_some() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("dropping CodeModeService should release live runtime sessions");
    }

    #[tokio::test]
    async fn max_output_tokens_truncates_oversized_result() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"globalThis.__coral_code_mode_result = "x".repeat(2000);"#.to_string(),
                yield_time_ms: None,
                max_output_tokens: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result, error_text, ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(error_text, None);
        let result = result.expect("truncated result");
        assert_eq!(
            result.get("type").and_then(JsonValue::as_str),
            Some("code_mode_truncated_result")
        );
        assert_eq!(
            result.get("truncated").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            result.get("complete").and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            result
                .pointer("/truncation/reason")
                .and_then(JsonValue::as_str),
            Some("max_output_tokens")
        );
        assert_eq!(
            result
                .pointer("/truncation/max_output_tokens")
                .and_then(JsonValue::as_u64),
            Some(1)
        );
        assert_eq!(
            result
                .pointer("/truncation/max_spill_bytes")
                .and_then(JsonValue::as_u64),
            Some(MAX_OUTPUT_RESULT_SPILL_BYTES_PER_CELL as u64)
        );
        let preview = result
            .get("preview")
            .and_then(JsonValue::as_str)
            .expect("preview");
        assert!(preview.len() <= 4, "{preview}");
        assert_eq!(
            result
                .pointer("/output_shaping/configured_bytes")
                .and_then(JsonValue::as_u64),
            Some(4)
        );
        let full_output_path = result
            .pointer("/truncation/full_output_path")
            .and_then(JsonValue::as_str)
            .expect("full output path");
        let full_output = std::fs::read_to_string(full_output_path).expect("spilled output");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&full_output).expect("spilled json"),
            json!("x".repeat(2000))
        );
        assert_eq!(
            result
                .pointer("/output_shaping/channel")
                .and_then(JsonValue::as_str),
            Some("result")
        );
        assert_eq!(
            result
                .pointer("/output_shaping/limit_name")
                .and_then(JsonValue::as_str),
            Some("max_output_tokens")
        );
        assert_eq!(
            result
                .pointer("/output_shaping/spilled")
                .and_then(JsonValue::as_bool),
            Some(true)
        );
    }

    #[tokio::test]
    async fn oversized_result_above_spill_cap_returns_preview_without_spill_path() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: format!(
                    r#"globalThis.__coral_code_mode_result = "x".repeat({});"#,
                    MAX_OUTPUT_RESULT_SPILL_BYTES_PER_CELL + 1
                ),
                yield_time_ms: None,
                max_output_tokens: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result, error_text, ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(error_text, None);
        let result = result.expect("truncated result");
        assert_eq!(
            result.get("type").and_then(JsonValue::as_str),
            Some("code_mode_truncated_result")
        );
        assert_eq!(
            result
                .pointer("/output_shaping/spilled")
                .and_then(JsonValue::as_bool),
            Some(false)
        );
        assert_eq!(
            result
                .pointer("/truncation/max_spill_bytes")
                .and_then(JsonValue::as_u64),
            Some(MAX_OUTPUT_RESULT_SPILL_BYTES_PER_CELL as u64)
        );
        assert_eq!(
            result.pointer("/truncation/full_output_path"),
            Some(&JsonValue::Null)
        );
        assert_eq!(
            result
                .pointer("/output_shaping/full_output_path")
                .and_then(JsonValue::as_str),
            None
        );
        let preview = result
            .get("preview")
            .and_then(JsonValue::as_str)
            .expect("preview");
        assert!(preview.len() <= 4, "{preview}");
    }

    #[tokio::test]
    async fn console_log_is_captured_as_text_output() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"console.log("debug", { ok: true }); return "done";"#.to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::Text {
                    text: r#"debug {"ok":true}"#.to_string(),
                }],
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("done")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn console_output_over_item_cap_is_shaped_not_failed() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
for (let i = 0; i < 100; i += 1) {
  console.log("line", i);
}
return "done";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            result,
            error_text,
            ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(error_text, None);
        assert_eq!(result, Some(json!("done")));
        assert_eq!(content_items.len(), MAX_OUTPUT_CONTENT_ITEMS_PER_CELL + 1);
        assert_eq!(
            content_items.first(),
            Some(&FunctionCallOutputContentItem::Text {
                text: "line 0".to_string(),
            })
        );

        let Some(FunctionCallOutputContentItem::OutputShaping { metadata }) = content_items.last()
        else {
            panic!("expected output shaping metadata");
        };
        assert_eq!(metadata.channel, "console");
        assert_eq!(metadata.limit_name, "max_output_content_items");
        assert!(metadata.truncated);
        assert!(metadata.spilled);
        assert_eq!(
            metadata.dropped_items,
            100 - MAX_OUTPUT_CONTENT_ITEMS_PER_CELL
        );
        assert_eq!(metadata.observed_items, 100);
        assert_eq!(
            metadata.configured_items,
            Some(MAX_OUTPUT_CONTENT_ITEMS_PER_CELL)
        );
        let full_output_path = metadata
            .full_output_path
            .as_ref()
            .expect("full console output path");
        let full_output =
            std::fs::read_to_string(full_output_path).expect("spilled console output");
        assert!(full_output.contains("line 0"));
        assert!(full_output.contains("line 99"));
    }

    #[tokio::test]
    async fn console_output_spill_is_capped() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
for (let i = 0; i < 40; i += 1) {
  console.log("x".repeat(64 * 1024));
}
return "done";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            result,
            error_text,
            ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(error_text, None);
        assert_eq!(result, Some(json!("done")));

        let Some(FunctionCallOutputContentItem::OutputShaping { metadata }) = content_items.last()
        else {
            panic!("expected output shaping metadata");
        };
        assert_eq!(metadata.channel, "console");
        assert_eq!(metadata.limit_name, "max_output_text_spill_bytes");
        assert_eq!(
            metadata.configured_bytes,
            Some(MAX_OUTPUT_TEXT_SPILL_BYTES_PER_CELL)
        );
        assert!(metadata.spilled);
        assert!(metadata.dropped_bytes > 0);
        assert_eq!(metadata.observed_items, 40);
        let full_output_path = metadata
            .full_output_path
            .as_ref()
            .expect("full console output path");
        let full_output_metadata =
            std::fs::metadata(full_output_path).expect("spilled console output metadata");
        assert!(full_output_metadata.len() <= MAX_OUTPUT_TEXT_SPILL_BYTES_PER_CELL as u64);
    }

    #[tokio::test]
    async fn oversized_console_log_uses_output_shaping_metadata() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
console.log("x".repeat(20000));
return "done";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            result,
            error_text,
            ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(error_text, None);
        assert_eq!(result, Some(json!("done")));
        assert_eq!(content_items.len(), 2);
        let Some(FunctionCallOutputContentItem::Text { text }) = content_items.first() else {
            panic!("expected truncated text item");
        };
        assert!(text.ends_with(" ..."));
        assert!(text.len() <= MAX_OUTPUT_TEXT_BYTES_PER_ITEM);

        let Some(FunctionCallOutputContentItem::OutputShaping { metadata }) = content_items.get(1)
        else {
            panic!("expected output shaping metadata");
        };
        assert_eq!(metadata.channel, "console");
        assert_eq!(metadata.limit_name, "max_output_text_bytes_per_item");
        assert_eq!(
            metadata.configured_bytes,
            Some(MAX_OUTPUT_TEXT_BYTES_PER_ITEM)
        );
        assert_eq!(metadata.dropped_items, 0);
        assert_eq!(metadata.observed_items, 1);
        assert!(metadata.dropped_bytes > 0);
        assert!(metadata.spilled);
    }

    #[tokio::test]
    async fn max_output_tokens_truncates_error_text() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"throw "x".repeat(100);"#.to_string(),
                yield_time_ms: None,
                max_output_tokens: Some(4),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result, error_text, ..
        } = response
        else {
            panic!("expected runtime result");
        };
        let error_text = error_text.expect("truncated error");
        assert_eq!(result, None);
        assert!(error_text.len() <= 4 * 4);
        assert!(error_text.ends_with("[truncated]"), "{error_text}");
    }

    #[tokio::test]
    async fn max_output_tokens_truncates_dense_arrays_after_materializing() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"return Array.from({ length: 1000 }, () => "x");"#.to_string(),
                yield_time_ms: None,
                max_output_tokens: Some(100),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result, error_text, ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(error_text, None);
        let result = result.expect("truncated result");
        assert_eq!(
            result.get("type").and_then(JsonValue::as_str),
            Some("code_mode_truncated_result")
        );
        assert_eq!(
            result.get("truncated").and_then(JsonValue::as_bool),
            Some(true)
        );
        assert_eq!(
            result
                .pointer("/truncation/reason")
                .and_then(JsonValue::as_str),
            Some("max_output_tokens")
        );
    }

    #[tokio::test]
    async fn max_output_tokens_clamp_rejects_sparse_arrays_with_huge_budget() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"return new Array(200000);"#.to_string(),
                yield_time_ms: None,
                max_output_tokens: Some(usize::MAX),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result, error_text, ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(result, None);
        assert!(
            error_text
                .expect("container cap error")
                .contains("container entry limit")
        );
    }

    #[tokio::test]
    async fn store_persists_values_across_execute_calls() {
        let service = CodeModeService::new();

        let first = service
            .execute(ExecuteRequest {
                source: r#"
store("kept", { count: 1 });
globalThis.__coral_code_mode_result = load("kept");
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            first,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::from([("kept".to_string(), json!({ "count": 1 }))]),
                stored_value_updates: HashMap::from([("kept".to_string(), json!({ "count": 1 }),)]),
                result: Some(json!({ "count": 1 })),
                error_text: None,
            }
        );

        let stored_values = service.stored_values().await;
        assert_eq!(
            stored_values,
            HashMap::from([("kept".to_string(), json!({ "count": 1 }))])
        );

        let second = service
            .execute(ExecuteRequest {
                cell_id: "2".to_string(),
                source: r#"globalThis.__coral_code_mode_result = load("kept");"#.to_string(),
                stored_values,
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            second,
            RuntimeResponse::Result {
                cell_id: "2".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::from([("kept".to_string(), json!({ "count": 1 }))]),
                stored_value_updates: HashMap::new(),
                result: Some(json!({ "count": 1 })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn completed_response_does_not_include_service_singleton_store() {
        let service = CodeModeService::new();
        service
            .replace_stored_values(HashMap::from([("other".to_string(), json!("workspace-a"))]))
            .await;

        let response = service
            .execute(ExecuteRequest {
                source: r#"store("mine", "workspace-b");"#.to_string(),
                stored_values: HashMap::new(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::from([("mine".to_string(), json!("workspace-b"))]),
                stored_value_updates: HashMap::from([("mine".to_string(), json!("workspace-b"),)]),
                result: None,
                error_text: None,
            }
        );
        assert_eq!(
            service.stored_values().await,
            HashMap::from([
                ("mine".to_string(), json!("workspace-b")),
                ("other".to_string(), json!("workspace-a")),
            ])
        );
    }

    #[tokio::test]
    async fn concurrent_cells_merge_store_updates_without_wiping_other_keys() {
        let service = CodeModeService::new();

        let first = service
            .execute(ExecuteRequest {
                source: r#"
store("first", { value: 1 });
await new Promise((resolve) => setTimeout(resolve, 60_000));
"#
                .to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            first,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        let second = service
            .execute(ExecuteRequest {
                cell_id: "2".to_string(),
                source: r#"store("second", { value: 2 });"#.to_string(),
                stored_values: service.stored_values().await,
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            second,
            RuntimeResponse::Result {
                cell_id: "2".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::from([("second".to_string(), json!({ "value": 2 }))]),
                stored_value_updates: HashMap::from([(
                    "second".to_string(),
                    json!({ "value": 2 }),
                )]),
                result: None,
                error_text: None,
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let first_completion = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 60_000,
                terminate: false,
            })
            .await
            .unwrap();

        let expected_store = HashMap::from([
            ("first".to_string(), json!({ "value": 1 })),
            ("second".to_string(), json!({ "value": 2 })),
        ]);
        assert_eq!(
            first_completion,
            WaitOutcome::LiveCell(RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::from([("first".to_string(), json!({ "value": 1 }))]),
                stored_value_updates: HashMap::from([
                    ("first".to_string(), json!({ "value": 1 }),)
                ]),
                result: None,
                error_text: None,
            })
        );
        assert_eq!(service.stored_values().await, expected_store);
    }

    #[tokio::test]
    async fn finishing_cell_does_not_resurrect_cleared_store_snapshot() {
        let service = CodeModeService::new();
        service
            .replace_stored_values(HashMap::from([("secret".to_string(), json!("redacted"))]))
            .await;

        let initial = service
            .execute(ExecuteRequest {
                source: r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
"#
                .to_string(),
                stored_values: service.stored_values().await,
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        service.replace_stored_values(HashMap::new()).await;
        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let completion = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 60_000,
                terminate: false,
            })
            .await
            .unwrap();

        assert_eq!(
            completion,
            WaitOutcome::LiveCell(RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::from([("secret".to_string(), json!("redacted"))]),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            })
        );
        assert_eq!(service.stored_values().await, HashMap::new());
    }

    #[tokio::test]
    async fn store_rejects_oversized_values() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"store("too_big", "x".repeat(300000));"#.to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected store size error");
        };
        assert!(
            error_text.contains("stored value exceeded the per-value size limit"),
            "{error_text}"
        );
        assert_eq!(service.stored_values().await, HashMap::new());
    }

    #[tokio::test]
    async fn store_rejects_too_many_values() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: format!(
                    r#"
for (let i = 0; i < {}; i += 1) {{
  store(`k${{i}}`, i);
}}
"#,
                    MAX_STORED_VALUES_PER_CELL + 1
                ),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected store count error");
        };
        assert!(
            error_text.contains("store exceeded the value count limit"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn result_slot_omits_undefined_object_properties_and_nulls_array_items() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
globalThis.__coral_code_mode_result = {
  keep: true,
  omit: undefined,
  nested: {
    keep: "yes",
    omit: undefined,
  },
  array: [1, undefined, { keep: true, omit: undefined }],
};
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "keep": true,
                    "nested": {
                        "keep": "yes",
                    },
                    "array": [1, null, { "keep": true }],
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn result_slot_rejects_non_json_values() {
        let cases = [
            (
                "globalThis.__coral_code_mode_result = Number.NaN;",
                "non-finite number at result",
            ),
            (
                "globalThis.__coral_code_mode_result = { bad: () => {} };",
                "function is not supported at result.bad",
            ),
            (
                "globalThis.__coral_code_mode_result = { bad: Symbol('x') };",
                "symbol is not supported at result.bad",
            ),
            (
                "const value = {}; value.self = value; globalThis.__coral_code_mode_result = value;",
                "cyclic value at result.self",
            ),
        ];

        for (source, expected_error) in cases {
            let service = CodeModeService::new();
            let response = service
                .execute(ExecuteRequest {
                    source: source.to_string(),
                    yield_time_ms: None,
                    ..execute_request("")
                })
                .await
                .unwrap();

            let RuntimeResponse::Result {
                result, error_text, ..
            } = response
            else {
                panic!("expected runtime result");
            };
            assert_eq!(result, None);
            assert_eq!(
                error_text,
                Some(format!(
                    "code mode result must be JSON-serializable; {expected_error}"
                ))
            );
        }
    }

    #[tokio::test]
    async fn result_slot_rejects_accessors_without_running_getters() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
return {
  get x() {
    store("k", 1);
    return 1;
  },
};
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result, error_text, ..
        } = response
        else {
            panic!("expected runtime result");
        };
        assert_eq!(result, None);
        assert!(
            error_text
                .expect("accessor error")
                .contains("accessor properties are not supported")
        );
        assert_eq!(service.stored_values().await, HashMap::new());
    }

    #[tokio::test]
    async fn execute_to_pending_returns_once_the_runtime_is_quiescent() {
        let service = CodeModeService::new();

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            service.execute_to_pending(ExecuteRequest {
                source: r#"await new Promise(() => {});"#.to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: Vec::new(),
            }
        );

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn execute_to_pending_watchdog_fails_cpu_bound_startup() {
        let service = CodeModeService::new();

        let response = tokio::time::timeout(
            Duration::from_secs(1),
            service.execute_to_pending(ExecuteRequest {
                source: "while (true) {}".to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let ExecuteToPendingOutcome::Completed(RuntimeResponse::Result {
            error_text: Some(error_text),
            ..
        }) = response
        else {
            panic!("expected startup watchdog failure");
        };
        assert!(
            error_text.contains("startup watchdog fired"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn execute_to_pending_identifies_tool_calls_in_paused_frontier() {
        let service = CodeModeService::new();

        let response = service
            .execute_to_pending(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
await Promise.all([
  tools.echo({ value: "first" }),
  tools.echo({ value: "second" }),
]);
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-1".to_string(), "tool-2".to_string()],
            }
        );

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn terminate_cancels_in_flight_tool_calls() {
        let service = CodeModeService::new();
        let (token_tx, token_rx) = oneshot::channel();
        let (cancelled_tx, cancelled_rx) = oneshot::channel();
        let _worker = service.start_turn_worker(Arc::new(CancellationRecordingHost {
            token_tx: Mutex::new(Some(token_tx)),
            cancelled_tx: Mutex::new(Some(cancelled_tx)),
        }));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "slow".to_string(),
                    tool_name: ToolName::plain("slow"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: "await tools.slow({});".to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );
        let cancellation_token = tokio::time::timeout(Duration::from_secs(1), token_rx)
            .await
            .expect("tool should receive cancellation token")
            .expect("token sender");
        assert!(!cancellation_token.is_cancelled());

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
        tokio::time::timeout(Duration::from_secs(1), cancelled_rx)
            .await
            .expect("tool call should observe cancellation")
            .expect("cancelled sender");
        assert!(cancellation_token.is_cancelled());
    }

    #[tokio::test]
    async fn tagged_template_tool_call_encodes_strings_and_values() {
        let service = CodeModeService::new();

        let response = service
            .execute_to_pending(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "query".to_string(),
                    tool_name: ToolName::plain("query"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
tools.query`select * from users where id = ${7} and active = ${true}`;
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-1".to_string()],
            }
        );

        let message = service.inner.turn_message_rx.recv().await.unwrap();
        let TurnMessage::ToolCall(tool_call) = message;
        assert_eq!(
            tool_call.input,
            Some(json!({
                "__coral_code_mode_tagged_template": {
                    "strings": [
                        "select * from users where id = ",
                        " and active = ",
                        "",
                    ],
                    "values": [7, true],
                },
            }))
        );

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn ordinary_tool_calls_do_not_use_tagged_template_encoding() {
        let service = CodeModeService::new();

        let response = service
            .execute_to_pending(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "query".to_string(),
                    tool_name: ToolName::plain("query"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
tools.query({ sql: "select 1", values: [1] });
tools.query({ sql: "select 2" });
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-1".to_string(), "tool-2".to_string()],
            }
        );

        let first = service.inner.turn_message_rx.recv().await.unwrap();
        let TurnMessage::ToolCall(first) = first;
        assert_eq!(
            first.input,
            Some(json!({
                "sql": "select 1",
                "values": [1],
            }))
        );

        let second = service.inner.turn_message_rx.recv().await.unwrap();
        let TurnMessage::ToolCall(second) = second;
        assert_eq!(second.input, Some(json!({ "sql": "select 2" })));

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn coral_sql_query_string_input_is_normalized_in_v8_callback() {
        let service = CodeModeService::new();

        let response = service
            .execute_to_pending(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "coral.sql.query".to_string(),
                    tool_name: ToolName::plain("coral.sql.query"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
coral.sql.query("select 1");
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-1".to_string()],
            }
        );

        let message = service.inner.turn_message_rx.recv().await.unwrap();
        let TurnMessage::ToolCall(tool_call) = message;
        assert_eq!(tool_call.tool_name.name, "coral.sql.query");
        assert_eq!(tool_call.input, Some(json!({ "sql": "select 1" })));

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn explicit_unserializable_tool_input_rejects() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "query".to_string(),
                    tool_name: ToolName::plain("query"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: "await tools.query(() => {});".to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected runtime error");
        };
        assert!(
            error_text.contains("tool input must be JSON-serializable"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn execute_to_pending_surfaces_unawaited_tool_calls_before_completion() {
        let service = CodeModeService::new();

        let response = service
            .execute_to_pending(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
tools.echo({ value: "side effect" });
return "done";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-1".to_string()],
            }
        );

        let message = service.inner.turn_message_rx.recv().await.unwrap();
        let TurnMessage::ToolCall(tool_call) = message;
        assert_eq!(tool_call.input, Some(json!({ "value": "side effect" })));

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn execute_waits_for_unawaited_tool_calls_before_completion() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
tools.echo({ value: "side effect" });
return "done";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("done")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn nested_call_budget_rejects_runaway_tool_loops() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
const result = await (async () => {
  for (let i = 0; i < 105; i += 1) {
    try {
      await tools.echo({ i });
    } catch (error) {
      return { i, error: String(error) };
    }
  }
  return { i: -1, error: "budget did not fire" };
})();
globalThis.__coral_code_mode_result = result;
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected top-level budget failure");
        };
        assert!(error_text.contains("budget exceeded"), "{error_text}");
        assert!(
            error_text.contains("limit_name=max_nested_tool_calls"),
            "{error_text}"
        );
        assert!(
            error_text.contains(&format!("configured={MAX_NESTED_CALLS_PER_CELL}")),
            "{error_text}"
        );
        assert!(error_text.contains("observed=101"), "{error_text}");
        assert!(
            error_text.contains("invocation_path=runtime/echo"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn nested_call_budget_rejects_synchronous_unawaited_tool_floods() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
let captured;
for (let i = 0; i < 105; i += 1) {
  try {
    tools.echo({ i });
  } catch (error) {
    captured = { i, error: String(error) };
    break;
  }
}
globalThis.__coral_code_mode_result = captured ?? { i: -1, error: "budget did not fire" };
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: None,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected top-level budget failure");
        };
        assert!(error_text.contains("budget exceeded"), "{error_text}");
        assert!(
            error_text.contains("limit_name=max_parallel_tool_calls"),
            "{error_text}"
        );
        assert!(
            error_text.contains(&format!("configured={MAX_NESTED_CALLS_PER_CELL}")),
            "{error_text}"
        );
        assert!(error_text.contains("observed=101"), "{error_text}");
        assert!(
            error_text.contains("invocation_path=runtime/echo"),
            "{error_text}"
        );
    }

    #[tokio::test]
    async fn nested_tool_input_budget_rejects_oversized_input() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: format!(
                    r#"
let captured;
try {{
  tools.echo({{ payload: "x".repeat({MAX_NESTED_TOOL_INPUT_BYTES}) }});
}} catch (error) {{
  captured = String(error);
}}
globalThis.__coral_code_mode_result = captured ?? "budget did not fire";
"#
                ),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: Some(result),
            error_text: None,
            ..
        } = response
        else {
            panic!("expected runtime result");
        };
        let result = result.as_str().expect("captured error string");
        assert!(
            result.contains("nested tool input exceeded the per-call size limit"),
            "{result}"
        );
        service.inner.turn_message_rx.try_recv().unwrap_err();
    }

    #[tokio::test]
    async fn nested_tool_input_budget_rejects_total_input() {
        let service = CodeModeService::new();
        let _worker = service.start_turn_worker(Arc::new(EchoHost));

        let response = service
            .execute(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: format!(
                    r#"
const payload = "x".repeat({});
let captured;
for (let i = 0; i < 10; i += 1) {{
  try {{
    tools.echo({{ i, payload }});
  }} catch (error) {{
    captured = String(error);
    break;
  }}
}}
globalThis.__coral_code_mode_result = captured ?? "budget did not fire";
"#,
                    MAX_NESTED_TOOL_INPUT_BYTES / 2
                ),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            result: Some(result),
            error_text: None,
            ..
        } = response
        else {
            panic!("expected runtime result");
        };
        let result = result.as_str().expect("captured error string");
        assert!(
            result.contains("nested tool input exceeded the total size limit"),
            "{result}"
        );
    }

    #[tokio::test]
    async fn normalized_tool_name_collisions_are_rejected() {
        let service = CodeModeService::new();

        let error = service
            .execute(ExecuteRequest {
                enabled_tools: vec![
                    ToolDefinition {
                        name: "foo-bar".to_string(),
                        tool_name: ToolName::plain("foo-bar"),
                        description: String::new(),
                        kind: CodeModeToolKind::Function,
                        input_schema: None,
                        output_schema: None,
                    },
                    ToolDefinition {
                        name: "foo_bar".to_string(),
                        tool_name: ToolName::plain("foo_bar"),
                        description: String::new(),
                        kind: CodeModeToolKind::Function,
                        input_schema: None,
                        output_schema: None,
                    },
                ],
                source: r#"globalThis.__coral_code_mode_result = true;"#.to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap_err();

        assert!(
            error.contains("multiple tools normalize to the same JavaScript tool name `foo_bar`"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn store_and_load_require_string_keys() {
        for (source, expected_error) in [
            ("store({ bad: true }, 1);", "store key must be a string"),
            ("load({ bad: true });", "load key must be a string"),
        ] {
            let service = CodeModeService::new();
            let response = service
                .execute(ExecuteRequest {
                    source: source.to_string(),
                    yield_time_ms: Some(60_000),
                    ..execute_request("")
                })
                .await
                .unwrap();

            let RuntimeResponse::Result {
                error_text: Some(error_text),
                ..
            } = response
            else {
                panic!("expected runtime error");
            };
            assert!(error_text.contains(expected_error));
        }
    }

    #[tokio::test]
    async fn execute_to_pending_excludes_delayed_timeout_tool_calls_until_wait() {
        let service = CodeModeService::new();

        let initial_response = service
            .execute_to_pending(ExecuteRequest {
                enabled_tools: vec![ToolDefinition {
                    name: "echo".to_string(),
                    tool_name: ToolName::plain("echo"),
                    description: String::new(),
                    kind: CodeModeToolKind::Function,
                    input_schema: None,
                    output_schema: None,
                }],
                source: r#"
setTimeout(() => {
  tools.echo({ value: "delayed" });
}, 1000);
await Promise.all([
  tools.echo({ value: "second" }),
  tools.echo({ value: "third" }),
]);
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial_response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-1".to_string(), "tool-2".to_string()],
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let resumed_response = tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_to_pending(WaitToPendingRequest {
                cell_id: "1".to_string(),
                yield_time_ms: None,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            resumed_response,
            WaitToPendingOutcome::LiveCell(ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: vec!["tool-3".to_string()],
            })
        );

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn wait_to_pending_returns_after_resumed_runtime_becomes_quiescent_again() {
        let service = CodeModeService::new();

        let initial_response = service
            .execute_to_pending(ExecuteRequest {
                source: r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
globalThis.__coral_code_mode_result = "after";
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial_response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let resumed_response = tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_to_pending(WaitToPendingRequest {
                cell_id: "1".to_string(),
                yield_time_ms: None,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            resumed_response,
            WaitToPendingOutcome::LiveCell(ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: Vec::new(),
            })
        );

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn wait_to_pending_watchdog_fails_cpu_bound_resume() {
        let service = CodeModeService::new();

        let initial_response = service
            .execute_to_pending(ExecuteRequest {
                source: r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
while (true) {}
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial_response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let resumed_response = tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_to_pending(WaitToPendingRequest {
                cell_id: "1".to_string(),
                yield_time_ms: Some(1),
            }),
        )
        .await
        .unwrap()
        .unwrap();

        let WaitToPendingOutcome::LiveCell(ExecuteToPendingOutcome::Completed(
            RuntimeResponse::Result {
                error_text: Some(error_text),
                ..
            },
        )) = resumed_response
        else {
            panic!("expected wait_to_pending watchdog failure");
        };
        assert!(error_text.contains("watchdog fired"), "{error_text}");
    }

    #[tokio::test]
    async fn wait_to_pending_returns_completed_after_resumed_runtime_finishes() {
        let service = CodeModeService::new();

        let initial_response = service
            .execute_to_pending(ExecuteRequest {
                source: r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
globalThis.__coral_code_mode_result = "done";
"#
                .to_string(),
                yield_time_ms: Some(60_000),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial_response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                pending_tool_call_ids: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();

        let resumed_response = tokio::time::timeout(
            Duration::from_secs(1),
            service.wait_to_pending(WaitToPendingRequest {
                cell_id: "1".to_string(),
                yield_time_ms: None,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            resumed_response,
            WaitToPendingOutcome::LiveCell(ExecuteToPendingOutcome::Completed(
                RuntimeResponse::Result {
                    cell_id: "1".to_string(),
                    content_items: Vec::new(),
                    stored_values: HashMap::new(),
                    stored_value_updates: HashMap::new(),
                    result: Some(json!("done")),
                    error_text: None,
                }
            ))
        );
    }

    #[tokio::test]
    async fn v8_console_only_exposes_guidance_methods() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
globalThis.__coral_code_mode_result = {
  console: Object.hasOwn(globalThis, "console"),
  consoleLog: typeof console.log === "function",
  fetch: Object.hasOwn(globalThis, "fetch"),
  WebAssembly: Object.hasOwn(globalThis, "WebAssembly")
};
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "console": true,
                    "consoleLog": true,
                    "fetch": false,
                    "WebAssembly": false,
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn date_locale_string_formats_with_icu_data() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
const value = new Date("2025-01-02T03:04:05Z")
  .toLocaleString("fr-FR", {
    weekday: "long",
    month: "long",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZone: "UTC",
  });
globalThis.__coral_code_mode_result = value;
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("jeudi 2 janvier \u{e0} 03:04:05")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn intl_date_time_format_formats_with_icu_data() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
const formatter = new Intl.DateTimeFormat("fr-FR", {
  weekday: "long",
  month: "long",
  day: "numeric",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "UTC",
});
globalThis.__coral_code_mode_result = formatter.format(new Date("2025-01-02T03:04:05Z"));
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("jeudi 2 janvier \u{e0} 03:04:05")),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn text_and_notify_are_not_exposed_as_output_helpers() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
globalThis.__coral_code_mode_result = {
  text: typeof text,
  notify: typeof notify,
  imageReturnsUndefined: image("https://example.com/image.jpg") === undefined,
};
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/image.jpg".to_string(),
                    detail: Some(crate::DEFAULT_IMAGE_DETAIL),
                }],
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!({
                    "text": "undefined",
                    "notify": "undefined",
                    "imageReturnsUndefined": true,
                })),
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn image_helper_accepts_raw_mcp_image_block_with_original_detail() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
image({
  type: "image",
  data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==",
  mimeType: "image/png",
  _meta: { "coral/imageDetail": "original" },
});
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==".to_string(),
                    detail: Some(crate::ImageDetail::Original),
                }],
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn image_helper_second_arg_overrides_explicit_object_detail() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
image(
  {
    image_url: "https://example.com/image.jpg",
    detail: "high",
  },
  "original",
);
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/image.jpg".to_string(),
                    detail: Some(crate::ImageDetail::Original),
                }],
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn image_helper_second_arg_overrides_raw_mcp_image_detail() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
image(
  {
    type: "image",
    data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==",
    mimeType: "image/png",
    _meta: { "coral/imageDetail": "original" },
  },
  "high",
);
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==".to_string(),
                    detail: Some(crate::ImageDetail::High),
                }],
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: None,
            }
        );
    }

    #[tokio::test]
    async fn image_helper_rejects_unsupported_detail() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
image({
  image_url: "https://example.com/image.jpg",
  detail: "low",
});
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: Some("image detail must be one of: high, original".to_string()),
            }
        );
    }

    #[tokio::test]
    async fn image_helper_rejects_oversized_image_urls() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: format!(
                    r#"image("https://example.com/" + "A".repeat({}));"#,
                    MAX_OUTPUT_IMAGE_URL_BYTES_PER_ITEM + 1
                ),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected oversized image error");
        };
        assert_eq!(content_items, Vec::new());
        assert!(error_text.contains("per-item size limit"), "{error_text}");
    }

    #[tokio::test]
    async fn image_helper_rejects_too_many_content_items() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: format!(
                    r#"
for (let i = 0; i < {}; i += 1) {{
  image(`https://example.com/${{i}}.png`);
}}
"#,
                    MAX_OUTPUT_CONTENT_ITEMS_PER_CELL + 1
                ),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        let RuntimeResponse::Result {
            content_items,
            error_text: Some(error_text),
            ..
        } = response
        else {
            panic!("expected content item limit error");
        };
        assert_eq!(content_items.len(), MAX_OUTPUT_CONTENT_ITEMS_PER_CELL);
        assert!(error_text.contains("content item limit"), "{error_text}");
    }

    #[tokio::test]
    async fn image_helper_rejects_raw_mcp_result_container() {
        let service = CodeModeService::new();

        let response = service
            .execute(ExecuteRequest {
                source: r#"
image({
  content: [
    {
      type: "image",
      data: "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR4nGP4z8DwHwAFAAH/iZk9HQAAAABJRU5ErkJggg==",
      mimeType: "image/png",
      _meta: { "coral/imageDetail": "original" },
    },
  ],
  isError: false,
});
"#
                .to_string(),
                yield_time_ms: None,
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: Some(
                    "image expects a non-empty image URL string, an object with image_url and optional detail, or a raw MCP image block".to_string(),
                ),
            }
        );
    }

    #[tokio::test]
    async fn wait_reports_missing_cell_separately_from_runtime_results() {
        let service = CodeModeService::new();

        let response = service
            .wait(WaitRequest {
                cell_id: "missing".to_string(),
                yield_time_ms: 1,
                terminate: false,
            })
            .await
            .unwrap();

        assert_eq!(
            response,
            WaitOutcome::MissingCell(RuntimeResponse::Result {
                cell_id: "missing".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: None,
                error_text: Some("exec cell missing not found".to_string()),
            })
        );
    }

    #[tokio::test]
    async fn service_rejects_concurrent_waits_and_releases_cancelled_wait_guard() {
        let service = Arc::new(CodeModeService::new());
        let initial = service
            .execute(ExecuteRequest {
                source: "await new Promise(() => {});".to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        let first_wait_service = Arc::clone(&service);
        let first_wait = tokio::spawn(async move {
            first_wait_service
                .wait(WaitRequest {
                    cell_id: "1".to_string(),
                    yield_time_ms: 60_000,
                    terminate: false,
                })
                .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;

        let duplicate = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: false,
            })
            .await
            .expect_err("duplicate wait should fail");
        assert!(duplicate.contains("already has a wait in progress"));

        first_wait.abort();
        let _ = first_wait.await;

        let termination = tokio::time::timeout(
            Duration::from_secs(1),
            service.wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            })
        );
    }

    #[tokio::test]
    async fn cancelled_wait_does_not_drop_buffered_completion() {
        let service = Arc::new(CodeModeService::new());
        let initial = service
            .execute(ExecuteRequest {
                source: r#"
await new Promise((resolve) => setTimeout(resolve, 60_000));
globalThis.__coral_code_mode_result = "done";
"#
                .to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        let wait_service = Arc::clone(&service);
        let abandoned_wait = tokio::spawn(async move {
            wait_service
                .wait(WaitRequest {
                    cell_id: "1".to_string(),
                    yield_time_ms: 60_000,
                    terminate: false,
                })
                .await
        });

        tokio::time::sleep(Duration::from_millis(20)).await;
        abandoned_wait.abort();
        let _ = abandoned_wait.await;

        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let completion = tokio::time::timeout(
            Duration::from_secs(1),
            service.wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: false,
            }),
        )
        .await
        .unwrap()
        .unwrap();

        assert_eq!(
            completion,
            WaitOutcome::LiveCell(RuntimeResponse::Result {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("done")),
                error_text: None,
            })
        );
    }

    #[tokio::test]
    async fn cancelled_wait_does_not_drop_yielded_content() {
        let service = Arc::new(CodeModeService::new());
        let initial = service
            .execute(ExecuteRequest {
                source: r#"
setTimeout(() => image("https://example.com/later.jpg"), 60_000);
await new Promise(() => {});
"#
                .to_string(),
                yield_time_ms: Some(1),
                ..execute_request("")
            })
            .await
            .unwrap();

        assert_eq!(
            initial,
            RuntimeResponse::Yielded {
                cell_id: "1".to_string(),
                content_items: Vec::new(),
            }
        );

        let runtime_tx = service
            .inner
            .sessions
            .lock()
            .await
            .get("1")
            .unwrap()
            .runtime_tx
            .clone();
        let wait_service = Arc::clone(&service);
        let abandoned_wait = tokio::spawn(async move {
            wait_service
                .wait(WaitRequest {
                    cell_id: "1".to_string(),
                    yield_time_ms: 100,
                    terminate: false,
                })
                .await
        });

        runtime_tx
            .send(RuntimeCommand::TimeoutFired { id: 1 })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
        abandoned_wait.abort();
        let _ = abandoned_wait.await;
        tokio::time::sleep(Duration::from_millis(150)).await;

        let termination = service
            .wait(WaitRequest {
                cell_id: "1".to_string(),
                yield_time_ms: 1,
                terminate: true,
            })
            .await
            .unwrap();

        assert_eq!(
            termination,
            WaitOutcome::LiveCell(RuntimeResponse::Terminated {
                cell_id: "1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/later.jpg".to_string(),
                    detail: Some(crate::DEFAULT_IMAGE_DETAIL),
                }],
            })
        );
    }

    #[tokio::test]
    async fn dropped_execute_to_pending_receiver_buffers_pending_frontier() {
        let inner = test_inner();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (initial_response_tx, initial_response_rx) = oneshot::channel();
        drop(initial_response_rx);
        let (runtime_event_tx, _runtime_event_rx) = mpsc::unbounded_channel();
        let (runtime_tx, runtime_control_tx, runtime_terminate_handle) = spawn_runtime(
            ExecuteRequest {
                source: "await new Promise(() => {})".to_string(),
                yield_time_ms: None,
                ..execute_request("")
            },
            runtime_event_tx,
            PendingRuntimeMode::PauseUntilResumed,
        )
        .unwrap();

        tokio::spawn(run_session_control(
            Arc::clone(&inner),
            SessionControlContext {
                cell_id: "cell-1".to_string(),
                runtime_tx: runtime_tx.clone(),
                runtime_control_tx,
                pending_mode: PendingRuntimeMode::PauseUntilResumed,
                runtime_terminate_handle,
                cancellation_token: CancellationToken::new(),
                shutdown_token: CancellationToken::new(),
            },
            event_rx,
            control_rx,
            SessionResponseSender::ExecuteToPending(initial_response_tx),
            None,
        ));

        event_tx.send(RuntimeEvent::Started).unwrap();
        event_tx
            .send(RuntimeEvent::ContentItem(
                FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/frontier.jpg".to_string(),
                    detail: Some(crate::DEFAULT_IMAGE_DETAIL),
                },
            ))
            .unwrap();
        event_tx
            .send(RuntimeEvent::ToolCall {
                id: "tool-1".to_string(),
                name: ToolName::plain("echo"),
                kind: CodeModeToolKind::Function,
                input: None,
                allow_error_result: false,
                envelope: false,
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Pending).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (next_response_tx, next_response_rx) = oneshot::channel();
        control_tx
            .send(SessionControlCommand::PollToPending {
                yield_time_ms: 60_000,
                response_tx: next_response_tx,
            })
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(1), next_response_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            response,
            ExecuteToPendingOutcome::Pending {
                cell_id: "cell-1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/frontier.jpg".to_string(),
                    detail: Some(crate::DEFAULT_IMAGE_DETAIL),
                }],
                pending_tool_call_ids: vec!["tool-1".to_string()],
            }
        );

        let _ = runtime_tx.send(RuntimeCommand::Terminate);
    }

    #[tokio::test]
    async fn plain_wait_drains_buffered_pending_frontier_after_dropped_receiver() {
        let inner = test_inner();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (initial_response_tx, initial_response_rx) = oneshot::channel();
        drop(initial_response_rx);
        let (runtime_event_tx, _runtime_event_rx) = mpsc::unbounded_channel();
        let (runtime_tx, runtime_control_tx, runtime_terminate_handle) = spawn_runtime(
            ExecuteRequest {
                source: "await new Promise(() => {})".to_string(),
                yield_time_ms: None,
                ..execute_request("")
            },
            runtime_event_tx,
            PendingRuntimeMode::PauseUntilResumed,
        )
        .unwrap();

        tokio::spawn(run_session_control(
            Arc::clone(&inner),
            SessionControlContext {
                cell_id: "cell-1".to_string(),
                runtime_tx: runtime_tx.clone(),
                runtime_control_tx,
                pending_mode: PendingRuntimeMode::PauseUntilResumed,
                runtime_terminate_handle,
                cancellation_token: CancellationToken::new(),
                shutdown_token: CancellationToken::new(),
            },
            event_rx,
            control_rx,
            SessionResponseSender::ExecuteToPending(initial_response_tx),
            None,
        ));

        event_tx.send(RuntimeEvent::Started).unwrap();
        event_tx
            .send(RuntimeEvent::ContentItem(
                FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/frontier.jpg".to_string(),
                    detail: Some(crate::DEFAULT_IMAGE_DETAIL),
                },
            ))
            .unwrap();
        event_tx
            .send(RuntimeEvent::ToolCall {
                id: "tool-1".to_string(),
                name: ToolName::plain("echo"),
                kind: CodeModeToolKind::Function,
                input: None,
                allow_error_result: false,
                envelope: false,
            })
            .unwrap();
        event_tx.send(RuntimeEvent::Pending).unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;

        let (next_response_tx, next_response_rx) = oneshot::channel();
        control_tx
            .send(SessionControlCommand::Poll {
                yield_time_ms: 60_000,
                response_tx: next_response_tx,
            })
            .unwrap();
        tokio::time::sleep(Duration::from_millis(20)).await;
        event_tx
            .send(RuntimeEvent::Result {
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("done")),
                error_text: None,
            })
            .unwrap();

        let response = tokio::time::timeout(Duration::from_secs(1), next_response_rx)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            response,
            RuntimeResponse::Result {
                cell_id: "cell-1".to_string(),
                content_items: vec![FunctionCallOutputContentItem::InputImage {
                    image_url: "https://example.com/frontier.jpg".to_string(),
                    detail: Some(crate::DEFAULT_IMAGE_DETAIL),
                }],
                stored_values: HashMap::new(),
                stored_value_updates: HashMap::new(),
                result: Some(json!("done")),
                error_text: None,
            }
        );

        let _ = runtime_tx.send(RuntimeCommand::Terminate);
    }

    #[tokio::test]
    async fn terminate_waits_for_runtime_shutdown_before_responding() {
        let inner = test_inner();
        let (event_tx, event_rx) = mpsc::unbounded_channel();
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let (initial_response_tx, initial_response_rx) = oneshot::channel();
        let (runtime_event_tx, _runtime_event_rx) = mpsc::unbounded_channel();
        let (runtime_tx, runtime_control_tx, runtime_terminate_handle) = spawn_runtime(
            ExecuteRequest {
                source: "await new Promise(() => {})".to_string(),
                yield_time_ms: None,
                ..execute_request("")
            },
            runtime_event_tx,
            PendingRuntimeMode::Continue,
        )
        .unwrap();

        tokio::spawn(run_session_control(
            inner,
            SessionControlContext {
                cell_id: "cell-1".to_string(),
                runtime_tx: runtime_tx.clone(),
                runtime_control_tx,
                pending_mode: PendingRuntimeMode::Continue,
                runtime_terminate_handle,
                cancellation_token: CancellationToken::new(),
                shutdown_token: CancellationToken::new(),
            },
            event_rx,
            control_rx,
            SessionResponseSender::Runtime(initial_response_tx),
            Some(/*initial_yield_time_ms*/ 60_000),
        ));

        event_tx.send(RuntimeEvent::Started).unwrap();
        event_tx.send(RuntimeEvent::YieldRequested).unwrap();
        assert_eq!(
            initial_response_rx.await.unwrap(),
            RuntimeResponse::Yielded {
                cell_id: "cell-1".to_string(),
                content_items: Vec::new(),
            }
        );

        let (terminate_response_tx, terminate_response_rx) = oneshot::channel();
        control_tx
            .send(SessionControlCommand::Terminate {
                response_tx: terminate_response_tx,
            })
            .unwrap();
        let terminate_response = async { terminate_response_rx.await.unwrap() };
        tokio::pin!(terminate_response);
        tokio::time::timeout(Duration::from_millis(100), terminate_response.as_mut())
            .await
            .unwrap_err();

        drop(event_tx);

        assert_eq!(
            terminate_response.await,
            RuntimeResponse::Terminated {
                cell_id: "cell-1".to_string(),
                content_items: Vec::new(),
            }
        );

        let _ = runtime_tx.send(RuntimeCommand::Terminate);
    }
}
