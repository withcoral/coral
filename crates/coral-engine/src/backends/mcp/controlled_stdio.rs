//! Hard-close wrapper for controlled stdio MCP child processes.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::Either;
use rmcp::RoleClient;
use rmcp::model::{ClientNotification, JsonRpcMessage, RequestId, ServerRequest};
use rmcp::service::{RxJsonRpcMessage, TxJsonRpcMessage};
use rmcp::transport::{TokioChildProcess, Transport};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::{Mutex as AsyncMutex, Notify};
use tokio::task::JoinHandle;

use crate::backends::CONTROLLED_RESPONSE_BODY_LIMIT_BYTES;
use crate::{QueryExecutionControls, QueryExecutionFailureKind};

const MAX_CONTROLLED_MCP_AUXILIARY_MESSAGES: usize = 256;

pub(super) enum CoralChildTransport {
    Raw(TokioChildProcess),
    Controlled(HardCloseChildTransport),
}

impl CoralChildTransport {
    pub(super) fn raw(inner: TokioChildProcess) -> Self {
        Self::Raw(inner)
    }

    pub(super) fn spawn_controlled(
        mut command: Command,
        tracker: ChildTransportTracker,
        controls: QueryExecutionControls,
    ) -> std::io::Result<Self> {
        controls.check_active().map_err(std::io::Error::other)?;
        command.kill_on_drop(true);
        let mut child = command.spawn()?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| std::io::Error::other("controlled MCP child stdout is not piped"))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| std::io::Error::other("controlled MCP child stdin is not piped"))?;
        Ok(Self::Controlled(HardCloseChildTransport::new(
            child, stdout, stdin, tracker, controls,
        )))
    }
}

impl Transport<RoleClient> for CoralChildTransport {
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        match self {
            Self::Raw(inner) => Either::Left(inner.send(item)),
            Self::Controlled(inner) => Either::Right(inner.send(item)),
        }
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        match self {
            Self::Raw(inner) => Either::Left(inner.receive()),
            Self::Controlled(inner) => Either::Right(inner.receive()),
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        match self {
            Self::Raw(inner) => inner.close().await,
            Self::Controlled(inner) => inner.close().await,
        }
    }
}

/// rmcp's stock child transport waits up to three seconds for graceful child
/// exit and its drop fallback spawns an untracked kill task. Controlled
/// execution instead signals the isolated `kill_on_drop` child immediately
/// and retains a tracked reaper task that callers can await through Coral's
/// shared cleanup deadline.
pub(super) struct HardCloseChildTransport {
    child: Option<Child>,
    stdout: BufReader<ChildStdout>,
    stdin: Arc<AsyncMutex<Option<ChildStdin>>>,
    line: Vec<u8>,
    received_bytes: usize,
    ignored_messages: usize,
    tracker: ChildTransportTracker,
    controls: QueryExecutionControls,
}

impl HardCloseChildTransport {
    pub(super) fn new(
        child: Child,
        stdout: ChildStdout,
        stdin: ChildStdin,
        tracker: ChildTransportTracker,
        controls: QueryExecutionControls,
    ) -> Self {
        tracker.0.active.store(true, Ordering::SeqCst);
        Self {
            child: Some(child),
            stdout: BufReader::new(stdout),
            stdin: Arc::new(AsyncMutex::new(Some(stdin))),
            line: Vec::new(),
            received_bytes: 0,
            ignored_messages: 0,
            tracker,
            controls,
        }
    }

    fn start_reaper(&mut self) {
        if let Some(mut child) = self.child.take() {
            let signal_result = child.start_kill();
            self.tracker.spawn_reaper(async move {
                // Always wait, even if signalling reported an error: a process
                // that exited concurrently still needs to be reaped.
                let wait_result = child.wait().await.map(|_status| ());
                match (signal_result, wait_result) {
                    (_, Ok(())) => Ok(()),
                    (Err(signal_error), Err(_wait_error)) => Err(signal_error),
                    (Ok(()), Err(wait_error)) => Err(wait_error),
                }
            });
        } else if lock_unpoisoned(&self.tracker.0.reaper).is_none() {
            self.tracker.finish(Ok(()));
        }
    }

    async fn receive_next(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
        loop {
            if let Err(kind) = self.controls.check_active() {
                self.tracker.record_failure(kind);
                return std::future::pending().await;
            }
            match read_bounded_line(&mut self.stdout, &mut self.line).await {
                Ok(BoundedLine::Eof) => return None,
                Ok(BoundedLine::Oversized) => {
                    self.tracker
                        .record_failure(QueryExecutionFailureKind::InvalidResponse);
                    return std::future::pending().await;
                }
                Ok(BoundedLine::Complete) => {}
                Err(error) => {
                    tracing::debug!(%error, "failed to read controlled MCP child response");
                    self.tracker
                        .record_failure(QueryExecutionFailureKind::UpstreamUnavailable);
                    return std::future::pending().await;
                }
            }
            let line = self.line.strip_suffix(b"\r").unwrap_or(&self.line);
            let line = line.strip_prefix(b"\xEF\xBB\xBF").unwrap_or(line);
            self.received_bytes = self.received_bytes.saturating_add(line.len());
            if self.received_bytes > CONTROLLED_RESPONSE_BODY_LIMIT_BYTES {
                self.tracker
                    .record_failure(QueryExecutionFailureKind::InvalidResponse);
                return std::future::pending().await;
            }
            if line.is_empty() {
                self.ignored_messages = self.ignored_messages.saturating_add(1);
                if self.ignored_messages > MAX_CONTROLLED_MCP_AUXILIARY_MESSAGES {
                    self.tracker
                        .record_failure(QueryExecutionFailureKind::InvalidResponse);
                    return std::future::pending().await;
                }
                tokio::task::yield_now().await;
                continue;
            }
            match serde_json::from_slice::<RxJsonRpcMessage<RoleClient>>(line) {
                Ok(message @ (JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_))) => {
                    if self.tracker.accept_response(&message) {
                        return Some(message);
                    }
                    self.tracker
                        .record_failure(QueryExecutionFailureKind::InvalidResponse);
                    return std::future::pending().await;
                }
                Ok(JsonRpcMessage::Notification(_)) => {
                    // Progress and logging are valid, but this isolated call has
                    // no notification consumer. Drop them before rmcp can create
                    // work that outlives the cleanup boundary.
                    self.ignored_messages = self.ignored_messages.saturating_add(1);
                    if self.ignored_messages > MAX_CONTROLLED_MCP_AUXILIARY_MESSAGES {
                        self.tracker
                            .record_failure(QueryExecutionFailureKind::InvalidResponse);
                        return std::future::pending().await;
                    }
                    tokio::task::yield_now().await;
                }
                Ok(JsonRpcMessage::Request(request))
                    if self
                        .tracker
                        .allow_server_request(&request.request, &request.id) =>
                {
                    return Some(JsonRpcMessage::Request(request));
                }
                Ok(JsonRpcMessage::Request(_)) => {
                    // One ping per outstanding request is handled by rmcp.
                    // Sampling, roots, elicitation, and request floods are
                    // rejected before they can create an unbounded handler drain.
                    self.tracker
                        .record_failure(QueryExecutionFailureKind::InvalidResponse);
                    return std::future::pending().await;
                }
                Err(error) => {
                    tracing::debug!(%error, "invalid controlled MCP child JSON response");
                    self.tracker
                        .record_failure(QueryExecutionFailureKind::InvalidResponse);
                    return std::future::pending().await;
                }
            }
        }
    }
}

impl Drop for HardCloseChildTransport {
    fn drop(&mut self) {
        // This path is exercised when initialization is cancelled before rmcp
        // can return a RunningService. `start_reaper` sends the kill signal
        // synchronously, then owns the child until `wait` has reaped it.
        self.start_reaper();
    }
}

impl Transport<RoleClient> for HardCloseChildTransport {
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<RoleClient>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let is_cancellation = matches!(
            &item,
            JsonRpcMessage::Notification(notification)
                if matches!(
                    notification.notification,
                    ClientNotification::CancelledNotification(_)
                )
        );
        let server_response_id = client_response_id(&item).cloned();
        if let JsonRpcMessage::Request(request) = &item {
            self.received_bytes = 0;
            self.ignored_messages = 0;
            self.tracker.expect_response(request.id.clone());
        }
        let stdin = Arc::clone(&self.stdin);
        let future = async move {
            let mut encoded = serde_json::to_vec(&item).map_err(std::io::Error::other)?;
            encoded.push(b'\n');
            let mut writer = stdin.lock().await;
            let writer = writer.as_mut().ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::BrokenPipe,
                    "controlled MCP child transport is closed",
                )
            })?;
            writer.write_all(&encoded).await?;
            writer.flush().await
        };
        let controls = self.controls.clone();
        let tracker = self.tracker.clone();
        async move {
            let result = match controls.check_active() {
                Ok(()) => controls
                    .run_until_stopped(async {
                        if !is_cancellation {
                            controls.mark_upstream_started();
                        }
                        future.await
                    })
                    .await
                    .map_err(std::io::Error::other)?,
                Err(kind) if is_cancellation => {
                    match tokio::time::timeout_at(controls.cleanup_deadline(kind), future).await {
                        Ok(result) => result,
                        Err(_elapsed) => Err(std::io::Error::other(kind)),
                    }
                }
                Err(kind) => Err(std::io::Error::other(kind)),
            };
            if let Some(id) = server_response_id {
                tracker.complete_server_request(&id);
            }
            result
        }
    }

    fn receive(&mut self) -> impl Future<Output = Option<RxJsonRpcMessage<RoleClient>>> + Send {
        self.receive_next()
    }

    fn close(&mut self) -> impl Future<Output = Result<(), Self::Error>> + Send {
        self.start_reaper();
        let stdin = Arc::clone(&self.stdin);
        async move {
            stdin.lock().await.take();
            Ok(())
        }
    }
}

enum BoundedLine {
    Complete,
    Oversized,
    Eof,
}

async fn read_bounded_line(
    reader: &mut BufReader<ChildStdout>,
    line: &mut Vec<u8>,
) -> std::io::Result<BoundedLine> {
    line.clear();
    loop {
        let available = reader.fill_buf().await?;
        if available.is_empty() {
            return Ok(if line.is_empty() {
                BoundedLine::Eof
            } else {
                BoundedLine::Complete
            });
        }
        let newline = available.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(available.len(), |index| index);
        if line.len().saturating_add(take) > CONTROLLED_RESPONSE_BODY_LIMIT_BYTES {
            let consume = newline.map_or(available.len(), |index| index.saturating_add(1));
            reader.consume(consume);
            return Ok(BoundedLine::Oversized);
        }
        line.extend(available.iter().copied().take(take));
        let consume = newline.map_or(take, |index| index.saturating_add(1));
        reader.consume(consume);
        if newline.is_some() {
            return Ok(BoundedLine::Complete);
        }
    }
}

#[derive(Debug, Default)]
struct ChildTransportState {
    active: AtomicBool,
    initialized: AtomicBool,
    outcome: Mutex<Option<std::result::Result<(), (std::io::ErrorKind, String)>>>,
    reaper: Mutex<Option<JoinHandle<()>>>,
    failure_kind: Mutex<Option<QueryExecutionFailureKind>>,
    expected_response_id: Mutex<Option<RequestId>>,
    server_ping_in_flight: Mutex<Option<RequestId>>,
    failed: Notify,
    finished: Notify,
}

#[derive(Clone, Debug, Default)]
pub(super) struct ChildTransportTracker(Arc<ChildTransportState>);

impl ChildTransportTracker {
    fn spawn_reaper(&self, reap: impl Future<Output = std::io::Result<()>> + Send + 'static) {
        self.0.active.store(true, Ordering::SeqCst);
        let tracker = self.clone();
        let reaper = tokio::spawn(async move {
            tracker.finish(reap.await);
        });
        *lock_unpoisoned(&self.0.reaper) = Some(reaper);
    }

    fn finish(&self, result: std::io::Result<()>) {
        *lock_unpoisoned(&self.0.outcome) =
            Some(result.map_err(|error| (error.kind(), error.to_string())));
        self.0.active.store(false, Ordering::SeqCst);
        self.0.finished.notify_waiters();
    }

    fn record_failure(&self, kind: QueryExecutionFailureKind) {
        let mut failure = lock_unpoisoned(&self.0.failure_kind);
        if failure.is_none() {
            *failure = Some(kind);
            drop(failure);
            self.0.failed.notify_waiters();
        }
    }

    pub(super) fn failure_kind(&self) -> Option<QueryExecutionFailureKind> {
        *lock_unpoisoned(&self.0.failure_kind)
    }

    fn expect_response(&self, id: RequestId) {
        *lock_unpoisoned(&self.0.expected_response_id) = Some(id);
    }

    fn allow_server_request(&self, request: &ServerRequest, id: &RequestId) -> bool {
        if !self.0.initialized.load(Ordering::SeqCst)
            || !matches!(request, ServerRequest::PingRequest(_))
        {
            return false;
        }
        let mut in_flight = lock_unpoisoned(&self.0.server_ping_in_flight);
        if in_flight.is_some() {
            return false;
        }
        *in_flight = Some(id.clone());
        true
    }

    fn complete_server_request(&self, response_id: &RequestId) {
        let mut in_flight = lock_unpoisoned(&self.0.server_ping_in_flight);
        if in_flight.as_ref() == Some(response_id) {
            *in_flight = None;
        }
    }

    fn accept_response(&self, message: &RxJsonRpcMessage<RoleClient>) -> bool {
        let is_result = matches!(message, JsonRpcMessage::Response(_));
        let actual = match message {
            JsonRpcMessage::Response(response) => Some(&response.id),
            JsonRpcMessage::Error(error) => error.id.as_ref(),
            JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_) => None,
        };
        let mut expected = lock_unpoisoned(&self.0.expected_response_id);
        if actual.is_some_and(|actual| expected.as_ref() == Some(actual)) {
            *expected = None;
            if is_result {
                self.0.initialized.store(true, Ordering::SeqCst);
            }
            true
        } else {
            false
        }
    }

    pub(super) async fn wait_failure(&self) -> QueryExecutionFailureKind {
        loop {
            let notified = self.0.failed.notified();
            if let Some(kind) = self.failure_kind() {
                return kind;
            }
            notified.await;
        }
    }

    pub(super) async fn wait_reaped(&self) -> std::io::Result<()> {
        loop {
            let notified = self.0.finished.notified();
            if !self.0.active.load(Ordering::SeqCst) {
                return self.outcome();
            }
            notified.await;
        }
    }

    pub(super) async fn wait_reaped_until(
        &self,
        deadline: tokio::time::Instant,
    ) -> std::io::Result<()> {
        tokio::time::timeout_at(deadline, self.wait_reaped())
            .await
            .map_err(|_elapsed| {
                std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "timed out reaping controlled MCP child process",
                )
            })?
    }

    fn outcome(&self) -> std::io::Result<()> {
        match lock_unpoisoned(&self.0.outcome).as_ref() {
            Some(Ok(())) | None => Ok(()),
            Some(Err((kind, detail))) => Err(std::io::Error::new(*kind, detail.clone())),
        }
    }
}

fn lock_unpoisoned<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn client_response_id(message: &TxJsonRpcMessage<RoleClient>) -> Option<&RequestId> {
    match message {
        JsonRpcMessage::Response(response) => Some(&response.id),
        JsonRpcMessage::Error(error) => error.id.as_ref(),
        JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_) => None,
    }
}
