//! Cancellation-aware Streamable HTTP adapter for controlled MCP execution.

use std::borrow::Cow;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::stream::BoxStream;
use futures::{StreamExt as _, future};
use reqwest::header::{ACCEPT, HeaderName, HeaderValue, WWW_AUTHENTICATE};
use rmcp::model::{
    ClientJsonRpcMessage, ClientNotification, JsonRpcMessage, RequestId, ServerJsonRpcMessage,
    ServerRequest,
};
use rmcp::transport::common::http_header::{
    EVENT_STREAM_MIME_TYPE, HEADER_LAST_EVENT_ID, HEADER_MCP_PROTOCOL_VERSION, HEADER_SESSION_ID,
    JSON_MIME_TYPE,
};
use rmcp::transport::streamable_http_client::{
    AuthRequiredError, InsufficientScopeError, SseError, StreamableHttpClient, StreamableHttpError,
    StreamableHttpPostResponse,
};
use sse_stream::{Sse, SseStream};
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

use crate::backends::CONTROLLED_RESPONSE_BODY_LIMIT_BYTES;
use crate::{QueryExecutionControls, QueryExecutionFailureKind};

/// One rmcp client type that preserves the stock adapter for ordinary SQL and
/// selects the hard execution boundary only for the controlled entrypoint.
#[derive(Clone, Debug)]
pub(super) enum CoralStreamableHttpClient {
    Raw(reqwest::Client),
    Controlled(ControlledStreamableHttpClient),
}

impl CoralStreamableHttpClient {
    pub(super) fn raw(client: reqwest::Client) -> Self {
        Self::Raw(client)
    }

    pub(super) fn controlled(
        client: reqwest::Client,
        controls: QueryExecutionControls,
        failures: HttpFailureSignal,
        operations: HttpOperationTracker,
    ) -> Self {
        Self::Controlled(ControlledStreamableHttpClient {
            client,
            controls,
            failures,
            operations,
        })
    }
}

impl StreamableHttpClient for CoralStreamableHttpClient {
    type Error = reqwest::Error;

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        match self {
            Self::Raw(client) => {
                client
                    .get_stream(uri, session_id, last_event_id, auth_token, custom_headers)
                    .await
            }
            Self::Controlled(client) => {
                client
                    .get_stream(uri, session_id, last_event_id, auth_token, custom_headers)
                    .await
            }
        }
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        match self {
            Self::Raw(client) => {
                client
                    .delete_session(uri, session_id, auth_token, custom_headers)
                    .await
            }
            Self::Controlled(client) => {
                client
                    .delete_session(uri, session_id, auth_token, custom_headers)
                    .await
            }
        }
    }

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        match self {
            Self::Raw(client) => {
                client
                    .post_message(uri, message, session_id, auth_token, custom_headers)
                    .await
            }
            Self::Controlled(client) => {
                client
                    .post_message(uri, message, session_id, auth_token, custom_headers)
                    .await
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct ControlledStreamableHttpClient {
    client: reqwest::Client,
    controls: QueryExecutionControls,
    failures: HttpFailureSignal,
    operations: HttpOperationTracker,
}

impl StreamableHttpClient for ControlledStreamableHttpClient {
    type Error = reqwest::Error;

    async fn get_stream(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        last_event_id: Option<String>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, StreamableHttpError<Self::Error>> {
        let response = match self
            .run_execution(async {
                let mut request = self
                    .client
                    .get(uri.as_ref())
                    .header(ACCEPT, accepted_content_types())
                    .header(HEADER_SESSION_ID, session_id.as_ref());
                if let Some(last_event_id) = last_event_id {
                    request = request.header(HEADER_LAST_EVENT_ID, last_event_id);
                }
                if let Some(auth_token) = auth_token {
                    request = request.bearer_auth(auth_token);
                }
                request = apply_custom_headers(request, custom_headers)?;
                request.send().await.map_err(StreamableHttpError::Client)
            })
            .await
        {
            Ok(response) => response,
            Err(error) => {
                self.failures
                    .record_stream(StreamOrigin::Common, stream_failure_kind(&error));
                return Err(error);
            }
        };
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            self.failures.record_stream(
                StreamOrigin::Common,
                QueryExecutionFailureKind::InvalidResponse,
            );
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            self.failures.record_stream(
                StreamOrigin::Common,
                QueryExecutionFailureKind::UpstreamUnavailable,
            );
            return Err(into_status_error(response));
        }
        if !response.status().is_success() {
            self.failures
                .record_stream_status(StreamOrigin::Common, response.status());
            return Err(into_status_error(response));
        }
        if let Err(error) = validate_event_stream_content_type(&response) {
            self.failures.record_stream(
                StreamOrigin::Common,
                QueryExecutionFailureKind::InvalidResponse,
            );
            return Err(error);
        }
        Ok(self.bounded_sse_stream(response, StreamOrigin::Common))
    }

    async fn delete_session(
        &self,
        uri: Arc<str>,
        session_id: Arc<str>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), StreamableHttpError<Self::Error>> {
        let operation = async {
            let mut request = self.client.delete(uri.as_ref());
            if let Some(auth_token) = auth_token {
                request = request.bearer_auth(auth_token);
            }
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
            request = apply_custom_headers(request, custom_headers)?;
            request.send().await.map_err(StreamableHttpError::Client)
        };
        // Session deletion is teardown even after a successful tool result. It
        // must not consume the function's entire remaining deadline (or hang
        // forever when there is no deadline), so it gets one short cutoff.
        let response = self.run_teardown(operation).await?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Ok(());
        }
        if !response.status().is_success() {
            return Err(into_status_error(response));
        }
        Ok(())
    }

    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<Self::Error>> {
        let is_cancellation = is_cancelled_notification(&message);
        let tracks_failure = tracks_client_operation_failure(&message);
        let server_response_id = client_response_id(&message).cloned();
        let is_server_response = server_response_id.is_some();
        let request_generation = if let ClientJsonRpcMessage::Request(request) = &message {
            Some(self.failures.begin_request(request.id.clone()))
        } else {
            None
        };
        if tracks_failure && request_generation.is_none() {
            // Initialize/initialized/tool POSTs are sequential for one rmcp
            // client. Server-request replies may POST concurrently over SSE,
            // so only client-authored operations own this causal sidecar.
            self.failures.reset_post();
        }
        let operation = self.post_message_unbounded(
            uri,
            message,
            session_id,
            auth_token,
            custom_headers,
            tracks_failure,
            request_generation,
        );
        let result = if is_cancellation {
            self.run_cleanup(operation).await
        } else if is_server_response {
            // rmcp handles server requests serially. If the peer stalls the
            // HTTP acknowledgement for Coral's Ping reply, the worker cannot
            // advance to a valid tool result already waiting on SSE. Bound
            // that protocol reply by the same short teardown window.
            self.run_teardown(operation).await
        } else {
            self.run_execution(operation).await
        };
        if tracks_failure && let Err(StreamableHttpError::Client(error)) = &result {
            self.failures.record_reqwest(error);
        }
        if let Some(id) = server_response_id {
            self.failures.complete_server_request(&id);
        }
        result
    }
}

impl ControlledStreamableHttpClient {
    #[expect(
        clippy::too_many_arguments,
        clippy::too_many_lines,
        reason = "This intentionally mirrors rmcp's request/response compatibility adapter while adding one controlled stop boundary."
    )]
    async fn post_message_unbounded(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session_id: Option<Arc<str>>,
        auth_token: Option<String>,
        custom_headers: HashMap<HeaderName, HeaderValue>,
        record_failure: bool,
        request_generation: Option<u64>,
    ) -> Result<StreamableHttpPostResponse, StreamableHttpError<reqwest::Error>> {
        let awaits_response_stream = request_generation.is_some();
        let mut request = self
            .client
            .post(uri.as_ref())
            .header(ACCEPT, accepted_content_types());
        if let Some(auth_token) = auth_token {
            request = request.bearer_auth(auth_token);
        }
        request = apply_custom_headers(request, custom_headers)?;
        let session_was_attached = session_id.is_some();
        if let Some(session_id) = session_id {
            request = request.header(HEADER_SESSION_ID, session_id.as_ref());
        }
        let response = request
            .json(&message)
            .send()
            .await
            .map_err(StreamableHttpError::Client)?;
        let status = response.status();
        if record_failure && !status.is_success() {
            self.failures.record_status(status);
        }
        if status == reqwest::StatusCode::UNAUTHORIZED
            && let Some(header) = response.headers().get(WWW_AUTHENTICATE)
        {
            let header = header
                .to_str()
                .map_err(|_error| {
                    StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                        "invalid www-authenticate header value",
                    ))
                })?
                .to_string();
            return Err(StreamableHttpError::AuthRequired(AuthRequiredError::new(
                header,
            )));
        }
        if status == reqwest::StatusCode::FORBIDDEN
            && let Some(header) = response.headers().get(WWW_AUTHENTICATE)
        {
            let header = header.to_str().map_err(|_error| {
                StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
                    "invalid www-authenticate header value",
                ))
            })?;
            return Err(StreamableHttpError::InsufficientScope(
                InsufficientScopeError::new(header.to_string(), extract_scope(header)),
            ));
        }
        if matches!(
            status,
            reqwest::StatusCode::ACCEPTED | reqwest::StatusCode::NO_CONTENT
        ) {
            if awaits_response_stream {
                if !session_was_attached {
                    if record_failure {
                        self.failures
                            .record(QueryExecutionFailureKind::InvalidResponse);
                    }
                    return Err(StreamableHttpError::UnexpectedServerResponse(
                        Cow::Borrowed(
                            "accepted MCP request cannot complete without a response stream",
                        ),
                    ));
                }
                if record_failure {
                    self.failures.mark_awaits_stream(ResponseStreamMode::Common(
                        request_generation.expect("request generation checked"),
                    ));
                }
            }
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if status == reqwest::StatusCode::NOT_FOUND && session_was_attached {
            if record_failure {
                self.failures
                    .record(QueryExecutionFailureKind::UpstreamUnavailable);
            }
            return Err(StreamableHttpError::SessionExpired);
        }
        let content_type = response_content_type(&response);
        let content_length = response.content_length();
        let response_session_id = response_session_id(&response);
        // Preserve rmcp's compatibility behavior for successful empty
        // notification/response acknowledgements. Only malformed JSON with
        // an actual body is rejected by the controlled adapter below.
        if status.is_success()
            && content_length == Some(0)
            && matches!(
                message,
                ClientJsonRpcMessage::Notification(_)
                    | ClientJsonRpcMessage::Response(_)
                    | ClientJsonRpcMessage::Error(_)
            )
        {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        if !status.is_success() {
            if record_failure {
                self.failures.record_status(status);
            }
            return Err(StreamableHttpError::UnexpectedServerResponse(Cow::Owned(
                format!("HTTP {status}"),
            )));
        }
        match content_type.as_deref() {
            Some(value)
                if value
                    .as_bytes()
                    .starts_with(EVENT_STREAM_MIME_TYPE.as_bytes()) =>
            {
                let Some(generation) = request_generation else {
                    if record_failure {
                        self.failures
                            .record(QueryExecutionFailureKind::InvalidResponse);
                    }
                    return Err(StreamableHttpError::UnexpectedServerResponse(
                        Cow::Borrowed("unexpected SSE response for MCP notification or reply"),
                    ));
                };
                if record_failure && awaits_response_stream {
                    self.failures
                        .mark_awaits_stream(ResponseStreamMode::Direct(generation));
                }
                Ok(StreamableHttpPostResponse::Sse(
                    self.bounded_sse_stream(response, StreamOrigin::Direct(generation)),
                    response_session_id,
                ))
            }
            Some(value) if value.as_bytes().starts_with(JSON_MIME_TYPE.as_bytes()) => {
                let body = read_bounded_mcp_body(response).await?;
                if body.truncated {
                    if record_failure {
                        self.failures
                            .record(QueryExecutionFailureKind::InvalidResponse);
                    }
                    return Err(StreamableHttpError::UnexpectedServerResponse(
                        Cow::Borrowed("controlled MCP response body exceeded limit"),
                    ));
                }
                match serde_json::from_slice::<ServerJsonRpcMessage>(&body.bytes) {
                    Ok(message @ (JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_))) => {
                        if request_generation.is_some() && !self.failures.accept_response(&message)
                        {
                            if record_failure {
                                self.failures
                                    .record(QueryExecutionFailureKind::InvalidResponse);
                            }
                            return Err(StreamableHttpError::UnexpectedServerResponse(
                                Cow::Borrowed("controlled MCP response id did not match request"),
                            ));
                        }
                        Ok(StreamableHttpPostResponse::Json(
                            message,
                            response_session_id,
                        ))
                    }
                    Ok(JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_)) => {
                        if record_failure {
                            self.failures
                                .record(QueryExecutionFailureKind::InvalidResponse);
                        }
                        Err(StreamableHttpError::UnexpectedServerResponse(
                            Cow::Borrowed(
                                "peer-driven work is disabled for controlled MCP execution",
                            ),
                        ))
                    }
                    Err(error) => {
                        tracing::warn!(%error, "could not parse controlled MCP JSON response");
                        if record_failure {
                            self.failures
                                .record(QueryExecutionFailureKind::InvalidResponse);
                        }
                        Err(StreamableHttpError::UnexpectedServerResponse(
                            Cow::Borrowed("malformed JSON response"),
                        ))
                    }
                }
            }
            _ => Err(StreamableHttpError::UnexpectedContentType(content_type)),
        }
    }

    async fn run_execution<T>(
        &self,
        operation: impl Future<Output = Result<T, StreamableHttpError<reqwest::Error>>>,
    ) -> Result<T, StreamableHttpError<reqwest::Error>> {
        self.controls.check_active().map_err(stopped_http_error)?;
        let _active = self.operations.begin();
        let operation = async {
            self.controls.mark_upstream_started();
            operation.await
        };
        tokio::select! {
            biased;
            kind = wait_until_stopped(self.controls.clone()) => {
                self.failures.record(kind);
                Err(stopped_http_error(kind))
            }
            () = self.failures.wait_transport_shutdown() => {
                Err(stopped_http_error(QueryExecutionFailureKind::Cancelled))
            }
            result = operation => {
                result
            }
        }
    }

    async fn run_cleanup<T>(
        &self,
        operation: impl Future<Output = Result<T, StreamableHttpError<reqwest::Error>>>,
    ) -> Result<T, StreamableHttpError<reqwest::Error>> {
        let kind = match self.controls.check_active() {
            Ok(()) => return self.run_execution(operation).await,
            Err(kind) => kind,
        };
        let cleanup_deadline = self.controls.cleanup_deadline(kind);
        if cleanup_deadline <= tokio::time::Instant::now() {
            return Err(stopped_http_error(kind));
        }
        let _active = self.operations.begin();
        match tokio::time::timeout_at(cleanup_deadline, operation).await {
            Ok(result) => result,
            Err(_elapsed) => Err(stopped_http_error(kind)),
        }
    }

    async fn run_teardown<T>(
        &self,
        operation: impl Future<Output = Result<T, StreamableHttpError<reqwest::Error>>>,
    ) -> Result<T, StreamableHttpError<reqwest::Error>> {
        let cleanup_deadline = match self.controls.check_active() {
            Err(kind) => self.controls.cleanup_deadline(kind),
            Ok(()) => self.controls.deadline().map_or_else(
                || {
                    self.controls
                        .cleanup_deadline(QueryExecutionFailureKind::Execution)
                },
                |deadline| {
                    deadline.min(
                        self.controls
                            .cleanup_deadline(QueryExecutionFailureKind::Execution),
                    )
                },
            ),
        };
        let _active = self.operations.begin();
        match tokio::time::timeout_at(cleanup_deadline, operation).await {
            Ok(result) => result,
            Err(_elapsed) => Err(stopped_http_error(
                self.controls
                    .check_active()
                    .err()
                    .unwrap_or(QueryExecutionFailureKind::Execution),
            )),
        }
    }

    #[expect(
        clippy::too_many_lines,
        reason = "SSE byte limits, JSON-RPC filtering, terminal detection, and cancellation share one stream pipeline so their ordering stays explicit."
    )]
    fn bounded_sse_stream(
        &self,
        response: reqwest::Response,
        origin: StreamOrigin,
    ) -> BoxStream<'static, Result<Sse, SseError>> {
        let controls = self.controls.clone();
        let transport_shutdown = self.failures.clone();
        let keep_active = self.operations.begin();
        let byte_failures = self.failures.clone();
        let bounded_bytes = response.bytes_stream().scan(0usize, move |seen, item| {
            let next = match item {
                Ok(bytes)
                    if seen.saturating_add(bytes.len()) <= CONTROLLED_RESPONSE_BODY_LIMIT_BYTES =>
                {
                    *seen = seen.saturating_add(bytes.len());
                    Some(Ok(bytes))
                }
                Ok(_oversized) => {
                    byte_failures.record_stream(origin, QueryExecutionFailureKind::InvalidResponse);
                    None
                }
                Err(error) => {
                    byte_failures.record_stream_reqwest(origin, &error);
                    Some(Err(error))
                }
            };
            future::ready(next)
        });
        let event_failures = self.failures.clone();
        let saw_response = Arc::new(AtomicBool::new(false));
        let event_saw_response = Arc::clone(&saw_response);
        let parsed = SseStream::from_bytes_stream(bounded_bytes)
            .filter_map(move |event| {
                let output = match event {
                    Ok(sse)
                        if matches!(sse.event.as_deref(), None | Some("" | "message"))
                            && sse
                                .data
                                .as_deref()
                                .is_some_and(|data| !data.trim().is_empty()) =>
                    {
                        match serde_json::from_str::<ServerJsonRpcMessage>(
                            sse.data.as_deref().expect("checked SSE data"),
                        ) {
                            Ok(
                                message @ (JsonRpcMessage::Response(_) | JsonRpcMessage::Error(_)),
                            ) => {
                                if event_failures.accept_response(&message) {
                                    event_saw_response.store(true, Ordering::SeqCst);
                                    Some((Ok(sse), true))
                                } else {
                                    event_failures.record_stream(
                                        origin,
                                        QueryExecutionFailureKind::InvalidResponse,
                                    );
                                    Some((Err(SseError::InvalidLine), true))
                                }
                            }
                            // Progress and logging are valid MCP traffic, but the
                            // isolated fanout call has no notification consumer.
                            // Dropping them here avoids rmcp handler tasks surviving
                            // the hard cleanup boundary.
                            Ok(JsonRpcMessage::Notification(_)) => None,
                            Ok(JsonRpcMessage::Request(request))
                                if event_failures
                                    .allow_server_request(&request.request, &request.id) =>
                            {
                                Some((Ok(sse), false))
                            }
                            Ok(JsonRpcMessage::Request(_)) | Err(_) => {
                                event_failures.record_stream(
                                    origin,
                                    QueryExecutionFailureKind::InvalidResponse,
                                );
                                Some((Err(SseError::InvalidLine), true))
                            }
                        }
                    }
                    Ok(sse) => Some((Ok(sse), false)),
                    Err(error) => {
                        event_failures
                            .record_stream(origin, QueryExecutionFailureKind::InvalidResponse);
                        Some((Err(error), true))
                    }
                };
                future::ready(output)
            })
            .boxed();
        // A matching JSON-RPC Response or Error is terminal for this isolated
        // request. End without polling the upstream body again so an HTTP
        // server cannot keep initialization alive by leaving SSE open.
        let parsed =
            futures::stream::unfold((parsed, false), |(mut parsed, terminal)| async move {
                if terminal {
                    return None;
                }
                parsed
                    .next()
                    .await
                    .map(|(event, terminal)| (event, (parsed, terminal)))
            });
        let terminal_failures = self.failures.clone();
        let terminal = futures::stream::once(async move {
            if saw_response.load(Ordering::SeqCst) {
                None
            } else {
                terminal_failures.record_stream(origin, QueryExecutionFailureKind::InvalidResponse);
                Some(Err(SseError::InvalidLine))
            }
        });
        parsed
            .map(Some)
            .chain(terminal)
            .filter_map(future::ready)
            .take_until(async move {
                tokio::select! {
                    _kind = wait_until_stopped(controls) => {}
                    () = transport_shutdown.wait_transport_shutdown() => {}
                }
            })
            .map(move |event| {
                let _keep_active = &keep_active;
                event
            })
            .boxed()
    }
}

#[derive(Clone, Copy, Debug)]
enum StreamOrigin {
    Common,
    Direct(u64),
}

#[derive(Clone, Copy, Debug)]
enum ResponseStreamMode {
    Common(u64),
    Direct(u64),
}

#[derive(Debug, Default)]
struct HttpFailureState {
    post: Mutex<Option<QueryExecutionFailureKind>>,
    common_stream: Mutex<Option<QueryExecutionFailureKind>>,
    direct_stream: Mutex<Option<(u64, QueryExecutionFailureKind)>>,
    response_stream_mode: Mutex<Option<ResponseStreamMode>>,
    expected_response: Mutex<Option<(RequestId, u64)>>,
    next_generation: AtomicU64,
    initialized: AtomicBool,
    server_ping_in_flight: Mutex<Option<RequestId>>,
    server_request_completed: Notify,
    stream_state_changed: Notify,
    transport_shutdown: CancellationToken,
}

#[derive(Clone, Debug, Default)]
pub(super) struct HttpFailureSignal(Arc<HttpFailureState>);

impl HttpFailureSignal {
    pub(super) fn get(&self) -> Option<QueryExecutionFailureKind> {
        *lock_failure_signal(&self.0.post)
    }

    pub(super) fn shutdown_transport(&self) {
        self.0.transport_shutdown.cancel();
    }

    pub(super) async fn wait_server_request_until(&self, deadline: tokio::time::Instant) -> bool {
        loop {
            let notified = self.0.server_request_completed.notified();
            if lock_failure_signal(&self.0.server_ping_in_flight).is_none() {
                return true;
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return false;
            }
        }
    }

    async fn wait_transport_shutdown(&self) {
        self.0.transport_shutdown.cancelled().await;
    }

    fn reset_post(&self) {
        *lock_failure_signal(&self.0.post) = None;
    }

    fn record(&self, kind: QueryExecutionFailureKind) {
        *lock_failure_signal(&self.0.post) = Some(kind);
    }

    fn begin_request(&self, id: RequestId) -> u64 {
        let generation = self
            .0
            .next_generation
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        self.reset_post();
        *lock_failure_signal(&self.0.response_stream_mode) = None;
        *lock_failure_signal(&self.0.expected_response) = Some((id, generation));
        self.0.stream_state_changed.notify_waiters();
        generation
    }

    fn accept_response(&self, message: &ServerJsonRpcMessage) -> bool {
        let is_result = matches!(message, JsonRpcMessage::Response(_));
        let is_error = matches!(message, JsonRpcMessage::Error(_));
        let actual = match message {
            JsonRpcMessage::Response(response) => Some(&response.id),
            JsonRpcMessage::Error(error) => error.id.as_ref(),
            JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_) => None,
        };
        let mut expected = lock_failure_signal(&self.0.expected_response);
        if actual.is_some_and(|actual| {
            expected
                .as_ref()
                .is_some_and(|(expected, _generation)| expected == actual)
        }) {
            *expected = None;
            drop(expected);
            if is_result {
                self.0.initialized.store(true, Ordering::SeqCst);
            } else if is_error {
                self.record(QueryExecutionFailureKind::Execution);
            }
            true
        } else {
            false
        }
    }

    fn allow_server_request(&self, request: &ServerRequest, id: &RequestId) -> bool {
        if !self.0.initialized.load(Ordering::SeqCst)
            || !matches!(request, ServerRequest::PingRequest(_))
        {
            return false;
        }
        let mut in_flight = lock_failure_signal(&self.0.server_ping_in_flight);
        if in_flight.is_some() {
            return false;
        }
        *in_flight = Some(id.clone());
        true
    }

    fn complete_server_request(&self, response_id: &RequestId) {
        let mut in_flight = lock_failure_signal(&self.0.server_ping_in_flight);
        if in_flight.as_ref() == Some(response_id) {
            *in_flight = None;
            drop(in_flight);
            self.0.server_request_completed.notify_waiters();
        }
    }

    fn record_stream(&self, origin: StreamOrigin, kind: QueryExecutionFailureKind) {
        match origin {
            StreamOrigin::Common => {
                let mut stream = lock_failure_signal(&self.0.common_stream);
                if stream.is_none() {
                    *stream = Some(kind);
                }
            }
            StreamOrigin::Direct(generation) => {
                let mut stream = lock_failure_signal(&self.0.direct_stream);
                if !matches!(*stream, Some((seen, _)) if seen == generation) {
                    *stream = Some((generation, kind));
                }
            }
        }
        self.0.stream_state_changed.notify_waiters();
    }

    fn mark_awaits_stream(&self, mode: ResponseStreamMode) {
        *lock_failure_signal(&self.0.response_stream_mode) = Some(mode);
        self.0.stream_state_changed.notify_waiters();
    }

    pub(super) async fn wait_stream_failure(&self) -> QueryExecutionFailureKind {
        loop {
            let notified = self.0.stream_state_changed.notified();
            if let Some(kind) = self.stream_kind() {
                return kind;
            }
            notified.await;
        }
    }

    pub(super) fn stream_kind(&self) -> Option<QueryExecutionFailureKind> {
        match *lock_failure_signal(&self.0.response_stream_mode) {
            Some(ResponseStreamMode::Common(_generation)) => {
                *lock_failure_signal(&self.0.common_stream)
            }
            Some(ResponseStreamMode::Direct(generation)) => {
                match *lock_failure_signal(&self.0.direct_stream) {
                    Some((seen, kind)) if seen == generation => Some(kind),
                    Some(_) | None => None,
                }
            }
            None => None,
        }
    }

    fn record_status(&self, status: reqwest::StatusCode) {
        self.record(match status.as_u16() {
            401 => QueryExecutionFailureKind::Authentication,
            403 => QueryExecutionFailureKind::PermissionDenied,
            429 => QueryExecutionFailureKind::RateLimited,
            500..=599 => QueryExecutionFailureKind::UpstreamUnavailable,
            _ => QueryExecutionFailureKind::Execution,
        });
    }

    fn record_reqwest(&self, error: &reqwest::Error) {
        if error.is_timeout() {
            self.record(QueryExecutionFailureKind::Timeout);
        } else if let Some(status) = error.status() {
            self.record_status(status);
        } else {
            self.record(QueryExecutionFailureKind::UpstreamUnavailable);
        }
    }

    fn record_stream_status(&self, origin: StreamOrigin, status: reqwest::StatusCode) {
        self.record_stream(origin, status_failure_kind(status));
    }

    fn record_stream_reqwest(&self, origin: StreamOrigin, error: &reqwest::Error) {
        self.record_stream(origin, reqwest_failure_kind(error));
    }
}

fn status_failure_kind(status: reqwest::StatusCode) -> QueryExecutionFailureKind {
    match status.as_u16() {
        401 => QueryExecutionFailureKind::Authentication,
        403 => QueryExecutionFailureKind::PermissionDenied,
        429 => QueryExecutionFailureKind::RateLimited,
        500..=599 => QueryExecutionFailureKind::UpstreamUnavailable,
        _ => QueryExecutionFailureKind::Execution,
    }
}

fn reqwest_failure_kind(error: &reqwest::Error) -> QueryExecutionFailureKind {
    if error.is_timeout() {
        QueryExecutionFailureKind::Timeout
    } else if let Some(status) = error.status() {
        status_failure_kind(status)
    } else {
        QueryExecutionFailureKind::UpstreamUnavailable
    }
}

fn stream_failure_kind(error: &StreamableHttpError<reqwest::Error>) -> QueryExecutionFailureKind {
    match error {
        StreamableHttpError::Client(error) => reqwest_failure_kind(error),
        StreamableHttpError::AuthRequired(_) => QueryExecutionFailureKind::Authentication,
        StreamableHttpError::InsufficientScope(_) => QueryExecutionFailureKind::PermissionDenied,
        StreamableHttpError::SessionExpired => QueryExecutionFailureKind::UpstreamUnavailable,
        StreamableHttpError::Io(error) => error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<QueryExecutionFailureKind>())
            .copied()
            .unwrap_or(QueryExecutionFailureKind::UpstreamUnavailable),
        _ => QueryExecutionFailureKind::InvalidResponse,
    }
}

fn lock_failure_signal<T>(signal: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match signal.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[derive(Clone, Debug, Default)]
pub(super) struct HttpOperationTracker(Arc<AtomicUsize>);

impl HttpOperationTracker {
    fn begin(&self) -> ActiveHttpOperation {
        self.0.fetch_add(1, Ordering::SeqCst);
        ActiveHttpOperation(self.clone())
    }

    #[cfg(test)]
    pub(super) fn active(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    pub(super) async fn wait_idle_until(&self, deadline: tokio::time::Instant) -> bool {
        while self.0.load(Ordering::SeqCst) != 0 {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::task::yield_now().await;
        }
        true
    }
}

struct ActiveHttpOperation(HttpOperationTracker);

impl Drop for ActiveHttpOperation {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn stopped_http_error(kind: QueryExecutionFailureKind) -> StreamableHttpError<reqwest::Error> {
    StreamableHttpError::Io(std::io::Error::other(kind))
}

fn into_status_error(response: reqwest::Response) -> StreamableHttpError<reqwest::Error> {
    match response.error_for_status() {
        Ok(_response) => StreamableHttpError::UnexpectedServerResponse(Cow::Borrowed(
            "non-success response passed HTTP status validation",
        )),
        Err(error) => StreamableHttpError::Client(error),
    }
}

async fn wait_until_stopped(controls: QueryExecutionControls) -> QueryExecutionFailureKind {
    match controls
        .run_until_stopped(future::pending::<std::convert::Infallible>())
        .await
    {
        Err(kind) => kind,
        Ok(never) => match never {},
    }
}

fn is_cancelled_notification(message: &ClientJsonRpcMessage) -> bool {
    matches!(
        message,
        JsonRpcMessage::Notification(notification)
            if matches!(
                notification.notification,
                ClientNotification::CancelledNotification(_)
            )
    )
}

fn client_response_id(message: &ClientJsonRpcMessage) -> Option<&RequestId> {
    match message {
        JsonRpcMessage::Response(response) => Some(&response.id),
        JsonRpcMessage::Error(error) => error.id.as_ref(),
        JsonRpcMessage::Request(_) | JsonRpcMessage::Notification(_) => None,
    }
}

fn tracks_client_operation_failure(message: &ClientJsonRpcMessage) -> bool {
    matches!(message, JsonRpcMessage::Request(_))
        || matches!(
            message,
            JsonRpcMessage::Notification(notification)
                if matches!(
                    notification.notification,
                    ClientNotification::InitializedNotification(_)
                )
        )
}

fn accepted_content_types() -> String {
    [EVENT_STREAM_MIME_TYPE, JSON_MIME_TYPE].join(", ")
}

fn apply_custom_headers(
    mut request: reqwest::RequestBuilder,
    custom_headers: HashMap<HeaderName, HeaderValue>,
) -> Result<reqwest::RequestBuilder, StreamableHttpError<reqwest::Error>> {
    for (name, value) in custom_headers {
        validate_custom_header(&name).map_err(StreamableHttpError::ReservedHeaderConflict)?;
        request = request.header(name, value);
    }
    Ok(request)
}

fn validate_custom_header(name: &HeaderName) -> Result<(), String> {
    let reserved = ["accept", HEADER_SESSION_ID, HEADER_LAST_EVENT_ID];
    if reserved
        .iter()
        .any(|reserved| name.as_str().eq_ignore_ascii_case(reserved))
        && !name
            .as_str()
            .eq_ignore_ascii_case(HEADER_MCP_PROTOCOL_VERSION)
    {
        return Err(name.to_string());
    }
    Ok(())
}

fn extract_scope(header: &str) -> Option<String> {
    let lower = header.to_ascii_lowercase();
    let start = lower.find("scope=")?.saturating_add("scope=".len());
    let value = header.get(start..)?;
    if let Some(quoted) = value.strip_prefix('"') {
        return quoted
            .find('"')
            .and_then(|end| quoted.get(..end))
            .map(ToString::to_string);
    }
    let end = value
        .find(|character: char| character == ',' || character == ';' || character.is_whitespace())
        .unwrap_or(value.len());
    value
        .get(..end)
        .filter(|scope| !scope.is_empty())
        .map(ToString::to_string)
}

fn response_content_type(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(reqwest::header::CONTENT_TYPE)
        .map(|value| String::from_utf8_lossy(value.as_bytes()).to_string())
}

fn response_session_id(response: &reqwest::Response) -> Option<String> {
    response
        .headers()
        .get(HEADER_SESSION_ID)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string)
}

fn validate_event_stream_content_type(
    response: &reqwest::Response,
) -> Result<(), StreamableHttpError<reqwest::Error>> {
    let content_type = response_content_type(response);
    if content_type.as_deref().is_some_and(|value| {
        value
            .as_bytes()
            .starts_with(EVENT_STREAM_MIME_TYPE.as_bytes())
    }) {
        return Ok(());
    }
    Err(StreamableHttpError::UnexpectedContentType(content_type))
}

struct BoundedMcpBody {
    bytes: Vec<u8>,
    truncated: bool,
}

async fn read_bounded_mcp_body(
    mut response: reqwest::Response,
) -> Result<BoundedMcpBody, StreamableHttpError<reqwest::Error>> {
    if response.content_length().is_some_and(|length| {
        length > u64::try_from(CONTROLLED_RESPONSE_BODY_LIMIT_BYTES).unwrap_or(u64::MAX)
    }) {
        return Ok(BoundedMcpBody {
            bytes: Vec::new(),
            truncated: true,
        });
    }
    let read_limit = CONTROLLED_RESPONSE_BODY_LIMIT_BYTES.saturating_add(1);
    let mut bytes = Vec::new();
    while bytes.len() < read_limit {
        let chunk = response
            .chunk()
            .await
            .map_err(StreamableHttpError::Client)?;
        let Some(chunk) = chunk else {
            break;
        };
        let remaining = read_limit.saturating_sub(bytes.len());
        let take = chunk.len().min(remaining);
        bytes.extend(chunk.iter().copied().take(take));
        if take < chunk.len() {
            break;
        }
    }
    let truncated = bytes.len() > CONTROLLED_RESPONSE_BODY_LIMIT_BYTES;
    bytes.truncate(CONTROLLED_RESPONSE_BODY_LIMIT_BYTES);
    Ok(BoundedMcpBody { bytes, truncated })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequential_server_pings_are_allowed_after_matching_response_completes() {
        let failures = HttpFailureSignal::default();
        failures.begin_request(RequestId::Number(1));
        let initialize_response: ServerJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 1,
            "result": {}
        }))
        .expect("initialize response");
        assert!(failures.accept_response(&initialize_response));
        failures.begin_request(RequestId::Number(2));
        let first: ServerJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 7,
            "method": "ping"
        }))
        .expect("first ping");
        let second: ServerJsonRpcMessage = serde_json::from_value(serde_json::json!({
            "jsonrpc": "2.0",
            "id": 8,
            "method": "ping"
        }))
        .expect("second ping");
        let JsonRpcMessage::Request(first) = first else {
            panic!("expected first ping request");
        };
        let JsonRpcMessage::Request(second) = second else {
            panic!("expected second ping request");
        };

        assert!(failures.allow_server_request(&first.request, &first.id));
        assert!(!failures.allow_server_request(&second.request, &second.id));
        failures.complete_server_request(&first.id);
        assert!(failures.allow_server_request(&second.request, &second.id));
    }
}
