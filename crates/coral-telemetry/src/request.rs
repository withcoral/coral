//! Server spans for inbound requests.
//!
//! A listener wraps its routes with [`trace_http_routes`] or
//! [`trace_grpc_routes`]. Each request to them then opens one server span
//! under [`REQUEST_TARGET`], which continues a valid inbound W3C trace and
//! carries only safe fields: never headers, bodies, queries, cookies, the full
//! URL, gRPC metadata or messages.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Instant;

use axum::Router;
use axum::body::Body;
use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use http_body::{Frame, SizeHint};
use opentelemetry::trace::TraceContextExt as _;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::metric::{Interface, Metrics, RequestOutcome};
use crate::safe::{self, Identifier};
use crate::{record_failure, set_parent_from_http_headers};

/// The target of every request span, for `trace_filter`.
pub const REQUEST_TARGET: &str = "coral.request";

const GRPC_STATUS: &str = "grpc-status";

/// gRPC status names, indexed by code.
const GRPC_STATUS_NAMES: [&str; 17] = [
    "OK",
    "CANCELLED",
    "UNKNOWN",
    "INVALID_ARGUMENT",
    "DEADLINE_EXCEEDED",
    "NOT_FOUND",
    "ALREADY_EXISTS",
    "PERMISSION_DENIED",
    "RESOURCE_EXHAUSTED",
    "FAILED_PRECONDITION",
    "ABORTED",
    "OUT_OF_RANGE",
    "UNIMPLEMENTED",
    "INTERNAL",
    "UNAVAILABLE",
    "DATA_LOSS",
    "UNAUTHENTICATED",
];

/// Codes that mark a server span failed: the server's faults, not the caller's.
const GRPC_SERVER_ERRORS: [i32; 6] = [2, 4, 12, 13, 14, 15];

const GRPC_CANCELLED: i32 = 1;
const GRPC_UNKNOWN: i32 = 2;
const GRPC_UNIMPLEMENTED: i32 = 12;

/// Opens an HTTP server span for each request to the routes `router` has so
/// far. Routes added after, such as health probes, open none.
///
/// The span is named `{method} {route}` after the matched route template,
/// never the request path, and records the response status. It stays open
/// until the response body ends, so a stream's span covers the stream. A
/// server error or a body that breaks off fails it; a client error is the
/// caller's. Each request's duration is recorded under `interface`, by its
/// route template and outcome, when its span ends.
pub fn trace_http_routes<S>(router: Router<S>, interface: Interface) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(axum::middleware::from_fn(
        move |request: Request, next: Next| trace_http_request(request, next, interface),
    ))
}

/// Opens a gRPC server span for each call to the routes `router` has so far.
///
/// A served method names the span, which stays open until the response body
/// ends. The call's status arrives in the response headers or in the trailers
/// after the body. A stream the caller cuts off before its end records
/// CANCELLED, and one that ends or breaks off without a status records
/// UNKNOWN. A response no gRPC handler started keeps the method `_OTHER`.
/// Only the server-fault statuses fail the span. Each call's duration is
/// recorded by its method and outcome when its span ends.
pub fn trace_grpc_routes<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(axum::middleware::from_fn(trace_grpc_request))
}

/// The span of the traced request whose extensions are `extensions`.
///
/// Work that runs outside the request's task, such as an MCP session's tool
/// handlers, takes it as its parent explicitly.
#[must_use]
pub fn request_span(extensions: &http::Extensions) -> Option<&tracing::Span> {
    extensions.get::<RequestSpan>().map(|span| &span.0)
}

#[derive(Clone)]
struct RequestSpan(tracing::Span);

/// The operation a traced request's handler found it carried, shared by
/// every copy of the request's extensions.
#[derive(Clone, Default)]
struct RequestOperation(std::sync::Arc<std::sync::OnceLock<String>>);

/// Names the operation the traced request whose extensions are
/// `extensions` carried, `{operation} {target}`, such as `tools/call exec`
/// for an MCP tool call, so its `coral.request.duration` records that name in
/// place of its route template. `target` is a name Coral serves, never
/// caller text. The first name stands.
pub fn name_request_operation(
    extensions: &http::Extensions,
    operation: &'static str,
    target: Option<&'static str>,
) {
    if let Some(slot) = extensions.get::<RequestOperation>() {
        slot.0
            .get_or_init(|| safe::span_name(operation, target.as_ref()));
    }
}

/// A request's duration, recorded when its response body drops.
struct Duration {
    metrics: Option<Metrics>,
    interface: Interface,
    started: Instant,
    /// The route template or served method, else `_OTHER`.
    operation: String,
    /// The operation the request's handler named, which takes its place.
    named: RequestOperation,
    outcome: RequestOutcome,
}

impl Duration {
    fn start(interface: Interface) -> Self {
        Self {
            metrics: crate::metric::current(),
            interface,
            started: Instant::now(),
            operation: OTHER.to_owned(),
            named: RequestOperation::default(),
            outcome: RequestOutcome::Ok,
        }
    }
}

impl Drop for Duration {
    fn drop(&mut self) {
        if let Some(metrics) = &self.metrics {
            metrics.request_duration(
                self.interface,
                self.named
                    .0
                    .get()
                    .cloned()
                    .unwrap_or_else(|| std::mem::take(&mut self.operation)),
                self.outcome,
                self.started.elapsed(),
            );
        }
    }
}

const OTHER: &str = "_OTHER";

async fn trace_http_request(mut request: Request, next: Next, interface: Interface) -> Response {
    let mut duration = Duration::start(interface);
    let method = request.method().clone();
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|path| Route(path.as_str()));
    let span = tracing::info_span!(
        target: REQUEST_TARGET,
        parent: None,
        "coral.http.request",
        otel.name = safe::http_span_name(&method, route.as_ref()),
        otel.kind = "server",
        http.request.method = safe::http_method(&method),
        http.route = route.as_ref().map(safe::id),
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
    );
    if let Some(route) = &route {
        route.0.clone_into(&mut duration.operation);
    }
    set_parent_from_http_headers(&span, request.headers());
    request.extensions_mut().insert(RequestSpan(span.clone()));
    request.extensions_mut().insert(duration.named.clone());
    let response = next.run(request).instrument(span.clone()).await;
    let status = response.status();
    span.record("http.response.status_code", safe::number(status.as_u16()));
    if status.is_server_error() {
        record_failure(&span, safe::http_status_code(status.as_u16()));
        duration.outcome = RequestOutcome::ServerError;
    } else if status.is_client_error() {
        duration.outcome = RequestOutcome::ClientError;
    }
    response.map(|body| {
        Body::new(TracedBody {
            body,
            span,
            grpc_path: None,
            duration,
        })
    })
}

/// The route template a request matched.
struct Route<'a>(&'a str);

/// The template of a route Coral registered, such as `/oauth/{id}`, never
/// the request's path.
impl Identifier for Route<'_> {}

impl std::fmt::Display for Route<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

/// The method of a gRPC call a gRPC handler answered, `package.Service/Method`.
struct ServedGrpcMethod<'a>(&'a str);

/// A method Coral serves: only a gRPC handler's answer makes one.
impl Identifier for ServedGrpcMethod<'_> {}

impl std::fmt::Display for ServedGrpcMethod<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.0)
    }
}

async fn trace_grpc_request(mut request: Request, next: Next) -> Response {
    let mut duration = Duration::start(Interface::Grpc);
    let span = tracing::info_span!(
        target: REQUEST_TARGET,
        parent: None,
        "coral.grpc.request",
        otel.name = "grpc",
        otel.kind = "server",
        rpc.system.name = "grpc",
        rpc.method = tracing::field::Empty,
        rpc.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
    );
    // The parent must be set before the handler enters the span and so starts it.
    set_parent_from_http_headers(&span, request.headers());
    request.extensions_mut().insert(RequestSpan(span.clone()));
    let path = request.uri().path().to_owned();
    let response = next.run(request).instrument(span.clone()).await;
    let grpc_path = match grpc_status(response.headers()) {
        Some(code) => {
            record_grpc_status(&span, &mut duration, Some(&path), code);
            None
        }
        // A gRPC handler started this response, so its method is served and
        // its status follows in the trailers.
        None if response.status() == http::StatusCode::OK => Some(path),
        // No gRPC handler answered, so the caller chose the path.
        None => {
            let code = if response.status() == http::StatusCode::NOT_FOUND {
                GRPC_UNIMPLEMENTED
            } else {
                GRPC_UNKNOWN
            };
            record_grpc_status(&span, &mut duration, None, code);
            None
        }
    };
    response.map(|body| {
        Body::new(TracedBody {
            body,
            span,
            grpc_path,
            duration,
        })
    })
}

fn grpc_status(headers: &http::HeaderMap) -> Option<i32> {
    headers.get(GRPC_STATUS)?.to_str().ok()?.parse().ok()
}

/// Records how a gRPC call ended. `path` is the request path,
/// `/package.Service/Method`, when a gRPC handler answered it. Without one,
/// or for a call that ended UNIMPLEMENTED, the method is `_OTHER`, since the
/// caller chose it.
fn record_grpc_status(
    span: &tracing::Span,
    duration: &mut Duration,
    path: Option<&str>,
    code: i32,
) {
    let method = path
        .and_then(|path| path.strip_prefix('/'))
        .filter(|method| !method.is_empty());
    match method {
        Some(method) if code != GRPC_UNIMPLEMENTED => {
            let method = ServedGrpcMethod(method);
            // The span has started by now, and a started span takes its new
            // name only through OpenTelemetry, not `otel.name`.
            span.context().span().update_name(method.to_string());
            span.record("rpc.method", safe::id(&method));
            method.0.clone_into(&mut duration.operation);
        }
        _ => {
            span.record("rpc.method", "_OTHER");
        }
    }
    let status = usize::try_from(code)
        .ok()
        .and_then(|code| GRPC_STATUS_NAMES.get(code))
        .copied()
        .unwrap_or("UNKNOWN");
    span.record("rpc.response.status_code", safe::kind(status));
    duration.outcome = if code == 0 {
        RequestOutcome::Ok
    } else if GRPC_SERVER_ERRORS.contains(&code) {
        record_failure(span, status);
        RequestOutcome::ServerError
    } else {
        RequestOutcome::ClientError
    };
}

/// A response body that holds its request's span open until it is dropped,
/// and fails the span if it breaks off. A gRPC body that ends without its
/// status records one.
struct TracedBody {
    body: Body,
    span: tracing::Span,
    /// The request path of a gRPC call whose status is still to come in the
    /// trailers.
    grpc_path: Option<String>,
    /// Recorded when the body drops, as the span ends.
    duration: Duration,
}

impl http_body::Body for TracedBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        let _entered = this.span.enter();
        let frame = Pin::new(&mut this.body).poll_frame(context);
        match &frame {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(code) = frame.trailers_ref().and_then(grpc_status)
                    && let Some(path) = this.grpc_path.take()
                {
                    record_grpc_status(&this.span, &mut this.duration, Some(&path), code);
                }
            }
            // The error's text may come from anywhere, so only its kind is kept.
            Poll::Ready(Some(Err(_error))) => {
                if let Some(path) = this.grpc_path.take() {
                    record_grpc_status(&this.span, &mut this.duration, Some(&path), GRPC_UNKNOWN);
                } else {
                    record_failure(&this.span, "response_body");
                    this.duration.outcome = RequestOutcome::ServerError;
                }
            }
            // A gRPC body that ends without a status broke the protocol.
            Poll::Ready(None) => {
                if let Some(path) = this.grpc_path.take() {
                    record_grpc_status(&this.span, &mut this.duration, Some(&path), GRPC_UNKNOWN);
                }
            }
            Poll::Pending => {}
        }
        frame
    }

    fn is_end_stream(&self) -> bool {
        self.body.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.body.size_hint()
    }
}

impl Drop for TracedBody {
    fn drop(&mut self) {
        // A gRPC stream dropped before its end was cut off by the caller.
        if let Some(path) = self.grpc_path.take() {
            record_grpc_status(&self.span, &mut self.duration, Some(&path), GRPC_CANCELLED);
        }
    }
}

#[cfg(test)]
mod tests;
