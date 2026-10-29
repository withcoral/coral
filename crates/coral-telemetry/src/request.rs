//! Server spans for inbound requests.
//!
//! A listener wraps its routes with [`trace_http_routes`] or
//! [`trace_grpc_routes`]. Each request to them then opens one server span
//! under [`REQUEST_TARGET`], which continues a valid inbound W3C trace and
//! carries only safe fields: never headers, bodies, queries, cookies, the full
//! URL, gRPC metadata or messages.

use std::pin::Pin;
use std::task::{Context, Poll};

use axum::Router;
use axum::body::Body;
use axum::extract::{MatchedPath, Request};
use axum::middleware::Next;
use axum::response::Response;
use http_body::{Frame, SizeHint};
use opentelemetry::trace::TraceContextExt as _;
use tracing::Instrument as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

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
/// caller's.
pub fn trace_http_routes<S>(router: Router<S>) -> Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(axum::middleware::from_fn(trace_http_request))
}

/// Opens a gRPC server span for each call to the routes `router` has so far.
///
/// A served method names the span, which stays open until the response body
/// ends. The call's status arrives in the response headers or in the trailers
/// after the body. A stream the caller cuts off before its end records
/// CANCELLED, and one that ends or breaks off without a status records
/// UNKNOWN. A response no gRPC handler started keeps the method `_OTHER`.
/// Only the server-fault statuses fail the span.
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

async fn trace_http_request(mut request: Request, next: Next) -> Response {
    let method = http_method(request.method());
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str);
    let span = tracing::info_span!(
        target: REQUEST_TARGET,
        parent: None,
        "coral.http.request",
        otel.name = http_span_name(method, route),
        otel.kind = "server",
        http.request.method = method,
        http.route = route,
        http.response.status_code = tracing::field::Empty,
        error.type = tracing::field::Empty,
    );
    set_parent_from_http_headers(&span, request.headers());
    request.extensions_mut().insert(RequestSpan(span.clone()));
    let response = next.run(request).instrument(span.clone()).await;
    let status = response.status();
    span.record("http.response.status_code", i64::from(status.as_u16()));
    if status.is_server_error() {
        record_failure(&span, status.as_str());
    }
    response.map(|body| {
        Body::new(TracedBody {
            body,
            span,
            grpc_path: None,
        })
    })
}

/// The exported name of an HTTP server span: `{method} {route}`, the method
/// alone without a route, and `HTTP` for a method outside the standard set.
fn http_span_name(method: &'static str, route: Option<&str>) -> String {
    let method = if method == "_OTHER" { "HTTP" } else { method };
    route.map_or_else(|| method.to_owned(), |route| format!("{method} {route}"))
}

/// The request method as a span field: a standard method's name, else
/// `_OTHER`, so a caller cannot choose the text.
fn http_method(method: &http::Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "HEAD" => "HEAD",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "CONNECT" => "CONNECT",
        "OPTIONS" => "OPTIONS",
        "TRACE" => "TRACE",
        "PATCH" => "PATCH",
        _ => "_OTHER",
    }
}

async fn trace_grpc_request(mut request: Request, next: Next) -> Response {
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
            record_grpc_status(&span, Some(&path), code);
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
            record_grpc_status(&span, None, code);
            None
        }
    };
    response.map(|body| {
        Body::new(TracedBody {
            body,
            span,
            grpc_path,
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
fn record_grpc_status(span: &tracing::Span, path: Option<&str>, code: i32) {
    let method = path
        .and_then(|path| path.strip_prefix('/'))
        .filter(|method| !method.is_empty());
    match method {
        Some(method) if code != GRPC_UNIMPLEMENTED => {
            // The span has started by now, and a started span takes its new
            // name only through OpenTelemetry, not `otel.name`.
            span.context().span().update_name(method.to_owned());
            span.record("rpc.method", method);
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
    span.record("rpc.response.status_code", status);
    if GRPC_SERVER_ERRORS.contains(&code) {
        record_failure(span, status);
    }
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
                    record_grpc_status(&this.span, Some(&path), code);
                }
            }
            // The error's text may come from anywhere, so only its kind is kept.
            Poll::Ready(Some(Err(_error))) => match this.grpc_path.take() {
                Some(path) => record_grpc_status(&this.span, Some(&path), GRPC_UNKNOWN),
                None => record_failure(&this.span, "response_body"),
            },
            // A gRPC body that ends without a status broke the protocol.
            Poll::Ready(None) => {
                if let Some(path) = this.grpc_path.take() {
                    record_grpc_status(&this.span, Some(&path), GRPC_UNKNOWN);
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
            record_grpc_status(&self.span, Some(&path), GRPC_CANCELLED);
        }
    }
}

#[cfg(test)]
mod tests;
