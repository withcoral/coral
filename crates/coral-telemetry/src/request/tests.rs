use std::convert::Infallible;
use std::pin::Pin;
use std::task::{Context, Poll};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::response::IntoResponse as _;
use axum::routing::any;
use http::{HeaderMap, HeaderValue, Request, StatusCode};
use http_body::Frame;
use opentelemetry::baggage::BaggageExt as _;
use opentelemetry::trace::{SpanKind, Status, TraceId};
use opentelemetry_sdk::trace::SpanData;
use tower::ServiceExt as _;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use super::{trace_grpc_routes, trace_http_routes};
use crate::testing::{SpanCapture, attribute};

const ROUTE: &str = "/workspaces/{workspace_id}/mcp";

/// Sends `request` to `router` and returns the spans it finished, with the
/// response body.
async fn served(router: Router, request: Request<Body>) -> (Vec<SpanData>, Bytes) {
    let capture = SpanCapture::default();
    let _subscriber = tracing::subscriber::set_default(capture.subscriber());
    let response = router.oneshot(request).await.expect("response");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .expect("body");
    (capture.finished(), body)
}

/// An HTTP request span carries the method, route template and status, never
/// the path, query, headers or cookies. A server error fails it. Routes
/// added after the layer open no span.
#[tokio::test]
async fn http_request_spans_carry_only_safe_fields() {
    for (method, status, expected_method, expected_name, failed) in [
        (
            "GET",
            200,
            "GET",
            "GET /workspaces/{workspace_id}/mcp",
            false,
        ),
        (
            "BREW-CANARY",
            404,
            "_OTHER",
            "HTTP /workspaces/{workspace_id}/mcp",
            false,
        ),
        (
            "POST",
            503,
            "POST",
            "POST /workspaces/{workspace_id}/mcp",
            true,
        ),
    ] {
        let status = StatusCode::from_u16(status).expect("status");
        let router =
            trace_http_routes(Router::new().route(ROUTE, any(move || async move { status })));
        let request = Request::builder()
            .method(method)
            .uri("/workspaces/canary-path/mcp?query=canary-query")
            .header("cookie", "session=canary-cookie")
            .header("authorization", "Bearer canary-token")
            .body(Body::empty())
            .expect("request");
        let (spans, _body) = served(router, request).await;
        let [span] = spans.as_slice() else {
            panic!("{method}: one request span: {spans:?}");
        };
        assert_eq!(span.name, expected_name, "{method}");
        assert_eq!(span.span_kind, SpanKind::Server, "{method}");
        assert_eq!(
            attribute(span, "http.request.method").as_deref(),
            Some(expected_method),
            "{method}"
        );
        assert_eq!(attribute(span, "http.route").as_deref(), Some(ROUTE));
        assert_eq!(
            attribute(span, "http.response.status_code"),
            Some(status.as_u16().to_string()),
            "{method}"
        );
        assert_eq!(
            span.status == Status::error("HTTP request failed"),
            failed,
            "{method}"
        );
        assert!(
            !format!("{span:?}").contains("canary"),
            "{method}: {span:?}"
        );
    }

    let router = trace_http_routes(Router::new().route(ROUTE, any(|| async {})))
        .route("/healthz", any(|| async {}));
    let probe = Request::get("/healthz").body(Body::empty()).expect("probe");
    let (spans, _body) = served(router, probe).await;
    assert!(spans.is_empty(), "a probe opens no span: {spans:?}");
}

/// A listener continues only one well-formed inbound trace, keeps every
/// `tracestate` line, and never takes `baggage`.
#[tokio::test]
async fn requests_continue_only_one_valid_trace_and_never_baggage() {
    const PARENT: &str = "00-00000000000000000000000000000077-0000000000000088-01";
    let continued = TraceId::from_hex("00000000000000000000000000000077").expect("trace id");
    for (case, headers, continues, trace_state) in [
        ("one traceparent", vec![("traceparent", PARENT)], true, ""),
        (
            "tracestate lines join in order",
            vec![
                ("traceparent", PARENT),
                ("tracestate", "first=1"),
                ("tracestate", "second=2"),
            ],
            true,
            "first=1,second=2",
        ),
        (
            "two traceparents are invalid",
            vec![
                ("traceparent", PARENT),
                (
                    "traceparent",
                    "00-00000000000000000000000000000099-0000000000000088-01",
                ),
            ],
            false,
            "",
        ),
        (
            "a malformed traceparent is ignored",
            vec![("traceparent", "00-not-a-trace-01")],
            false,
            "",
        ),
        (
            "a malformed tracestate is dropped and the trace kept",
            vec![("traceparent", PARENT), ("tracestate", "not a list member")],
            true,
            "",
        ),
        (
            "baggage is never taken",
            vec![("traceparent", PARENT), ("baggage", "tenant=canary")],
            true,
            "",
        ),
    ] {
        // The handler answers with how much baggage its request context holds.
        let router = trace_http_routes(Router::new().route(
            "/",
            any(|| async {
                tracing::Span::current()
                    .context()
                    .baggage()
                    .len()
                    .to_string()
            }),
        ));
        let mut request = Request::get("/").body(Body::empty()).expect("request");
        for (name, value) in headers {
            request
                .headers_mut()
                .append(name, HeaderValue::from_static(value));
        }
        let (spans, baggage) = served(router, request).await;
        let request = spans.first().expect("request span");
        assert_eq!(
            request.span_context.trace_id() == continued,
            continues,
            "{case}"
        );
        assert_eq!(request.parent_span_is_remote, continues, "{case}");
        assert_eq!(
            request.span_context.trace_state().header(),
            trace_state,
            "{case}"
        );
        assert_eq!(baggage, "0", "{case}");
    }
}

/// How a gRPC handler sends its status.
#[derive(Debug, Clone, Copy)]
enum Delivery {
    /// A call that fails before any message sends its status in the headers.
    Headers,
    /// Any other call sends it in trailers after the body.
    Trailers,
}

/// A response body that is only trailers.
struct Trailers(Option<HeaderMap>);

impl http_body::Body for Trailers {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        Poll::Ready(
            self.get_mut()
                .0
                .take()
                .map(|trailers| Ok(Frame::trailers(trailers))),
        )
    }
}

/// A gRPC request span is named by its method, unless the call ended
/// UNIMPLEMENTED: then the caller chose the method, and it stays off. Only
/// the server-fault statuses fail the span, wherever the status arrives.
#[tokio::test]
async fn grpc_request_spans_name_only_served_methods() {
    const METHOD: &str = "coral.management.v1.WorkspaceService/UpdateWorkspace";
    for (path, code, delivery, name, method, status, failed) in [
        (METHOD, 0, Delivery::Trailers, METHOD, METHOD, "OK", false),
        (
            METHOD,
            3,
            Delivery::Headers,
            METHOD,
            METHOD,
            "INVALID_ARGUMENT",
            false,
        ),
        (
            METHOD,
            13,
            Delivery::Trailers,
            METHOD,
            METHOD,
            "INTERNAL",
            true,
        ),
        (
            "coral.management.v1.WorkspaceService/CanaryMethod",
            12,
            Delivery::Headers,
            "grpc",
            "_OTHER",
            "UNIMPLEMENTED",
            true,
        ),
    ] {
        let context = format!("{path} {code} {delivery:?}");
        let router = trace_grpc_routes(Router::new().route(
            "/{*method}",
            any(move || async move {
                let mut status = HeaderMap::new();
                status.insert("grpc-status", HeaderValue::from(code));
                match delivery {
                    Delivery::Headers => (status, Body::empty()).into_response(),
                    Delivery::Trailers => Body::new(Trailers(Some(status))).into_response(),
                }
            }),
        ));
        let request = Request::post(format!("/{path}"))
            .header("grpc-metadata-canary", "Canary")
            .body(Body::empty())
            .expect("request");
        let (spans, _body) = served(router, request).await;
        let [span] = spans.as_slice() else {
            panic!("{context}: one request span: {spans:?}");
        };
        assert_eq!(span.name, name, "{context}");
        assert_eq!(span.span_kind, SpanKind::Server, "{context}");
        assert_eq!(attribute(span, "rpc.system.name").as_deref(), Some("grpc"));
        assert_eq!(
            attribute(span, "rpc.method").as_deref(),
            Some(method),
            "{context}"
        );
        assert_eq!(
            attribute(span, "rpc.response.status_code").as_deref(),
            Some(status),
            "{context}"
        );
        assert_eq!(
            span.status == Status::error("gRPC request failed"),
            failed,
            "{context}"
        );
        assert_eq!(
            attribute(span, "error.type").as_deref(),
            failed.then_some(status),
            "{context}"
        );
        assert!(
            !format!("{span:?}").contains("Canary"),
            "{context}: {span:?}"
        );
    }
}
