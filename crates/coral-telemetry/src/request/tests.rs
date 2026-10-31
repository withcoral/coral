use std::collections::VecDeque;
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

use tracing_subscriber::layer::SubscriberExt as _;

use super::{trace_grpc_routes, trace_http_routes};
use crate::metric::Interface;
use crate::testing::{MetricCapture, MetricPoint, SpanCapture, attribute};

const ROUTE: &str = "/workspaces/{workspace_id}/mcp";

#[tokio::test]
async fn request_cpu_is_attributed_to_the_named_operation_without_counting_awaits() {
    let metrics = MetricCapture::default();
    let _subscriber = tracing::subscriber::set_default(metrics.subscriber());
    let router = trace_http_routes(
        Router::new().route(
            ROUTE,
            any(|request: Request<Body>| async move {
                super::name_request_operation(request.extensions(), "tools/call", Some("exec"));
                let until = std::time::Instant::now() + std::time::Duration::from_millis(2);
                while std::time::Instant::now() < until {
                    std::hint::black_box(42_u64.wrapping_mul(13));
                }
                tokio::time::sleep(std::time::Duration::from_millis(40)).await;
                "done"
            }),
        ),
        Interface::Mcp,
    );
    let response = router
        .oneshot(
            Request::get("/workspaces/private-id/mcp")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    axum::body::to_bytes(response.into_body(), 1024)
        .await
        .unwrap();
    let cpu = metrics.named("coral.request.cpu.duration");
    if !cfg!(any(target_os = "linux", target_os = "macos")) {
        assert!(cpu.is_empty(), "unsupported CPU clocks omit the signal");
        assert!(!metrics.named("coral.request.duration").is_empty());
        return;
    }
    assert_eq!(cpu.len(), 1, "one CPU measurement per completed request");
    let cpu = cpu.first().expect("CPU sample");
    assert_eq!(cpu.count, 1);
    assert_eq!(
        cpu.attributes.get("operation").map(String::as_str),
        Some("tools/call exec")
    );
    assert!(cpu.value > 0.0);
    let wall = metrics.named("coral.request.duration");
    let wall = wall.first().expect("wall duration");
    assert!(
        cpu.value < wall.value / 2.0,
        "await time is not CPU time: {cpu:?} {wall:?}"
    );
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
#[tokio::test]
async fn canceled_handlers_keep_cpu_on_the_span_and_metric() {
    use std::future::Future as _;
    for grpc in [false, true] {
        let spans = SpanCapture::default();
        let metrics = MetricCapture::default();
        let _subscriber = tracing::subscriber::set_default(
            tracing_subscriber::Registry::default()
                .with(spans.layer())
                .with(metrics.layer()),
        );
        let router = Router::new().route(
            "/service/Call",
            any(|| async {
                let until = std::time::Instant::now() + std::time::Duration::from_millis(2);
                while std::time::Instant::now() < until {
                    std::hint::black_box(42_u64.wrapping_mul(13));
                }
                std::future::pending::<&'static str>().await
            }),
        );
        let router = if grpc {
            trace_grpc_routes(router)
        } else {
            trace_http_routes(router, Interface::Mcp)
        };
        let mut request =
            Box::pin(router.oneshot(Request::get("/service/Call").body(Body::empty()).unwrap()));
        std::future::poll_fn(|context| {
            assert!(request.as_mut().poll(context).is_pending());
            Poll::Ready(())
        })
        .await;
        drop(request);
        assert!(
            metrics
                .values("coral.request.cpu.duration")
                .iter()
                .any(|cpu| *cpu > 0.0)
        );
        let recorded = spans.finished();
        let request = recorded.first().expect("canceled request span closes");
        let cpu =
            attribute(request, "coral.request.cpu_ns").and_then(|value| value.parse::<u64>().ok());
        assert!(
            cpu.is_some_and(|cpu| cpu > 0),
            "canceled handler keeps its CPU evidence: {request:?}"
        );
    }
}

/// Sends `request` to `router`, reads at most `frames` frames of the
/// response body, then drops it, as a caller that hangs up would. Returns the
/// spans that finished, with the data read or the body's error.
async fn served(
    router: Router,
    request: Request<Body>,
    frames: usize,
) -> (Vec<SpanData>, Result<Bytes, axum::Error>) {
    let (spans, body, _durations) = measured(router, request, frames).await;
    (spans, body)
}

/// Serves `request` as [`served`] does, and also returns the request
/// duration points it recorded.
async fn measured(
    router: Router,
    request: Request<Body>,
    frames: usize,
) -> (Vec<SpanData>, Result<Bytes, axum::Error>, Vec<MetricPoint>) {
    let capture = SpanCapture::default();
    let metrics = MetricCapture::default();
    let _subscriber = tracing::subscriber::set_default(
        tracing_subscriber::Registry::default()
            .with(capture.layer())
            .with(metrics.layer()),
    );
    let response = router.oneshot(request).await.expect("response");
    let mut body = response.into_body();
    let mut data = Vec::new();
    let mut read = Ok(());
    for _ in 0..frames {
        let frame = std::future::poll_fn(|context| {
            http_body::Body::poll_frame(Pin::new(&mut body), context)
        })
        .await;
        match frame {
            Some(Ok(frame)) => {
                if let Ok(bytes) = frame.into_data() {
                    data.extend_from_slice(&bytes);
                }
            }
            Some(Err(error)) => {
                read = Err(error);
                break;
            }
            None => break,
        }
    }
    drop(body);
    (
        capture.finished(),
        read.map(|()| Bytes::from(data)),
        metrics.named("coral.request.duration"),
    )
}

/// The one request duration point in `points`, as its interface, operation
/// and outcome, and its count.
fn duration(points: &[MetricPoint]) -> Option<(String, String, String, u64)> {
    let [point] = points else {
        return None;
    };
    let attribute = |key: &str| point.attributes.get(key).cloned().unwrap_or_default();
    Some((
        attribute("interface"),
        attribute("operation"),
        attribute("outcome"),
        point.count,
    ))
}

/// An HTTP request span carries the method, route template and status, never
/// the path, query, headers or cookies. A server error fails it. Its
/// duration is recorded by interface, route template and outcome. Routes
/// added after the layer open no span.
#[tokio::test]
async fn http_request_spans_carry_only_safe_fields() {
    for (method, status, expected_method, expected_name, failed, outcome) in [
        (
            "GET",
            200,
            "GET",
            "GET /workspaces/{workspace_id}/mcp",
            false,
            "ok",
        ),
        (
            "BREW-CANARY",
            404,
            "_OTHER",
            "HTTP /workspaces/{workspace_id}/mcp",
            false,
            "client_error",
        ),
        (
            "POST",
            503,
            "POST",
            "POST /workspaces/{workspace_id}/mcp",
            true,
            "server_error",
        ),
    ] {
        let status = StatusCode::from_u16(status).expect("status");
        let router = trace_http_routes(
            Router::new().route(ROUTE, any(move || async move { status })),
            Interface::Mcp,
        );
        let request = Request::builder()
            .method(method)
            .uri("/workspaces/canary-path/mcp?query=canary-query")
            .header("cookie", "session=canary-cookie")
            .header("authorization", "Bearer canary-token")
            .body(Body::empty())
            .expect("request");
        let (spans, _body, durations) = measured(router, request, usize::MAX).await;
        let [span] = spans.as_slice() else {
            panic!("{method}: one request span: {spans:?}");
        };
        assert_eq!(
            duration(&durations),
            Some(("mcp".into(), ROUTE.into(), outcome.into(), 1)),
            "{method}"
        );
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
        assert_eq!(span.status == Status::error(""), failed, "{method}");
        assert!(
            !format!("{span:?}").contains("canary"),
            "{method}: {span:?}"
        );
    }

    let router = trace_http_routes(Router::new().route(ROUTE, any(|| async {})), Interface::Mcp)
        .route("/healthz", any(|| async {}));
    let probe = Request::get("/healthz").body(Body::empty()).expect("probe");
    let (spans, _body) = served(router, probe, usize::MAX).await;
    assert!(spans.is_empty(), "a probe opens no span: {spans:?}");
}

/// An HTTP request span stays open while its response body streams, as an
/// MCP event stream does, and a body that breaks off fails it.
#[tokio::test]
async fn http_request_spans_last_until_their_body_ends() {
    for (case, breaks_off) in [
        ("a body that ends", false),
        ("a body that breaks off", true),
    ] {
        let capture = SpanCapture::default();
        let _subscriber = tracing::subscriber::set_default(capture.subscriber());
        let router = trace_http_routes(
            Router::new().route(
                "/",
                any(move || async move {
                    let event = Ok(Frame::data(Bytes::from_static(b"event")));
                    let failure = breaks_off.then(|| Err(std::io::Error::other("canary")));
                    Frames::body([event].into_iter().chain(failure))
                }),
            ),
            Interface::Mcp,
        );
        let request = Request::get("/").body(Body::empty()).expect("request");
        let response = router.oneshot(request).await.expect("response");
        assert!(
            capture.finished().is_empty(),
            "{case}: the span is open while the body streams"
        );
        let _body = axum::body::to_bytes(response.into_body(), usize::MAX).await;
        let spans = capture.finished();
        let [span] = spans.as_slice() else {
            panic!("{case}: one request span: {spans:?}");
        };
        assert_eq!(span.status == Status::error(""), breaks_off, "{case}");
        assert_eq!(
            attribute(span, "error.type").as_deref(),
            breaks_off.then_some("response_body"),
            "{case}"
        );
        assert!(!format!("{span:?}").contains("canary"), "{case}: {span:?}");
    }
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
        let router = trace_http_routes(
            Router::new().route(
                "/",
                any(|| async {
                    tracing::Span::current()
                        .context()
                        .baggage()
                        .len()
                        .to_string()
                }),
            ),
            Interface::Mcp,
        );
        let mut request = Request::get("/").body(Body::empty()).expect("request");
        for (name, value) in headers {
            request
                .headers_mut()
                .append(name, HeaderValue::from_static(value));
        }
        let (spans, baggage) = served(router, request, usize::MAX).await;
        let baggage = baggage.expect("body");
        let [request] = spans.as_slice() else {
            panic!("{case}: one request span: {spans:?}");
        };
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
    /// The caller cuts a stream off before its end, so no status arrives.
    CutOff,
    /// The stream ends without a status.
    NoStatus,
    /// The stream breaks off with an error before its status.
    BrokenOff,
    /// No gRPC handler answers: a plain 404 without a status.
    NotGrpc,
}

/// A response body that yields the given frames, then ends.
struct Frames(VecDeque<Result<Frame<Bytes>, std::io::Error>>);

impl Frames {
    fn body(frames: impl IntoIterator<Item = Result<Frame<Bytes>, std::io::Error>>) -> Body {
        Body::new(Self(frames.into_iter().collect()))
    }
}

impl http_body::Body for Frames {
    type Data = Bytes;
    type Error = std::io::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, std::io::Error>>> {
        Poll::Ready(self.get_mut().0.pop_front())
    }
}

/// A gRPC request span is named by its method, unless the call ended
/// UNIMPLEMENTED: then the caller chose the method, and it stays off. Only
/// the server-fault statuses fail the span, wherever the status arrives. A
/// stream that ends early still names its method and records a status, but a
/// response no gRPC handler started never names one.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of how a gRPC call's status arrives, or does not"
)]
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
        // No code arrives in these; the span records one.
        (
            METHOD,
            0,
            Delivery::CutOff,
            METHOD,
            METHOD,
            "CANCELLED",
            false,
        ),
        (
            METHOD,
            0,
            Delivery::NoStatus,
            METHOD,
            METHOD,
            "UNKNOWN",
            true,
        ),
        (
            METHOD,
            0,
            Delivery::BrokenOff,
            METHOD,
            METHOD,
            "UNKNOWN",
            true,
        ),
        (
            "coral.management.v1.CanaryService/Method",
            0,
            Delivery::NotGrpc,
            "grpc",
            "_OTHER",
            "UNIMPLEMENTED",
            true,
        ),
    ] {
        // A call's duration takes its method as the span does; a server
        // fault is the server's error, any other status but OK the caller's.
        let outcome = match (status, failed) {
            ("OK", _) => "ok",
            (_, true) => "server_error",
            (_, false) => "client_error",
        };
        let context = format!("{path} {code} {delivery:?}");
        let router = trace_grpc_routes(Router::new().route(
            "/{*method}",
            any(move || async move {
                let mut status = HeaderMap::new();
                status.insert("grpc-status", HeaderValue::from(code));
                match delivery {
                    Delivery::Headers => (status, Body::empty()).into_response(),
                    Delivery::Trailers => {
                        Frames::body([Ok(Frame::trailers(status))]).into_response()
                    }
                    Delivery::CutOff => Frames::body([
                        Ok(Frame::data(Bytes::from_static(b"message"))),
                        Ok(Frame::data(Bytes::from_static(b"message"))),
                    ])
                    .into_response(),
                    Delivery::NoStatus => {
                        Frames::body([Ok(Frame::data(Bytes::from_static(b"message")))])
                            .into_response()
                    }
                    Delivery::NotGrpc => StatusCode::NOT_FOUND.into_response(),
                    Delivery::BrokenOff => {
                        Frames::body([Err(std::io::Error::other("Canary"))]).into_response()
                    }
                }
            }),
        ));
        let request = Request::post(format!("/{path}"))
            .header("grpc-metadata-canary", "Canary")
            .body(Body::empty())
            .expect("request");
        let frames = if matches!(delivery, Delivery::CutOff) {
            1
        } else {
            usize::MAX
        };
        let (spans, body, durations) = measured(router, request, frames).await;
        assert_eq!(
            body.is_err(),
            matches!(delivery, Delivery::BrokenOff),
            "{context}"
        );
        assert_eq!(
            duration(&durations),
            Some(("grpc".into(), method.into(), outcome.into(), 1)),
            "{context}"
        );
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
        assert_eq!(span.status == Status::error(""), failed, "{context}");
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

/// A handler that knows what a request carried, such as the MCP tool a
/// `tools/call` names, names its operation: its duration records that name
/// in place of the route template.
#[tokio::test]
async fn a_handler_names_the_operation_its_request_carried() {
    let router = trace_http_routes(
        Router::new().route(
            ROUTE,
            any(|request: Request<Body>| async move {
                super::name_request_operation(request.extensions(), "tools/call", Some("exec"));
                // A later name does not replace the first.
                super::name_request_operation(request.extensions(), "tools/call", None);
                StatusCode::OK
            }),
        ),
        Interface::Mcp,
    );
    let request = Request::post("/workspaces/one/mcp")
        .body(Body::empty())
        .expect("request");
    let (_spans, _body, durations) = measured(router, request, usize::MAX).await;
    assert_eq!(
        duration(&durations),
        Some(("mcp".into(), "tools/call exec".into(), "ok".into(), 1))
    );
}
