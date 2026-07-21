//! Instrumented tonic transport for local Coral gRPC clients.
//!
//! Tonic exposes final gRPC status in response trailers, after the generated
//! client has consumed the HTTP body. The service wrapper creates the client
//! span and the body wrapper records the final trailer status before the span
//! closes.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use coral_api::grpc_response_status_code;
use opentelemetry::trace::Status as OtelStatus;
use tonic::codegen::{Body, Bytes, Service, StdError, http};
use tonic::{Code, Status};
use tracing::{Instrument as _, field};
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::status_error::{DecodedStatusError, decode_status_error};

const MISSING_GRPC_STATUS_ERROR_TYPE: &str = "MISSING_GRPC_STATUS";
const MISSING_GRPC_STATUS_MESSAGE: &str = "missing gRPC response status";
const GRPC_REQUEST_ERROR_MESSAGE: &str = "gRPC request failed";

/// Tonic `Service` wrapper that records one client span per gRPC request.
#[derive(Clone)]
pub struct InstrumentedGrpcService<S> {
    inner: S,
    endpoint: GrpcClientEndpoint,
}

impl<S> InstrumentedGrpcService<S> {
    pub(crate) fn new(inner: S, endpoint: GrpcClientEndpoint) -> Self {
        Self { inner, endpoint }
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct GrpcClientEndpoint {
    server_address: Option<String>,
    server_port: Option<u16>,
}

impl GrpcClientEndpoint {
    pub(crate) fn from_endpoint_uri(endpoint_uri: &str) -> Self {
        let Ok(uri) = endpoint_uri.parse::<http::Uri>() else {
            return Self::default();
        };
        Self {
            server_address: uri.host().map(str::to_string),
            server_port: uri.port_u16().or_else(|| default_port(uri.scheme_str())),
        }
    }
}

fn default_port(scheme: Option<&str>) -> Option<u16> {
    match scheme {
        Some("http") => Some(80),
        Some("https") => Some(443),
        _ => None,
    }
}

impl<S, B> Service<http::Request<tonic::body::Body>> for InstrumentedGrpcService<S>
where
    S: Service<http::Request<tonic::body::Body>, Response = http::Response<B>> + Send + 'static,
    S::Future: Send + 'static,
    S::Error: Into<StdError> + Send + 'static,
    B: Body<Data = Bytes> + Send + 'static,
    B::Error: Into<StdError> + Send + 'static,
{
    type Response = http::Response<InstrumentedGrpcBody<B>>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<tonic::body::Body>) -> Self::Future {
        let metadata = GrpcClientSpanMetadata::from_request(&request);
        let span = grpc_client_span(&metadata, &self.endpoint);

        let future = {
            let _entered = span.enter();
            self.inner.call(request)
        };

        Box::pin(async move {
            match future.instrument(span.clone()).await {
                Ok(response) => {
                    let (parts, body) = response.into_parts();
                    let status_recorded = record_grpc_status_from_headers(&span, &parts.headers);
                    Ok(http::Response::from_parts(
                        parts,
                        InstrumentedGrpcBody::new(body, span, status_recorded),
                    ))
                }
                Err(error) => {
                    record_transport_error(&span);
                    Err(error)
                }
            }
        })
    }
}

pub struct InstrumentedGrpcBody<B> {
    inner: Pin<Box<B>>,
    span: tracing::Span,
    status_recorded: bool,
}

impl<B> InstrumentedGrpcBody<B> {
    fn new(inner: B, span: tracing::Span, status_recorded: bool) -> Self {
        Self {
            inner: Box::pin(inner),
            span,
            status_recorded,
        }
    }

    fn record_status_from_headers(&mut self, headers: &http::HeaderMap) {
        if !self.status_recorded {
            self.status_recorded = record_grpc_status_from_headers(&self.span, headers);
        }
    }

    fn record_missing_status(&mut self) {
        if !self.status_recorded {
            record_missing_grpc_status(&self.span);
            self.status_recorded = true;
        }
    }

    fn record_body_error(&mut self) {
        if !self.status_recorded {
            record_error(&self.span, "TRANSPORT", "gRPC response body error");
            self.status_recorded = true;
        }
    }
}

impl<B> Body for InstrumentedGrpcBody<B>
where
    B: Body<Data = Bytes>,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        let this = self.get_mut();
        match this.inner.as_mut().poll_frame(cx) {
            Poll::Ready(Some(Ok(frame))) => {
                if let Some(trailers) = frame.trailers_ref() {
                    this.record_status_from_headers(trailers);
                }
                Poll::Ready(Some(Ok(frame)))
            }
            Poll::Ready(Some(Err(error))) => {
                this.record_body_error();
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(None) => {
                this.record_missing_status();
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

#[derive(Debug, Eq, PartialEq)]
struct GrpcClientSpanMetadata {
    service: String,
    method: String,
    span_name: String,
}

impl GrpcClientSpanMetadata {
    fn from_request(request: &http::Request<tonic::body::Body>) -> Self {
        request
            .extensions()
            .get::<tonic::GrpcMethod<'static>>()
            .map_or_else(
                || Self::from_path(request.uri().path()),
                |method| Self::new(method.service(), method.method()),
            )
    }

    fn from_path(path: &str) -> Self {
        let trimmed = path.trim_start_matches('/');
        let (service, method) = trimmed
            .split_once('/')
            .unwrap_or(("coral.v1.UnknownService", "Unknown"));
        Self::new(service, method)
    }

    fn new(service: &str, method: &str) -> Self {
        Self {
            service: service.to_string(),
            method: method.to_string(),
            span_name: format!("{service}/{method}"),
        }
    }
}

fn grpc_client_span(
    metadata: &GrpcClientSpanMetadata,
    endpoint: &GrpcClientEndpoint,
) -> tracing::Span {
    let span = tracing::info_span!(
        target: "coral_client::grpc",
        "grpc.client",
        error.type = field::Empty,
        exception.message = field::Empty,
        grpc.method = metadata.method.as_str(),
        grpc.status_code = field::Empty,
        grpc.code = field::Empty,
        otel.kind = "client",
        otel.name = metadata.span_name.as_str(),
        peer.service = "coral",
        rpc.method = metadata.method.as_str(),
        rpc.response.status_code = field::Empty,
        rpc.service = metadata.service.as_str(),
        rpc.system = "grpc",
        rpc.system.name = "grpc",
        server.address = field::Empty,
        server.port = field::Empty,
        status = field::Empty,
    );
    record_grpc_endpoint(&span, endpoint);
    span
}

fn record_grpc_endpoint(span: &tracing::Span, endpoint: &GrpcClientEndpoint) {
    if let Some(address) = endpoint.server_address.as_deref() {
        span.record("server.address", address);
    }
    if let Some(port) = endpoint.server_port {
        span.record("server.port", i64::from(port));
    }
}

fn record_grpc_status_from_headers(span: &tracing::Span, headers: &http::HeaderMap) -> bool {
    let Some(status) = Status::from_header_map(headers) else {
        return false;
    };
    record_grpc_status(span, status.code(), &status);
    true
}

fn record_grpc_status(span: &tracing::Span, code: Code, status: &Status) {
    record_grpc_status_attributes(span, code);
    if code == Code::Ok {
        span.record("status", "ok");
        span.set_status(OtelStatus::Ok);
    } else {
        let error_type = grpc_client_error_type(status);
        record_error(span, error_type.as_str(), GRPC_REQUEST_ERROR_MESSAGE);
    }
}

fn record_grpc_status_attributes(span: &tracing::Span, code: Code) {
    let response_status_code = grpc_response_status_code(code);
    span.record("grpc.status_code", code as i64);
    span.record("grpc.code", response_status_code);
    span.record("rpc.response.status_code", response_status_code);
}

fn record_missing_grpc_status(span: &tracing::Span) {
    record_grpc_status_attributes(span, Code::Unknown);
    record_error(
        span,
        MISSING_GRPC_STATUS_ERROR_TYPE,
        MISSING_GRPC_STATUS_MESSAGE,
    );
}

fn record_transport_error(span: &tracing::Span) {
    record_error(span, "TRANSPORT", "gRPC transport error");
}

fn record_error(span: &tracing::Span, error_type: impl AsRef<str>, message: &'static str) {
    span.record("status", "error");
    span.record("error.type", error_type.as_ref());
    span.record("exception.message", message);
    span.set_status(OtelStatus::error(message));
}

fn grpc_client_error_type(status: &Status) -> String {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => error.reason,
        DecodedStatusError::Plain(_) => grpc_response_status_code(status.code()).to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use coral_api::{CORAL_ERROR_DOMAIN, CORAL_ERROR_METADATA_SUMMARY};
    use opentelemetry::trace::{Status as OtelStatus, TracerProvider as _};
    use opentelemetry::{KeyValue, Value};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider, SpanData};
    use tonic::codegen::http;
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};
    use tracing_subscriber::prelude::*;

    use super::{
        GRPC_REQUEST_ERROR_MESSAGE, GrpcClientEndpoint, GrpcClientSpanMetadata,
        MISSING_GRPC_STATUS_ERROR_TYPE, MISSING_GRPC_STATUS_MESSAGE, grpc_client_span,
        record_grpc_status, record_missing_grpc_status,
    };

    #[test]
    fn grpc_client_span_metadata_derives_names_from_path() {
        let metadata = GrpcClientSpanMetadata::from_path("/coral.v1.QueryService/ExecuteSql");

        assert_eq!(metadata.service, "coral.v1.QueryService");
        assert_eq!(metadata.method, "ExecuteSql");
        assert_eq!(metadata.span_name, "coral.v1.QueryService/ExecuteSql");
    }

    #[test]
    fn grpc_client_span_metadata_prefers_tonic_extension() {
        let mut request = http::Request::builder()
            .uri("/fallback.Service/Fallback")
            .body(tonic::body::Body::empty())
            .expect("request");
        request.extensions_mut().insert(tonic::GrpcMethod::new(
            "coral.v1.SourceService",
            "ListSources",
        ));

        let metadata = GrpcClientSpanMetadata::from_request(&request);

        assert_eq!(metadata.service, "coral.v1.SourceService");
        assert_eq!(metadata.method, "ListSources");
        assert_eq!(metadata.span_name, "coral.v1.SourceService/ListSources");
    }

    #[test]
    fn grpc_client_endpoint_derives_address_and_port() {
        let endpoint = GrpcClientEndpoint::from_endpoint_uri("http://127.0.0.1:50051");

        assert_eq!(endpoint.server_address.as_deref(), Some("127.0.0.1"));
        assert_eq!(endpoint.server_port, Some(50051));

        let endpoint = GrpcClientEndpoint::from_endpoint_uri("https://coral.example.com");

        assert_eq!(
            endpoint.server_address.as_deref(),
            Some("coral.example.com")
        );
        assert_eq!(endpoint.server_port, Some(443));
    }

    #[test]
    fn missing_grpc_status_records_unknown_error() {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("coral-client-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let metadata = GrpcClientSpanMetadata::new("coral.v1.QueryService", "ExecuteSql");
            let span = grpc_client_span(&metadata, &GrpcClientEndpoint::default());
            let _entered = span.enter();
            record_missing_grpc_status(&span);
        });

        provider.force_flush().expect("spans should flush");
        let spans = exporter
            .get_finished_spans()
            .expect("finished spans should be readable");
        let span = spans
            .iter()
            .find(|span| span.name == "coral.v1.QueryService/ExecuteSql")
            .expect("client span should export");

        assert_eq!(i64_attr(&span.attributes, "grpc.status_code"), Some(2));
        assert_eq!(string_attr(&span.attributes, "grpc.code"), Some("UNKNOWN"));
        assert_eq!(
            string_attr(&span.attributes, "rpc.response.status_code"),
            Some("UNKNOWN")
        );
        assert_eq!(
            string_attr(&span.attributes, "error.type"),
            Some(MISSING_GRPC_STATUS_ERROR_TYPE)
        );
        assert_eq!(span.status, OtelStatus::error(MISSING_GRPC_STATUS_MESSAGE));
    }

    #[test]
    fn plain_grpc_status_message_is_not_exported() {
        let sentinel = "SENSITIVE_CLIENT_GRPC_ERROR_MARKER";
        let status = Status::invalid_argument(format!("invalid input: {sentinel}"));

        let span = export_grpc_status(&status);

        assert!(status.message().contains(sentinel));
        assert_eq!(
            string_attr(&span.attributes, "error.type"),
            Some("INVALID_ARGUMENT")
        );
        assert_eq!(
            string_attr(&span.attributes, "exception.message"),
            Some(GRPC_REQUEST_ERROR_MESSAGE)
        );
        assert_eq!(span.status, OtelStatus::error(GRPC_REQUEST_ERROR_MESSAGE));
        assert!(!format!("{span:?}").contains(sentinel));
    }

    #[test]
    fn structured_grpc_status_keeps_only_its_categorical_reason() {
        let sentinel = "SENSITIVE_CLIENT_STRUCTURED_ERROR_MARKER";
        let metadata = HashMap::from([(
            CORAL_ERROR_METADATA_SUMMARY.to_string(),
            format!("summary containing {sentinel}"),
        )]);
        let status = Status::with_error_details_vec(
            Code::InvalidArgument,
            format!("fallback containing {sentinel}"),
            vec![ErrorDetail::ErrorInfo(tonic_types::ErrorInfo::new(
                "INVALID_CATALOG_KIND",
                CORAL_ERROR_DOMAIN,
                metadata,
            ))],
        );

        let span = export_grpc_status(&status);

        assert!(status.message().contains(sentinel));
        assert_eq!(
            string_attr(&span.attributes, "error.type"),
            Some("INVALID_CATALOG_KIND")
        );
        assert_eq!(
            string_attr(&span.attributes, "exception.message"),
            Some(GRPC_REQUEST_ERROR_MESSAGE)
        );
        assert_eq!(span.status, OtelStatus::error(GRPC_REQUEST_ERROR_MESSAGE));
        assert!(!format!("{span:?}").contains(sentinel));
    }

    fn export_grpc_status(status: &Status) -> SpanData {
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("coral-client-error-privacy-test");
        let subscriber =
            tracing_subscriber::registry().with(tracing_opentelemetry::layer().with_tracer(tracer));

        tracing::subscriber::with_default(subscriber, || {
            let metadata = GrpcClientSpanMetadata::new("coral.v1.QueryService", "ExecuteSql");
            let span = grpc_client_span(&metadata, &GrpcClientEndpoint::default());
            record_grpc_status(&span, status.code(), status);
        });

        provider.force_flush().expect("spans should flush");
        exporter
            .get_finished_spans()
            .expect("finished spans should be readable")
            .into_iter()
            .find(|span| span.name == "coral.v1.QueryService/ExecuteSql")
            .expect("client span should export")
    }

    fn i64_attr(attributes: &[KeyValue], key: &str) -> Option<i64> {
        attributes.iter().find_map(|attribute| {
            if attribute.key.as_str() == key
                && let Value::I64(value) = attribute.value
            {
                Some(value)
            } else {
                None
            }
        })
    }

    fn string_attr<'a>(attributes: &'a [KeyValue], key: &str) -> Option<&'a str> {
        attributes.iter().find_map(|attribute| {
            if attribute.key.as_str() == key
                && let Value::String(value) = &attribute.value
            {
                Some(value.as_ref())
            } else {
                None
            }
        })
    }
}
