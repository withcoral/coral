//! HTTP request execution, retry, tracing, and response decoding.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use opentelemetry::Context as OtelContext;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use tracing::Instrument as _;
use tracing::field;
use tracing_opentelemetry::OpenTelemetrySpanExt as _;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::auth::{
    ensure_auth_uses_credential_safe_transport, request_requires_credential_safe_transport,
    resolve_auth_headers,
};
use crate::backends::http::client::HttpClients;
use crate::backends::http::error::{
    CredentialTransportError, REDACTED_CREDENTIAL_RESPONSE_DETAIL, provider_error,
};
use crate::backends::http::rate_limit::{RateLimitDecision, check_rate_limit};
use crate::backends::http::request::RequestBody;
use crate::backends::http::response::{ResponseDecodeContext, decode_response_body};
use crate::backends::http::trace::{
    HttpBodyCapture, inject_trace_context, record_http_processing_error, record_http_status_error,
    record_trace_http_endpoint, request_body_size, sanitize_trace_url, trace_http_endpoint,
    trace_reqwest_error, trace_reqwest_error_type,
};
use crate::backends::shared::template::{RenderContext, resolve_value_source, value_to_string};
use crate::{
    BoundRequestIdentityHttpAuthenticator, RequestAuthenticator,
    RequestIdentityHttpAuthenticatorError,
};
use coral_spec::backends::http::RateLimitSpec;
use coral_spec::{AuthSpec, HeaderSpec, HttpMethod, ResponseBodyFormat};

static NEXT_HTTP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
const REDACTED_SECRET_URL: &str = "[redacted secret-derived URL]";

#[derive(Clone, Copy)]
pub(super) enum SecretProvenance {
    Public,
    Request,
    Url,
}

pub(super) struct OutgoingHttpRequest<'a> {
    pub(super) auth: &'a AuthSpec,
    pub(super) request_headers: &'a [HeaderSpec],
    pub(super) request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
    pub(super) require_credential_safe_auth_transport: bool,
    pub(super) secret_provenance: SecretProvenance,
    pub(super) request_identity_http_authenticator:
        Option<&'a BoundRequestIdentityHttpAuthenticator>,
    pub(super) trace_context: Option<&'a OtelContext>,
    pub(super) table_headers: &'a [HeaderSpec],
    pub(super) table_name: &'a str,
    pub(super) method: HttpMethod,
    pub(super) url: &'a str,
    pub(super) query_pairs: &'a [(String, String)],
    pub(super) body: Option<&'a RequestBody>,
    pub(super) response_format: ResponseBodyFormat,
    pub(super) source_schema: &'a str,
    pub(super) rate_limit: &'a RateLimitSpec,
    pub(super) body_capture: HttpBodyCapture,
    pub(super) render_context: RenderContext<'a>,
    pub(super) allow_404_empty: bool,
}

#[derive(Debug)]
pub(super) struct DecodedHttpResponse {
    pub(super) payload: Value,
    pub(super) headers: HeaderMap,
    pub(super) credential_tainted: bool,
}

#[expect(
    clippy::too_many_lines,
    reason = "HTTP request execution keeps retry, auth, logging, and response handling in one audited flow"
)]
pub(super) async fn execute_request(
    http: &HttpClients,
    request_timeout: Duration,
    request: OutgoingHttpRequest<'_>,
) -> Result<Option<DecodedHttpResponse>> {
    enum ResponseOutcome {
        Done(Result<Option<DecodedHttpResponse>>),
        Retry(Duration),
    }

    let OutgoingHttpRequest {
        auth,
        request_headers,
        request_authenticators,
        require_credential_safe_auth_transport,
        secret_provenance,
        request_identity_http_authenticator,
        trace_context,
        table_headers,
        table_name,
        method,
        url,
        query_pairs,
        body,
        response_format,
        source_schema,
        rate_limit,
        body_capture,
        render_context,
        allow_404_empty,
    } = request;
    let contains_secret_value = require_credential_safe_auth_transport
        && !matches!(secret_provenance, SecretProvenance::Public);
    let redact_url = require_credential_safe_auth_transport
        && matches!(secret_provenance, SecretProvenance::Url);
    let mut server_error_retries = 0usize;
    let mut throttle_retries = 0usize;
    let mut decode_retries = 0usize;
    let mut credential_tainted = false;
    loop {
        let method_label = http_method_label(method);
        let mut request = build_http_request(http.proxy_aware(), method, url);

        let mut header_map = HeaderMap::new();
        for header in request_headers.iter().chain(table_headers.iter()) {
            if let Some(value) = resolve_value_source(&header.value, &render_context)? {
                let name = HeaderName::try_from(header.name.as_str()).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "invalid request header name '{}': {error}",
                        header.name
                    ))
                })?;
                let value =
                    HeaderValue::try_from(value_to_string(&value).as_str()).map_err(|error| {
                        DataFusionError::Execution(format!(
                            "invalid request header value for '{}': {error}",
                            header.name
                        ))
                    })?;
                header_map.insert(name, value);
            }
        }
        let has_authored_headers = !header_map.is_empty();
        if matches!(body, Some(RequestBody::Text(_)))
            && !header_map.contains_key(reqwest::header::CONTENT_TYPE)
        {
            header_map.insert(
                reqwest::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            );
        }
        let logged_url = if redact_url {
            REDACTED_SECRET_URL.to_string()
        } else {
            build_logged_url(url, query_pairs)
        };

        let request_id = NEXT_HTTP_REQUEST_ID.fetch_add(1, Ordering::Relaxed);
        let attempt = server_error_retries + throttle_retries + decode_retries + 1;
        let traced_url = sanitize_trace_url(&logged_url);
        let trace_endpoint = trace_http_endpoint(&traced_url);
        let request_span = tracing::info_span!(
            target: "coral_engine::http",
            "http.request",
            coral.http.attempt = attempt,
            coral.http.error.connect = field::Empty,
            coral.http.error.request = field::Empty,
            coral.http.error.timeout = field::Empty,
            coral.http.request_id = request_id,
            coral.source = source_schema,
            coral.table = table_name,
            error = field::Empty,
            error.type = field::Empty,
            exception.message = field::Empty,
            http.host = field::Empty,
            http.request.body.present = body.is_some(),
            http.request.body.size = request_body_size(body).unwrap_or_default(),
            http.request.method = method_label,
            http.request.query_count = query_pairs.len(),
            http.request.resend_count = field::Empty,
            http.response.body.size = field::Empty,
            http.response.status_code = field::Empty,
            net.peer.name = field::Empty,
            otel.kind = "client",
            otel.name = method_label,
            otel.status_code = field::Empty,
            otel.status_description = field::Empty,
            peer.service = field::Empty,
            server.address = field::Empty,
            server.port = field::Empty,
            url.full = %traced_url,
        );
        if let Some(trace_context) = trace_context {
            drop(request_span.set_parent(trace_context.clone()));
        }
        record_trace_http_endpoint(&request_span, &trace_endpoint);
        if attempt > 1 {
            request_span.record(
                "http.request.resend_count",
                i64::try_from(attempt - 1).unwrap_or(i64::MAX),
            );
        }
        if contains_secret_value
            && let Err(error) = ensure_secret_values_use_credential_safe_transport(url)
        {
            record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
            return Err(error);
        }

        inject_trace_context(&request_span, &mut header_map);
        if !header_map.is_empty() {
            request = request.headers(header_map);
        }

        if !query_pairs.is_empty() {
            request = request.query(query_pairs);
        }

        match body {
            Some(RequestBody::Json(value)) => {
                request = request.json(value);
            }
            Some(RequestBody::Text(text)) => {
                request = request.body(text.clone());
            }
            None => {}
        }

        body_capture.record_request(&request_span, request_id, body);
        let (mut built, has_auth_headers) = match resolve_auth_headers(
            auth,
            request,
            request_authenticators,
            render_context.resolved_inputs,
            require_credential_safe_auth_transport,
        ) {
            Ok(request) => request,
            Err(error) => {
                record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                return Err(error);
            }
        };
        if require_credential_safe_auth_transport
            && request_requires_credential_safe_transport(&built, has_authored_headers)
            && let Err(error) = ensure_auth_uses_credential_safe_transport(built.url())
        {
            record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
            return Err(error);
        }
        if require_credential_safe_auth_transport
            && request_identity_http_authenticator.is_some()
            && let Err(error) = ensure_auth_uses_credential_safe_transport(built.url())
        {
            record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
            return Err(error);
        }
        let mut has_identity_headers = false;
        if let Some(authenticator) = request_identity_http_authenticator {
            let identity_headers = match authenticator(&built, render_context.resolved_inputs).await
            {
                Ok(headers) => headers,
                Err(error) => {
                    let error =
                        identity_http_authenticator_error(source_schema, table_name, &error);
                    record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                    return Err(error);
                }
            };
            has_identity_headers = !identity_headers.is_empty();
            if require_credential_safe_auth_transport
                && has_identity_headers
                && let Err(error) = ensure_auth_uses_credential_safe_transport(built.url())
            {
                record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                return Err(error);
            }
            for (name, value) in identity_headers {
                if built.headers().contains_key(&name) {
                    let error = identity_header_conflict_error(source_schema, table_name, &name);
                    record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                    return Err(error);
                }
                built.headers_mut().insert(name, value);
            }
        }
        let credential_bearing = contains_secret_value
            || has_auth_headers
            || has_identity_headers
            || request_requires_credential_safe_transport(&built, has_authored_headers);
        credential_tainted |= credential_bearing;
        let request_http = match http.for_request(built.url(), credential_bearing) {
            Ok(http) => http,
            Err(error) => {
                record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                return Err(error);
            }
        };
        let response = match request_http
            .execute(built)
            .instrument(request_span.clone())
            .await
        {
            Ok(response) => response,
            Err(error) => {
                record_http_processing_error(
                    &request_span,
                    trace_reqwest_error_type(&error),
                    trace_reqwest_error(&error),
                );
                request_span.record("coral.http.error.timeout", error.is_timeout());
                request_span.record("coral.http.error.connect", error.is_connect());
                request_span.record("coral.http.error.request", error.is_request());
                return Err(request_error(
                    source_schema,
                    table_name,
                    method_label,
                    &logged_url,
                    request_timeout,
                    &error,
                ));
            }
        };

        let status = response.status();
        request_span.record("http.response.status_code", status.as_u16());
        let outcome = 'response: {
            if let Some(length) = response.content_length() {
                request_span.record("http.response.body.size", length);
            }

            match check_rate_limit(status, response.headers(), rate_limit, throttle_retries) {
                RateLimitDecision::Continue => {}
                RateLimitDecision::Retry(wait) => {
                    record_http_status_error(&request_span, status, "rate limited; retrying");
                    body_capture
                        .record_unconsumed_response(&request_span, request_id, response)
                        .await;
                    throttle_retries += 1;
                    break 'response ResponseOutcome::Retry(wait);
                }
                RateLimitDecision::Fail(error) => {
                    let error_message = error.to_string();
                    record_http_status_error(&request_span, status, error_message.as_str());
                    body_capture
                        .record_unconsumed_response(&request_span, request_id, response)
                        .await;
                    break 'response ResponseOutcome::Done(Err(DataFusionError::External(
                        Box::new(ProviderQueryError::RateLimited {
                            source_schema: source_schema.to_string(),
                            table: table_name.to_string(),
                            method: Some(method_label.to_string()),
                            url: Some(logged_url.clone()),
                            detail: error_message,
                        }),
                    )));
                }
            }

            if status.is_server_error() && server_error_retries < 2 {
                record_http_status_error(&request_span, status, "server error; retrying");
                body_capture
                    .record_unconsumed_response(&request_span, request_id, response)
                    .await;
                server_error_retries += 1;
                break 'response ResponseOutcome::Retry(Duration::from_secs(2));
            }

            if status == reqwest::StatusCode::NOT_FOUND && allow_404_empty {
                body_capture
                    .record_unconsumed_response(&request_span, request_id, response)
                    .await;
                break 'response ResponseOutcome::Done(Ok(None));
            }

            if !status.is_success() {
                let body = response
                    .text()
                    .instrument(request_span.clone())
                    .await
                    .unwrap_or_default();
                record_http_status_error(
                    &request_span,
                    status,
                    response_error_summary(status, &body),
                );
                request_span.record("http.response.body.size", body.len());
                body_capture.record_response(&request_span, request_id, &body);
                let detail = if credential_tainted {
                    REDACTED_CREDENTIAL_RESPONSE_DETAIL.to_string()
                } else {
                    body
                };
                break 'response ResponseOutcome::Done(Err(DataFusionError::External(Box::new(
                    ProviderQueryError::ApiRequest {
                        source_schema: source_schema.to_string(),
                        table: table_name.to_string(),
                        status: Some(status.as_u16()),
                        method: Some(method_label.to_string()),
                        url: Some(logged_url.clone()),
                        filters: render_context.filters.clone(),
                        detail,
                    },
                ))));
            }

            let response_headers = response.headers().clone();

            match decode_response_body(
                response,
                response_format,
                ResponseDecodeContext {
                    source_schema,
                    table_name,
                    method_label,
                    logged_url: &logged_url,
                    body_capture: &body_capture,
                    response_span: &request_span,
                    request_id,
                },
            )
            .instrument(request_span.clone())
            .await
            {
                Ok(payload) => ResponseOutcome::Done(Ok(Some(DecodedHttpResponse {
                    payload,
                    headers: response_headers,
                    credential_tainted,
                }))),
                Err(mut error) => {
                    // `Decode { retryable }` marks a transient (truncated/EOF) body. Only
                    // idempotent GET requests may be retried or surfaced as retryable.
                    let is_get = matches!(method, HttpMethod::GET);
                    let retry = is_get
                        && matches!(
                            error,
                            ProviderQueryError::Decode {
                                retryable: true,
                                ..
                            }
                        );
                    if retry && decode_retries < 2 {
                        record_http_processing_error(
                            &request_span,
                            "DECODE_RETRY",
                            provider_error(error),
                        );
                        decode_retries += 1;
                        ResponseOutcome::Retry(Duration::from_secs(2))
                    } else {
                        if let ProviderQueryError::Decode { retryable, .. } = &mut error {
                            *retryable &= is_get;
                        }
                        let error = provider_error(error);
                        record_http_processing_error(&request_span, "DECODE", &error);
                        ResponseOutcome::Done(Err(error))
                    }
                }
            }
        };

        drop(request_span);
        match outcome {
            ResponseOutcome::Done(result) => return result,
            ResponseOutcome::Retry(wait) => {
                tokio::time::sleep(wait).await;
            }
        }
    }
}

fn ensure_secret_values_use_credential_safe_transport(url: &str) -> Result<()> {
    let url = reqwest::Url::parse(url).map_err(|_error| {
        DataFusionError::External(Box::new(CredentialTransportError(
            "DSL v4 secret values require a valid HTTPS or loopback HTTP URL".to_string(),
        )))
    })?;
    ensure_auth_uses_credential_safe_transport(&url).map_err(|_error| {
        DataFusionError::External(Box::new(CredentialTransportError(
            "DSL v4 secret values require HTTPS or loopback HTTP".to_string(),
        )))
    })
}

fn identity_http_authenticator_error(
    source_schema: &str,
    table_name: &str,
    error: &RequestIdentityHttpAuthenticatorError,
) -> DataFusionError {
    let detail = format!(
        "request identity HTTP authenticator failed for source '{source_schema}' table '{table_name}': {error}"
    );
    let error = match error {
        RequestIdentityHttpAuthenticatorError::InvalidInput(_) => {
            RequestIdentityHttpAuthenticatorError::invalid_input(detail)
        }
        RequestIdentityHttpAuthenticatorError::FailedPrecondition(_) => {
            RequestIdentityHttpAuthenticatorError::failed_precondition(detail)
        }
    };
    DataFusionError::External(Box::new(error))
}

fn identity_header_conflict_error(
    source_schema: &str,
    table_name: &str,
    name: &HeaderName,
) -> DataFusionError {
    DataFusionError::External(Box::new(
        RequestIdentityHttpAuthenticatorError::failed_precondition(format!(
            "request identity HTTP authenticator attempted to overwrite header '{}' for source '{}' table '{}'",
            name.as_str(),
            source_schema,
            table_name
        )),
    ))
}

fn request_error(
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
    request_timeout: Duration,
    error: &reqwest::Error,
) -> DataFusionError {
    let detail = if error.is_timeout() {
        format!(
            "source API request timed out after {}s",
            request_timeout.as_secs_f64()
        )
    } else {
        "source API request failed before a response was received".to_string()
    };

    provider_error(ProviderQueryError::Request {
        source_schema: source_schema.to_string(),
        table: table_name.to_string(),
        method: Some(method_label.to_string()),
        url: Some(logged_url.to_string()),
        detail,
        timed_out: error.is_timeout(),
    })
}

fn response_error_summary(status: reqwest::StatusCode, body: &str) -> String {
    format!(
        "upstream returned HTTP {}; body_bytes={}",
        status.as_u16(),
        body.len()
    )
}

fn http_method_label(method: HttpMethod) -> &'static str {
    match method {
        HttpMethod::GET => "GET",
        HttpMethod::POST => "POST",
    }
}

fn build_http_request(
    http: &reqwest::Client,
    method: HttpMethod,
    url: &str,
) -> reqwest::RequestBuilder {
    match method {
        HttpMethod::GET => http.get(url),
        HttpMethod::POST => http.post(url),
    }
}

fn build_logged_url(url: &str, query_pairs: &[(String, String)]) -> String {
    if query_pairs.is_empty() {
        return url.to_string();
    }
    let suffix = query_pairs
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    if url.contains('?') {
        format!("{url}&{suffix}")
    } else {
        format!("{url}?{suffix}")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, HashMap};
    use std::sync::Arc;
    use std::time::Duration;

    use datafusion::error::DataFusionError;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use reqwest::header::{AUTHORIZATION, HeaderValue};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tracing_subscriber::layer::SubscriberExt as _;
    use wiremock::MockServer;

    use super::{
        OutgoingHttpRequest as TestOutgoingHttpRequest, SecretProvenance, execute_request,
    };
    use crate::backends::http::ProviderQueryError;
    use crate::backends::http::client::HttpClients;
    use crate::backends::http::trace::HttpBodyCapture;
    use crate::backends::shared::template::RenderContext;
    use crate::{BoundRequestIdentityHttpAuthenticator, RequestIdentityHttpAuthenticatorError};
    use coral_spec::backends::http::RateLimitSpec;
    use coral_spec::{
        AuthSpec, HeaderAuthSpec, HeaderSpec, HttpMethod, ResponseBodyFormat, ValueSourceSpec,
    };

    async fn spawn_hanging_http_server() -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind hanging http server");
        let addr = listener.local_addr().expect("local addr");
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.expect("accept hanging request");
            let _socket = socket;
            std::future::pending::<()>().await;
        });

        (format!("http://{addr}"), task)
    }

    async fn spawn_header_recorder(response_body: &'static str) -> (String, JoinHandle<String>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind header recorder");
        let addr = listener.local_addr().expect("local addr");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut buffer = vec![0_u8; 8192];
            let mut request = Vec::new();
            loop {
                let read = socket.read(&mut buffer).await.expect("read request");
                if read == 0 {
                    break;
                }
                request.extend(buffer.iter().take(read).copied());
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            socket
                .write_all(response.as_bytes())
                .await
                .expect("write response");
            String::from_utf8_lossy(&request).into_owned()
        });

        (format!("http://{addr}"), task)
    }

    #[tokio::test]
    async fn execute_request_times_out_when_upstream_stalls() {
        let (base_url, task) = spawn_hanging_http_server().await;
        let request_timeout = Duration::from_millis(100);
        let http = HttpClients::legacy(
            reqwest::Client::builder()
                .timeout(request_timeout)
                .build()
                .expect("build test client"),
        );
        let url = format!("{base_url}/items");
        let query_pairs = vec![("api_key".to_string(), "secret-token".to_string())];
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let error = execute_request(
            &http,
            request_timeout,
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                require_credential_safe_auth_transport: false,
                secret_provenance: SecretProvenance::Url,
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &url,
                query_pairs: &query_pairs,
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
        )
        .await
        .expect_err("hung upstream should time out");

        match error {
            DataFusionError::External(inner) => {
                let provider_error = inner
                    .downcast_ref::<ProviderQueryError>()
                    .expect("timeout should be a provider query error");
                match provider_error {
                    ProviderQueryError::Request {
                        source_schema,
                        table,
                        detail,
                        timed_out,
                        ..
                    } => {
                        assert_eq!(source_schema, "demo");
                        assert_eq!(table, "items");
                        assert!(*timed_out);
                        assert!(detail.contains("timed out"));
                        assert!(!detail.contains("secret-token"));
                    }
                    other => panic!("expected request provider error, got {other:?}"),
                }
                let structured = provider_error.to_structured();
                assert_eq!(
                    structured.metadata().get("url").map(String::as_str),
                    Some(format!("{base_url}/items").as_str())
                );
                assert!(!structured.detail().contains("secret-token"));
            }
            other => panic!("expected external provider error, got {other:?}"),
        }
        task.abort();
    }

    #[tokio::test]
    async fn request_identity_headers_cannot_overwrite_existing_headers() {
        let request_timeout = Duration::from_secs(1);
        let http = HttpClients::legacy(
            reqwest::Client::builder()
                .timeout(request_timeout)
                .build()
                .expect("build test client"),
        );
        let base_url = "https://api.example.test";
        let url = format!("{base_url}/items");
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);
        let request_headers = vec![HeaderSpec {
            name: "Authorization".to_string(),
            value: ValueSourceSpec::Literal {
                value: serde_json::Value::String("manifest-token".to_string()),
            },
        }];
        let identity_authenticator: BoundRequestIdentityHttpAuthenticator =
            Arc::new(|_request, _resolved_inputs| {
                Box::pin(async {
                    Ok::<Vec<_>, RequestIdentityHttpAuthenticatorError>(vec![(
                        AUTHORIZATION,
                        HeaderValue::from_static("identity-token"),
                    )])
                })
            });

        let error = execute_request(
            &http,
            request_timeout,
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &request_headers,
                request_authenticators: &HashMap::new(),
                require_credential_safe_auth_transport: true,
                secret_provenance: SecretProvenance::Public,
                request_identity_http_authenticator: Some(&identity_authenticator),
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &url,
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
        )
        .await
        .expect_err("identity headers must not overwrite existing headers");

        let DataFusionError::External(inner) = error else {
            panic!("expected external identity error, got {error:?}");
        };
        let identity_error = inner
            .downcast_ref::<RequestIdentityHttpAuthenticatorError>()
            .expect("identity conflict should use identity authenticator error");
        assert!(
            identity_error
                .to_string()
                .contains("attempted to overwrite header 'authorization'"),
            "{identity_error}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    #[expect(
        clippy::too_many_lines,
        reason = "auditable retry and non-egress contract"
    )]
    async fn credential_tainted_body_read_errors_strip_response_url() {
        let canary = "credential-read-url-canary";
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind truncated response server");
        let addr = listener.local_addr().expect("truncated server address");
        let server = tokio::spawn(async move {
            for _ in 0..6 {
                let (mut socket, _) = listener.accept().await.expect("accept request");
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let read = socket.read(&mut buffer).await.expect("read request");
                    if read == 0 {
                        break;
                    }
                    request.extend(buffer.iter().take(read).copied());
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                socket
                    .write_all(
                        b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 64\r\nconnection: close\r\n\r\n{",
                    )
                    .await
                    .expect("write truncated response");
            }
        });
        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let subscriber = tracing_subscriber::Registry::default()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("decode-url-test")));
        let _subscriber = tracing::subscriber::set_default(subscriber);
        let request_timeout = Duration::from_secs(1);
        let http = HttpClients::credential_safe(
            reqwest::Client::builder()
                .timeout(request_timeout)
                .build()
                .expect("build proxy-aware client"),
            reqwest::Client::builder()
                .timeout(request_timeout)
                .no_proxy()
                .build()
                .expect("build direct client"),
        );
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let url = format!("http://{addr}/{canary}");

        for response_format in [ResponseBodyFormat::Json, ResponseBodyFormat::JsonEachRow] {
            let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);
            let error = execute_request(
                &http,
                request_timeout,
                TestOutgoingHttpRequest {
                    auth: &AuthSpec::default(),
                    request_headers: &[],
                    request_authenticators: &HashMap::new(),
                    require_credential_safe_auth_transport: true,
                    secret_provenance: SecretProvenance::Url,
                    request_identity_http_authenticator: None,
                    trace_context: None,
                    table_headers: &[],
                    table_name: "items",
                    method: HttpMethod::GET,
                    url: &url,
                    query_pairs: &[],
                    body: None,
                    response_format,
                    source_schema: "demo",
                    rate_limit: &RateLimitSpec::default(),
                    body_capture: HttpBodyCapture::default(),
                    render_context,
                    allow_404_empty: false,
                },
            )
            .await
            .expect_err("truncated response must fail after retries");

            let DataFusionError::External(inner) = &error else {
                panic!("expected provider error, got {error:?}");
            };
            let provider_error = inner
                .downcast_ref::<ProviderQueryError>()
                .expect("provider decode error");
            let structured = provider_error.to_structured();
            assert!(!error.to_string().contains(canary), "{error}");
            assert!(!structured.detail().contains(canary));
            assert!(
                structured
                    .metadata()
                    .values()
                    .all(|value| !value.contains(canary))
            );
        }

        server.await.expect("truncated server task");
        provider.force_flush().expect("flush spans");
        let spans = exporter.get_finished_spans().expect("finished spans");
        assert_eq!(spans.len(), 6);
        let span_dump = format!("{spans:#?}");
        assert!(!span_dump.contains(canary), "{span_dump}");
        provider.shutdown().expect("shutdown provider");
    }

    #[tokio::test]
    async fn request_identity_headers_are_injected() {
        let (base_url, task) = spawn_header_recorder(r#"{"ok":true}"#).await;
        let proxy = MockServer::start().await;
        let request_timeout = Duration::from_secs(1);
        let http = HttpClients::credential_safe(
            reqwest::Client::builder()
                .timeout(request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .proxy(reqwest::Proxy::all(proxy.uri()).expect("proxy URL"))
                .build()
                .expect("build proxy-aware client"),
            reqwest::Client::builder()
                .timeout(request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()
                .expect("build direct client"),
        );
        let url = format!("{base_url}/items");
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);
        let identity_authenticator: BoundRequestIdentityHttpAuthenticator =
            Arc::new(|_request, _resolved_inputs| {
                Box::pin(async {
                    Ok::<Vec<_>, RequestIdentityHttpAuthenticatorError>(vec![(
                        reqwest::header::HeaderName::from_static("x-identity-token"),
                        HeaderValue::from_static("identity-token"),
                    )])
                })
            });

        let response = execute_request(
            &http,
            request_timeout,
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                require_credential_safe_auth_transport: true,
                secret_provenance: SecretProvenance::Public,
                request_identity_http_authenticator: Some(&identity_authenticator),
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &url,
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
        )
        .await
        .expect("identity-authenticated request should succeed");

        assert!(response.expect("decoded response").credential_tainted);
        let raw_request = task.await.expect("header recorder should finish");
        assert!(
            raw_request.contains("\r\nx-identity-token: identity-token\r\n"),
            "{raw_request}"
        );
        assert!(
            proxy
                .received_requests()
                .await
                .expect("proxy request recording")
                .is_empty(),
            "the loopback identity request and token must bypass the proxy"
        );
    }

    #[tokio::test]
    async fn non_authorization_auth_headers_bypass_a_hostile_proxy() {
        let (base_url, task) = spawn_header_recorder(r#"{"ok":true}"#).await;
        let proxy = MockServer::start().await;
        let request_timeout = Duration::from_secs(1);
        let http = HttpClients::credential_safe(
            reqwest::Client::builder()
                .timeout(request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .proxy(reqwest::Proxy::all(proxy.uri()).expect("proxy URL"))
                .build()
                .expect("build proxy-aware client"),
            reqwest::Client::builder()
                .timeout(request_timeout)
                .redirect(reqwest::redirect::Policy::none())
                .no_proxy()
                .build()
                .expect("build direct client"),
        );
        let url = format!("{base_url}/items");
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);
        let auth = AuthSpec::HeaderAuth(HeaderAuthSpec {
            headers: vec![HeaderSpec {
                name: "X-Api-Key".to_string(),
                value: ValueSourceSpec::Literal {
                    value: serde_json::Value::String("runtime-secret".to_string()),
                },
            }],
        });

        let response = execute_request(
            &http,
            request_timeout,
            TestOutgoingHttpRequest {
                auth: &auth,
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                require_credential_safe_auth_transport: true,
                secret_provenance: SecretProvenance::Public,
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &url,
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
        )
        .await
        .expect("API-key request should succeed");

        assert!(response.expect("decoded response").credential_tainted);
        let raw_request = task.await.expect("header recorder should finish");
        assert!(
            raw_request.contains("\r\nx-api-key: runtime-secret\r\n"),
            "{raw_request}"
        );
        assert!(
            proxy
                .received_requests()
                .await
                .expect("proxy request recording")
                .is_empty(),
            "the loopback API-key request and credential must bypass the proxy"
        );
    }
}
