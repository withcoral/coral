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

use crate::backends::CONTROLLED_RESPONSE_BODY_LIMIT_BYTES;
use crate::backends::http::ProviderQueryError;
use crate::backends::http::auth::{
    ensure_identity_headers_use_credential_safe_transport, resolve_auth_headers,
};
use crate::backends::http::error::{execution_stopped_error, provider_error};
use crate::backends::http::rate_limit::{RateLimitDecision, check_rate_limit};
use crate::backends::http::request::RequestBody;
use crate::backends::http::response::{ResponseDecodeContext, decode_response_body};
use crate::backends::http::trace::{
    HttpBodyCapture, inject_trace_context, record_http_processing_error, record_http_status_error,
    record_trace_http_endpoint, request_body_size, sanitize_trace_url, trace_http_endpoint,
    trace_reqwest_error, trace_reqwest_error_type,
};
use crate::backends::shared::template::{RenderContext, resolve_text_value_source};
use crate::{
    BoundRequestIdentityHttpAuthenticator, QueryExecutionControls, QueryRetryPolicy,
    RequestAuthenticator, RequestIdentityHttpAuthenticatorError,
};
use coral_spec::backends::http::RateLimitSpec;
use coral_spec::{AuthSpec, HeaderSpec, HttpMethod, ResponseBodyFormat};

static NEXT_HTTP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct OutgoingHttpRequest<'a> {
    pub(super) auth: &'a AuthSpec,
    pub(super) request_headers: &'a [HeaderSpec],
    pub(super) request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
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
}

#[expect(
    clippy::too_many_lines,
    reason = "HTTP request execution keeps retry, auth, logging, and response handling in one audited flow"
)]
pub(super) async fn execute_request(
    http: &reqwest::Client,
    request_timeout: Duration,
    request: OutgoingHttpRequest<'_>,
    controls: &QueryExecutionControls,
) -> Result<Option<DecodedHttpResponse>> {
    enum ResponseOutcome {
        Done(Result<Option<DecodedHttpResponse>>),
        Retry(Duration),
    }

    let OutgoingHttpRequest {
        auth,
        request_headers,
        request_authenticators,
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
    let controlled_body_limit = controls
        .requires_transport_enforcement()
        .then_some(CONTROLLED_RESPONSE_BODY_LIMIT_BYTES);
    let body_capture = match controlled_body_limit {
        Some(limit) => body_capture.capped(limit),
        None => body_capture,
    };
    let mut server_error_retries = 0usize;
    let mut throttle_retries = 0usize;
    let mut decode_retries = 0usize;
    loop {
        controls
            .check_active()
            .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?;
        let effective_timeout = controls
            .effective_timeout(request_timeout)
            .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?;
        let method_label = http_method_label(method);
        let mut request = build_http_request(http, method, url).timeout(effective_timeout);

        let mut header_map = HeaderMap::new();
        for header in request_headers.iter().chain(table_headers.iter()) {
            if let Some(value) = resolve_text_value_source(&header.value, &render_context)? {
                let name = HeaderName::try_from(header.name.as_str()).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "invalid request header name '{}': {error}",
                        header.name
                    ))
                })?;
                let value = HeaderValue::try_from(value.as_str()).map_err(|error| {
                    DataFusionError::Execution(format!(
                        "invalid request header value for '{}': {error}",
                        header.name
                    ))
                })?;
                header_map.insert(name, value);
            }
        }
        if matches!(body, Some(RequestBody::Text(_)))
            && !header_map.contains_key(reqwest::header::CONTENT_TYPE)
        {
            header_map.insert(
                reqwest::header::CONTENT_TYPE,
                HeaderValue::from_static("text/plain"),
            );
        }
        let logged_url = build_logged_url(url, query_pairs);

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
        let mut built = match resolve_auth_headers(
            auth,
            request,
            request_authenticators,
            render_context.resolved_inputs,
        ) {
            Ok(request) => request,
            Err(error) => {
                record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                return Err(error);
            }
        };
        if let Some(authenticator) = request_identity_http_authenticator {
            let identity_headers = match controls
                .run_until_stopped(authenticator(&built, render_context.resolved_inputs))
                .await
                .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?
            {
                Ok(headers) => headers,
                Err(error) => {
                    let error =
                        identity_http_authenticator_error(source_schema, table_name, &error);
                    record_http_processing_error(&request_span, "REQUEST_SETUP", &error);
                    return Err(error);
                }
            };
            if !identity_headers.is_empty()
                && let Err(error) =
                    ensure_identity_headers_use_credential_safe_transport(built.url())
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
        let response = controls
            .run_until_stopped(async {
                controls.mark_upstream_started();
                http.execute(built).instrument(request_span.clone()).await
            })
            .await
            .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?;
        let response = match response {
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
                    effective_timeout,
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

            let rate_limit_retries = match controls.retry_policy() {
                QueryRetryPolicy::SourceDefault => throttle_retries,
                QueryRetryPolicy::Disabled => usize::MAX,
            };
            match check_rate_limit(status, response.headers(), rate_limit, rate_limit_retries) {
                RateLimitDecision::Continue => {}
                RateLimitDecision::Retry(wait) => {
                    record_http_status_error(&request_span, status, "rate limited; retrying");
                    if controlled_body_limit.is_none() {
                        controls
                            .run_until_stopped(body_capture.record_unconsumed_response(
                                &request_span,
                                request_id,
                                response,
                            ))
                            .await
                            .map_err(|kind| {
                                execution_stopped_error(source_schema, table_name, kind)
                            })?;
                    }
                    throttle_retries += 1;
                    break 'response ResponseOutcome::Retry(wait);
                }
                RateLimitDecision::Fail(error) => {
                    let error_message = error.to_string();
                    record_http_status_error(&request_span, status, error_message.as_str());
                    if controlled_body_limit.is_none() {
                        controls
                            .run_until_stopped(body_capture.record_unconsumed_response(
                                &request_span,
                                request_id,
                                response,
                            ))
                            .await
                            .map_err(|kind| {
                                execution_stopped_error(source_schema, table_name, kind)
                            })?;
                    }
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

            if status.is_server_error()
                && server_error_retries < 2
                && controls.retry_policy() == QueryRetryPolicy::SourceDefault
            {
                record_http_status_error(&request_span, status, "server error; retrying");
                if controlled_body_limit.is_none() {
                    controls
                        .run_until_stopped(body_capture.record_unconsumed_response(
                            &request_span,
                            request_id,
                            response,
                        ))
                        .await
                        .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?;
                }
                server_error_retries += 1;
                break 'response ResponseOutcome::Retry(Duration::from_secs(2));
            }

            if status == reqwest::StatusCode::NOT_FOUND && allow_404_empty {
                if controlled_body_limit.is_none() {
                    controls
                        .run_until_stopped(body_capture.record_unconsumed_response(
                            &request_span,
                            request_id,
                            response,
                        ))
                        .await
                        .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?;
                }
                break 'response ResponseOutcome::Done(Ok(None));
            }

            if !status.is_success() {
                let body = match controlled_body_limit {
                    // A known HTTP status is already a complete safe failure
                    // classification. Do not let an untrusted error body turn
                    // 401/403/429/5xx into a later timeout.
                    Some(_) => String::new(),
                    None => controls
                        .run_until_stopped(response.text().instrument(request_span.clone()))
                        .await
                        .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?
                        .unwrap_or_default(),
                };
                record_http_status_error(
                    &request_span,
                    status,
                    response_error_summary(status, &body),
                );
                request_span.record("http.response.body.size", body.len());
                body_capture.record_response(&request_span, request_id, &body);
                break 'response ResponseOutcome::Done(Err(DataFusionError::External(Box::new(
                    ProviderQueryError::ApiRequest {
                        source_schema: source_schema.to_string(),
                        table: table_name.to_string(),
                        status: Some(status.as_u16()),
                        method: Some(method_label.to_string()),
                        url: Some(logged_url.clone()),
                        filters: render_context.filters.clone(),
                        detail: body,
                    },
                ))));
            }

            let response_headers = response.headers().clone();

            match controls
                .run_until_stopped(
                    decode_response_body(
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
                            max_body_bytes: controlled_body_limit,
                        },
                    )
                    .instrument(request_span.clone()),
                )
                .await
                .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?
            {
                Ok(payload) => ResponseOutcome::Done(Ok(Some(DecodedHttpResponse {
                    payload,
                    headers: response_headers,
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
                    if retry
                        && decode_retries < 2
                        && controls.retry_policy() == QueryRetryPolicy::SourceDefault
                    {
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
                controls
                    .sleep(wait)
                    .await
                    .map_err(|kind| execution_stopped_error(source_schema, table_name, kind))?;
            }
        }
    }
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
    use reqwest::header::{AUTHORIZATION, HeaderValue};
    use serde_json::json;
    use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;
    use tokio::time::Instant;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use super::{OutgoingHttpRequest as TestOutgoingHttpRequest, execute_request};
    use crate::backends::BackendRegistrationContext;
    use crate::backends::CONTROLLED_RESPONSE_BODY_LIMIT_BYTES;
    use crate::backends::http::ProviderQueryError;
    use crate::backends::http::client::single_attempt_http_client;
    use crate::backends::http::trace::HttpBodyCapture;
    use crate::backends::shared::template::RenderContext;
    use crate::{
        BoundRequestIdentityHttpAuthenticator, QueryCancellationToken, QueryExecutionControls,
        QueryPaginationPolicy, QueryRetryPolicy, RequestIdentityHttpAuthenticatorError,
    };
    use coral_spec::backends::http::RateLimitSpec;
    use coral_spec::{AuthSpec, HeaderSpec, HttpMethod, ResponseBodyFormat, ValueSourceSpec};

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

    async fn spawn_partial_body_server(status: u16) -> (String, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind partial-body http server");
        let addr = listener.local_addr().expect("local addr");
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.expect("accept request");
            let mut request = [0_u8; 4096];
            let _read = socket.read(&mut request).await.expect("read request");
            let prefix = b"{\"data\":[";
            let headers = format!(
                "HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\nConnection: keep-alive\r\n\r\n"
            );
            socket
                .write_all(headers.as_bytes())
                .await
                .expect("write headers");
            socket
                .write_all(format!("{:X}\r\n", prefix.len()).as_bytes())
                .await
                .expect("write first chunk length");
            socket.write_all(prefix).await.expect("write first chunk");
            socket
                .write_all(b"\r\n100\r\nprovider-private")
                .await
                .expect("write partial second chunk");
            socket.flush().await.expect("flush partial response");
            std::future::pending::<()>().await;
        });

        (format!("http://{addr}"), task)
    }

    async fn controlled_request_error(
        http: &reqwest::Client,
        url: &str,
        request_timeout: Duration,
        execution_budget: Duration,
    ) -> DataFusionError {
        let controls = QueryExecutionControls::new(
            Some(Instant::now() + execution_budget),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        execute_request(
            http,
            request_timeout,
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url,
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::Json,
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
            &controls,
        )
        .await
        .expect_err("controlled test request should fail")
    }

    #[tokio::test]
    async fn execute_request_times_out_when_upstream_stalls() {
        let (base_url, task) = spawn_hanging_http_server().await;
        let request_timeout = Duration::from_millis(100);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("build test client");
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
            &QueryExecutionControls::default(),
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
    async fn execution_deadline_caps_ordinary_http_timeout_when_upstream_stalls() {
        let (base_url, task) = spawn_hanging_http_server().await;
        let request_timeout = Duration::from_secs(30);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("build test client");
        let controls = QueryExecutionControls::new(
            Some(Instant::now() + Duration::from_millis(100)),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::SourceDefault,
            QueryRetryPolicy::SourceDefault,
        );
        let url = format!("{base_url}/items");
        let query_pairs = vec![("api_key".to_string(), "secret-token".to_string())];
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let error = tokio::time::timeout(
            Duration::from_secs(1),
            execute_request(
                &http,
                request_timeout,
                TestOutgoingHttpRequest {
                    auth: &AuthSpec::default(),
                    request_headers: &[],
                    request_authenticators: &HashMap::new(),
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
                &controls,
            ),
        )
        .await
        .expect("execution deadline should beat the outer test guard")
        .expect_err("hung upstream should time out");

        match error {
            DataFusionError::External(inner) => {
                let provider_error = inner
                    .downcast_ref::<ProviderQueryError>()
                    .expect("timeout should be a provider query error");
                match provider_error {
                    ProviderQueryError::ExecutionTimedOut {
                        source_schema,
                        table,
                    } => {
                        assert_eq!(source_schema, "demo");
                        assert_eq!(table, "items");
                    }
                    other => panic!("expected execution timeout, got {other:?}"),
                }
                let structured = provider_error.to_structured();
                assert_eq!(structured.reason(), "QUERY_EXECUTION_TIMEOUT");
                assert!(!structured.detail().contains("secret-token"));
            }
            other => panic!("expected external provider error, got {other:?}"),
        }
        assert!(controls.upstream_started());
        task.abort();
    }

    #[tokio::test]
    async fn request_identity_headers_cannot_overwrite_existing_headers() {
        let request_timeout = Duration::from_secs(1);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("build test client");
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
            &QueryExecutionControls::default(),
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

    #[tokio::test]
    async fn request_identity_headers_require_safe_transport() {
        let request_timeout = Duration::from_secs(1);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("build test client");
        let base_url = "http://api.example.test";
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
                request_headers: &[],
                request_authenticators: &HashMap::new(),
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
            &QueryExecutionControls::default(),
        )
        .await
        .expect_err("identity headers must not use unsafe transport");

        let error = error.to_string();
        assert!(error.contains("request identity HTTP headers require https"));
        assert!(error.contains(base_url));
        assert!(!error.contains("identity-token"));
    }

    #[tokio::test]
    async fn request_identity_headers_are_injected() {
        let (base_url, task) = spawn_header_recorder(r#"{"ok":true}"#).await;
        let request_timeout = Duration::from_secs(1);
        let http = reqwest::Client::builder()
            .timeout(request_timeout)
            .build()
            .expect("build test client");
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
            &QueryExecutionControls::default(),
        )
        .await
        .expect("identity-authenticated request should succeed");

        assert!(response.is_some());
        let raw_request = task.await.expect("header recorder should finish");
        assert!(
            raw_request.contains("\r\nx-identity-token: identity-token\r\n"),
            "{raw_request}"
        );
    }

    #[tokio::test]
    async fn controlled_partial_success_body_timeout_is_typed_timeout() {
        let (url, task) = spawn_partial_body_server(200).await;
        let http = reqwest::Client::new();
        let error = tokio::time::timeout(
            Duration::from_secs(2),
            controlled_request_error(
                &http,
                &url,
                Duration::from_millis(100),
                Duration::from_secs(1),
            ),
        )
        .await
        .expect("source body timeout should beat the outer guard");

        let DataFusionError::External(inner) = error else {
            panic!("expected external timeout error");
        };
        assert!(matches!(
            inner.downcast_ref::<ProviderQueryError>(),
            Some(ProviderQueryError::ExecutionTimedOut { .. })
        ));
        task.abort();
    }

    #[tokio::test]
    async fn ordinary_partial_body_timeout_keeps_legacy_decode_shape() {
        let (url, task) = spawn_partial_body_server(200).await;
        let http = reqwest::Client::new();
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let error = execute_request(
            &http,
            Duration::from_millis(100),
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::POST,
                url: &url,
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::Json,
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
            &QueryExecutionControls::default(),
        )
        .await
        .expect_err("ordinary partial response must retain its legacy decode failure");

        let DataFusionError::External(inner) = error else {
            panic!("expected external decode error");
        };
        let provider_error = inner
            .downcast_ref::<ProviderQueryError>()
            .expect("provider decode error");
        assert!(matches!(
            provider_error,
            ProviderQueryError::Decode {
                retryable: false,
                ..
            }
        ));
        assert_eq!(
            provider_error.execution_failure_kind(),
            Some(crate::QueryExecutionFailureKind::InvalidResponse)
        );
        task.abort();
    }

    #[tokio::test]
    async fn controlled_known_status_does_not_wait_for_stalled_error_body() {
        for (status, expected_kind) in [
            (401, crate::QueryExecutionFailureKind::Authentication),
            (503, crate::QueryExecutionFailureKind::UpstreamUnavailable),
        ] {
            let (url, task) = spawn_partial_body_server(status).await;
            let http = reqwest::Client::new();
            let error = tokio::time::timeout(
                Duration::from_secs(1),
                controlled_request_error(
                    &http,
                    &url,
                    Duration::from_secs(30),
                    Duration::from_millis(250),
                ),
            )
            .await
            .expect("known status should not wait for the stalled response body");
            let DataFusionError::External(inner) = error else {
                panic!("expected external status error");
            };
            let provider_error = inner
                .downcast_ref::<ProviderQueryError>()
                .expect("provider status error");
            assert!(
                matches!(
                    provider_error,
                    ProviderQueryError::ApiRequest {
                        status: Some(actual),
                        detail,
                        ..
                    } if *actual == status && detail.is_empty()
                ),
                "unexpected provider error: {provider_error:?}"
            );
            assert_eq!(provider_error.execution_failure_kind(), Some(expected_kind));
            task.abort();
        }
    }

    #[tokio::test]
    async fn pre_cancelled_execution_makes_no_http_request() {
        let server = MockServer::start().await;
        let cancellation = QueryCancellationToken::new();
        cancellation.cancel();
        let controls = QueryExecutionControls::new(
            None,
            cancellation,
            QueryPaginationPolicy::SourceDefault,
            QueryRetryPolicy::SourceDefault,
        );
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let error = execute_request(
            &reqwest::Client::new(),
            Duration::from_secs(30),
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &format!("{}/items", server.uri()),
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
            &controls,
        )
        .await
        .expect_err("pre-cancelled execution should fail before transport work");

        let DataFusionError::External(inner) = error else {
            panic!("expected external cancellation error");
        };
        assert!(matches!(
            inner.downcast_ref::<ProviderQueryError>(),
            Some(ProviderQueryError::ExecutionCancelled { .. })
        ));
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty()
        );
        assert!(!controls.upstream_started());
    }

    #[tokio::test]
    async fn controlled_execution_rejects_oversized_json_before_unbounded_decode() {
        let server = MockServer::start().await;
        let private_body = "provider-private"
            .repeat(CONTROLLED_RESPONSE_BODY_LIMIT_BYTES / "provider-private".len() + 2);
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": private_body })))
            .expect(1)
            .mount(&server)
            .await;
        let http = reqwest::Client::new();
        let deadline = Instant::now() + Duration::from_secs(1);
        let controls = QueryExecutionControls::new(
            Some(deadline),
            QueryCancellationToken::new(),
            QueryPaginationPolicy::FirstPageOnly,
            QueryRetryPolicy::Disabled,
        );
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);

        let error = execute_request(
            &http,
            Duration::from_secs(30),
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &format!("{}/items", server.uri()),
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::Json,
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
            &controls,
        )
        .await
        .expect_err("oversized controlled JSON must fail before serde decoding");

        let DataFusionError::External(inner) = error else {
            panic!("expected typed provider error");
        };
        let provider_error = inner
            .downcast_ref::<ProviderQueryError>()
            .expect("provider error");
        assert_eq!(
            provider_error.execution_failure_kind(),
            Some(crate::QueryExecutionFailureKind::InvalidResponse)
        );
        assert!(!provider_error.to_string().contains("provider-private"));
        assert!(Instant::now() <= deadline + Duration::from_millis(100));
    }

    #[tokio::test]
    async fn disabled_retry_policy_makes_one_request_for_server_error() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let controls = QueryExecutionControls::new(
            None,
            QueryCancellationToken::new(),
            QueryPaginationPolicy::SourceDefault,
            QueryRetryPolicy::Disabled,
        );
        let filters = HashMap::new();
        let args = HashMap::new();
        let state = HashMap::new();
        let resolved_inputs = BTreeMap::new();
        let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);
        let registration = BackendRegistrationContext::default();
        let http = single_attempt_http_client(&registration, "demo")
            .expect("build production single-attempt client");

        let error = execute_request(
            &http,
            Duration::from_secs(30),
            TestOutgoingHttpRequest {
                auth: &AuthSpec::default(),
                request_headers: &[],
                request_authenticators: &HashMap::new(),
                request_identity_http_authenticator: None,
                trace_context: None,
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                url: &format!("{}/items", server.uri()),
                query_pairs: &[],
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
            },
            &controls,
        )
        .await
        .expect_err("disabled retries should surface the first server error");

        let DataFusionError::External(inner) = error else {
            panic!("expected external API error");
        };
        assert!(matches!(
            inner.downcast_ref::<ProviderQueryError>(),
            Some(ProviderQueryError::ApiRequest {
                status: Some(500),
                ..
            })
        ));
        assert_eq!(server.received_requests().await.expect("requests").len(), 1);
    }

    #[tokio::test]
    async fn cancelling_retry_sleep_prevents_a_second_request() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(500))
            .mount(&server)
            .await;
        let cancellation = QueryCancellationToken::new();
        let controls = QueryExecutionControls::new(
            None,
            cancellation.clone(),
            QueryPaginationPolicy::SourceDefault,
            QueryRetryPolicy::SourceDefault,
        );
        let url = format!("{}/items", server.uri());
        let task = tokio::spawn(async move {
            let filters = HashMap::new();
            let args = HashMap::new();
            let state = HashMap::new();
            let resolved_inputs = BTreeMap::new();
            let render_context = RenderContext::new(&filters, &args, &state, &resolved_inputs);
            execute_request(
                &reqwest::Client::new(),
                Duration::from_secs(30),
                TestOutgoingHttpRequest {
                    auth: &AuthSpec::default(),
                    request_headers: &[],
                    request_authenticators: &HashMap::new(),
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
                &controls,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if !server
                    .received_requests()
                    .await
                    .expect("requests")
                    .is_empty()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("first request should arrive");
        tokio::time::sleep(Duration::from_millis(25)).await;
        cancellation.cancel();

        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("cancellation should interrupt retry sleep")
            .expect("request task should join")
            .expect_err("cancelled retry should fail");
        let DataFusionError::External(inner) = error else {
            panic!("expected external cancellation error");
        };
        assert!(matches!(
            inner.downcast_ref::<ProviderQueryError>(),
            Some(ProviderQueryError::ExecutionCancelled { .. })
        ));
        assert_eq!(server.received_requests().await.expect("requests").len(), 1);
    }
}
