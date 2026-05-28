//! HTTP request execution, retry, tracing, and response decoding.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use datafusion::error::{DataFusionError, Result};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use serde_json::Value;
use tracing::Instrument as _;
use tracing::field;

use crate::RequestAuthenticator;
use crate::backends::http::ProviderQueryError;
use crate::backends::http::auth::resolve_auth_headers;
use crate::backends::http::error::{pagination_error, provider_error};
use crate::backends::http::pagination::extract_next_link_url;
use crate::backends::http::rate_limit::{RateLimitDecision, check_rate_limit};
use crate::backends::http::request::RequestBody;
use crate::backends::http::response::{ResponseDecodeContext, decode_response_body};
use crate::backends::http::trace::{
    HttpBodyCapture, inject_trace_context, record_http_processing_error, record_http_status_error,
    record_trace_http_endpoint, request_body_size, sanitize_trace_url, trace_http_endpoint,
    trace_reqwest_error, trace_reqwest_error_type,
};
use crate::backends::shared::template::{RenderContext, resolve_value_source, value_to_string};
use coral_spec::backends::http::RateLimitSpec;
use coral_spec::{AuthSpec, HeaderSpec, HttpMethod, ResponseBodyFormat};

static NEXT_HTTP_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

pub(super) struct OutgoingHttpRequest<'a> {
    pub(super) auth: &'a AuthSpec,
    pub(super) request_headers: &'a [HeaderSpec],
    pub(super) request_authenticators: &'a HashMap<String, Arc<dyn RequestAuthenticator>>,
    pub(super) table_headers: &'a [HeaderSpec],
    pub(super) table_name: &'a str,
    pub(super) method: HttpMethod,
    pub(super) base_url: &'a str,
    pub(super) url: &'a str,
    pub(super) query_pairs: &'a [(String, String)],
    pub(super) body: Option<&'a RequestBody>,
    pub(super) response_format: ResponseBodyFormat,
    pub(super) source_schema: &'a str,
    pub(super) rate_limit: &'a RateLimitSpec,
    pub(super) body_capture: HttpBodyCapture,
    pub(super) render_context: RenderContext<'a>,
    pub(super) allow_404_empty: bool,
    pub(super) link_header_require_results: bool,
}

#[expect(
    clippy::too_many_lines,
    reason = "HTTP request execution keeps retry, auth, logging, and response handling in one audited flow"
)]
pub(super) async fn execute_request(
    http: &reqwest::Client,
    request_timeout: Duration,
    request: OutgoingHttpRequest<'_>,
) -> Result<Option<(Value, Option<String>)>> {
    enum ResponseOutcome {
        Done(Result<Option<(Value, Option<String>)>>),
        Retry(Duration),
    }

    let OutgoingHttpRequest {
        auth,
        request_headers,
        request_authenticators,
        table_headers,
        table_name,
        method,
        base_url,
        url,
        query_pairs,
        body,
        response_format,
        source_schema,
        rate_limit,
        body_capture,
        render_context,
        allow_404_empty,
        link_header_require_results,
    } = request;
    let mut server_error_retries = 0usize;
    let mut throttle_retries = 0usize;
    loop {
        let method_label = http_method_label(method);
        let mut request = build_http_request(http, method, url);

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
        let attempt = server_error_retries + throttle_retries + 1;
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
        let built = match resolve_auth_headers(
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
        let response = match http.execute(built).instrument(request_span.clone()).await {
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

            let next_url =
                extract_next_link_url(response.headers(), base_url, link_header_require_results)
                    .map_err(|error| {
                        record_http_processing_error(&request_span, "PAGINATION", &error);
                        pagination_error(
                            source_schema,
                            table_name,
                            Some(method_label),
                            Some(&logged_url),
                            &error,
                        )
                    });
            let next_url = match next_url {
                Ok(next_url) => next_url,
                Err(error) => break 'response ResponseOutcome::Done(Err(error)),
            };

            let payload = decode_response_body(
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
            .inspect_err(|error| {
                record_http_processing_error(&request_span, "DECODE", error);
            })
            .map(|payload| Some((payload, next_url)));
            ResponseOutcome::Done(payload)
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
    use std::time::Duration;

    use datafusion::error::DataFusionError;
    use tokio::net::TcpListener;
    use tokio::task::JoinHandle;

    use super::{OutgoingHttpRequest as TestOutgoingHttpRequest, execute_request};
    use crate::backends::http::ProviderQueryError;
    use crate::backends::http::trace::HttpBodyCapture;
    use crate::backends::shared::template::RenderContext;
    use coral_spec::backends::http::RateLimitSpec;
    use coral_spec::{AuthSpec, HttpMethod, ResponseBodyFormat};

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
                table_headers: &[],
                table_name: "items",
                method: HttpMethod::GET,
                base_url: &base_url,
                url: &url,
                query_pairs: &query_pairs,
                body: None,
                response_format: ResponseBodyFormat::default(),
                source_schema: "demo",
                rate_limit: &RateLimitSpec::default(),
                body_capture: HttpBodyCapture::default(),
                render_context,
                allow_404_empty: false,
                link_header_require_results: false,
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
}
