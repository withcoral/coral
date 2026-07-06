//! HTTP response body decoding.

use serde_json::Value;

use crate::backends::http::ProviderQueryError;
use crate::backends::http::trace::HttpBodyCapture;
use coral_spec::ResponseBodyFormat;

pub(super) struct ResponseDecodeContext<'a> {
    pub(super) source_schema: &'a str,
    pub(super) table_name: &'a str,
    pub(super) method_label: &'a str,
    pub(super) logged_url: &'a str,
    pub(super) body_capture: &'a HttpBodyCapture,
    pub(super) response_span: &'a tracing::Span,
    pub(super) request_id: u64,
}

pub(super) async fn decode_response_body(
    response: reqwest::Response,
    format: ResponseBodyFormat,
    context: ResponseDecodeContext<'_>,
) -> Result<Value, ProviderQueryError> {
    let ResponseDecodeContext {
        source_schema,
        table_name,
        method_label,
        logged_url,
        body_capture,
        response_span,
        request_id,
    } = context;
    match format {
        ResponseBodyFormat::Json => {
            let bytes = response.bytes().await.map_err(|error| {
                // A read failure mid-body is transient, so it is eligible for retry.
                decode_error(
                    source_schema,
                    table_name,
                    method_label,
                    logged_url,
                    format!("source API response decoding failed: {error}"),
                    true,
                )
            })?;
            response_span.record("http.response.body.size", bytes.len());
            let trace_body = String::from_utf8_lossy(&bytes);
            body_capture.record_response(response_span, request_id, trace_body.as_ref());
            serde_json::from_slice(&bytes).map_err(|error| {
                decode_error(
                    source_schema,
                    table_name,
                    method_label,
                    logged_url,
                    format!("source API response decoding failed: {error}"),
                    is_retryable_partial_json_decode_error(&error, &bytes),
                )
            })
        }
        ResponseBodyFormat::JsonEachRow => {
            let text = response.text().await.map_err(|error| {
                // A read failure mid-body is transient, so it is eligible for retry.
                decode_error(
                    source_schema,
                    table_name,
                    method_label,
                    logged_url,
                    format!("source API response decoding failed: {error}"),
                    true,
                )
            })?;
            response_span.record("http.response.body.size", text.len());
            body_capture.record_response(response_span, request_id, &text);
            let mut rows = Vec::new();
            for (index, line) in text.lines().enumerate() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                let row: Value = serde_json::from_str(trimmed).map_err(|error| {
                    decode_error(
                        source_schema,
                        table_name,
                        method_label,
                        logged_url,
                        format!(
                            "source API response decoding failed: json_each_row line {} is not valid JSON: {error}",
                            index + 1
                        ),
                        is_retryable_partial_json_decode_error(&error, trimmed.as_bytes()),
                    )
                })?;
                rows.push(row);
            }
            Ok(Value::Array(rows))
        }
    }
}

/// Builds a decode failure for an HTTP response body. `retryable` marks transient
/// failures — a truncated/EOF body or a mid-stream read error — that the transport
/// layer may retry for idempotent requests.
fn decode_error(
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
    detail: String,
    retryable: bool,
) -> ProviderQueryError {
    ProviderQueryError::Decode {
        source_schema: source_schema.to_string(),
        table: table_name.to_string(),
        method: Some(method_label.to_string()),
        url: Some(logged_url.to_string()),
        detail,
        retryable,
    }
}

/// Returns `true` when a JSON decode failure looks like a truncated response — an
/// unexpected EOF over a body that carried at least some content — rather than a
/// structurally malformed payload, which would fail identically on every attempt.
fn is_retryable_partial_json_decode_error(error: &serde_json::Error, body: &[u8]) -> bool {
    matches!(error.classify(), serde_json::error::Category::Eof)
        && body.iter().any(|byte| !byte.is_ascii_whitespace())
}
