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
    pub(super) max_body_bytes: Option<usize>,
}

pub(super) struct BoundedResponseBody {
    pub(super) bytes: Vec<u8>,
    pub(super) truncated: bool,
}

pub(super) async fn read_bounded_response_body(
    mut response: reqwest::Response,
    max_body_bytes: usize,
) -> reqwest::Result<BoundedResponseBody> {
    if response
        .content_length()
        .is_some_and(|length| length > u64::try_from(max_body_bytes).unwrap_or(u64::MAX))
    {
        return Ok(BoundedResponseBody {
            bytes: Vec::new(),
            truncated: true,
        });
    }

    let read_limit = max_body_bytes.saturating_add(1);
    let mut bytes = Vec::new();
    while bytes.len() < read_limit {
        let Some(chunk) = response.chunk().await? else {
            break;
        };
        let remaining = read_limit.saturating_sub(bytes.len());
        let take = chunk.len().min(remaining);
        bytes.extend(chunk.iter().copied().take(take));
        if take < chunk.len() {
            break;
        }
    }
    let truncated = bytes.len() > max_body_bytes;
    bytes.truncate(max_body_bytes);
    Ok(BoundedResponseBody { bytes, truncated })
}

#[expect(
    clippy::too_many_lines,
    reason = "The two response formats intentionally share one decode boundary so controlled and ordinary error behavior cannot drift."
)]
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
        max_body_bytes,
    } = context;
    match format {
        ResponseBodyFormat::Json => {
            let bytes = match max_body_bytes {
                Some(max_body_bytes) => {
                    let body = read_bounded_response_body(response, max_body_bytes)
                        .await
                        .map_err(|error| {
                            response_read_error(
                                source_schema,
                                table_name,
                                method_label,
                                logged_url,
                                &error,
                                true,
                            )
                        })?;
                    if body.truncated {
                        return Err(controlled_body_limit_error(
                            source_schema,
                            table_name,
                            method_label,
                            logged_url,
                            max_body_bytes,
                        ));
                    }
                    body.bytes
                }
                None => response
                    .bytes()
                    .await
                    .map_err(|error| {
                        response_read_error(
                            source_schema,
                            table_name,
                            method_label,
                            logged_url,
                            &error,
                            false,
                        )
                    })?
                    .to_vec(),
            };
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
            let text = match max_body_bytes {
                Some(max_body_bytes) => {
                    let body = read_bounded_response_body(response, max_body_bytes)
                        .await
                        .map_err(|error| {
                            response_read_error(
                                source_schema,
                                table_name,
                                method_label,
                                logged_url,
                                &error,
                                true,
                            )
                        })?;
                    if body.truncated {
                        return Err(controlled_body_limit_error(
                            source_schema,
                            table_name,
                            method_label,
                            logged_url,
                            max_body_bytes,
                        ));
                    }
                    String::from_utf8_lossy(&body.bytes).into_owned()
                }
                None => response.text().await.map_err(|error| {
                    response_read_error(
                        source_schema,
                        table_name,
                        method_label,
                        logged_url,
                        &error,
                        false,
                    )
                })?,
            };
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

fn response_read_error(
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
    error: &reqwest::Error,
    controlled: bool,
) -> ProviderQueryError {
    if controlled && error.is_timeout() {
        return ProviderQueryError::ExecutionTimedOut {
            source_schema: source_schema.to_string(),
            table: table_name.to_string(),
        };
    }
    // A non-timeout read failure mid-body is transient, so it remains eligible
    // for an ordinary idempotent retry.
    decode_error(
        source_schema,
        table_name,
        method_label,
        logged_url,
        format!("source API response decoding failed: {error}"),
        true,
    )
}

fn controlled_body_limit_error(
    source_schema: &str,
    table_name: &str,
    method_label: &str,
    logged_url: &str,
    max_body_bytes: usize,
) -> ProviderQueryError {
    decode_error(
        source_schema,
        table_name,
        method_label,
        logged_url,
        format!("source API response exceeded the controlled {max_body_bytes}-byte limit"),
        false,
    )
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
