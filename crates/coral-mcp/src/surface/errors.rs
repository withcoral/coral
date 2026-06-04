use coral_client::{DecodedStatusError, decode_status_error, render_error_block};
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content},
};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;

pub(crate) fn tool_error_result_from_status(
    operation: &str,
    status: &tonic::Status,
) -> CallToolResult {
    let grpc_code = status.code().to_string();

    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => tool_error_result(ToolErrorValue {
            summary: &error.summary,
            detail: &error.detail,
            hint: error.hint.as_deref(),
            grpc_code: &grpc_code,
            reason: Some(&error.reason),
            retryable: error.retryable,
            metadata: metadata_value(&error.metadata),
        }),
        DecodedStatusError::Plain(message) => {
            let code = status.code();
            let (summary, hint) = plain_fallback(operation, code);
            tool_error_result(ToolErrorValue {
                summary: &summary,
                detail: &message,
                hint: hint.as_deref(),
                grpc_code: &grpc_code,
                reason: None,
                retryable: code == tonic::Code::Unavailable,
                metadata: Value::Object(Map::new()),
            })
        }
    }
}

fn plain_fallback(operation: &str, code: tonic::Code) -> (String, Option<String>) {
    match code {
        tonic::Code::InvalidArgument => (
            format!("{operation} request is invalid"),
            Some(
                "Check the SQL and retry. Use `coral://guide`, `coral.tables`, \
                 and `coral.columns` for discovery."
                    .to_string(),
            ),
        ),
        tonic::Code::NotFound => (
            format!("{operation} target was not found"),
            Some(
                "Confirm the visible SQL schema and table names before retrying.".to_string(),
            ),
        ),
        tonic::Code::FailedPrecondition => (
            format!("{operation} prerequisites are not satisfied"),
            Some("Check database catalog metadata, configured inputs, and required filters, then retry.".to_string()),
        ),
        tonic::Code::Unavailable => (
            format!("{operation} is unavailable"),
            Some("Retry once the local query runtime is available.".to_string()),
        ),
        _ => (format!("{operation} failed"), None),
    }
}

fn tool_error_result(error: ToolErrorValue<'_>) -> CallToolResult {
    let text = render_error_block(error.summary, error.detail, error.hint);
    let structured = serde_json::to_value(StructuredToolErrorValue { error })
        .expect("tool error value serializes");
    let mut result = CallToolResult::structured_error(structured);
    result.content = vec![Content::text(text)];
    result
}

fn metadata_value(metadata: &HashMap<String, String>) -> Value {
    Value::Object(
        metadata
            .iter()
            .map(|(key, value)| (key.clone(), Value::String(value.clone())))
            .collect::<Map<_, _>>(),
    )
}

pub(crate) fn status_to_error_data(status: &tonic::Status) -> ErrorData {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => {
            let data = serde_json::to_value(StatusErrorDataValue {
                detail: &error.detail,
                grpc_code: status.code().to_string(),
                reason: &error.reason,
                retryable: error.retryable,
                metadata: &error.metadata,
                hint: error.hint.as_deref(),
            })
            .expect("status error data value serializes");
            match status.code() {
                tonic::Code::NotFound => ErrorData::resource_not_found(error.summary, Some(data)),
                tonic::Code::InvalidArgument => {
                    ErrorData::invalid_params(error.summary, Some(data))
                }
                _ => ErrorData::internal_error(error.summary, Some(data)),
            }
        }
        DecodedStatusError::Plain(message) => match status.code() {
            tonic::Code::NotFound => ErrorData::resource_not_found(message, None),
            tonic::Code::InvalidArgument => ErrorData::invalid_params(message, None),
            _ => ErrorData::internal_error(message, None),
        },
    }
}

#[derive(Serialize)]
struct StructuredToolErrorValue<'a> {
    error: ToolErrorValue<'a>,
}

#[derive(Serialize)]
struct ToolErrorValue<'a> {
    summary: &'a str,
    detail: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'a str>,
    grpc_code: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason: Option<&'a str>,
    retryable: bool,
    metadata: Value,
}

#[derive(Serialize)]
struct StatusErrorDataValue<'a> {
    detail: &'a str,
    grpc_code: String,
    reason: &'a str,
    retryable: bool,
    metadata: &'a HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hint: Option<&'a str>,
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "JSON shape assertions intentionally fail loudly in tests"
    )]

    use std::collections::HashMap;

    use rmcp::model::ErrorCode;
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use coral_client::CORAL_ERROR_DOMAIN;

    use super::{status_to_error_data, tool_error_result_from_status};

    #[test]
    fn tool_error_result_includes_structured_error_payload() {
        let status = Status::new(Code::InvalidArgument, "planner error");
        let result = tool_error_result_from_status("Query", &status);
        assert_eq!(result.is_error, Some(true));
        let json = result.structured_content.expect("structured content");
        assert_eq!(
            json["error"]["grpc_code"],
            Code::InvalidArgument.to_string()
        );
        assert_eq!(json["error"]["retryable"], false);
    }

    fn tool_error_json(status: &Status) -> serde_json::Value {
        tool_error_result_from_status("Query", status)
            .structured_content
            .expect("structured content")
    }

    fn build_coral_status(reason: &str, metadata: Vec<(&str, &str)>, retryable: bool) -> Status {
        let meta: HashMap<String, String> = metadata
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        let mut details: Vec<ErrorDetail> = vec![ErrorDetail::ErrorInfo(
            tonic_types::ErrorInfo::new(reason, CORAL_ERROR_DOMAIN, meta),
        )];
        if retryable {
            details.push(ErrorDetail::RetryInfo(tonic_types::RetryInfo::new(None)));
        }
        Status::with_error_details_vec(Code::FailedPrecondition, "plain fallback", details)
    }

    #[test]
    fn structured_status_produces_engine_provided_fields() {
        let status = build_coral_status(
            "MISSING_REQUIRED_FILTER",
            vec![
                (
                    "summary",
                    "github.pulls requires `WHERE owner = <constant>`",
                ),
                ("detail", "missing required filter"),
                ("hint", "Add a constant equality filter on `owner`."),
                ("schema", "github"),
                ("table", "pulls"),
                ("field", "owner"),
            ],
            false,
        );
        let json = tool_error_json(&status);
        assert_eq!(
            json["error"]["summary"],
            "github.pulls requires `WHERE owner = <constant>`"
        );
        assert_eq!(json["error"]["detail"], "missing required filter");
        assert_eq!(
            json["error"]["hint"],
            "Add a constant equality filter on `owner`."
        );
        assert_eq!(json["error"]["reason"], "MISSING_REQUIRED_FILTER");
        assert_eq!(json["error"]["retryable"], false);
        assert_eq!(json["error"]["metadata"]["schema"], "github");
    }

    #[test]
    fn structured_result_exposes_metadata_in_json() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source authentication failed (401)"),
                ("detail", "bad credentials"),
                ("hint", "Re-install the source."),
                ("source", "github"),
                ("http_status", "401"),
            ],
            false,
        );
        let json = tool_error_json(&status);
        assert_eq!(json["error"]["reason"], "PROVIDER_REQUEST_FAILED");
        assert_eq!(json["error"]["retryable"], false);
        assert_eq!(json["error"]["metadata"]["source"], "github");
        assert_eq!(json["error"]["metadata"]["http_status"], "401");
        // Reserved top-level fields must not be shadowed by provider metadata.
        assert!(
            json["error"]["source"].is_null(),
            "provider metadata must not leak into the reserved top-level namespace"
        );
    }

    #[test]
    fn retryable_status_sets_flag() {
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source rate limit exceeded (429)"),
                ("detail", "rate limited"),
            ],
            true,
        );
        assert_eq!(tool_error_json(&status)["error"]["retryable"], true);
    }

    #[test]
    fn provider_metadata_cannot_shadow_reserved_fields() {
        // A misbehaving source could stuff keys like `retryable` or `grpc_code`
        // into `ErrorInfo.metadata`. Nesting provider metadata under
        // `error.metadata` keeps the top-level shape stable for clients that
        // pattern-match on `retryable` / `reason` / `grpc_code`.
        let status = build_coral_status(
            "PROVIDER_REQUEST_FAILED",
            vec![
                ("summary", "Source error"),
                ("detail", "boom"),
                ("retryable", "true"),
                ("grpc_code", "Ok"),
                ("reason", "SPOOFED"),
            ],
            false,
        );
        let json = tool_error_json(&status);
        assert_eq!(json["error"]["retryable"], false);
        assert_eq!(
            json["error"]["grpc_code"],
            Code::FailedPrecondition.to_string()
        );
        assert_eq!(json["error"]["reason"], "PROVIDER_REQUEST_FAILED");
        assert_eq!(json["error"]["metadata"]["retryable"], "true");
        assert_eq!(json["error"]["metadata"]["grpc_code"], "Ok");
        assert_eq!(json["error"]["metadata"]["reason"], "SPOOFED");
    }

    #[test]
    fn plain_status_falls_back_to_static_dispatch() {
        let status = Status::new(Code::InvalidArgument, "SQL must not be empty");
        let json = tool_error_json(&status);
        assert_eq!(json["error"]["summary"], "Query request is invalid");
        assert_eq!(json["error"]["detail"], "SQL must not be empty");
        assert!(json["error"]["hint"].is_string());
        assert!(json["error"]["reason"].is_null());
        assert_eq!(json["error"]["retryable"], false);
    }

    #[test]
    fn plain_unavailable_is_retryable() {
        let status = Status::new(Code::Unavailable, "transport error");
        let json = tool_error_json(&status);
        assert_eq!(
            json["error"]["retryable"], true,
            "plain Unavailable should be retryable"
        );
        assert_eq!(json["error"]["summary"], "Query is unavailable");
    }

    #[test]
    fn structured_status_to_error_data_preserves_summary_and_metadata() {
        let status = build_coral_status(
            "MISSING_REQUIRED_FILTER",
            vec![
                (
                    "summary",
                    "github.pulls requires `WHERE owner = <constant>`",
                ),
                ("detail", "missing required filter"),
                ("hint", "Add a constant equality filter on `owner`."),
                ("schema", "github"),
                ("table", "pulls"),
            ],
            false,
        );

        let error = status_to_error_data(&status);

        assert_eq!(error.code, ErrorCode::INTERNAL_ERROR);
        assert_eq!(
            error.message,
            "github.pulls requires `WHERE owner = <constant>`"
        );
        let data = error.data.expect("structured data");
        assert_eq!(data["detail"], "missing required filter");
        assert_eq!(data["hint"], "Add a constant equality filter on `owner`.");
        assert_eq!(data["reason"], "MISSING_REQUIRED_FILTER");
        assert_eq!(data["retryable"], false);
        assert_eq!(data["metadata"]["schema"], "github");
        assert_eq!(data["metadata"]["table"], "pulls");
    }

    #[test]
    fn plain_status_to_error_data_keeps_legacy_message() {
        let status = Status::new(Code::NotFound, "resource not found: github.pulls");

        let error = status_to_error_data(&status);

        assert_eq!(error.code, ErrorCode::RESOURCE_NOT_FOUND);
        assert_eq!(error.message, "resource not found: github.pulls");
        assert!(error.data.is_none());
    }
}
