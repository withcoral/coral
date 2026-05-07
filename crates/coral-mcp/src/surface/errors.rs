use std::collections::HashMap;
use std::fmt::Write as _;

use coral_client::{DecodedStatusError, decode_status_error};
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content},
};
use serde_json::{Map, Value, json};

#[derive(Debug, Clone)]
pub(crate) struct ToolError {
    pub(crate) summary: String,
    pub(crate) detail: String,
    pub(crate) hint: Option<String>,
    pub(crate) grpc_code: String,
    pub(crate) reason: Option<String>,
    pub(crate) retryable: bool,
    pub(crate) metadata: HashMap<String, String>,
}

#[allow(
    clippy::needless_pass_by_value,
    reason = "callers always pass an owned ToolError that is not used after this call"
)]
pub(crate) fn tool_error_result(error: ToolError) -> CallToolResult {
    let mut text = format!("Error: {}", error.summary);
    if !error.detail.is_empty() {
        write!(text, "\nDetail: {}", error.detail).expect("writing to String cannot fail");
    }
    if let Some(hint) = &error.hint {
        write!(text, "\nHint: {hint}").expect("writing to String cannot fail");
    }

    let mut error_obj = json!({
        "summary": error.summary,
        "detail": error.detail,
        "grpc_code": error.grpc_code,
        "retryable": error.retryable,
    });
    let obj = error_obj.as_object_mut().expect("just created as object");
    if let Some(hint) = &error.hint {
        obj.insert("hint".to_string(), Value::String(hint.clone()));
    }
    if let Some(reason) = &error.reason {
        obj.insert("reason".to_string(), Value::String(reason.clone()));
    }
    let metadata = error
        .metadata
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect::<Map<_, _>>();
    obj.insert("metadata".to_string(), Value::Object(metadata));

    let structured = json!({ "error": error_obj });
    let mut result = CallToolResult::structured_error(structured);
    result.content = vec![Content::text(text)];
    result
}

pub(crate) fn tool_error_from_status(operation: &str, status: &tonic::Status) -> ToolError {
    let grpc_code = status.code().to_string();

    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => ToolError {
            summary: error.summary.clone(),
            detail: error.detail.clone(),
            hint: error.hint.clone(),
            grpc_code,
            reason: Some(error.reason.clone()),
            retryable: error.retryable,
            metadata: error.metadata.clone(),
        },
        DecodedStatusError::Plain(message) => {
            let code = status.code();
            let (summary, hint) = plain_fallback(operation, code);
            ToolError {
                summary,
                detail: message,
                hint,
                grpc_code,
                reason: None,
                retryable: code == tonic::Code::Unavailable,
                metadata: HashMap::new(),
            }
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
                "Confirm the visible source, schema, and table names before retrying.".to_string(),
            ),
        ),
        tonic::Code::FailedPrecondition => (
            format!("{operation} prerequisites are not satisfied"),
            Some("Check source configuration and required filters, then retry.".to_string()),
        ),
        tonic::Code::Unavailable => (
            format!("{operation} is unavailable"),
            Some("Retry once the local query runtime is available.".to_string()),
        ),
        _ => (format!("{operation} failed"), None),
    }
}

pub(crate) fn status_to_error_data(status: &tonic::Status) -> ErrorData {
    match decode_status_error(status) {
        DecodedStatusError::Structured(error) => {
            let mut data = json!({
                "detail": error.detail,
                "grpc_code": status.code().to_string(),
                "reason": error.reason,
                "retryable": error.retryable,
                "metadata": error.metadata,
            });
            if let Some(hint) = error.hint {
                data["hint"] = Value::String(hint);
            }
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

pub(crate) fn internal_status(error: &serde_json::Error) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use rmcp::model::ErrorCode;
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use coral_client::CORAL_ERROR_DOMAIN;

    use super::{ToolError, status_to_error_data, tool_error_from_status, tool_error_result};

    #[test]
    fn tool_error_result_includes_structured_error_payload() {
        let result = tool_error_result(ToolError {
            summary: "Query failed".to_string(),
            detail: "planner error".to_string(),
            hint: Some("Retry with valid SQL.".to_string()),
            grpc_code: "InvalidArgument".to_string(),
            reason: None,
            retryable: false,
            metadata: HashMap::new(),
        });
        assert_eq!(result.is_error, Some(true));
        let json = result.structured_content.expect("structured content");
        assert_eq!(json["error"]["grpc_code"], "InvalidArgument");
        assert_eq!(json["error"]["retryable"], false);
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
        let error = tool_error_from_status("Query", &status);
        assert_eq!(
            error.summary,
            "github.pulls requires `WHERE owner = <constant>`"
        );
        assert_eq!(error.detail, "missing required filter");
        assert_eq!(
            error.hint.as_deref(),
            Some("Add a constant equality filter on `owner`.")
        );
        assert_eq!(error.reason.as_deref(), Some("MISSING_REQUIRED_FILTER"));
        assert!(!error.retryable);
        assert_eq!(error.metadata.get("schema").unwrap(), "github");
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
        let error = tool_error_from_status("Query", &status);
        let result = tool_error_result(error);
        let json = result.structured_content.expect("structured content");
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
        let error = tool_error_from_status("Query", &status);
        assert!(error.retryable);
        let result = tool_error_result(error);
        assert_eq!(
            result.structured_content.expect("structured content")["error"]["retryable"],
            true
        );
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
        let error = tool_error_from_status("Query", &status);
        let result = tool_error_result(error);
        let json = result.structured_content.expect("structured content");
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
        let error = tool_error_from_status("Query", &status);
        assert_eq!(error.summary, "Query request is invalid");
        assert_eq!(error.detail, "SQL must not be empty");
        assert!(error.hint.is_some());
        assert!(error.reason.is_none());
        assert!(!error.retryable);
    }

    #[test]
    fn plain_unavailable_is_retryable() {
        let status = Status::new(Code::Unavailable, "transport error");
        let error = tool_error_from_status("Query", &status);
        assert!(error.retryable, "plain Unavailable should be retryable");
        assert_eq!(error.summary, "Query is unavailable");
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
