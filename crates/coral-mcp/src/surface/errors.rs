use coral_client::{DecodedStatusError, decode_status_error};
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content},
};
use schemars::JsonSchema;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;

#[derive(Debug, Clone, Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct ToolError {
    pub(crate) summary: String,
    pub(crate) detail: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hint: Option<String>,
    pub(crate) grpc_code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
    pub(crate) retryable: bool,
    pub(crate) metadata: HashMap<String, String>,
}

pub(crate) fn tool_error_result(error: ToolError, data: Option<Value>) -> CallToolResult {
    let content = vec![Content::text(tool_error_text(&error, data.as_ref()))];
    let structured = match data {
        Some(data) => serde_json::to_value(ToolErrorWithData { error, data })
            .expect("tool error value with data serializes"),
        None => {
            serde_json::to_value(ToolErrorValue { error }).expect("tool error value serializes")
        }
    };
    let mut result = CallToolResult::structured_error(structured);
    result.content = content;
    result
}

fn tool_error_text(error: &ToolError, data: Option<&Value>) -> String {
    let mut lines = vec![format!("Error: {}", error.summary)];
    if !error.detail.trim().is_empty() && error.detail != error.summary {
        lines.push(format!("Detail: {}", error.detail));
    }
    if let Some(hint) = error.hint.as_deref() {
        lines.push(format!("Hint: {hint}"));
    }
    if error.reason.as_deref() == Some("SQL_BATCH_PARTIAL_FAILURE") {
        lines.extend(query_error_text(data));
    }
    lines.join("\n")
}

fn query_error_text(data: Option<&Value>) -> Vec<String> {
    let Some(results) = data
        .and_then(|data| data.get("results"))
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    results
        .iter()
        .filter(|result| result.get("status").and_then(Value::as_str) == Some("error"))
        .filter_map(|result| {
            let error = result.get("error")?;
            let summary = error
                .get("summary")
                .and_then(Value::as_str)
                .unwrap_or("Query failed");
            let detail = error.get("detail").and_then(Value::as_str).unwrap_or("");
            let label = result
                .get("index")
                .and_then(Value::as_u64)
                .map_or_else(|| "Query".to_string(), |index| format!("Query [{index}]"));
            let mut lines = vec![format!("{label}: {summary}")];
            if !detail.trim().is_empty() && detail != summary {
                lines.push(format!("  Detail: {detail}"));
            }
            if let Some(hint) = error
                .get("hint")
                .and_then(Value::as_str)
                .filter(|hint| !hint.trim().is_empty())
            {
                lines.push(format!("  Hint: {hint}"));
            }
            Some(lines.join("\n"))
        })
        .collect()
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

#[derive(Serialize, JsonSchema)]
#[schemars(deny_unknown_fields)]
pub(crate) struct ToolErrorWithData<T> {
    pub(crate) error: ToolError,
    pub(crate) data: T,
}

#[derive(Serialize)]
struct ToolErrorValue {
    error: ToolError,
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
        clippy::get_unwrap,
        clippy::indexing_slicing,
        reason = "JSON shape assertions intentionally fail loudly in tests"
    )]

    use std::collections::HashMap;

    use rmcp::model::{CallToolResult, ErrorCode};
    use tonic::{Code, Status};
    use tonic_types::{ErrorDetail, StatusExt as _};

    use coral_client::CORAL_ERROR_DOMAIN;

    use super::{ToolError, status_to_error_data, tool_error_from_status, tool_error_result};

    fn first_text_content(result: &CallToolResult) -> &str {
        result
            .content
            .first()
            .and_then(|content| content.as_text())
            .map(|text| text.text.as_str())
            .expect("tool error text content")
    }

    #[test]
    fn tool_error_result_includes_structured_error_payload() {
        let result = tool_error_result(
            ToolError {
                summary: "Query failed".to_string(),
                detail: "planner error".to_string(),
                hint: Some("Retry with valid SQL.".to_string()),
                grpc_code: "InvalidArgument".to_string(),
                reason: None,
                retryable: false,
                metadata: HashMap::new(),
            },
            None,
        );
        assert_eq!(result.is_error, Some(true));
        let text = first_text_content(&result);
        assert!(text.contains("Error: Query failed"));
        assert!(text.contains("Detail: planner error"));
        let json = result.structured_content.expect("structured content");
        assert_eq!(json["error"]["grpc_code"], "InvalidArgument");
        assert_eq!(json["error"]["retryable"], false);
        assert!(
            json.as_object()
                .expect("structured object")
                .get("data")
                .is_none()
        );
    }

    #[test]
    fn tool_error_result_can_include_data_in_canonical_error_envelope() {
        let result = tool_error_result(
            ToolError {
                summary: "One or more SQL queries failed".to_string(),
                detail: "Inspect `data.results` for per-query successes and errors.".to_string(),
                hint: None,
                grpc_code: "Unknown".to_string(),
                reason: Some("SQL_BATCH_PARTIAL_FAILURE".to_string()),
                retryable: false,
                metadata: HashMap::new(),
            },
            Some(serde_json::json!({ "total_count": 2, "results": [] })),
        );

        assert_eq!(result.is_error, Some(true));
        let json = result.structured_content.expect("structured content");
        assert_eq!(json["error"]["reason"], "SQL_BATCH_PARTIAL_FAILURE");
        assert_eq!(json["data"]["total_count"], 2);
    }

    #[test]
    fn tool_error_result_surfaces_per_query_detail_in_content() {
        let result = tool_error_result(
            ToolError {
                summary: "One or more SQL queries failed".to_string(),
                detail: "Inspect `data.results` for per-query successes and errors.".to_string(),
                hint: None,
                grpc_code: "Unknown".to_string(),
                reason: Some("SQL_BATCH_PARTIAL_FAILURE".to_string()),
                retryable: false,
                metadata: HashMap::new(),
            },
            Some(serde_json::json!({
                "total_count": 2,
                "results": [
                    {
                        "index": 0,
                        "status": "success",
                        "rows": []
                    },
                    {
                        "index": 1,
                        "status": "error",
                        "error": {
                            "summary": "Query request is invalid",
                            "detail": "table 'nope' not found",
                            "hint": "Check the SQL and retry.",
                            "grpc_code": "InvalidArgument",
                            "retryable": false,
                            "metadata": {}
                        }
                    }
                ]
            })),
        );

        let text = first_text_content(&result);
        assert!(text.contains("Query [1]: Query request is invalid"));
        assert!(text.contains("  Detail: table 'nope' not found"));
        assert!(text.contains("  Hint: Check the SQL and retry."));
        assert!(!text.contains("Query [0]"));
    }

    #[test]
    fn tool_error_serializes_directly_with_structured_vocabulary() {
        let json = serde_json::to_value(ToolError {
            summary: "Query failed".to_string(),
            detail: "planner error".to_string(),
            hint: Some("Retry with valid SQL.".to_string()),
            grpc_code: "InvalidArgument".to_string(),
            reason: Some("INVALID_SQL".to_string()),
            retryable: false,
            metadata: HashMap::from([("schema".to_string(), "local".to_string())]),
        })
        .expect("tool error serializes");

        assert_eq!(json["summary"], "Query failed");
        assert_eq!(json["detail"], "planner error");
        assert_eq!(json["hint"], "Retry with valid SQL.");
        assert_eq!(json["grpc_code"], "InvalidArgument");
        assert_eq!(json["reason"], "INVALID_SQL");
        assert_eq!(json["retryable"], false);
        assert_eq!(json["metadata"]["schema"], "local");
    }

    #[test]
    fn tool_error_direct_serialization_skips_absent_optional_fields() {
        let json = serde_json::to_value(ToolError {
            summary: "Query request is invalid".to_string(),
            detail: "DML not supported".to_string(),
            hint: None,
            grpc_code: "InvalidArgument".to_string(),
            reason: None,
            retryable: false,
            metadata: HashMap::new(),
        })
        .expect("tool error serializes");

        let object = json.as_object().expect("tool error object");
        assert!(!object.contains_key("hint"));
        assert!(!object.contains_key("reason"));
        assert_eq!(
            json["metadata"].as_object().expect("metadata object").len(),
            0
        );
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
        let result = tool_error_result(error, None);
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
        let result = tool_error_result(error, None);
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
        let result = tool_error_result(error, None);
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
