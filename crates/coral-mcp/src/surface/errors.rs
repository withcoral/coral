use std::collections::HashMap;
use std::fmt::Write as _;

use coral_api::{
    CORAL_ERROR_REASON_CONFIG_DIR_NOT_FOUND, CORAL_ERROR_REASON_CONFIG_WRITE_FAILED,
    CORAL_ERROR_REASON_EMPTY_SQL, CORAL_ERROR_REASON_INVALID_INPUT,
    CORAL_ERROR_REASON_INVALID_SECRETS_FILE, CORAL_ERROR_REASON_LOCAL_FILE_ERROR,
    CORAL_ERROR_REASON_SECRETS_FILE_ERROR, CORAL_ERROR_REASON_SETUP_REQUIRED,
    CORAL_ERROR_REASON_SOURCE_NOT_FOUND, CORAL_ERROR_REASON_TABLE_NOT_FOUND,
};
use coral_client::{DecodedStatusError, decode_status_error};
use rmcp::{
    ErrorData,
    model::{CallToolResult, Content},
};
use serde::Serialize;
use serde_json::{Map, Value};

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

#[expect(
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

    let metadata = error
        .metadata
        .iter()
        .map(|(key, value)| (key.clone(), Value::String(value.clone())))
        .collect::<Map<_, _>>();

    let structured = serde_json::to_value(StructuredToolErrorValue {
        error: ToolErrorValue {
            summary: &error.summary,
            detail: &error.detail,
            hint: error.hint.as_deref(),
            grpc_code: &error.grpc_code,
            reason: error.reason.as_deref(),
            retryable: error.retryable,
            metadata: Value::Object(metadata),
        },
    })
    .expect("tool error value serializes");
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
            hint: mcp_hint_for_reason(&error.reason, &error.metadata)
                .map(str::to_string)
                .or_else(|| error.hint.clone()),
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
            let hint = mcp_hint_for_reason(&error.reason, &error.metadata)
                .map(str::to_string)
                .or(error.hint);
            let data = serde_json::to_value(StatusErrorDataValue {
                detail: &error.detail,
                grpc_code: status.code().to_string(),
                reason: &error.reason,
                retryable: error.retryable,
                metadata: &error.metadata,
                hint: hint.as_deref(),
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

fn mcp_hint_for_reason(reason: &str, metadata: &HashMap<String, String>) -> Option<&'static str> {
    match reason {
        CORAL_ERROR_REASON_SOURCE_NOT_FOUND => Some(
            "Use `list_tables` or `search_tables` to inspect visible tables and sources before retrying.",
        ),
        CORAL_ERROR_REASON_INVALID_INPUT => {
            Some("Check the tool arguments and retry with values that match the tool schema.")
        }
        CORAL_ERROR_REASON_SETUP_REQUIRED => Some(
            "Use `list_tables` to inspect configured sources, then retry once the source is ready.",
        ),
        CORAL_ERROR_REASON_INVALID_SECRETS_FILE => Some(
            "Ask the user to refresh saved credentials for the affected source before retrying.",
        ),
        CORAL_ERROR_REASON_CONFIG_DIR_NOT_FOUND
        | CORAL_ERROR_REASON_LOCAL_FILE_ERROR
        | CORAL_ERROR_REASON_CONFIG_WRITE_FAILED
        | CORAL_ERROR_REASON_SECRETS_FILE_ERROR => Some(
            "Ask the host process owner to check Coral's config directory and file permissions.",
        ),
        CORAL_ERROR_REASON_EMPTY_SQL => Some(
            "Retry with a non-empty SQL statement, for example `SELECT * FROM coral.tables LIMIT 10`.",
        ),
        CORAL_ERROR_REASON_TABLE_NOT_FOUND
            if metadata
                .get("catalog_empty")
                .is_some_and(|value| value == "true") =>
        {
            Some("Use `list_tables` or `search_tables` to inspect visible tables before retrying.")
        }
        _ => None,
    }
}

pub(crate) fn internal_status(error: &serde_json::Error) -> tonic::Status {
    tonic::Status::internal(error.to_string())
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
        clippy::get_unwrap,
        clippy::indexing_slicing,
        reason = "JSON shape assertions intentionally fail loudly in tests"
    )]

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
    fn structured_app_reason_uses_mcp_specific_hint() {
        let status = build_coral_status(
            "SOURCE_NOT_FOUND",
            vec![
                ("summary", "Source `github` was not found"),
                ("detail", "No source named `github` is installed."),
                (
                    "hint",
                    "List installed sources or discover available sources, then retry.",
                ),
            ],
            false,
        );

        let error = tool_error_from_status("Query", &status);
        assert_eq!(
            error.hint.as_deref(),
            Some(
                "Use `list_tables` or `search_tables` to inspect visible tables and sources before retrying."
            )
        );

        let data = status_to_error_data(&status).data.expect("error data");
        assert_eq!(
            data["hint"],
            "Use `list_tables` or `search_tables` to inspect visible tables and sources before retrying."
        );
    }

    #[test]
    fn table_not_found_preserves_engine_hint_when_catalog_is_not_empty() {
        let status = build_coral_status(
            "TABLE_NOT_FOUND",
            vec![
                ("summary", "Table `github.issuse` not found"),
                ("detail", "No table `issuse` exists in schema `github`."),
                ("hint", "Did you mean `github.issues`?"),
            ],
            false,
        );

        let error = tool_error_from_status("Query", &status);
        assert_eq!(error.hint.as_deref(), Some("Did you mean `github.issues`?"));
    }

    #[test]
    fn empty_catalog_table_not_found_uses_mcp_hint() {
        let status = build_coral_status(
            "TABLE_NOT_FOUND",
            vec![
                ("summary", "Table `github.issues` not found"),
                ("detail", "No table `issues` exists in schema `github`."),
                ("catalog_empty", "true"),
                (
                    "hint",
                    "No source tables are currently queryable. Discover available sources, connect one, then retry the query.",
                ),
            ],
            false,
        );

        let error = tool_error_from_status("Query", &status);
        assert_eq!(
            error.hint.as_deref(),
            Some("Use `list_tables` or `search_tables` to inspect visible tables before retrying.")
        );
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
