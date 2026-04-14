use std::fmt::Write as _;

use rmcp::{
    ErrorData,
    model::{CallToolResult, Content},
};
use serde_json::{Value, json};

#[derive(Debug, Clone)]
pub(crate) struct ToolError {
    pub(crate) summary: String,
    pub(crate) detail: String,
    pub(crate) hint: Option<String>,
    pub(crate) grpc_code: Option<String>,
}

pub(crate) fn tool_error_result(error: ToolError) -> CallToolResult {
    let mut text = format!("Error: {}\nDetail: {}", error.summary, error.detail);
    if let Some(hint) = &error.hint {
        write!(text, "\nHint: {hint}").expect("writing to String should not fail");
    }

    let mut structured = json!({
        "error": {
            "summary": error.summary,
            "detail": error.detail,
        }
    });
    if let Some(hint) = error.hint
        && let Some(value) = structured
            .get_mut("error")
            .and_then(serde_json::Value::as_object_mut)
    {
        value.insert("hint".to_string(), Value::String(hint));
    }
    if let Some(grpc_code) = error.grpc_code
        && let Some(value) = structured
            .get_mut("error")
            .and_then(serde_json::Value::as_object_mut)
    {
        value.insert("grpc_code".to_string(), Value::String(grpc_code));
    }

    let mut result = CallToolResult::structured_error(structured);
    result.content = vec![Content::text(text)];
    result
}

pub(crate) fn tool_error_from_status(operation: &str, status: &tonic::Status) -> ToolError {
    let (summary, hint) = match status.code() {
        tonic::Code::InvalidArgument => (
            format!("{operation} request is invalid"),
            Some(
                "Check the SQL and retry. Use `coral://guide`, `coral.tables`, and `coral.columns` for discovery.".to_string(),
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
            Some(
                "Check source configuration and required filters, then retry.".to_string(),
            ),
        ),
        tonic::Code::Unavailable => (
            format!("{operation} is unavailable"),
            Some("Retry once the local query runtime is available.".to_string()),
        ),
        _ => (format!("{operation} failed"), None),
    };

    ToolError {
        summary,
        detail: status.message().to_string(),
        hint,
        grpc_code: Some(status.code().to_string()),
    }
}

pub(crate) fn status_to_error_data(status: &tonic::Status) -> ErrorData {
    match status.code() {
        tonic::Code::NotFound => ErrorData::resource_not_found(status.message().to_string(), None),
        tonic::Code::InvalidArgument => {
            ErrorData::invalid_params(status.message().to_string(), None)
        }
        _ => ErrorData::internal_error(status.message().to_string(), None),
    }
}

pub(crate) fn internal_status(error: &serde_json::Error) -> tonic::Status {
    tonic::Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::{ToolError, tool_error_result};

    #[test]
    fn tool_error_result_includes_structured_error_payload() {
        let result = tool_error_result(ToolError {
            summary: "Query failed".to_string(),
            detail: "planner error".to_string(),
            hint: Some("Retry with valid SQL.".to_string()),
            grpc_code: Some("InvalidArgument".to_string()),
        });
        assert_eq!(result.is_error, Some(true));
        assert_eq!(
            result.structured_content.expect("structured content")["error"]["grpc_code"],
            "InvalidArgument"
        );
    }
}
