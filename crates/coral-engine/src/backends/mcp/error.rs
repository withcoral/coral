//! Structured query-time errors for MCP-backed sources.

use std::collections::HashMap;

use crate::contracts::{StatusCode, StructuredQueryError};

/// Structured query-time failures for MCP-backed tables and functions.
#[derive(Debug, thiserror::Error)]
pub(crate) enum McpProviderQueryError {
    #[error(
        "{schema}.{table} table requires a constant equality filter: WHERE {column} = <constant>"
    )]
    MissingRequiredFilter {
        schema: String,
        table: String,
        column: String,
    },

    #[error("{schema}.{function} missing required argument(s): {}", args.join(", "))]
    MissingRequiredFunctionArg {
        schema: String,
        function: String,
        args: Vec<String>,
    },

    #[error("failed to start MCP server for source '{source_schema}': {detail}")]
    ServerStart {
        source_schema: String,
        detail: String,
    },

    #[error("failed to initialize MCP server for source '{source_schema}': {detail}")]
    Initialize {
        source_schema: String,
        detail: String,
    },

    #[error("{source_schema}.{relation}: MCP tool '{tool}' call failed: {detail}")]
    ToolCall {
        source_schema: String,
        relation: String,
        tool: String,
        detail: String,
    },

    #[error("{source_schema}.{relation}: MCP tool '{tool}' returned an error: {detail}")]
    ToolReturnedError {
        source_schema: String,
        relation: String,
        tool: String,
        detail: String,
    },

    #[error(
        "{source_schema}.{relation}: MCP tool '{tool}' returned content that could not be decoded: {detail}"
    )]
    ResultDecode {
        source_schema: String,
        relation: String,
        tool: String,
        detail: String,
    },

    #[error("{source_schema}.{relation}: MCP tool '{tool}' pagination failed: {detail}")]
    Pagination {
        source_schema: String,
        relation: String,
        tool: String,
        detail: String,
    },
}

impl McpProviderQueryError {
    /// Converts this MCP-specific error into the canonical structured error.
    #[expect(
        clippy::too_many_lines,
        reason = "Per-variant mapping reads as a single dispatch table; breaking it up adds indirection without clarity"
    )]
    pub(crate) fn to_structured(&self) -> StructuredQueryError {
        match self {
            Self::MissingRequiredFilter {
                schema,
                table,
                column,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("schema".to_string(), schema.clone());
                metadata.insert("table".to_string(), table.clone());
                metadata.insert("column".to_string(), column.clone());
                StructuredQueryError::new(
                    "MISSING_REQUIRED_FILTER",
                    format!("{schema}.{table} requires `WHERE {column} = <constant>`"),
                    format!("{schema}.{table} requires a constant equality filter on {column}"),
                    Some(format!(
                        "Add a constant equality filter on `{column}` or inspect \
                         `coral.columns` / `coral.tables` first."
                    )),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::MissingRequiredFunctionArg {
                schema,
                function,
                args,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("schema".to_string(), schema.clone());
                metadata.insert("function".to_string(), function.clone());
                metadata.insert("missing_args".to_string(), args.join(","));
                StructuredQueryError::new(
                    "MISSING_REQUIRED_FUNCTION_ARG",
                    format!("{schema}.{function} missing required argument(s)"),
                    format!(
                        "{schema}.{function} requires argument(s): {}",
                        args.join(", ")
                    ),
                    Some(format!(
                        "Pass the required argument(s) by name, e.g. \
                         SELECT * FROM {schema}.{function}({} => ...).",
                        args.first().cloned().unwrap_or_default()
                    )),
                    false,
                    StatusCode::InvalidArgument,
                    metadata,
                )
            }
            Self::ServerStart {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "server_start".to_string());
                StructuredQueryError::new(
                    "MCP_SERVER_START_FAILED",
                    format!("MCP server for source `{source_schema}` failed to start"),
                    detail.clone(),
                    Some(
                        "Verify the MCP server command path, executable permissions, and that \
                         any required system dependencies are installed."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::Initialize {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "initialize".to_string());
                StructuredQueryError::new(
                    "MCP_INITIALIZE_FAILED",
                    format!("MCP server for source `{source_schema}` failed to initialize"),
                    detail.clone(),
                    Some(
                        "The MCP server started but its initialize handshake failed. Check the \
                         server's stderr output and confirm the configured environment variables."
                            .to_string(),
                    ),
                    true,
                    StatusCode::Unavailable,
                    metadata,
                )
            }
            Self::ToolCall {
                source_schema,
                relation,
                tool,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("relation".to_string(), relation.clone());
                metadata.insert("tool".to_string(), tool.clone());
                metadata.insert("mcp_stage".to_string(), "tool_call".to_string());
                StructuredQueryError::new(
                    "MCP_TOOL_CALL_FAILED",
                    format!("MCP tool `{tool}` call failed for {source_schema}.{relation}"),
                    detail.clone(),
                    Some(
                        "The MCP transport or protocol layer rejected the tool call. Retry, or \
                         inspect the server's stderr output."
                            .to_string(),
                    ),
                    true,
                    StatusCode::Unavailable,
                    metadata,
                )
            }
            Self::ToolReturnedError {
                source_schema,
                relation,
                tool,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("relation".to_string(), relation.clone());
                metadata.insert("tool".to_string(), tool.clone());
                metadata.insert("mcp_stage".to_string(), "tool_error".to_string());
                StructuredQueryError::new(
                    "MCP_TOOL_RETURNED_ERROR",
                    format!("MCP tool `{tool}` returned an error for {source_schema}.{relation}"),
                    detail.clone(),
                    Some(
                        "The MCP tool ran but reported a business-logic failure. Adjust the \
                         arguments or inspect the upstream system's state."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::ResultDecode {
                source_schema,
                relation,
                tool,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("relation".to_string(), relation.clone());
                metadata.insert("tool".to_string(), tool.clone());
                metadata.insert("mcp_stage".to_string(), "result_decode".to_string());
                StructuredQueryError::new(
                    "MCP_RESULT_DECODE_FAILED",
                    format!("MCP tool `{tool}` returned content that could not be decoded"),
                    detail.clone(),
                    Some(
                        "The MCP tool returned content that did not match the source manifest's \
                         response shape. Confirm `response.rows_path` and column types match the \
                         tool's actual output."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::Pagination {
                source_schema,
                relation,
                tool,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("relation".to_string(), relation.clone());
                metadata.insert("tool".to_string(), tool.clone());
                metadata.insert("mcp_stage".to_string(), "pagination".to_string());
                StructuredQueryError::new(
                    "MCP_PAGINATION_FAILED",
                    format!("MCP tool `{tool}` pagination failed for {source_schema}.{relation}"),
                    detail.clone(),
                    Some(
                        "The MCP source pagination configuration did not terminate before \
                         max_pages. Check the cursor argument and response cursor path."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::McpProviderQueryError;
    use crate::contracts::StatusCode;

    #[test]
    fn missing_required_filter_sets_reason_and_metadata() {
        let error = McpProviderQueryError::MissingRequiredFilter {
            schema: "demo_mcp".to_string(),
            table: "issues".to_string(),
            column: "state".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MISSING_REQUIRED_FILTER");
        assert_eq!(error.metadata().get("schema").unwrap(), "demo_mcp");
        assert_eq!(error.metadata().get("table").unwrap(), "issues");
        assert_eq!(error.metadata().get("column").unwrap(), "state");
        assert!(error.summary().contains("state"));
        assert!(error.hint().is_some());
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
        assert!(!error.retryable());
    }

    #[test]
    fn missing_required_function_arg_has_actionable_hint() {
        let error = McpProviderQueryError::MissingRequiredFunctionArg {
            schema: "demo_mcp".to_string(),
            function: "search".to_string(),
            args: vec!["query".to_string()],
        }
        .to_structured();
        assert_eq!(error.reason(), "MISSING_REQUIRED_FUNCTION_ARG");
        assert_eq!(error.status(), StatusCode::InvalidArgument);
        let hint = error.hint().expect("missing-arg should hint");
        assert!(hint.contains("query => ..."));
    }

    #[test]
    fn server_start_failure_is_not_retryable() {
        let error = McpProviderQueryError::ServerStart {
            source_schema: "demo_mcp".to_string(),
            detail: "no such file or directory".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_SERVER_START_FAILED");
        assert_eq!(error.metadata().get("mcp_stage").unwrap(), "server_start");
        assert!(!error.retryable());
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
    }

    #[test]
    fn initialize_failure_is_retryable() {
        let error = McpProviderQueryError::Initialize {
            source_schema: "demo_mcp".to_string(),
            detail: "handshake timed out".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_INITIALIZE_FAILED");
        assert!(error.retryable());
        assert_eq!(error.status(), StatusCode::Unavailable);
    }

    #[test]
    fn tool_call_failure_is_retryable_unavailable() {
        let error = McpProviderQueryError::ToolCall {
            source_schema: "demo_mcp".to_string(),
            relation: "issues".to_string(),
            tool: "list_issues".to_string(),
            detail: "broken pipe".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_TOOL_CALL_FAILED");
        assert_eq!(error.metadata().get("tool").unwrap(), "list_issues");
        assert!(error.retryable());
    }

    #[test]
    fn tool_returned_error_is_failed_precondition() {
        let error = McpProviderQueryError::ToolReturnedError {
            source_schema: "demo_mcp".to_string(),
            relation: "issues".to_string(),
            tool: "list_issues".to_string(),
            detail: "rate limit exceeded".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_TOOL_RETURNED_ERROR");
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
        assert!(!error.retryable());
    }

    #[test]
    fn result_decode_failure_carries_tool_metadata() {
        let error = McpProviderQueryError::ResultDecode {
            source_schema: "demo_mcp".to_string(),
            relation: "issues".to_string(),
            tool: "list_issues".to_string(),
            detail: "expected value at line 1 column 1".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_RESULT_DECODE_FAILED");
        assert_eq!(error.metadata().get("mcp_stage").unwrap(), "result_decode");
    }
}
