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

    #[error("MCP HTTP transport for source '{source_schema}' requires authorization: {detail}")]
    AuthRequired {
        source_schema: String,
        detail: String,
    },

    #[error("MCP HTTP transport authorization failed for source '{source_schema}': {detail}")]
    AuthFailed {
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

    #[error("MCP HTTP request for source '{source_schema}' failed: {detail}")]
    HttpRequestFailed {
        source_schema: String,
        detail: String,
    },

    #[error("MCP HTTP server for source '{source_schema}' returned an unexpected status: {detail}")]
    HttpStatusFailed {
        source_schema: String,
        detail: String,
    },

    #[error(
        "MCP HTTP server for source '{source_schema}' returned an undecodable SSE stream: {detail}"
    )]
    HttpSseDecodeFailed {
        source_schema: String,
        detail: String,
    },

    #[error("MCP HTTP session expired for source '{source_schema}'")]
    SessionExpired { source_schema: String },
    // NOTE: `MCP_OAUTH_DISCOVERY_FAILED` and `MCP_OAUTH_REFRESH_FAILED` from
    // `PLAN_mcp_http.md` are intentionally not yet defined. They map to
    // OAuth code paths Coral doesn't implement today: protected-resource /
    // authorization-server metadata discovery on 401, and refresh-token
    // exchange before retrying expired-token requests. Add the variants
    // (and their `to_structured` mappings) at the same time those features
    // land, so we don't introduce dead enum variants in the meantime.
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
            Self::AuthRequired {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "auth".to_string());
                StructuredQueryError::new(
                    "MCP_AUTH_REQUIRED",
                    format!("MCP HTTP server for source `{source_schema}` requires authorization"),
                    detail.clone(),
                    Some(
                        "Install or update the source with the required OAuth or bearer-token \
                         credential, then retry the query."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::AuthFailed {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "auth".to_string());
                StructuredQueryError::new(
                    "MCP_AUTH_FAILED",
                    format!("MCP HTTP authorization failed for source `{source_schema}`"),
                    detail.clone(),
                    Some(
                        "Refresh or replace the source credential. If the server reports an \
                         insufficient scope, update the manifest OAuth scopes and reinstall."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
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
            Self::HttpRequestFailed {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "http_request".to_string());
                StructuredQueryError::new(
                    "MCP_HTTP_REQUEST_FAILED",
                    format!("MCP HTTP request for source `{source_schema}` failed"),
                    detail.clone(),
                    Some(
                        "The HTTP request to the MCP server failed before a response was \
                         received. Check network connectivity and TLS configuration."
                            .to_string(),
                    ),
                    true,
                    StatusCode::Unavailable,
                    metadata,
                )
            }
            Self::HttpStatusFailed {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "http_status".to_string());
                StructuredQueryError::new(
                    "MCP_HTTP_STATUS_FAILED",
                    format!(
                        "MCP HTTP server for source `{source_schema}` returned an unexpected status"
                    ),
                    detail.clone(),
                    Some(
                        "The MCP server returned a non-success HTTP status that is not an \
                         authentication failure. Inspect the server's response body for diagnostics."
                            .to_string(),
                    ),
                    true,
                    StatusCode::Unavailable,
                    metadata,
                )
            }
            Self::HttpSseDecodeFailed {
                source_schema,
                detail,
            } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "http_sse_decode".to_string());
                StructuredQueryError::new(
                    "MCP_HTTP_SSE_DECODE_FAILED",
                    format!(
                        "MCP HTTP server for source `{source_schema}` returned an undecodable SSE stream"
                    ),
                    detail.clone(),
                    Some(
                        "The MCP server's SSE response could not be parsed, or its content \
                         type was unexpected. Confirm the server speaks MCP Streamable HTTP."
                            .to_string(),
                    ),
                    false,
                    StatusCode::FailedPrecondition,
                    metadata,
                )
            }
            Self::SessionExpired { source_schema } => {
                let mut metadata = HashMap::new();
                metadata.insert("source".to_string(), source_schema.clone());
                metadata.insert("mcp_stage".to_string(), "session_expired".to_string());
                StructuredQueryError::new(
                    "MCP_SESSION_EXPIRED",
                    format!("MCP HTTP session expired for source `{source_schema}`"),
                    format!("MCP HTTP session expired for source `{source_schema}`"),
                    Some(
                        "The MCP server returned HTTP 404 for an attached session ID and the \
                         transport could not transparently reinitialize. Retry the query."
                            .to_string(),
                    ),
                    true,
                    StatusCode::Unavailable,
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

    #[test]
    fn http_request_failed_is_retryable_unavailable() {
        let error = McpProviderQueryError::HttpRequestFailed {
            source_schema: "demo_mcp".to_string(),
            detail: "connection refused".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_HTTP_REQUEST_FAILED");
        assert_eq!(error.metadata().get("mcp_stage").unwrap(), "http_request");
        assert_eq!(error.status(), StatusCode::Unavailable);
        assert!(error.retryable());
    }

    #[test]
    fn http_status_failed_is_retryable_unavailable() {
        let error = McpProviderQueryError::HttpStatusFailed {
            source_schema: "demo_mcp".to_string(),
            detail: "HTTP 502: bad gateway".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_HTTP_STATUS_FAILED");
        assert_eq!(error.metadata().get("mcp_stage").unwrap(), "http_status");
        assert!(error.retryable());
    }

    #[test]
    fn http_sse_decode_failed_is_not_retryable_failed_precondition() {
        let error = McpProviderQueryError::HttpSseDecodeFailed {
            source_schema: "demo_mcp".to_string(),
            detail: "unexpected content type".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_HTTP_SSE_DECODE_FAILED");
        assert_eq!(
            error.metadata().get("mcp_stage").unwrap(),
            "http_sse_decode"
        );
        assert_eq!(error.status(), StatusCode::FailedPrecondition);
        assert!(!error.retryable());
    }

    #[test]
    fn session_expired_is_retryable_unavailable() {
        let error = McpProviderQueryError::SessionExpired {
            source_schema: "demo_mcp".to_string(),
        }
        .to_structured();
        assert_eq!(error.reason(), "MCP_SESSION_EXPIRED");
        assert_eq!(
            error.metadata().get("mcp_stage").unwrap(),
            "session_expired"
        );
        assert!(error.retryable());
    }
}
