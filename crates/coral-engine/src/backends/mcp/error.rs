//! Structured query-time errors for MCP-backed sources.

use std::collections::HashMap;

use crate::backends::shared::error::missing_required_filter_error;
use crate::contracts::{StatusCode, StructuredQueryError};

type McpStructuredPolicy = (&'static str, &'static str, bool, StatusCode);

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
    pub(crate) fn reason(&self) -> &'static str {
        match self {
            Self::MissingRequiredFilter { .. } => "MISSING_REQUIRED_FILTER",
            Self::MissingRequiredFunctionArg { .. } => "MISSING_REQUIRED_FUNCTION_ARG",
            Self::ServerStart { .. } => "MCP_SERVER_START_FAILED",
            Self::Initialize { .. } => "MCP_INITIALIZE_FAILED",
            Self::AuthRequired { .. } => "MCP_AUTH_REQUIRED",
            Self::AuthFailed { .. } => "MCP_AUTH_FAILED",
            Self::ToolCall { .. } => "MCP_TOOL_CALL_FAILED",
            Self::ToolReturnedError { .. } => "MCP_TOOL_RETURNED_ERROR",
            Self::ResultDecode { .. } => "MCP_RESULT_DECODE_FAILED",
            Self::Pagination { .. } => "MCP_PAGINATION_FAILED",
            Self::HttpRequestFailed { .. } => "MCP_HTTP_REQUEST_FAILED",
            Self::HttpStatusFailed { .. } => "MCP_HTTP_STATUS_FAILED",
            Self::HttpSseDecodeFailed { .. } => "MCP_HTTP_SSE_DECODE_FAILED",
            Self::SessionExpired { .. } => "MCP_SESSION_EXPIRED",
        }
    }

    /// Converts this MCP-specific error into the canonical structured error.
    #[expect(
        clippy::too_many_lines,
        reason = "Per-variant mapping stays explicit so reason codes, metadata, retryability, and status remain auditable together."
    )]
    pub(crate) fn to_structured(&self) -> StructuredQueryError {
        match self {
            Self::MissingRequiredFilter {
                schema,
                table,
                column,
            } => missing_required_filter_error(schema, table, column),
            Self::MissingRequiredFunctionArg {
                schema,
                function,
                args,
            } => {
                StructuredQueryError::new(
                    self.reason(),
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
                    HashMap::from([
                        ("schema".to_string(), schema.clone()),
                        ("function".to_string(), function.clone()),
                        ("missing_args".to_string(), args.join(",")),
                    ]),
                )
            }
            Self::ServerStart {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP server for source `{source_schema}` failed to start"),
                detail,
                (
                    "server_start",
                    "Verify the MCP server command path, executable permissions, and that \
                     any required system dependencies are installed.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::Initialize {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP server for source `{source_schema}` failed to initialize"),
                detail,
                (
                    "initialize",
                    "The MCP server started but its initialize handshake failed. Check the \
                     server's stderr output and confirm the configured environment variables.",
                    true,
                    StatusCode::Unavailable,
                ),
            ),
            Self::AuthRequired {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP HTTP server for source `{source_schema}` requires authorization"),
                detail,
                (
                    "auth",
                    "Install or update the source with the required OAuth or bearer-token \
                     credential, then retry the query.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::AuthFailed {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP HTTP authorization failed for source `{source_schema}`"),
                detail,
                (
                    "auth",
                    "Refresh or replace the source credential. If the server reports an \
                     insufficient scope, update the manifest OAuth scopes and reinstall.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::ToolCall {
                source_schema,
                relation,
                tool,
                detail,
            } => self.tool_structured(
                source_schema,
                relation,
                tool,
                format!("MCP tool `{tool}` call failed for {source_schema}.{relation}"),
                detail,
                (
                    "tool_call",
                    "The MCP transport or protocol layer rejected the tool call. Retry, or \
                     inspect the server's stderr output.",
                    true,
                    StatusCode::Unavailable,
                ),
            ),
            Self::ToolReturnedError {
                source_schema,
                relation,
                tool,
                detail,
            } => self.tool_structured(
                source_schema,
                relation,
                tool,
                format!("MCP tool `{tool}` returned an error for {source_schema}.{relation}"),
                detail,
                (
                    "tool_error",
                    "The MCP tool ran but reported a business-logic failure. Adjust the \
                     arguments or inspect the upstream system's state.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::ResultDecode {
                source_schema,
                relation,
                tool,
                detail,
            } => self.tool_structured(
                source_schema,
                relation,
                tool,
                format!("MCP tool `{tool}` returned content that could not be decoded"),
                detail,
                (
                    "result_decode",
                    "The MCP tool returned content that did not match the source manifest's \
                     response shape. Confirm `response.rows_path` and column types match the \
                     tool's actual output.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::Pagination {
                source_schema,
                relation,
                tool,
                detail,
            } => self.tool_structured(
                source_schema,
                relation,
                tool,
                format!("MCP tool `{tool}` pagination failed for {source_schema}.{relation}"),
                detail,
                (
                    "pagination",
                    "The MCP source pagination configuration did not terminate before \
                     max_pages. Check the cursor argument and response cursor path.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::HttpRequestFailed {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP HTTP request for source `{source_schema}` failed"),
                detail,
                (
                    "http_request",
                    "The HTTP request to the MCP server failed before a response was \
                     received. Check network connectivity and TLS configuration.",
                    true,
                    StatusCode::Unavailable,
                ),
            ),
            Self::HttpStatusFailed {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP HTTP server for source `{source_schema}` returned an unexpected status"),
                detail,
                (
                    "http_status",
                    "The MCP server returned a non-success HTTP status that is not an \
                     authentication failure. Inspect the server's response body for diagnostics.",
                    true,
                    StatusCode::Unavailable,
                ),
            ),
            Self::HttpSseDecodeFailed {
                source_schema,
                detail,
            } => self.source_structured(
                source_schema,
                format!("MCP HTTP server for source `{source_schema}` returned an undecodable SSE stream"),
                detail,
                (
                    "http_sse_decode",
                    "The MCP server's SSE response could not be parsed, or its content \
                     type was unexpected. Confirm the server speaks MCP Streamable HTTP.",
                    false,
                    StatusCode::FailedPrecondition,
                ),
            ),
            Self::SessionExpired { source_schema } => self.source_structured(
                source_schema,
                format!("MCP HTTP session expired for source `{source_schema}`"),
                format!("MCP HTTP session expired for source `{source_schema}`"),
                (
                    "session_expired",
                    "The MCP server returned HTTP 404 for an attached session ID and the \
                     transport could not transparently reinitialize. Retry the query.",
                    true,
                    StatusCode::Unavailable,
                ),
            ),
        }
    }

    fn source_structured(
        &self,
        source_schema: &str,
        summary: impl Into<String>,
        detail: impl Into<String>,
        policy: McpStructuredPolicy,
    ) -> StructuredQueryError {
        let (stage, hint, retryable, status) = policy;
        StructuredQueryError::new(
            self.reason(),
            summary,
            detail,
            Some(hint.to_string()),
            retryable,
            status,
            source_metadata(source_schema, stage),
        )
    }

    fn tool_structured(
        &self,
        source_schema: &str,
        relation: &str,
        tool: &str,
        summary: impl Into<String>,
        detail: impl Into<String>,
        policy: McpStructuredPolicy,
    ) -> StructuredQueryError {
        let (stage, hint, retryable, status) = policy;
        StructuredQueryError::new(
            self.reason(),
            summary,
            detail,
            Some(hint.to_string()),
            retryable,
            status,
            tool_metadata(source_schema, relation, tool, stage),
        )
    }
}

fn source_metadata(source_schema: &str, stage: &str) -> HashMap<String, String> {
    HashMap::from([
        ("source".to_string(), source_schema.to_string()),
        ("mcp_stage".to_string(), stage.to_string()),
    ])
}

fn tool_metadata(
    source_schema: &str,
    relation: &str,
    tool: &str,
    stage: &str,
) -> HashMap<String, String> {
    let mut metadata = source_metadata(source_schema, stage);
    metadata.insert("relation".to_string(), relation.to_string());
    metadata.insert("tool".to_string(), tool.to_string());
    metadata
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
    fn source_failures_map_to_structured_policy() {
        for (error, reason, stage, retryable, status) in [
            (
                McpProviderQueryError::ServerStart {
                    source_schema: "demo_mcp".to_string(),
                    detail: "no such file or directory".to_string(),
                },
                "MCP_SERVER_START_FAILED",
                "server_start",
                false,
                StatusCode::FailedPrecondition,
            ),
            (
                McpProviderQueryError::Initialize {
                    source_schema: "demo_mcp".to_string(),
                    detail: "handshake timed out".to_string(),
                },
                "MCP_INITIALIZE_FAILED",
                "initialize",
                true,
                StatusCode::Unavailable,
            ),
            (
                McpProviderQueryError::HttpRequestFailed {
                    source_schema: "demo_mcp".to_string(),
                    detail: "connection refused".to_string(),
                },
                "MCP_HTTP_REQUEST_FAILED",
                "http_request",
                true,
                StatusCode::Unavailable,
            ),
            (
                McpProviderQueryError::HttpStatusFailed {
                    source_schema: "demo_mcp".to_string(),
                    detail: "HTTP 502: bad gateway".to_string(),
                },
                "MCP_HTTP_STATUS_FAILED",
                "http_status",
                true,
                StatusCode::Unavailable,
            ),
            (
                McpProviderQueryError::HttpSseDecodeFailed {
                    source_schema: "demo_mcp".to_string(),
                    detail: "unexpected content type".to_string(),
                },
                "MCP_HTTP_SSE_DECODE_FAILED",
                "http_sse_decode",
                false,
                StatusCode::FailedPrecondition,
            ),
            (
                McpProviderQueryError::SessionExpired {
                    source_schema: "demo_mcp".to_string(),
                },
                "MCP_SESSION_EXPIRED",
                "session_expired",
                true,
                StatusCode::Unavailable,
            ),
        ] {
            let error = error.to_structured();
            assert_eq!(error.reason(), reason);
            assert_eq!(error.metadata().get("mcp_stage").unwrap(), stage);
            assert_eq!(error.retryable(), retryable);
            assert_eq!(error.status(), status);
        }
    }

    #[test]
    fn tool_failures_map_to_structured_policy() {
        for (error, reason, stage, retryable, status) in [
            (
                McpProviderQueryError::ToolCall {
                    source_schema: "demo_mcp".to_string(),
                    relation: "issues".to_string(),
                    tool: "list_issues".to_string(),
                    detail: "broken pipe".to_string(),
                },
                "MCP_TOOL_CALL_FAILED",
                "tool_call",
                true,
                StatusCode::Unavailable,
            ),
            (
                McpProviderQueryError::ToolReturnedError {
                    source_schema: "demo_mcp".to_string(),
                    relation: "issues".to_string(),
                    tool: "list_issues".to_string(),
                    detail: "rate limit exceeded".to_string(),
                },
                "MCP_TOOL_RETURNED_ERROR",
                "tool_error",
                false,
                StatusCode::FailedPrecondition,
            ),
            (
                McpProviderQueryError::ResultDecode {
                    source_schema: "demo_mcp".to_string(),
                    relation: "issues".to_string(),
                    tool: "list_issues".to_string(),
                    detail: "expected value at line 1 column 1".to_string(),
                },
                "MCP_RESULT_DECODE_FAILED",
                "result_decode",
                false,
                StatusCode::FailedPrecondition,
            ),
        ] {
            let error = error.to_structured();
            assert_eq!(error.reason(), reason);
            assert_eq!(error.metadata().get("mcp_stage").unwrap(), stage);
            assert_eq!(error.metadata().get("tool").unwrap(), "list_issues");
            assert_eq!(error.retryable(), retryable);
            assert_eq!(error.status(), status);
        }
    }
}
