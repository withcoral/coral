//! The span and trace of one MCP tool call.

use coral_task::TaskId;
use coral_workspace::{AuthenticatedPrincipal, TraceId, WorkspaceId};
use rmcp::ErrorData;
use rmcp::model::CallToolResponse;
use tracing::Instrument as _;

use super::DispatchError;
use crate::{arguments, projection};

/// Stable message recorded on a failed tool call's span.
const TOOL_FAILURE_MESSAGE: &str = "MCP tool call failed";

/// JSON-RPC codes for the caller's mistakes, such as unknown tools and
/// invalid arguments. An MCP server span does not count them as failures.
const CALLER_ERROR_CODES: [i32; 5] = [-32700, -32600, -32601, -32602, -32002];

/// One tool call, traced as an MCP server span named `tools/call {tool}`.
///
/// Everything the call records, and the failure it returns, carries the
/// trace of that span.
pub(super) struct ToolCall {
    span: tracing::Span,
    trace: TraceId,
}

impl ToolCall {
    /// Opens the span of a call to the tool `name`, as a child of `request`,
    /// the span of the HTTP request that carried it. The caller is
    /// authenticated, so the span names its Workspace and Principal kind.
    pub(super) fn open(
        name: &str,
        request: Option<&tracing::Span>,
        workspace: &WorkspaceId,
        principal: &AuthenticatedPrincipal,
    ) -> Self {
        // A name Coral does not serve is caller text, so it stays off the span.
        let tool = arguments::served_name(name);
        let span = tracing::info_span!(
            parent: request.and_then(tracing::Span::id),
            "coral.mcp.tool",
            otel.name = tool.map_or_else(|| "tools/call".to_owned(), |tool| format!("tools/call {tool}")),
            otel.kind = "server",
            mcp.method.name = "tools/call",
            gen_ai.tool.name = tool,
            coral.workspace.id = %workspace,
            coral.principal.kind = principal.principal().kind().as_storage_str(),
            coral.task.id = tracing::field::Empty,
            coral.mcp.failure.code = tracing::field::Empty,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
            exception.message = tracing::field::Empty,
            status = tracing::field::Empty,
        );
        let trace = span.in_scope(TraceId::current_or_new);
        Self { span, trace }
    }

    /// The trace this call's records and failure carry.
    pub(super) const fn trace(&self) -> &TraceId {
        &self.trace
    }

    /// Names the Task the call acts in.
    pub(super) fn record_task(&self, task: &TaskId) {
        self.span
            .record("coral.task.id", tracing::field::display(task));
    }

    /// Runs `dispatched` inside the call's span and records how it ended. A
    /// tool failure becomes a result that names the call's trace. A protocol
    /// error keeps its JSON-RPC code and fails the span only for a server
    /// fault.
    pub(super) async fn run(
        &self,
        dispatched: impl Future<Output = Result<CallToolResponse, DispatchError>>,
    ) -> Result<CallToolResponse, ErrorData> {
        let span = &self.span;
        match dispatched.instrument(span.clone()).await {
            Ok(response) => {
                span.record("status", "ok");
                Ok(response)
            }
            Err(DispatchError::Tool(error)) => {
                span.record("coral.mcp.failure.code", error.code());
                coral_telemetry::record_failure(span, "tool_error", TOOL_FAILURE_MESSAGE);
                Ok(projection::failure(&error, &self.trace).into())
            }
            Err(DispatchError::Protocol(error)) => {
                let code = error.code.0;
                span.record("rpc.response.status_code", tracing::field::display(code));
                if CALLER_ERROR_CODES.contains(&code) {
                    span.record("status", "ok");
                } else {
                    coral_telemetry::record_failure(span, code.to_string(), TOOL_FAILURE_MESSAGE);
                }
                Err(error)
            }
        }
    }
}
