//! The span and trace of one MCP tool call.

use coral_task::FoundTaskId;
use coral_telemetry::safe;
use coral_workspace::{AuthenticatedPrincipal, TraceId, WorkspaceId};
use rmcp::ErrorData;
use rmcp::model::CallToolResponse;
use tracing::Instrument as _;

use super::DispatchError;
use crate::{arguments, projection};

/// JSON-RPC codes for the caller's mistakes, such as unknown tools and
/// invalid arguments. An MCP server span does not count them as failures.
const CALLER_ERROR_CODES: [i32; 5] = [-32700, -32600, -32601, -32602, -32002];

/// One tool call, traced as an MCP server span named `tools/call {tool}`.
///
/// The call's handlers run inside that span, so the trace they read with
/// [`TraceId::current_or_new`] is the one its failure names.
pub(super) struct ToolCall {
    span: tracing::Span,
}

impl ToolCall {
    /// Opens the span of a call to the tool `name`, as a child of `request`,
    /// the span of the HTTP request that carried it. The caller is
    /// authenticated, so the span names its Workspace and Principal.
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
            otel.name = safe::span_name("tools/call", tool.as_ref()),
            otel.kind = "server",
            mcp.method.name = "tools/call",
            gen_ai.tool.name = tool.map(safe::kind),
            coral.workspace.id = safe::id(workspace),
            coral.principal.id = safe::id(&principal.telemetry_id()),
            coral.principal.kind = safe::kind(principal.principal().kind().as_storage_str()),
            coral.task.id = tracing::field::Empty,
            coral.mcp.failure.code = tracing::field::Empty,
            rpc.response.status_code = tracing::field::Empty,
            error.type = tracing::field::Empty,
        );
        Self { span }
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
            Ok(response) => Ok(response),
            Err(DispatchError::Tool(error)) => {
                span.record("coral.mcp.failure.code", safe::kind(error.code()));
                coral_telemetry::record_failure(span, "tool_error");
                let trace = span.in_scope(TraceId::current_or_new);
                Ok(projection::failure(&error, &trace).into())
            }
            Err(DispatchError::Protocol(error)) => {
                let code = error.code.0;
                let status = safe::json_rpc_code(i64::from(code));
                span.record("rpc.response.status_code", safe::kind(status));
                if !CALLER_ERROR_CODES.contains(&code) {
                    coral_telemetry::record_failure(span, status);
                }
                Err(error)
            }
        }
    }
}

/// Names the Task that the current tool call started or ended. Call it from
/// the call's handler, where the tool span is current, not from inside a
/// child span. A command in a Task names it itself, once it finds the Task.
pub(super) fn record_task(task: FoundTaskId<'_>) {
    tracing::Span::current().record("coral.task.id", safe::id(&task));
}
