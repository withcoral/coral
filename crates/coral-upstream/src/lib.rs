//! Neutral upstream invocation plans and provider response envelopes.
//!
//! This crate owns runtime request/response contracts for HTTP, upstream MCP
//! tool calls, and GraphQL. App state resolves credentials before constructing
//! these plans; SQL and capability invocation consume the same plan shapes.

#![allow(
    missing_docs,
    reason = "Serializable upstream contracts are documented by the capability projection plan and focused tests."
)]
#![allow(
    clippy::module_name_repetitions,
    reason = "Contract type names intentionally include upstream protocol domains."
)]

mod graphql;
mod http;
mod mcp;
mod model;

pub use graphql::graphql_request_body;
pub use mcp::list_mcp_tools;
pub use model::{
    GraphqlRequestPlan, GraphqlUpstreamResponse, HttpRequestPlan, HttpUpstreamResponse,
    MAX_PROVIDER_DIAGNOSTIC_JSON_BYTES, McpConnectionTarget, McpContentBlock, McpToolCallPlan,
    McpUpstreamResponse, ProviderErrorKind, RedactableString, Result, UpstreamError,
    UpstreamInvocationPlan, UpstreamRequestBody, UpstreamResponseBody, UpstreamResponseEnvelope,
    bounded_provider_diagnostic_value,
};

/// Executes a neutral upstream invocation plan.
///
/// # Errors
///
/// Returns [`UpstreamError`] when the provider rejects the request, transport
/// fails, or the plan uses an unsupported provider transport.
pub async fn execute_plan(plan: &UpstreamInvocationPlan) -> Result<UpstreamResponseEnvelope> {
    match plan {
        UpstreamInvocationPlan::Http(plan) => http::execute_http_plan(plan)
            .await
            .map(UpstreamResponseEnvelope::Http),
        UpstreamInvocationPlan::Graphql(plan) => graphql::execute_graphql_plan(plan)
            .await
            .map(UpstreamResponseEnvelope::Graphql),
        UpstreamInvocationPlan::McpToolCall(plan) => mcp::execute_mcp_tool_call_plan(plan)
            .await
            .map(UpstreamResponseEnvelope::Mcp),
    }
}

#[cfg(test)]
mod tests;
