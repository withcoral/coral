//! `MCP` stdio server for Coral.
//!
//! This crate adapts the local Coral client from `coral-client` to the
//! official Rust `MCP` SDK on stdio.
//!
//! # Primary Entry Points
//!
//! - [`run_stdio_with_client`] serves `MCP` messages on stdio using an
//!   existing [`coral_client::AppClient`], typically bootstrapped by
//!   `coral-cli`.
//!
//! The exposed MCP surface is intentionally small:
//!
//! - tools: `sql`, `search`, paginated `list_catalog`, `describe_table`, `list_columns`, and optionally `feedback`
//! - resources: `coral://guide`, `coral://tables`
//!
//! Protocol lifecycle, initialization, and stdio transport behavior should stay
//! inside the SDK integration rather than being reimplemented locally.

#![allow(
    unused_crate_dependencies,
    reason = "Library test targets inherit package dependencies that are consumed by sibling targets."
)]

mod error;
mod server;
mod surface;
mod telemetry;

#[cfg(test)]
mod tests;

use coral_client::AppClient;
use rmcp::ServiceExt;

pub use error::McpError;
pub(crate) use server::CoralMcpServer;

/// Renders a search response using the same JSON shape advertised by the MCP
/// search tool schema.
#[must_use]
pub fn search_response_value(response: &coral_api::v1::SearchResponse) -> serde_json::Value {
    surface::search_value(response)
}

/// Serializes a search response using the MCP search tool JSON contract.
///
/// # Errors
///
/// Returns [`serde_json::Error`] if the rendered response cannot be serialized.
pub fn search_response_json(
    response: &coral_api::v1::SearchResponse,
) -> Result<String, serde_json::Error> {
    serde_json::to_string(&search_response_value(response))
}

/// Optional MCP surface features.
#[derive(Debug, Clone, Default)]
pub struct McpOptions {
    /// Expose the feedback submission tool.
    pub feedback_enabled: bool,
    /// Allow the search tool to execute opted-in provider-native search
    /// functions through configured source credentials.
    pub search_provider_fanout_enabled: bool,
    /// Optional W3C traceparent used to parent each MCP request span.
    pub trace_parent: Option<String>,
}

/// Runs the `MCP` stdio server using an existing Coral client.
///
/// # Errors
///
/// Returns [`McpError`] if the stdio server cannot complete its `MCP`
/// lifecycle.
pub async fn run_stdio_with_client(app: AppClient, options: McpOptions) -> Result<(), McpError> {
    let server = Box::pin(
        CoralMcpServer::new(&app, options).serve((tokio::io::stdin(), tokio::io::stdout())),
    )
    .await?;
    let _ = server.waiting().await?;
    Ok(())
}
