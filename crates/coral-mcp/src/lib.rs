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
//! - tools: `sql`, paginated `list_catalog`, `search_catalog`, `describe_table`, `list_columns`, and optionally `feedback`
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

/// A successful SQL query example for MCP initialize instructions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct McpQueryExample {
    sql: String,
    sources: Vec<String>,
    row_count: Option<u64>,
}

impl McpQueryExample {
    /// Creates a query example from SQL text.
    #[must_use]
    pub fn new(sql: impl Into<String>) -> Self {
        Self {
            sql: sql.into(),
            sources: Vec::new(),
            row_count: None,
        }
    }

    /// Adds installed source names used by this query.
    #[must_use]
    pub fn with_sources(mut self, sources: impl IntoIterator<Item = String>) -> Self {
        self.sources = sources
            .into_iter()
            .map(|source| source.trim().to_string())
            .filter(|source| !source.is_empty())
            .collect();
        self.sources.sort_unstable();
        self.sources.dedup();
        self
    }

    /// Adds the number of rows returned by this query.
    #[must_use]
    pub fn with_row_count(mut self, row_count: u64) -> Self {
        self.row_count = Some(row_count);
        self
    }

    /// SQL text for this query example.
    #[must_use]
    pub fn sql(&self) -> &str {
        &self.sql
    }

    /// Installed source names used by this query.
    #[must_use]
    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    /// Number of rows returned by this query, when known.
    #[must_use]
    pub fn row_count(&self) -> Option<u64> {
        self.row_count
    }
}

/// Optional MCP surface features.
#[derive(Debug, Clone, Default)]
pub struct McpOptions {
    /// Expose the feedback submission tool.
    pub feedback_enabled: bool,
    /// Optional W3C traceparent used to parent each MCP request span.
    pub trace_parent: Option<String>,
    /// Installed source names to include in MCP initialize instructions.
    pub source_names: Vec<String>,
    /// Successful SQL examples to include in MCP initialize instructions.
    pub query_examples: Vec<McpQueryExample>,
    /// Workspace scoped to this MCP server instance.
    pub workspace: Option<coral_api::v1::Workspace>,
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
