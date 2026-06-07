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
//! The primary MCP workflow surface is intentionally small:
//!
//! - tools: `search`, `describe`, `exec`, `wait`, and `feedback`
//!
//! Runtime exposure controls TypeScript and SQL discovery metadata plus Code
//! Mode globals; it does not add top-level SQL MCP tools.
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

/// MCP surface options.
#[derive(Debug, Clone, Default)]
pub struct McpOptions {
    /// Runtime bindings visible through discovery and Code Mode guidance.
    pub runtime_exposure: McpRuntimeExposure,
    /// Optional W3C traceparent used to parent each MCP request span.
    pub trace_parent: Option<String>,
}

/// MCP-visible runtime exposure policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct McpRuntimeExposure {
    /// Whether generated TypeScript invocation bindings are visible.
    pub typescript_enabled: bool,
    /// Whether SQL projection bindings and `coral.sql.query(...)` are visible.
    pub sql_enabled: bool,
}

impl McpRuntimeExposure {
    /// Expose both generated TypeScript and SQL bindings.
    #[must_use]
    pub const fn both() -> Self {
        Self {
            typescript_enabled: true,
            sql_enabled: true,
        }
    }

    /// Expose only generated TypeScript bindings.
    #[must_use]
    pub const fn typescript_only() -> Self {
        Self {
            typescript_enabled: true,
            sql_enabled: false,
        }
    }

    /// Expose only SQL bindings.
    #[must_use]
    pub const fn sql_only() -> Self {
        Self {
            typescript_enabled: false,
            sql_enabled: true,
        }
    }

    /// Whether a search kind is visible under this exposure.
    #[must_use]
    pub fn exposes_tool_kind(self, kind: &str) -> bool {
        match kind {
            "typescript" => self.typescript_enabled,
            "sql_table" | "sql_function" => self.sql_enabled,
            _ => false,
        }
    }

    /// Stable label for this exposure policy.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match (self.typescript_enabled, self.sql_enabled) {
            (true, true) => "both",
            (true, false) => "typescript",
            (false, true) => "sql",
            (false, false) => "none",
        }
    }
}

impl Default for McpRuntimeExposure {
    fn default() -> Self {
        Self::both()
    }
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
