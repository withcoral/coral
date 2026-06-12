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
//! - tools: `sql`, paginated `list_catalog`, `search_catalog`, `describe_table`,
//!   `list_columns`, benchmark-gated `search_trajectory`, and optionally
//!   `feedback`
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

use coral_api::v1::OpenEpisodeRequest;
use coral_client::{AppClient, default_workspace};
use rmcp::ServiceExt;
use tonic::Request;

pub use error::McpError;
pub(crate) use server::CoralMcpServer;

/// Optional benchmark-owned episode context for trajectory-memory validation.
#[derive(Debug, Clone, Default)]
pub struct McpEpisodeOptions {
    /// Client-minted episode id to attach to outgoing Coral calls.
    pub episode_id: String,
    /// Natural-language intent registered by `OpenEpisode`.
    pub intent: String,
    /// Optional parent episode id.
    pub parent_episode_id: Option<String>,
}

/// Optional MCP surface features.
#[derive(Debug, Clone, Default)]
pub struct McpOptions {
    /// Expose the feedback submission tool.
    pub feedback_enabled: bool,
    /// Optional W3C traceparent used to parent each MCP request span.
    pub trace_parent: Option<String>,
    /// Optional benchmark-owned episode context for trajectory-memory validation.
    pub episode: Option<McpEpisodeOptions>,
}

/// Runs the `MCP` stdio server using an existing Coral client.
///
/// # Errors
///
/// Returns [`McpError`] if the stdio server cannot complete its `MCP`
/// lifecycle.
pub async fn run_stdio_with_client(app: AppClient, options: McpOptions) -> Result<(), McpError> {
    if let Some(episode) = &options.episode {
        open_episode(&app, episode).await?;
    }
    let server = Box::pin(
        CoralMcpServer::new(&app, options).serve((tokio::io::stdin(), tokio::io::stdout())),
    )
    .await?;
    let _ = server.waiting().await?;
    Ok(())
}

async fn open_episode(app: &AppClient, episode: &McpEpisodeOptions) -> Result<(), McpError> {
    let mut episode_client = app.episode_client();
    episode_client
        .open_episode(Request::new(OpenEpisodeRequest {
            workspace: Some(default_workspace()),
            episode_id: episode.episode_id.clone(),
            intent: episode.intent.clone(),
            parent_episode_id: episode.parent_episode_id.clone().unwrap_or_default(),
        }))
        .await?;
    Ok(())
}
