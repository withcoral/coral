//! `MCP` servers for Coral.
//!
//! This crate adapts the local Coral client from `coral-client` to the
//! official Rust `MCP` SDK on stdio and streamable HTTP transports.
//!
//! # Primary Entry Points
//!
//! - [`run_stdio_with_client`] serves `MCP` messages on stdio using an
//!   existing [`coral_client::AppClient`], typically bootstrapped by
//!   `coral-cli`.
//! - [`run_http_with_client`] serves streamable HTTP `MCP` messages using an
//!   existing [`coral_client::AppClient`], typically behind a cluster Service.
//!
//! The exposed MCP surface is intentionally small:
//!
//! - tools: `sql`, `list_tables`
//! - resources: `coral://guide`, `coral://tables`
//!
//! Protocol lifecycle, initialization, and transport behavior should stay inside
//! the SDK integration rather than being reimplemented locally.

#![allow(
    unused_crate_dependencies,
    reason = "Library test targets inherit package dependencies that are consumed by sibling targets."
)]

mod error;
mod server;
mod surface;

#[cfg(test)]
mod tests;

use coral_client::AppClient;
use rmcp::{
    ServiceExt,
    transport::streamable_http_server::{
        StreamableHttpServerConfig, StreamableHttpService, session::local::LocalSessionManager,
    },
};
use tokio_util::sync::CancellationToken;

pub use error::McpError;
pub(crate) use server::CoralMcpServer;

/// Runs the `MCP` stdio server using an existing Coral client.
///
/// # Errors
///
/// Returns [`McpError`] if the stdio server cannot complete its `MCP`
/// lifecycle.
pub async fn run_stdio_with_client(app: AppClient) -> Result<(), McpError> {
    let server = CoralMcpServer::new(&app)
        .serve((tokio::io::stdin(), tokio::io::stdout()))
        .await?;
    let _ = server.waiting().await?;
    Ok(())
}

/// Runs the streamable HTTP `MCP` server using an existing Coral client.
///
/// # Errors
///
/// Returns [`McpError`] if the listener cannot bind or the HTTP server cannot
/// run.
pub async fn run_http_with_client(
    app: AppClient,
    host: std::net::IpAddr,
    port: u16,
    path: &str,
) -> Result<(), McpError> {
    let listener = tokio::net::TcpListener::bind((host, port)).await?;
    let cancellation_token = CancellationToken::new();
    let config = StreamableHttpServerConfig::default()
        .with_cancellation_token(cancellation_token.child_token());
    let service: StreamableHttpService<CoralMcpServer, LocalSessionManager> =
        StreamableHttpService::new(
            move || Ok(CoralMcpServer::new(&app)),
            Default::default(),
            config,
        );
    let router = axum::Router::new().nest_service(path, service);

    axum::serve(listener, router)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            cancellation_token.cancel();
        })
        .await?;
    Ok(())
}
