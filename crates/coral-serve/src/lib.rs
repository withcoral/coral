//! Long-running Coral server composition.
//!
//! This crate owns the lifecycle boundary that pairs the app-owned gRPC server
//! with its optional MCP Streamable HTTP companion. Transport behavior remains
//! in `coral-mcp-http`; app bootstrap remains in `coral-app`.

#![cfg_attr(
    test,
    expect(
        unused_crate_dependencies,
        reason = "integration-test dependencies are available to the library test target"
    )
)]

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use coral_app::{AppError, McpHttpServeConfig, ServerBuilder};
use coral_client::{AppClient, ClientError};
use coral_mcp::McpOptions;
use coral_mcp_http::{McpHttpConfig, McpHttpError, RunningMcpHttpServer, start_auth_disabled};

/// Failure while starting or stopping the composite server.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct ServeError(ServeErrorKind);

#[derive(Debug, thiserror::Error)]
enum ServeErrorKind {
    #[error("failed to resolve server configuration: {0}")]
    Config(#[source] AppError),
    #[error("failed to start gRPC server: {0}")]
    GrpcStart(#[source] AppError),
    #[error("failed to start MCP HTTP server: {0}")]
    McpStart(#[source] McpStartError),
    #[error("failed to start MCP HTTP server: {mcp}; gRPC cleanup also failed: {grpc}")]
    McpStartCleanup { mcp: McpStartError, grpc: AppError },
    #[error("failed to stop MCP HTTP server: {0}")]
    McpShutdown(#[source] McpHttpError),
    #[error("failed to stop gRPC server: {0}")]
    GrpcShutdown(#[source] AppError),
    #[error("failed to stop MCP HTTP server: {mcp}; gRPC shutdown also failed: {grpc}")]
    Shutdown { mcp: McpHttpError, grpc: AppError },
}

#[derive(Debug, thiserror::Error)]
enum McpStartError {
    #[error("gRPC listener at {0} has no safe loopback route for MCP HTTP")]
    UnsafeGrpcAddress(SocketAddr),
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Http(#[from] McpHttpError),
}

/// Running gRPC server and optional MCP HTTP companion.
pub struct RunningServer {
    grpc: coral_app::RunningServer,
    mcp_http: Option<RunningMcpHttpServer>,
}

impl RunningServer {
    /// Returns the bound gRPC endpoint URI.
    #[must_use]
    pub fn endpoint_uri(&self) -> &str {
        self.grpc.endpoint_uri()
    }

    /// Returns the MCP HTTP listener address when that companion is enabled.
    #[must_use]
    pub fn mcp_http_addr(&self) -> Option<SocketAddr> {
        self.mcp_http.as_ref().map(RunningMcpHttpServer::local_addr)
    }

    /// Stops MCP HTTP first, then always attempts to stop gRPC.
    ///
    /// # Errors
    ///
    /// Returns both failures when both transports fail to shut down.
    pub async fn shutdown(self) -> Result<(), ServeError> {
        let Self { grpc, mcp_http } = self;
        let mcp_result = match mcp_http {
            Some(server) => server.shutdown().await,
            None => Ok(()),
        };
        let grpc_result = grpc.shutdown().await;
        match (mcp_result, grpc_result) {
            (Ok(()), Ok(())) => Ok(()),
            (Err(mcp), Ok(())) => Err(ServeError(ServeErrorKind::McpShutdown(mcp))),
            (Ok(()), Err(grpc)) => Err(ServeError(ServeErrorKind::GrpcShutdown(grpc))),
            (Err(mcp), Err(grpc)) => Err(ServeError(ServeErrorKind::Shutdown { mcp, grpc })),
        }
    }
}

/// Starts the configured gRPC server and its optional MCP HTTP companion.
///
/// If MCP setup fails after gRPC has started, gRPC is shut down before the
/// error is returned.
///
/// # Errors
///
/// Returns [`ServeError`] when configuration resolution or either transport
/// fails, including cleanup after a partial startup.
pub async fn start(
    builder: ServerBuilder,
    options: McpOptions,
) -> Result<RunningServer, ServeError> {
    let mcp_config = builder
        .resolve_mcp_http_serve_config()
        .map_err(|error| ServeError(ServeErrorKind::Config(error)))?;
    let grpc = builder
        .start()
        .await
        .map_err(|error| ServeError(ServeErrorKind::GrpcStart(error)))?;
    let grpc_addr = grpc.local_addr();
    let mcp_http = match start_mcp_http(mcp_config, grpc_addr, options).await {
        Ok(server) => server,
        Err(mcp) => {
            let error = match grpc.shutdown().await {
                Ok(()) => ServeErrorKind::McpStart(mcp),
                Err(grpc) => ServeErrorKind::McpStartCleanup { mcp, grpc },
            };
            return Err(ServeError(error));
        }
    };
    Ok(RunningServer { grpc, mcp_http })
}

async fn start_mcp_http(
    settings: Option<McpHttpServeConfig>,
    grpc_addr: SocketAddr,
    options: McpOptions,
) -> Result<Option<RunningMcpHttpServer>, McpStartError> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let grpc_endpoint_uri = loopback_grpc_endpoint_uri(grpc_addr)?;
    let config = McpHttpConfig::new(settings.bind_addr())?;
    let app = AppClient::connect(&grpc_endpoint_uri).await?;
    let server = start_auth_disabled(config, app, options).await?;
    Ok(Some(server))
}

fn loopback_grpc_endpoint_uri(address: SocketAddr) -> Result<String, McpStartError> {
    let ip = match address.ip() {
        IpAddr::V4(ip) if ip.is_unspecified() => IpAddr::V4(Ipv4Addr::LOCALHOST),
        IpAddr::V6(ip) if ip.is_unspecified() => IpAddr::V6(Ipv6Addr::LOCALHOST),
        ip if ip.is_loopback() => ip,
        _ => return Err(McpStartError::UnsafeGrpcAddress(address)),
    };
    Ok(format!("http://{}", SocketAddr::new(ip, address.port())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn loopback_grpc_endpoint_maps_wildcards_and_rejects_public_addresses() {
        assert_eq!(
            loopback_grpc_endpoint_uri(SocketAddr::from((Ipv4Addr::UNSPECIFIED, 14555)))
                .expect("IPv4 wildcard"),
            "http://127.0.0.1:14555"
        );
        assert_eq!(
            loopback_grpc_endpoint_uri(SocketAddr::from((Ipv6Addr::UNSPECIFIED, 14555)))
                .expect("IPv6 wildcard"),
            "http://[::1]:14555"
        );
        assert!(loopback_grpc_endpoint_uri(SocketAddr::from(([192, 0, 2, 1], 14555))).is_err());
        assert!(
            loopback_grpc_endpoint_uri(SocketAddr::new(
                Ipv4Addr::LOCALHOST.to_ipv6_mapped().into(),
                14555,
            ))
            .is_err()
        );
    }
}
