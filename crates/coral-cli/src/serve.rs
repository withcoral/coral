use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use coral_app::McpHttpServeConfig;
use coral_client::{
    AppClient, ClientError,
    local::{LocalServerError, RunningServer as GrpcServer, ServerBuilder},
};
use coral_mcp::{
    McpOptions,
    http::{McpHttpConfig, McpHttpError, RunningMcpHttpServer, start_auth_disabled},
};

#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub(crate) struct ServeError(ServeErrorKind);

#[derive(Debug, thiserror::Error)]
enum ServeErrorKind {
    #[error("failed to resolve server configuration: {0}")]
    Config(#[source] LocalServerError),
    #[error("failed to start gRPC server: {0}")]
    GrpcStart(#[source] LocalServerError),
    #[error("failed to start MCP HTTP server: {0}")]
    McpStart(#[source] McpStartError),
    #[error("failed to start MCP HTTP server: {mcp}; gRPC cleanup also failed: {grpc}")]
    McpStartCleanup {
        mcp: McpStartError,
        grpc: LocalServerError,
    },
    #[error("failed to stop MCP HTTP server: {0}")]
    McpShutdown(#[source] McpHttpError),
    #[error("failed to stop gRPC server: {0}")]
    GrpcShutdown(#[source] LocalServerError),
    #[error("failed to stop MCP HTTP server: {mcp}; gRPC shutdown also failed: {grpc}")]
    Shutdown {
        mcp: McpHttpError,
        grpc: LocalServerError,
    },
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

pub(crate) struct RunningServer {
    grpc: GrpcServer,
    mcp_http: Option<RunningMcpHttpServer>,
}

impl RunningServer {
    pub(crate) fn endpoint_uri(&self) -> &str {
        self.grpc.endpoint_uri()
    }

    pub(crate) fn mcp_http_addr(&self) -> Option<SocketAddr> {
        self.mcp_http.as_ref().map(RunningMcpHttpServer::local_addr)
    }

    pub(crate) async fn wait_for_exit(&self) {
        self.grpc.wait_for_exit().await;
    }

    pub(crate) async fn shutdown(self) -> Result<(), ServeError> {
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

pub(crate) async fn start(
    builder: ServerBuilder,
    mcp_options: McpOptions,
) -> Result<RunningServer, ServeError> {
    let mcp_config = builder
        .resolve_mcp_http_serve_config()
        .map_err(|error| ServeError(ServeErrorKind::Config(error)))?;
    let grpc = builder
        .start()
        .await
        .map_err(|error| ServeError(ServeErrorKind::GrpcStart(error)))?;
    let grpc_addr = grpc.local_addr();
    let mcp_http = match start_mcp_http(mcp_config, grpc_addr, mcp_options).await {
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
    mcp_options: McpOptions,
) -> Result<Option<RunningMcpHttpServer>, McpStartError> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let grpc_endpoint_uri = loopback_grpc_endpoint_uri(grpc_addr)?;
    let config = McpHttpConfig::new(settings.bind_addr())?;
    let app = AppClient::connect(&grpc_endpoint_uri).await?;
    let server = start_auth_disabled(config, app, mcp_options).await?;
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
mod tests;
