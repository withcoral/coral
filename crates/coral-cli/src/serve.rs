use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use coral_api::v1::{CatalogItemKind, ListCatalogRequest, PaginationRequest};
use coral_app::McpHttpServeConfig;
use coral_client::{
    AppClient, BearerToken, ClientError, default_workspace,
    local::{
        LocalServerError, RunningServer as GrpcServer, ServerBuilder, connect_with_loopback_bearer,
    },
};
use coral_mcp::{
    McpOptions,
    http::{
        AuthenticatedMcpHttpConfig, AuthenticatedMcpHttpRuntime, McpHttpConfig, McpHttpError,
        RunningMcpHttpServer, start_auth_disabled, start_authenticated,
    },
};
use tonic::metadata::{MetadataMap, MetadataValue};
use tonic::{Code, Request};

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
    #[error("authenticated MCP HTTP requires the prepared session principal provider")]
    MissingSessionProvider,
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Http(#[from] McpHttpError),
}

pub(crate) struct RunningServer {
    grpc: GrpcServer,
    mcp_http: Option<RunningMcpHttpServer>,
    grpc_authentication_enabled: bool,
}

impl RunningServer {
    pub(crate) fn endpoint_uri(&self) -> &str {
        self.grpc.endpoint_uri()
    }

    pub(crate) fn mcp_http_addr(&self) -> Option<SocketAddr> {
        self.mcp_http.as_ref().map(RunningMcpHttpServer::local_addr)
    }

    pub(crate) fn grpc_authentication_enabled(&self) -> bool {
        self.grpc_authentication_enabled
    }

    pub(crate) async fn wait_for_exit(&self) {
        self.grpc.wait_for_exit().await;
    }

    pub(crate) async fn shutdown(self) -> Result<(), ServeError> {
        let Self {
            grpc,
            mcp_http,
            grpc_authentication_enabled: _,
        } = self;
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
    let (grpc, companions) = builder
        .prepare_for_serve()
        .map_err(|error| ServeError(ServeErrorKind::Config(error)))?;
    let mcp_config = companions.mcp_http().cloned();
    let grpc_authentication_enabled = companions.grpc_principal_provider().is_some();
    let mcp_principal_provider = companions.mcp_principal_provider();
    let grpc = grpc
        .start()
        .await
        .map_err(|error| ServeError(ServeErrorKind::GrpcStart(error)))?;
    let grpc_addr = grpc.local_addr();
    let mcp_http =
        match start_mcp_http(mcp_config, mcp_principal_provider, grpc_addr, mcp_options).await {
            Ok(server) => server,
            Err(mcp) => {
                let error = match grpc.shutdown().await {
                    Ok(()) => ServeErrorKind::McpStart(mcp),
                    Err(grpc) => ServeErrorKind::McpStartCleanup { mcp, grpc },
                };
                return Err(ServeError(error));
            }
        };
    Ok(RunningServer {
        grpc,
        mcp_http,
        grpc_authentication_enabled,
    })
}

async fn start_mcp_http(
    settings: Option<McpHttpServeConfig>,
    mcp_principal_provider: Option<Arc<dyn coral_app::PrincipalProvider>>,
    grpc_addr: SocketAddr,
    mcp_options: McpOptions,
) -> Result<Option<RunningMcpHttpServer>, McpStartError> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let grpc_endpoint_uri = loopback_grpc_endpoint_uri(grpc_addr)?;
    let server = match settings {
        McpHttpServeConfig::AuthDisabled { bind_addr } => {
            let config = McpHttpConfig::new(bind_addr)?;
            let app = AppClient::connect(&grpc_endpoint_uri).await?;
            start_auth_disabled(config, app, mcp_options).await?
        }
        McpHttpServeConfig::Authenticated {
            bind_addr,
            public_url,
            authorization_server,
        } => {
            let principal_provider =
                mcp_principal_provider.ok_or(McpStartError::MissingSessionProvider)?;
            let config =
                AuthenticatedMcpHttpConfig::new(bind_addr, public_url, authorization_server)?;
            let gate_provider = Arc::clone(&principal_provider);
            let session_endpoint = grpc_endpoint_uri.clone();
            let readiness_endpoint = grpc_endpoint_uri;
            let runtime = AuthenticatedMcpHttpRuntime::new(
                move |token| {
                    let provider = Arc::clone(&gate_provider);
                    async move {
                        let metadata = bearer_metadata(&token)?;
                        provider
                            .principal_for_metadata(&metadata)
                            .await
                            .map(|_principal| ())
                            .map_err(|_error| ())
                    }
                },
                move |token| {
                    let endpoint = session_endpoint.clone();
                    async move {
                        let bearer = BearerToken::new(token).map_err(|_error| ())?;
                        connect_with_loopback_bearer(&endpoint, bearer)
                            .await
                            .map_err(|_error| ())
                    }
                },
                mcp_options,
                move || {
                    let endpoint = readiness_endpoint.clone();
                    async move { probe_catalog_reachability(&endpoint).await }
                },
            );
            start_authenticated(config, runtime).await?
        }
    };
    Ok(Some(server))
}

fn bearer_metadata(token: &str) -> Result<MetadataMap, ()> {
    BearerToken::new(token).map_err(|_error| ())?;
    let mut metadata = MetadataMap::new();
    metadata.insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).map_err(|_error| ())?,
    );
    Ok(metadata)
}

async fn probe_catalog_reachability(endpoint: &str) -> Result<(), Code> {
    let app = AppClient::connect(endpoint)
        .await
        .map_err(|_error| Code::Unavailable)?;
    app.catalog_client()
        .list_catalog(Request::new(ListCatalogRequest {
            workspace: Some(default_workspace()),
            schema_name: String::new(),
            kind: CatalogItemKind::Unspecified as i32,
            pagination: Some(PaginationRequest {
                limit: 1,
                offset: 0,
            }),
        }))
        .await
        .map(|_response| ())
        .map_err(|status| status.code())
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
