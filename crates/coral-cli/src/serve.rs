use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use coral_api::v1::{CatalogItemKind, ListCatalogRequest, PaginationRequest};
use coral_app::{McpHttpServeConfig, OidcAuthConfig, RunningOidcAuthServer};
use coral_client::{
    AppClient, BearerToken, ClientError, default_workspace,
    local::{
        LocalServerError, RunningServer as GrpcServer, ServerBuilder, connect_with_loopback_bearer,
    },
};
use coral_mcp::{CoralMcpServerFactory, McpOptions};
use coral_mcp_http::{
    AuthenticatedMcpHttpConfig, AuthenticatedMcpHttpRuntime, McpHttpConfig, McpHttpError,
    RunningMcpHttpServer, start_auth_disabled, start_authenticated,
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
    #[error("failed to start OAuth authorization server: {0}")]
    OAuthStart(#[source] OAuthLifecycleError),
    #[error("failed to start OAuth authorization server: {oauth}; cleanup also failed: {cleanup}")]
    OAuthStartCleanup {
        oauth: OAuthLifecycleError,
        cleanup: ShutdownFailures,
    },
    #[error("failed to start MCP HTTP server: {0}")]
    McpStart(#[source] McpStartError),
    #[error("failed to start MCP HTTP server: {mcp}; cleanup also failed: {cleanup}")]
    McpStartCleanup {
        mcp: McpStartError,
        cleanup: ShutdownFailures,
    },
    #[error("failed to stop server components: {0}")]
    Shutdown(ShutdownFailures),
}

#[derive(Debug, thiserror::Error)]
#[error("{0}")]
struct OAuthLifecycleError(String);

#[derive(Debug)]
struct ShutdownFailures {
    mcp: Option<McpHttpError>,
    oauth: Option<OAuthLifecycleError>,
    grpc: Option<Box<LocalServerError>>,
}

impl ShutdownFailures {
    fn from_results(
        mcp: Result<(), McpHttpError>,
        oauth: Result<(), OAuthLifecycleError>,
        grpc: Result<(), LocalServerError>,
    ) -> Result<(), Self> {
        let failures = Self {
            mcp: mcp.err(),
            oauth: oauth.err(),
            grpc: grpc.err().map(Box::new),
        };
        if failures.mcp.is_none() && failures.oauth.is_none() && failures.grpc.is_none() {
            Ok(())
        } else {
            Err(failures)
        }
    }
}

impl fmt::Display for ShutdownFailures {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut wrote_failure = false;
        if let Some(error) = &self.mcp {
            write_shutdown_failure(formatter, &mut wrote_failure, "MCP HTTP", error)?;
        }
        if let Some(error) = &self.oauth {
            write_shutdown_failure(formatter, &mut wrote_failure, "OAuth", error)?;
        }
        if let Some(error) = &self.grpc {
            write_shutdown_failure(formatter, &mut wrote_failure, "gRPC", error)?;
        }
        Ok(())
    }
}

impl std::error::Error for ShutdownFailures {}

fn write_shutdown_failure(
    formatter: &mut fmt::Formatter<'_>,
    wrote_failure: &mut bool,
    component: &str,
    error: &dyn fmt::Display,
) -> fmt::Result {
    if *wrote_failure {
        formatter.write_str("; ")?;
    }
    *wrote_failure = true;
    write!(formatter, "{component}: {error}")
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
    oauth: Option<RunningOidcAuthServer>,
    mcp_http: Option<RunningMcpHttpServer>,
}

impl RunningServer {
    pub(crate) fn endpoint_uri(&self) -> &str {
        self.grpc.endpoint_uri()
    }

    pub(crate) fn mcp_http_addr(&self) -> Option<SocketAddr> {
        self.mcp_http.as_ref().map(RunningMcpHttpServer::local_addr)
    }

    #[cfg(test)]
    fn oauth_addr(&self) -> Option<SocketAddr> {
        self.oauth.as_ref().map(RunningOidcAuthServer::local_addr)
    }

    pub(crate) async fn wait_for_exit(&self) {
        self.grpc.wait_for_exit().await;
    }

    pub(crate) async fn shutdown(self) -> Result<(), ServeError> {
        let Self {
            grpc,
            oauth,
            mcp_http,
        } = self;
        shutdown_components(grpc, oauth, mcp_http)
            .await
            .map_err(|failures| ServeError(ServeErrorKind::Shutdown(failures)))
    }
}

pub(crate) async fn start(builder: ServerBuilder) -> Result<RunningServer, ServeError> {
    let (grpc, companions) = builder
        .prepare_for_serve()
        .map_err(|error| ServeError(ServeErrorKind::Config(error)))?;
    let mcp_config = companions.mcp_http().cloned();
    let oauth_config = companions.oidc_auth().cloned();
    let session_principal_provider = companions.session_principal_provider();
    let grpc = grpc
        .start()
        .await
        .map_err(|error| ServeError(ServeErrorKind::GrpcStart(error)))?;
    let grpc_addr = grpc.local_addr();
    let oauth = match start_oauth(oauth_config).await {
        Ok(server) => server,
        Err(oauth) => {
            let error = match shutdown_components(grpc, None, None).await {
                Ok(()) => ServeErrorKind::OAuthStart(oauth),
                Err(cleanup) => ServeErrorKind::OAuthStartCleanup { oauth, cleanup },
            };
            return Err(ServeError(error));
        }
    };
    let mcp_http = match start_mcp_http(mcp_config, session_principal_provider, grpc_addr).await {
        Ok(server) => server,
        Err(mcp) => {
            let error = match shutdown_components(grpc, oauth, None).await {
                Ok(()) => ServeErrorKind::McpStart(mcp),
                Err(cleanup) => ServeErrorKind::McpStartCleanup { mcp, cleanup },
            };
            return Err(ServeError(error));
        }
    };
    Ok(RunningServer {
        grpc,
        oauth,
        mcp_http,
    })
}

async fn start_oauth(
    config: Option<OidcAuthConfig>,
) -> Result<Option<RunningOidcAuthServer>, OAuthLifecycleError> {
    match config {
        Some(config) => config.start().await.map(Some).map_err(OAuthLifecycleError),
        None => Ok(None),
    }
}

async fn shutdown_components(
    grpc: GrpcServer,
    oauth: Option<RunningOidcAuthServer>,
    mcp_http: Option<RunningMcpHttpServer>,
) -> Result<(), ShutdownFailures> {
    let mcp_result = match mcp_http {
        Some(server) => server.shutdown().await,
        None => Ok(()),
    };
    let oauth_result = match oauth {
        Some(server) => server.shutdown().await.map_err(OAuthLifecycleError),
        None => Ok(()),
    };
    let grpc_result = grpc.shutdown().await;
    ShutdownFailures::from_results(mcp_result, oauth_result, grpc_result)
}

async fn start_mcp_http(
    settings: Option<McpHttpServeConfig>,
    session_principal_provider: Option<Arc<dyn coral_app::UserPrincipalProvider>>,
    grpc_addr: SocketAddr,
) -> Result<Option<RunningMcpHttpServer>, McpStartError> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let grpc_endpoint_uri = loopback_grpc_endpoint_uri(grpc_addr)?;
    let options = McpOptions::default();
    let server = match settings {
        McpHttpServeConfig::AuthDisabled { bind_addr } => {
            let config = McpHttpConfig::new(bind_addr)?;
            let app = AppClient::connect(&grpc_endpoint_uri).await?;
            start_auth_disabled(config, app, options).await?
        }
        McpHttpServeConfig::Authenticated {
            bind_addr,
            resource_url,
            authorization_server,
            scope,
        } => {
            let principal_provider =
                session_principal_provider.ok_or(McpStartError::MissingSessionProvider)?;
            let config = AuthenticatedMcpHttpConfig::new(
                bind_addr,
                resource_url,
                authorization_server,
                scope,
            )?;
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
                    let options = options.clone();
                    async move {
                        let bearer = BearerToken::new(token).map_err(|_error| ())?;
                        let app = connect_with_loopback_bearer(&endpoint, bearer)
                            .await
                            .map_err(|_error| ())?;
                        Ok(CoralMcpServerFactory::new(app, options))
                    }
                },
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
