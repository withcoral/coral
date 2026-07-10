use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use coral_app::{
    BearerAuthenticator, CoralAuthorizationServer, McpHttpServeConfig,
    RunningCoralAuthorizationServer, SessionAuthSettings,
};
use coral_client::{
    AppClient, BearerToken, ClientError,
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
use tonic::Code;

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
    #[error("authenticated MCP HTTP requires a session authenticator")]
    MissingSessionProvider,
    #[error(transparent)]
    Client(#[from] ClientError),
    #[error(transparent)]
    Http(#[from] McpHttpError),
}

pub(crate) struct RunningServer {
    grpc: GrpcServer,
    oauth: Option<RunningCoralAuthorizationServer>,
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

    #[cfg(test)]
    fn oauth_addr(&self) -> Option<SocketAddr> {
        self.oauth
            .as_ref()
            .map(RunningCoralAuthorizationServer::local_addr)
    }

    pub(crate) async fn wait_for_exit(&self) {
        self.grpc.wait_for_exit().await;
    }

    pub(crate) async fn shutdown(self) -> Result<(), ServeError> {
        let Self {
            grpc,
            oauth,
            mcp_http,
            grpc_authentication_enabled: _,
        } = self;
        shutdown_components(grpc, oauth, mcp_http)
            .await
            .map_err(|failures| ServeError(ServeErrorKind::Shutdown(failures)))
    }
}

/// Composes the served surfaces and runs them.
///
/// The gRPC bootstrap resolves configuration and starts a gRPC server; deciding
/// what else runs beside it, and wiring the session policies each surface
/// enforces, happens here — where the transports' lifecycles are already owned.
pub(crate) async fn start(
    builder: ServerBuilder,
    mcp_options: McpOptions,
) -> Result<RunningServer, ServeError> {
    let mut settings = builder
        .serve_settings()
        .map_err(|error| ServeError(ServeErrorKind::Config(error)))?;
    let mcp_config = settings.mcp_http().cloned();
    let session_auth = settings.take_session_auth();
    let grpc_authentication_enabled = session_auth.is_some();
    let (builder, mcp_principal_provider) =
        compose_session_policies(builder, session_auth.as_ref(), mcp_config.as_ref());
    // Built after the providers, which only borrow the settings; this consumes them.
    let oauth_server = match session_auth {
        Some(session_auth) => Some(
            session_auth
                .into_authorization_server()
                .map_err(|error| ServeError(ServeErrorKind::Config(error)))?,
        ),
        None => None,
    };
    let grpc = builder
        .start()
        .await
        .map_err(|error| ServeError(ServeErrorKind::GrpcStart(error)))?;
    let grpc_addr = grpc.local_addr();
    let oauth = match start_oauth(oauth_server).await {
        Ok(server) => server,
        Err(oauth) => {
            let error = match shutdown_components(grpc, None, None).await {
                Ok(()) => ServeErrorKind::OAuthStart(oauth),
                Err(cleanup) => ServeErrorKind::OAuthStartCleanup { oauth, cleanup },
            };
            return Err(ServeError(error));
        }
    };
    let mcp_http =
        match start_mcp_http(mcp_config, mcp_principal_provider, grpc_addr, mcp_options).await {
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
        grpc_authentication_enabled,
    })
}

/// Installs the session policy each served surface enforces.
///
/// The two policies differ by design. The gRPC API is private — reached through
/// the public surfaces in front of it — so it admits a token minted for any of
/// them. MCP HTTP is a public surface and admits only its own audience, which is
/// what stops a token minted for a sibling surface being replayed at it.
fn compose_session_policies(
    builder: ServerBuilder,
    session_auth: Option<&SessionAuthSettings>,
    mcp_config: Option<&McpHttpServeConfig>,
) -> (ServerBuilder, Option<Arc<dyn BearerAuthenticator>>) {
    let Some(session_auth) = session_auth else {
        return (builder, None);
    };
    let private_api = session_auth.principal_provider(session_auth.public_audiences().to_vec());
    let mcp_authenticator = match mcp_config {
        Some(McpHttpServeConfig::Authenticated { public_url, .. }) => {
            Some(session_auth.principal_provider([public_url.clone()])
                as Arc<dyn BearerAuthenticator>)
        }
        _ => None,
    };
    (
        builder.with_principal_provider(private_api),
        mcp_authenticator,
    )
}

async fn start_oauth(
    server: Option<CoralAuthorizationServer>,
) -> Result<Option<RunningCoralAuthorizationServer>, OAuthLifecycleError> {
    match server {
        Some(server) => server.start().await.map(Some).map_err(OAuthLifecycleError),
        None => Ok(None),
    }
}

async fn shutdown_components(
    grpc: GrpcServer,
    oauth: Option<RunningCoralAuthorizationServer>,
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
    mcp_principal_provider: Option<Arc<dyn BearerAuthenticator>>,
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
            let authenticator =
                mcp_principal_provider.ok_or(McpStartError::MissingSessionProvider)?;
            let config =
                AuthenticatedMcpHttpConfig::new(bind_addr, public_url, authorization_server)?;
            let session_endpoint = grpc_endpoint_uri.clone();
            // One unauthenticated client, connected once and reused, drives every
            // readiness probe; the gRPC health service answers without a bearer.
            let readiness_client = AppClient::connect(&grpc_endpoint_uri).await?;
            let runtime = AuthenticatedMcpHttpRuntime::new(
                move |token| {
                    let authenticator = Arc::clone(&authenticator);
                    async move {
                        authenticator
                            .principal_for_bearer(&token)
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
                    let client = readiness_client.clone();
                    async move { probe_serving_health(&client).await }
                },
            );
            start_authenticated(config, runtime).await?
        }
    };
    Ok(Some(server))
}

/// Probes engine readiness over the unauthenticated gRPC health service.
///
/// A data-plane call cannot serve here: with `[auth]` on it would need a bearer
/// token the probe does not hold, and its `Unauthenticated` rejection reads as
/// "reachable" — turning `/readyz` into a port check. The health service reports
/// catalog reachability server-side instead, under its readiness service name so
/// the aggregate liveness check stays a constant.
async fn probe_serving_health(client: &AppClient) -> Result<(), Code> {
    match client.check_engine_ready().await {
        Ok(true) => Ok(()),
        Ok(false) => Err(Code::Unavailable),
        Err(status) => Err(status.code()),
    }
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
