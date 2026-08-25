use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::Arc;

use coral_app::{
    AuthServerError, CanonicalOauthUrl, CoralAuthorizationServer, McpHttpServeConfig,
    RunningCoralAuthorizationServer, SessionAuthSettings, SessionPrincipalProvider,
    WorkspaceMcpUrls,
};
use coral_client::{
    AppClient, BearerToken, ClientError,
    local::{
        LocalServerError, RunningServer as GrpcServer, ServerBuilder, connect_with_loopback_bearer,
    },
};
use coral_mcp::{
    McpOptions, McpSurfaceProvider, McpSurfaceProviderError,
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
    OAuthStart(#[source] AuthServerError),
    #[error("failed to start OAuth authorization server: {oauth}; cleanup also failed: {cleanup}")]
    OAuthStartCleanup {
        oauth: AuthServerError,
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

#[derive(Debug)]
struct ShutdownFailures {
    mcp: Option<McpHttpError>,
    oauth: Option<AuthServerError>,
    grpc: Option<Box<LocalServerError>>,
}

impl ShutdownFailures {
    fn from_results(
        mcp: Result<(), McpHttpError>,
        oauth: Result<(), AuthServerError>,
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
    #[error("failed to build MCP surface: {0}")]
    SurfaceProvider(#[source] McpSurfaceProviderError),
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
    mcp_http_authentication_enabled: bool,
    mcp_http_workspace_url_path: Option<String>,
}

impl RunningServer {
    pub(crate) fn endpoint_uri(&self) -> &str {
        self.grpc.endpoint_uri()
    }

    pub(crate) fn mcp_http_addr(&self) -> Option<SocketAddr> {
        self.mcp_http.as_ref().map(RunningMcpHttpServer::local_addr)
    }

    /// The listener-relative per-workspace URL template, when MCP HTTP serves.
    ///
    /// The authenticated surface mounts the workspace family under the path of
    /// its `public_url`, so a root public URL serves `/workspace/{workspace}`
    /// and the usual `/mcp` base serves `/mcp/workspace/{workspace}`. The
    /// auth-disabled surface has no public URL and always uses `/mcp`. Printing
    /// a hardcoded path would name one that 404s for any other base.
    pub(crate) fn mcp_http_workspace_url_path(&self) -> Option<&str> {
        self.mcp_http_workspace_url_path.as_deref()
    }

    pub(crate) fn grpc_authentication_enabled(&self) -> bool {
        self.grpc_authentication_enabled
    }

    pub(crate) fn mcp_http_authentication_enabled(&self) -> bool {
        self.mcp_http_authentication_enabled
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
            mcp_http_authentication_enabled: _,
            mcp_http_workspace_url_path: _,
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
    mut mcp_options: McpOptions,
    mcp_surface_provider: Option<Arc<dyn McpSurfaceProvider>>,
) -> Result<RunningServer, ServeError> {
    let mut settings = builder
        .serve_settings()
        .map_err(|error| ServeError(ServeErrorKind::Config(error)))?;
    let mcp_config = settings.mcp_http().cloned();
    let session_auth = settings.take_session_auth();
    let grpc_authentication_enabled = session_auth.is_some();
    let mcp_http_authentication_enabled =
        matches!(mcp_config, Some(McpHttpServeConfig::Authenticated { .. }));
    if mcp_config.is_some()
        && let Some(provider) = mcp_surface_provider
    {
        mcp_options.surface = provider.surface().map_err(|error| {
            ServeError(ServeErrorKind::McpStart(McpStartError::SurfaceProvider(
                error,
            )))
        })?;
    }
    let mcp_http_workspace_url_path = mcp_config.as_ref().map(mcp_http_workspace_url_path);
    let (builder, mcp_principal_provider) =
        compose_session_policies(builder, session_auth, mcp_config.as_ref());
    // App startup owns the state database, so it also builds the authorization
    // server that provisions logins into it. Failing there fails this call
    // before any companion listener exists.
    let mut grpc = builder
        .start()
        .await
        .map_err(|error| ServeError(ServeErrorKind::GrpcStart(error)))?;
    let oauth_server = grpc.take_authorization_server();
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
    let mcp_http = match Box::pin(start_mcp_http(
        mcp_config,
        mcp_principal_provider,
        grpc_addr,
        mcp_options,
    ))
    .await
    {
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
        mcp_http_authentication_enabled,
        mcp_http_workspace_url_path,
    })
}

/// The listener-relative per-workspace URL template a resolved MCP config serves.
///
/// The authenticated surface mounts the workspace family under its
/// `public_url`'s path (a root URL → `/workspace/{workspace}`, `…/mcp` →
/// `/mcp/workspace/{workspace}`); the auth-disabled surface has no public URL
/// and always mounts under `/mcp`. The `public_url` was canonicalized during
/// resolution, so parsing it here is idempotent — a failure would only mean the
/// resolver changed, and the loopback template is a safe fallback for a print.
fn mcp_http_workspace_url_path(config: &McpHttpServeConfig) -> String {
    let base_path = match config {
        McpHttpServeConfig::AuthDisabled { .. } => "/mcp".to_string(),
        McpHttpServeConfig::Authenticated { public_url, .. } => {
            CanonicalOauthUrl::parse(public_url).map_or_else(
                |_| "/mcp".to_string(),
                |base| WorkspaceMcpUrls::new(base).base_path().to_string(),
            )
        }
    };
    format!("{base_path}/workspace/{{workspace}}")
}

/// Installs the session policy each served surface enforces.
///
/// The two policies differ by design. The gRPC API is private — reached through
/// the public surfaces in front of it — so it admits a token minted for any of
/// them (including every per-workspace MCP resource); handing the settings back
/// to the builder is what installs that policy, alongside the authorization
/// server app startup builds from them. MCP HTTP admits only the exact audience
/// of the workspace URL a request arrives at, so its authenticator takes the
/// expected audience per call — which is what stops a bearer minted for one
/// workspace being replayed at another's URL.
fn compose_session_policies(
    builder: ServerBuilder,
    session_auth: Option<SessionAuthSettings>,
    mcp_config: Option<&McpHttpServeConfig>,
) -> (ServerBuilder, Option<Arc<SessionPrincipalProvider>>) {
    let Some(session_auth) = session_auth else {
        return (builder, None);
    };
    let mcp_authenticator = match mcp_config {
        Some(McpHttpServeConfig::Authenticated { .. }) => {
            Some(session_auth.mcp_route_authenticator())
        }
        _ => None,
    };
    (builder.with_session_auth(session_auth), mcp_authenticator)
}

async fn start_oauth(
    server: Option<CoralAuthorizationServer>,
) -> Result<Option<RunningCoralAuthorizationServer>, AuthServerError> {
    match server {
        Some(server) => server.start().await.map(Some),
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
        Some(server) => server.shutdown().await,
        None => Ok(()),
    };
    let grpc_result = grpc.shutdown().await;
    ShutdownFailures::from_results(mcp_result, oauth_result, grpc_result)
}

async fn start_mcp_http(
    settings: Option<McpHttpServeConfig>,
    mcp_principal_provider: Option<Arc<SessionPrincipalProvider>>,
    grpc_addr: SocketAddr,
    mcp_options: McpOptions,
) -> Result<Option<RunningMcpHttpServer>, McpStartError> {
    let Some(settings) = settings else {
        return Ok(None);
    };
    let grpc_endpoint_uri = loopback_grpc_endpoint_uri(grpc_addr)?;
    let server = match settings {
        McpHttpServeConfig::AuthDisabled {
            bind_addr,
            expose_non_loopback,
            allowed_hosts,
        } => {
            // Configuration resolution only sets `expose_non_loopback` for a
            // non-loopback bind the operator opted into, so a loopback bind
            // keeps flowing through the constructor that enforces it.
            let config = if expose_non_loopback {
                McpHttpConfig::allow_unauthenticated_non_loopback(bind_addr)
            } else {
                McpHttpConfig::new(bind_addr)?
            };
            let config = config.with_allowed_hosts(allowed_hosts)?;
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
            // readiness probe and nothing else. Deciding admission with it would
            // list one deployment-wide set of workspaces for every caller, so the
            // per-caller concealment admission exists to provide would be gone;
            // the runtime's per-token client below is what admission reads.
            let readiness_client = AppClient::connect(&grpc_endpoint_uri).await?;
            // The workspace comes from each request's URL, so no workspace is
            // composed into the surface's options — and the bearer check takes
            // the route's exact resource as the expected audience per call.
            let runtime = AuthenticatedMcpHttpRuntime::new(
                move |token, audience| {
                    let authenticator = Arc::clone(&authenticator);
                    async move {
                        authenticator
                            .principal_for_bearer_with_audience(&token, &audience)
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

/// Probes server readiness over the unauthenticated gRPC health service.
///
/// A data-plane call cannot serve here: with `[auth]` on it would need a bearer
/// token the probe does not hold, and its `Unauthenticated` rejection reads as
/// "reachable" — turning `/readyz` into a port check. The health service reports
/// server-side instead whether this instance can reach the database it serves
/// out of, under its readiness service name so the aggregate liveness check
/// stays a constant.
///
/// Twin of `ReadinessProbe::from_app` in `coral-mcp/src/http.rs`, which maps
/// server readiness onto `/readyz` for the loopback surface. The mapping — ready,
/// not-ready as `Unavailable`, the status code otherwise — is duplicated on
/// purpose: only the surface-specific reasoning around it differs, and no shared
/// helper reconstructs workspace access for a caller. Change one mapping and the
/// other needs the same change, or the two `/readyz` surfaces drift apart.
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
