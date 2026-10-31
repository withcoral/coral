//! HTTP lifecycle for Coral's authorization server.

use std::collections::BTreeSet;
use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use super::OIDC_CALLBACK_PATH;
use super::config::{AuthSettings, ResolvedAuthSettings, signing_key_env_error};
use super::error::AuthServerError;
use super::provider_client::OidcProviderClient;
use crate::oauth_resource::{CanonicalOauthUrl, OauthUrlError};
use crate::workspace_mcp_urls::WorkspaceMcpUrls;
use axum::Router;
use axum::extract::State;
use axum::http::header;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::serve::ListenerExt as _;
use coral_auth::authorization_codes::AuthorizationCodes;
use coral_auth::oauth_clients::OAuthClients;
use coral_auth::{SessionTokenIssuer, UserDirectory};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

mod approval_page;
mod authorize;
mod callback;
mod client_metadata;
mod cookies;
mod provider_callback;
mod query;
mod registration;
mod response;
mod sign_in_cookie;
mod token;
mod workspace_choice;

use self::client_metadata::{ClientMetadataResolver, HttpClientMetadataResolver};

const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Prepared Coral authorization server with validated settings and runtime dependencies.
pub struct CoralAuthorizationServer {
    outbound_policy: coral_outbound::DestinationPolicy,
    settings: Arc<ResolvedAuthSettings>,
    session_tokens: SessionTokenIssuer,
    authorization_resources: BTreeSet<String>,
    workspace_resource_family: Option<WorkspaceMcpUrls>,
    database: Arc<coral_db::Database>,
    readiness: Option<crate::ListenerReadiness>,
    providers: Option<coral_provider::ProviderModule>,
}

impl CoralAuthorizationServer {
    /// Builds the Coral authorization server from validated auth settings.
    ///
    /// [`AuthSettings`] can only be produced by [`AuthSettings::from_toml`],
    /// so the settings arrive already validated and are not rechecked here.
    /// This resolves what parsing cannot: the provider secret and the session
    /// signing key.
    ///
    /// `config_path` is used only to resolve a relative session signing-key
    /// path. Config parsing and the `config.toml` filesystem read remain the
    /// caller's responsibility.
    ///
    /// `database` is the initialized deployment database. The server records
    /// verified logins, authorization codes, and registered clients in it.
    /// Processes that share it and the session signing key can each finish a
    /// sign-in another one started.
    ///
    /// # Errors
    ///
    /// Returns an error when the provider secret cannot be resolved from its
    /// environment variable, the session signing key cannot be resolved, the
    /// session-token key material is invalid, or a configured authorization
    /// resource is invalid.
    pub fn from_settings(
        config_path: &Path,
        settings: AuthSettings,
        outbound_policy: coral_outbound::DestinationPolicy,
        database: Arc<coral_db::Database>,
    ) -> Result<Self, AuthServerError> {
        let authorization_resources = settings.allowed_audiences().to_vec();
        let (settings, session_tokens) = settings
            .resolve_runtime_dependencies(config_path, &|name| {
                crate::bootstrap::env_var(name).map_err(|error| signing_key_env_error(&error))
            })?;
        let mut server =
            Self::from_resolved_settings(settings, session_tokens, outbound_policy, database)?;
        for resource in authorization_resources {
            server = server
                .with_authorization_resource(resource)
                .map_err(AuthServerError::Config)?;
        }
        Ok(server)
    }

    pub(crate) fn from_resolved_settings(
        settings: ResolvedAuthSettings,
        session_tokens: SessionTokenIssuer,
        outbound_policy: coral_outbound::DestinationPolicy,
        database: Arc<coral_db::Database>,
    ) -> Result<Self, AuthServerError> {
        if !settings.matches_session_token_issuer(&session_tokens) {
            return Err(AuthServerError::SessionIssuerMismatch);
        }
        Ok(Self {
            outbound_policy,
            settings: Arc::new(settings),
            session_tokens,
            authorization_resources: BTreeSet::new(),
            workspace_resource_family: None,
            database,
            readiness: None,
            providers: None,
        })
    }

    pub(crate) fn with_readiness(mut self, readiness: crate::ListenerReadiness) -> Self {
        self.readiness = Some(readiness);
        self
    }

    /// Names each Workspace's Providers on the Organization MCP Endpoint's
    /// Workspace picker, from the deployment's Provider Module.
    pub(crate) fn with_providers(mut self, providers: coral_provider::ProviderModule) -> Self {
        self.providers = Some(providers);
        self
    }

    /// Starts the distinct local auth-support listener without User OAuth routes.
    ///
    /// # Errors
    /// Rejects non-loopback binds and reports listener startup failures.
    pub async fn start_local(
        bind: SocketAddr,
        readiness: crate::ListenerReadiness,
    ) -> Result<RunningCoralAuthorizationServer, AuthServerError> {
        if !crate::bootstrap::is_loopback_ip(bind.ip()) {
            return Err(AuthServerError::Config(
                "local auth-support bind must be loopback".to_owned(),
            ));
        }
        start_listener(bind, health_routes(Router::new(), Some(readiness))).await
    }

    /// Starts local auth support with the Provider domain's upstream callback.
    /// # Errors
    /// Requires loopback and reports the configured port's bind failure unchanged.
    pub async fn start_local_with_provider_oauth(
        bind: SocketAddr,
        readiness: crate::ListenerReadiness,
        accounts: coral_provider::account::AccountModule,
    ) -> Result<RunningCoralAuthorizationServer, AuthServerError> {
        if !crate::bootstrap::is_loopback_ip(bind.ip()) {
            return Err(AuthServerError::Config(
                "local auth-support bind must be loopback".into(),
            ));
        }
        start_listener(
            bind,
            health_routes(
                coral_telemetry::trace_http_routes(
                    provider_callback::router(accounts),
                    coral_telemetry::metric::Interface::Auth,
                ),
                Some(readiness),
            ),
        )
        .await
    }

    /// Registers a resource identifier that authorization requests may target.
    ///
    /// Call this once for each public resource server that shares this
    /// authorization server.
    ///
    /// # Errors
    ///
    /// Returns an error when `resource` is not an HTTPS URL or an explicit
    /// loopback HTTP URL, or when it contains credentials, a query, or a
    /// fragment.
    pub fn with_authorization_resource(
        mut self,
        resource: impl AsRef<str>,
    ) -> Result<Self, String> {
        self.authorization_resources.insert(
            canonical_authorization_resource(resource.as_ref())
                .map_err(|error| format!("authorization resource {error}"))?,
        );
        Ok(self)
    }

    /// Registers the per-workspace MCP resource family under one base URL.
    ///
    /// Authorization requests may then target any canonical per-workspace
    /// resource — `<origin>/workspaces/<workspace_id>/mcp` — in addition to the exact
    /// resources registered individually. Membership in the family is decided
    /// by the URL template alone, never by whether a workspace by that name
    /// exists: existence is concealed from the OAuth flow on purpose, and a
    /// token minted for a workspace nobody created authorizes nothing because
    /// MCP admission checks the caller's memberships per session.
    #[must_use]
    pub fn with_workspace_resource_family(mut self, family: WorkspaceMcpUrls) -> Self {
        self.workspace_resource_family = Some(family);
        self
    }

    /// Starts the HTTP listener.
    ///
    /// The server is intended to run on loopback or behind a TLS-terminating
    /// reverse proxy that forwards at the origin root. Every endpoint is served
    /// and advertised relative to `listeners.auth.public_url`, which must
    /// mount at the root, so a proxy that adds a path prefix strands discovery,
    /// the authorize route, and the OIDC callback.
    /// # Errors
    /// Returns an error when the listener cannot start.
    pub async fn start(self) -> Result<RunningCoralAuthorizationServer, AuthServerError> {
        let bind_addr = self.settings.bind_addr();
        let state = AuthorizationServerHttpState::new(
            self.settings,
            self.session_tokens,
            Arc::new(self.authorization_resources),
            self.workspace_resource_family,
            self.database,
            self.outbound_policy,
            self.providers,
        )?;
        start_listener(
            bind_addr,
            health_routes(
                coral_telemetry::trace_http_routes(
                    auth_router(state),
                    coral_telemetry::metric::Interface::Auth,
                ),
                self.readiness,
            ),
        )
        .await
    }
}

fn auth_router(state: AuthorizationServerHttpState) -> Router {
    Router::new()
        .route(
            "/.well-known/oauth-authorization-server",
            get(authorization_server_metadata),
        )
        .route(
            "/oauth/authorize",
            get(authorize::oauth_authorize_get).post(authorize::oauth_authorize_post),
        )
        .route("/oauth/token", post(token::oauth_token))
        .route("/oauth/register", post(registration::register))
        .route(OIDC_CALLBACK_PATH, get(callback::oidc_callback))
        .route(
            workspace_choice::WORKSPACE_CHOICE_PATH,
            post(workspace_choice::oauth_workspace_post),
        )
        .with_state(state)
}

fn health_routes(router: Router, readiness: Option<crate::ListenerReadiness>) -> Router {
    use axum::http::StatusCode;
    router
        .route("/healthz", get(|| async { StatusCode::OK }))
        .route(
            "/readyz",
            get(move || {
                let readiness = readiness.clone();
                async move {
                    match readiness {
                        Some(readiness) if readiness.is_ready().await => StatusCode::OK,
                        _ => StatusCode::SERVICE_UNAVAILABLE,
                    }
                }
            }),
        )
}

async fn start_listener(
    bind_addr: SocketAddr,
    router: Router,
) -> Result<RunningCoralAuthorizationServer, AuthServerError> {
    let listener = TcpListener::bind(bind_addr)
        .await
        .map_err(|source| AuthServerError::Bind {
            address: bind_addr,
            source,
        })?;
    let local_addr = listener.local_addr().map_err(AuthServerError::LocalAddr)?;
    let endpoint_uri = format!("http://{local_addr}");
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    // axum leaves Nagle's algorithm on, so a small write can wait for the
    // client's delayed ACK.
    let listener = listener.tap_io(|stream| {
        if stream.set_nodelay(true).is_err() {
            tracing::warn!("failed to set TCP_NODELAY on an accepted connection");
        }
    });
    let task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _result = shutdown_rx.await;
            })
            .await
    });
    Ok(RunningCoralAuthorizationServer {
        local_addr,
        endpoint_uri,
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
    })
}

/// An active Coral authorization-server listener with deterministic graceful shutdown.
///
/// Only [`Self::shutdown`] guarantees the listening port has been released when
/// it returns: it joins the serve task. Dropping the handle signals shutdown but
/// leaves the task detached, so a caller that drops and immediately rebinds the
/// same fixed address can still lose the race and see `EADDRINUSE`.
pub struct RunningCoralAuthorizationServer {
    local_addr: SocketAddr,
    endpoint_uri: String,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<std::io::Result<()>>>,
}

impl RunningCoralAuthorizationServer {
    /// Returns the listener address, including an OS-assigned port.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Returns the cleartext listener endpoint, including an assigned port.
    #[must_use]
    pub fn endpoint_uri(&self) -> &str {
        &self.endpoint_uri
    }

    /// Requests graceful shutdown and joins the HTTP server task.
    /// # Errors
    /// Returns an error when the server task fails.
    pub async fn shutdown(self) -> Result<(), AuthServerError> {
        self.shutdown_with_timeout(SHUTDOWN_TIMEOUT).await
    }

    async fn shutdown_with_timeout(mut self, timeout: Duration) -> Result<(), AuthServerError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
        if let Some(mut task) = self.task.take() {
            let Ok(result) = tokio::time::timeout(timeout, &mut task).await else {
                task.abort();
                let _result = task.await;
                return Err(AuthServerError::ShutdownTimedOut);
            };
            result
                .map_err(AuthServerError::Join)?
                .map_err(AuthServerError::Server)?;
        }
        Ok(())
    }
}

impl Drop for RunningCoralAuthorizationServer {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _result = shutdown_tx.send(());
        }
    }
}

#[derive(Clone)]
struct AuthorizationServerHttpState {
    settings: Arc<ResolvedAuthSettings>,
    session_tokens: SessionTokenIssuer,
    /// Authorization codes awaiting redemption, shared by every process.
    codes: AuthorizationCodes,
    provider_client: OidcProviderClient,
    authorization_resources: Arc<BTreeSet<String>>,
    workspace_resource_family: Option<Arc<WorkspaceMcpUrls>>,
    client_metadata_resolver: Arc<dyn ClientMetadataResolver>,
    clients: OAuthClients,
    /// Domain Module that provisions stable User identities after OIDC validation.
    user_directory: UserDirectory,
    /// The Workspaces a User can pick when they sign in at the Organization
    /// MCP Endpoint.
    workspaces: coral_workspace::WorkspaceModule,
    /// The current authority of a User who signs in there.
    organization: coral_workspace::OrganizationModule,
    /// Names each Workspace's Providers on that page; display only. Absent
    /// until the server is composed with the Provider Module.
    providers: Option<coral_provider::ProviderModule>,
}

impl AuthorizationServerHttpState {
    fn new(
        settings: Arc<ResolvedAuthSettings>,
        session_tokens: SessionTokenIssuer,
        authorization_resources: Arc<BTreeSet<String>>,
        workspace_resource_family: Option<WorkspaceMcpUrls>,
        database: Arc<coral_db::Database>,
        outbound_policy: coral_outbound::DestinationPolicy,
        providers: Option<coral_provider::ProviderModule>,
    ) -> Result<Self, AuthServerError> {
        // Preserve local MCP client-ID derivation, but never treat an HTTPS
        // MCP base as an operator-registered Reef metadata destination. Only
        // the explicit Reef resource registry can grant private HTTPS fetching.
        let mut client_id_derivation_resources = (*authorization_resources).clone();
        if let Some(family) = &workspace_resource_family
            && family.base().url().scheme() == "http"
        {
            client_id_derivation_resources.insert(family.base().identifier().to_string());
        }
        let client_metadata_resolver = HttpClientMetadataResolver::new(
            settings.authorization_server().issuer(),
            &client_id_derivation_resources,
            outbound_policy,
        )
        .map_err(|error| AuthServerError::ClientMetadataResolver(error.to_string()))?;
        // Config validation already held these entries to canonical spellings;
        // only the constructed resolver knows the topology that decides
        // whether each one can ever equal an accepted client ID.
        for client_id in settings.authorization_server().trusted_clients() {
            client_metadata_resolver
                .check_trusted_client_id(client_id)
                .map_err(|error| {
                    AuthServerError::Config(format!(
                        "invalid auth configuration: auth.trusted_clients \
                         entry `{client_id}` cannot match a client ID this server accepts: {error}"
                    ))
                })?;
        }
        // Registered clients resolve from the database; every other client ID
        // falls through to its metadata document.
        let clients = OAuthClients::new(database.clone(), Arc::new(client_metadata_resolver));
        let mut user_directory = UserDirectory::new(database.clone());
        if let Some((issuer, subject)) = settings.bootstrap_owner() {
            user_directory = user_directory.with_bootstrap_owner(issuer, subject);
        }
        let workspaces = coral_workspace::WorkspaceModule::new(database.clone());
        let organization = coral_workspace::OrganizationModule::new(database.clone());
        Ok(Self {
            settings,
            session_tokens,
            codes: AuthorizationCodes::new(database),
            provider_client: OidcProviderClient::new(outbound_policy)
                .map_err(|error| AuthServerError::ProviderClient(error.to_string()))?,
            authorization_resources,
            workspace_resource_family: workspace_resource_family.map(Arc::new),
            client_metadata_resolver: Arc::new(clients.clone()),
            clients,
            user_directory,
            workspaces,
            organization,
            providers,
        })
    }

    /// What authorization requests that target `resource` reach, or `None`
    /// when they may not target it.
    ///
    /// A canonical resource is acceptable when it was registered exactly or
    /// when it is one of the MCP endpoints. Workspace existence is
    /// deliberately not consulted: the OAuth flow answers identically for
    /// every well-formed workspace resource, so it discloses nothing about
    /// which workspaces exist.
    pub(super) fn authorization_resource(&self, resource: &str) -> Option<AuthorizationResource> {
        if let Some(endpoint) = self
            .workspace_resource_family
            .as_ref()
            .and_then(|family| family.parse_endpoint(resource))
        {
            return Some(AuthorizationResource::Mcp(endpoint));
        }
        self.authorization_resources
            .contains(resource)
            .then_some(AuthorizationResource::Registered)
    }

    /// Whether the browser cookies this server sets carry `Secure` and the
    /// `__Host-` prefix, which only an HTTPS issuer can use.
    pub(super) fn secure_cookies(&self) -> bool {
        self.settings
            .authorization_server()
            .issuer()
            .starts_with("https://")
    }
}

/// What an acceptable authorization resource names on this server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum AuthorizationResource {
    /// One of the MCP endpoints.
    Mcp(coral_mcp::McpEndpoint),
    /// A resource registered exactly, such as the web interface.
    Registered,
}

impl AuthorizationResource {
    /// Whether a sign-in for it ends with the User picking a Workspace, on
    /// the page where they also approve the client.
    pub(super) fn picks_workspace(&self) -> bool {
        *self == Self::Mcp(coral_mcp::McpEndpoint::Organization)
    }
}

/// Handler state for the route tests in this module's children.
#[cfg(test)]
mod test_state {
    use std::sync::Arc;

    use coral_test_support::TestDatabase;

    use super::AuthorizationServerHttpState;
    use crate::auth::test_config::{loopback_policy, resolved_settings};

    /// Opens a migrated database for the server to record its sign-ins in.
    pub(super) async fn database() -> TestDatabase {
        TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("test database")
    }

    /// Builds the production state over `database` for a provider at
    /// `provider_issuer`, accepting only the exact `resources`.
    ///
    /// `extra` is passed to [`resolved_settings`]. Tests replace individual
    /// fields, such as the client metadata resolver, on the returned state.
    pub(super) fn http_state(
        database: &TestDatabase,
        provider_issuer: &str,
        extra: &str,
        resources: &[&str],
    ) -> AuthorizationServerHttpState {
        let (settings, session_tokens) = resolved_settings(provider_issuer, extra);
        AuthorizationServerHttpState::new(
            Arc::new(settings),
            session_tokens,
            Arc::new(
                resources
                    .iter()
                    .map(|resource| (*resource).to_owned())
                    .collect(),
            ),
            None,
            database.database(),
            loopback_policy(),
            Some(coral_provider::ProviderModule::new(
                loopback_policy(),
                database.database(),
                coral_workspace::WorkspaceModule::new(database.database()),
                coral_catalog::BuiltinCatalog::running().expect("the built-in catalog prepares"),
            )),
        )
        .expect("authorization server state")
    }
}

/// Canonicalizes an RFC 8707 `resource` for comparison and recording.
///
/// The identifier a client asks for must land on exactly the string the
/// operator configured, so both sides run the one canonicalizer in
/// [`crate::oauth_resource`].
fn canonical_authorization_resource(value: &str) -> Result<String, OauthUrlError> {
    CanonicalOauthUrl::parse(value).map(CanonicalOauthUrl::into_identifier)
}

async fn authorization_server_metadata(
    State(state): State<AuthorizationServerHttpState>,
) -> impl IntoResponse {
    let authorization_server = state.settings.authorization_server();
    json_response(&serde_json::json!({
        "issuer": authorization_server.issuer,
        "authorization_endpoint": format!("{}/oauth/authorize", authorization_server.issuer),
        "token_endpoint": format!("{}/oauth/token", authorization_server.issuer),
        "registration_endpoint": format!("{}/oauth/register", authorization_server.issuer),
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "client_id_metadata_document_supported": true,
    }))
}

fn json_response(value: &serde_json::Value) -> impl IntoResponse + use<> {
    (
        [(header::CONTENT_TYPE, "application/json")],
        value.to_string(),
    )
}

#[cfg(test)]
mod tests {
    use crate::auth::test_config::loopback_policy;
    use std::fs;
    use std::net::SocketAddr;

    use coral_db::Database;
    use serde_json::{Value, json};

    use super::test_state::database;
    use super::{Arc, AuthSettings, CoralAuthorizationServer};

    use crate::auth::test_config::{AUTHORIZATION_SERVER, PROVIDER, SESSION};

    fn config(auth: &str) -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        let signing_key = coral_test_support::p256_pkcs8();
        fs::write(dir.path().join("session.key"), &signing_key).expect("session key");
        let config = format!("unowned = 'preserved'\n[auth]\n{auth}{SESSION}");
        fs::write(dir.path().join("config.toml"), config).expect("config");
        dir
    }

    fn authorization_server(auth_fields: &str) -> tempfile::TempDir {
        config(&format!("{auth_fields}{AUTHORIZATION_SERVER}\n{PROVIDER}"))
    }

    fn server(dir: &tempfile::TempDir, database: Arc<Database>) -> CoralAuthorizationServer {
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let settings = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings");
        CoralAuthorizationServer::from_settings(&config_path, settings, loopback_policy(), database)
            .expect("valid server dependencies")
    }

    async fn json(client: &reqwest::Client, url: String) -> Value {
        let response = client.get(url).send().await.expect("request");
        assert!(response.status().is_success());
        response.json().await.expect("JSON")
    }

    #[tokio::test]
    async fn uses_passed_settings_without_reopening_config() {
        let fixture = database().await;
        let dir = authorization_server("");
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let settings = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings");
        fs::remove_file(&config_path).expect("remove config after parsing");

        CoralAuthorizationServer::from_settings(
            &config_path,
            settings,
            loopback_policy(),
            fixture.database(),
        )
        .expect("prepared from snapshot");
    }

    #[tokio::test]
    async fn rejects_session_tokens_resolved_from_different_settings() {
        let fixture = database().await;
        let dir = authorization_server("");
        let config_path = dir.path().join("config.toml");
        let raw = fs::read_to_string(&config_path).expect("config snapshot");
        let (settings, _issuer) = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings")
            .resolve_runtime_dependencies(&config_path, &|_| Ok(None))
            .expect("resolved runtime dependencies");
        let signing_key = fs::read(dir.path().join("session.key")).expect("session key");
        let session_tokens = super::SessionTokenIssuer::new(
            Some("http://localhost:9999"),
            signing_key,
            std::time::Duration::from_hours(720),
        )
        .expect("session token issuer");

        let Err(error) = CoralAuthorizationServer::from_resolved_settings(
            settings,
            session_tokens,
            loopback_policy(),
            fixture.database(),
        ) else {
            panic!("expected mismatched session settings");
        };
        assert!(
            error
                .to_string()
                .contains("does not match authorization-server settings")
        );
    }

    /// The per-workspace MCP family is an authorization resource an authorize
    /// request may target, decided by the URL template rather than by any
    /// registered set: every canonical stable-ID Workspace MCP URL is accepted,
    /// the base itself is not, and no non-canonical spelling sneaks in. Every
    /// exactly-registered resource, configured or added, still stands beside
    /// the family in its canonical spelling. This is the acceptance
    /// `authorize` consults, so a regression that dropped the family would
    /// refuse every real MCP login with `invalid_target`.
    #[tokio::test]
    async fn accepts_the_workspace_family_beside_exact_resources() {
        use crate::oauth_resource::CanonicalOauthUrl;
        use crate::workspace_mcp_urls::WorkspaceMcpUrls;

        let fixture = database().await;
        let dir =
            authorization_server("allowed_audiences = ['https://CONFIGURED.example.test/']\n");
        let base = CanonicalOauthUrl::parse("https://mcp.example.test").expect("origin");
        let prepared = server(&dir, fixture.database())
            .with_authorization_resource("https://CORAL-WEB.example.test/")
            .expect("register the exact extra")
            .with_workspace_resource_family(WorkspaceMcpUrls::new(base).expect("Workspace URLs"));

        let state = super::AuthorizationServerHttpState::new(
            prepared.settings,
            prepared.session_tokens,
            Arc::new(prepared.authorization_resources),
            prepared.workspace_resource_family,
            prepared.database,
            prepared.outbound_policy,
            None,
        )
        .expect("authorization server state");

        for accepted in [
            "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp",
            "https://mcp.example.test/workspaces/11234567-89ab-4cde-8fab-0123456789ab/mcp",
            // The Organization MCP Endpoint; its sign-in picks the Workspace.
            "https://mcp.example.test/mcp",
            "https://coral-web.example.test",
            "https://configured.example.test",
        ] {
            assert!(
                state.authorization_resource(accepted).is_some(),
                "must accept {accepted}"
            );
        }

        for refused in [
            // Neither the origin nor the retired name-based endpoint is a resource.
            "https://mcp.example.test",
            "https://mcp.example.test/mcp/workspace/team",
            "https://mcp.example.test/workspaces/team/mcp",
            // Non-canonical Workspace identifiers, trailing slashes, and other origins fail.
            "https://mcp.example.test/workspaces/01234567-89AB-4CDE-8FAB-0123456789AB/mcp",
            "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp/",
            "https://mcp.example.test/workspaces/%301234567-89ab-4cde-8fab-0123456789ab/mcp",
            "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/extra/mcp",
            "https://other.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp",
            "https://mcp.example.test/mcp/",
            "https://other.example.test/mcp",
            // Registration stores the canonical spelling only.
            "https://coral-web.example.test/",
        ] {
            assert!(
                state.authorization_resource(refused).is_none(),
                "must refuse {refused}"
            );
        }

        for resource in [
            "http://mcp.example.test",
            "https://user@mcp.example.test",
            "https://mcp.example.test?tenant=one",
            "https://mcp.example.test/#fragment",
        ] {
            let error = server(&dir, fixture.database())
                .with_authorization_resource(resource)
                .err()
                .expect("invalid resource");
            assert!(error.starts_with("authorization resource"), "{error}");
        }
    }

    /// `from_toml` holds `trusted_clients` to canonical spellings, but whether
    /// an entry can ever equal an accepted client ID also depends on the
    /// issuer's scheme and the loopback IDs derived from registered resources.
    /// Every entry here passes config validation, so the topology check at
    /// construction is the only thing standing between it and a server that
    /// silently never skips the approval page.
    #[tokio::test]
    async fn trusted_clients_no_request_could_name_fail_startup() {
        let fixture = database().await;
        for (trusted_client, expected) in [
            (
                "https://client.example.test/",
                "must include a non-root path",
            ),
            ("https://127.0.0.1/oauth/client.json", "host must be public"),
            (
                "http://127.0.0.1:9080/.well-known/oauth-client",
                "authorized explicit-loopback HTTP endpoint",
            ),
        ] {
            let dir = config(&format!(
                "trusted_clients = ['{trusted_client}']\n{AUTHORIZATION_SERVER}{PROVIDER}"
            ));
            let Err(error) = server(&dir, fixture.database()).start().await else {
                panic!("started with dead trusted client `{trusted_client}`");
            };
            let error = error.to_string();
            assert!(error.contains(trusted_client), "{error}");
            assert!(error.contains(expected), "{error}");
        }
    }

    #[tokio::test]
    async fn https_mcp_origin_does_not_authorize_private_reef_metadata() {
        use crate::oauth_resource::CanonicalOauthUrl;
        use crate::workspace_mcp_urls::WorkspaceMcpUrls;

        let client_id = "https://100.64.0.1/.well-known/oauth-client";
        let dir = config(&format!(
            "trusted_clients = ['{client_id}']\n{AUTHORIZATION_SERVER}{PROVIDER}"
        ));
        let base = CanonicalOauthUrl::parse("https://100.64.0.1").expect("MCP origin");
        let fixture = database().await;
        let prepared = server(&dir, fixture.database())
            .with_workspace_resource_family(WorkspaceMcpUrls::new(base).expect("MCP family"));
        let Err(error) = prepared.start().await else {
            panic!("MCP origin must not grant private Reef metadata fetching");
        };
        assert!(error.to_string().contains("host must be public"));
    }

    /// Public HTTPS needs no registration. Private HTTPS and loopback HTTP
    /// require their exact operator-derived client IDs.
    #[tokio::test]
    async fn trusted_clients_the_topology_can_name_start_and_serve() {
        let fixture = database().await;
        for (auth_fields, trusted_client) in [
            ("", "https://client.example.test/oauth/client.json"),
            (
                "allowed_audiences = ['https://100.64.0.1']\n",
                "https://100.64.0.1/.well-known/oauth-client",
            ),
            (
                "allowed_audiences = ['http://127.0.0.1:9080/']\n",
                "http://127.0.0.1:9080/.well-known/oauth-client",
            ),
        ] {
            let dir = config(&format!(
                "{auth_fields}trusted_clients = ['{trusted_client}']\n{AUTHORIZATION_SERVER}{PROVIDER}"
            ));
            let server = server(&dir, fixture.database())
                .start()
                .await
                .expect("start");
            server.shutdown().await.expect("shutdown");
        }
    }

    #[tokio::test]
    async fn serves_metadata_and_stops_cleanly() {
        let dir = authorization_server("");
        let fixture = database().await;
        let server = server(&dir, fixture.database())
            .start()
            .await
            .expect("start");
        let endpoint = server.endpoint_uri().to_string();
        let client = reqwest::Client::new();
        let metadata_url = format!("{endpoint}/.well-known/oauth-authorization-server");
        let metadata = json(&client, metadata_url).await;
        assert_eq!(
            metadata,
            json!({
                "issuer": "http://localhost:9080",
                "authorization_endpoint": "http://localhost:9080/oauth/authorize",
                "token_endpoint": "http://localhost:9080/oauth/token",
                "registration_endpoint": "http://localhost:9080/oauth/register",
                "response_types_supported": ["code"],
                "grant_types_supported": ["authorization_code"],
                "code_challenge_methods_supported": ["S256"],
                "token_endpoint_auth_methods_supported": ["none"],
                "client_id_metadata_document_supported": true,
            })
        );
        let response = client
            .get(format!("{endpoint}/oauth/clients/anything"))
            .send()
            .await
            .expect("request");
        assert_eq!(response.status(), reqwest::StatusCode::NOT_FOUND);
        server.shutdown().await.expect("shutdown");
        let bind_addr = endpoint
            .strip_prefix("http://")
            .expect("HTTP endpoint")
            .parse::<SocketAddr>()
            .expect("socket address");
        tokio::net::TcpListener::bind(bind_addr)
            .await
            .expect("listener released after shutdown");
    }

    #[tokio::test]
    #[expect(
        clippy::indexing_slicing,
        clippy::too_many_lines,
        reason = "one public HTTP registration and restart journey"
    )]
    async fn public_registration_is_bounded_and_durable_and_cannot_spoof_trust() {
        let dir = authorization_server(
            "trusted_clients = ['https://reef.example.test/.well-known/oauth-client']\n",
        );
        let fixture = database().await;
        let running = server(&dir, fixture.database())
            .with_authorization_resource("https://resource.example.test")
            .unwrap()
            .start()
            .await
            .unwrap();
        let client = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .unwrap();
        let endpoint = format!("{}/oauth/register", running.endpoint_uri());
        for (body, content_type, code) in [
            ("{".to_string(), "application/json", "invalid_client_metadata"),
            (r#"[["https://safe.test/callback"],null,null,null,null]"#.to_string(), "application/json", "invalid_client_metadata"),
            (r#"{"redirect_uris":["https://safe.test/callback"],"client_name":"one","client_name":"two"}"#.to_string(), "application/json", "invalid_client_metadata"),
            ("{}".to_string(), "text/plain", "invalid_client_metadata"),
            (json!({"redirect_uris": []}).to_string(), "application/json", "invalid_redirect_uri"),
            (json!({"redirect_uris": ["http://unsafe.test/callback"]}).to_string(), "application/json", "invalid_redirect_uri"),
            (json!({"redirect_uris": ["https://safe.test/callback"], "token_endpoint_auth_method": "client_secret_post"}).to_string(), "application/json", "invalid_client_metadata"),
            (json!({"redirect_uris": ["https://safe.test/callback"], "unknown": "x".repeat(5120)}).to_string(), "application/json", "invalid_client_metadata"),
        ] {
            let response = client.post(&endpoint).header("content-type", content_type).body(body).send().await.unwrap();
            assert_eq!(response.status(), 400);
            assert_eq!(response.headers()["cache-control"], "no-store");
            let value: Value = response.json().await.unwrap();
            assert_eq!(value["error"], code);
            assert!(value.to_string().len() < 256);
        }
        let response = client
            .post(&endpoint)
            .json(&json!({
                "client_id": "https://reef.example.test/.well-known/oauth-client",
                "client_name": "Reef <script>alert(1)</script>",
                "software_id": "reef", "client_uri": "https://reef.example.test",
                "redirect_uris": ["http://127.0.0.1:1/callback"],
                "grant_types": ["authorization_code", "refresh_token"],
                "token_endpoint_auth_method": "none"
            }))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 201);
        assert_eq!(response.headers()["cache-control"], "no-store");
        assert_eq!(response.headers()["content-type"], "application/json");
        let registered: Value = response.json().await.unwrap();
        assert_eq!(registered["grant_types"], json!(["authorization_code"]));
        let id = registered["client_id"].as_str().unwrap();
        assert!(id.starts_with("dcr_"));
        for _ in 0..59 {
            assert_eq!(
                client
                    .post(&endpoint)
                    .json(&json!({"redirect_uris": ["https://safe.test/callback"]}))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                201
            );
        }
        let throttled = client
            .post(&endpoint)
            .header("x-forwarded-for", "192.0.2.1")
            .json(&json!({"redirect_uris": ["https://safe.test/callback"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(throttled.status(), 429);
        assert_eq!(throttled.headers()["retry-after"], "60");
        assert_eq!(throttled.headers()["cache-control"], "no-store");
        assert_eq!(
            throttled.json::<Value>().await.unwrap()["error"],
            "temporarily_unavailable"
        );
        running.shutdown().await.unwrap();
        let restarted = server(&dir, Arc::new(fixture.connect_peer().await.unwrap()))
            .with_authorization_resource("https://resource.example.test")
            .unwrap()
            .start()
            .await
            .unwrap();
        let authorize = format!("{}/oauth/authorize", restarted.endpoint_uri());
        let fields = [
            ("client_id", id),
            ("redirect_uri", "http://127.0.0.1:55555/callback"),
            ("response_type", "code"),
            ("code_challenge_method", "S256"),
            (
                "code_challenge",
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ",
            ),
            ("resource", "https://resource.example.test"),
        ];
        let response = client.get(&authorize).query(&fields).send().await.unwrap();
        assert_eq!(
            response.status(),
            200,
            "DCR must require consent, not inherit Reef trust: {:?}",
            response.headers()
        );
        let html = response.text().await.unwrap();
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("<script>alert(1)</script>"));
        for invalid_redirect in [
            "http://127.0.0.1:55555/other",
            "http://localhost:55555/callback",
            "https://evil.test/callback",
        ] {
            let mut invalid = fields;
            invalid[1].1 = invalid_redirect;
            assert_eq!(
                client
                    .get(&authorize)
                    .query(&invalid)
                    .send()
                    .await
                    .unwrap()
                    .status(),
                400
            );
        }
        restarted.shutdown().await.unwrap();
    }

    /// An auth-support request opens a request span in the caller's trace.
    /// Probes open none.
    #[tokio::test]
    async fn requests_open_a_span_in_the_callers_trace() {
        const TRACE: &str = "0af7651916cd43dd8448eb211c80319c";

        let capture = coral_telemetry::testing::SpanCapture::default();
        let _subscriber = tracing::subscriber::set_default(capture.subscriber());
        let dir = authorization_server("");
        let fixture = database().await;
        let server = server(&dir, fixture.database())
            .start()
            .await
            .expect("start");
        let endpoint = server.endpoint_uri().to_string();
        let client = reqwest::Client::new();
        let metadata = client
            .get(format!("{endpoint}/.well-known/oauth-authorization-server"))
            .header("traceparent", format!("00-{TRACE}-b7ad6b7169203331-01"))
            .send()
            .await
            .expect("metadata");
        assert!(metadata.status().is_success());
        let probe = client
            .get(format!("{endpoint}/healthz"))
            .send()
            .await
            .expect("probe");
        assert!(probe.status().is_success());
        server.shutdown().await.expect("shutdown");

        let spans = capture.finished();
        assert!(
            spans
                .iter()
                .all(|span| span.span_context.trace_id().to_string() == TRACE),
            "the probe opens no span: {spans:?}"
        );
        assert!(
            spans
                .iter()
                .any(|span| span.name == "GET /.well-known/oauth-authorization-server"),
            "{spans:?}"
        );
    }

    #[tokio::test]
    async fn local_auth_support_has_no_user_oauth_or_registration_routes() {
        let fixture = database().await;
        let local = CoralAuthorizationServer::start_local(
            SocketAddr::from(([127, 0, 0, 1], 0)),
            crate::ListenerReadiness::from_database(fixture.database()),
        )
        .await
        .unwrap();
        let http = reqwest::Client::new();
        for path in [
            "/.well-known/oauth-authorization-server",
            "/oauth/authorize",
        ] {
            assert_eq!(
                http.get(format!("{}{path}", local.endpoint_uri()))
                    .send()
                    .await
                    .unwrap()
                    .status(),
                404
            );
        }
        assert_eq!(
            http.post(format!("{}/oauth/register", local.endpoint_uri()))
                .json(&json!({"redirect_uris": ["http://127.0.0.1/callback"]}))
                .send()
                .await
                .unwrap()
                .status(),
            404
        );
        local.shutdown().await.unwrap();
    }
}
