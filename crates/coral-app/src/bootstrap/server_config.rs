//! Configuration owned by the long-running Coral server surface.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use serde::Deserialize;
use serde::de::IgnoredAny;
use zeroize::Zeroizing;

use super::env::AppEnvironment;
use super::{AppError, is_loopback_ip};
use crate::auth::{
    AuthListenerSettings, AuthSettings, CoralAuthorizationServer, ResolvedAuthSettings, WebSettings,
};
use crate::oauth_resource::CanonicalOauthUrl;
use crate::state::AppStateLayout;
use crate::workspace_mcp_urls::WorkspaceMcpUrls;
use coral_auth::SessionTokenIssuer;

/// Bootstrap owns the document's section names; section parsers own their fields.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServerConfigFile {
    listeners: ListenerSettings,
    runtime: RuntimeSettings,
    outbound: OutboundSettings,
    search: SearchSettings,
    credentials: CredentialSettings,
    #[serde(rename = "auth")]
    _auth: IgnoredAny,
    #[serde(rename = "database")]
    _database: IgnoredAny,
    #[serde(rename = "otel")]
    _otel: IgnoredAny,
}

/// What the launching process supplies beside the configuration file.
///
/// These are not settings: the process that launches Coral reads them at its
/// own bootstrap and passes them in through `ServerBuilder`.
#[derive(Clone, Default)]
pub(crate) struct Launcher {
    /// Desktop owns this launch, so local auth support defaults to its fixed port.
    pub(crate) desktop: bool,
    /// A local launcher's web origin for human handoff, used only without `[auth]`.
    pub(crate) local_web_url: Option<coral_mcp::HumanInterfaceUrl>,
}

/// Local auth support's default port under Desktop, which registers it as a
/// fixed Provider OAuth callback.
const DESKTOP_AUTH_PORT: u16 = 8779;

/// One fail-closed configuration snapshot shared by state preparation and serving.
///
/// Capture does not parse settings or resolve secrets; state initialization must
/// reject retired state before bootstrap interprets deployment settings.
pub(crate) struct LoadedServerConfig {
    config_path: PathBuf,
    raw: Zeroizing<String>,
}

impl LoadedServerConfig {
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        let config_path = layout.config_file().to_path_buf();
        let raw = match std::fs::symlink_metadata(&config_path) {
            Ok(_) => std::fs::read_to_string(&config_path)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
            Err(error) => return Err(error.into()),
        };
        Ok(Self {
            config_path,
            raw: Zeroizing::new(raw),
        })
    }

    pub(crate) fn raw(&self) -> &str {
        &self.raw
    }

    pub(crate) fn grpc_settings(&self) -> Result<GrpcSettings, AppError> {
        Ok(self.parse()?.listeners.grpc)
    }

    pub(crate) fn outbound_policy(
        &self,
        defaults: coral_outbound::DestinationPolicy,
    ) -> Result<coral_outbound::DestinationPolicy, AppError> {
        Ok(self.parse()?.outbound.policy(defaults))
    }

    /// The policy for new automatic MCP Provider OAuth clients.
    pub(crate) fn mcp_oauth_client_bootstrap(
        &self,
    ) -> Result<coral_provider::account::ClientBootstrapPolicy, AppError> {
        Ok(self.parse()?.outbound.mcp_oauth_client_bootstrap())
    }

    /// How often the search index's reconcile task runs a pass.
    pub(crate) fn search_reconcile_interval(&self) -> Result<std::time::Duration, AppError> {
        self.parse()?.search.reconcile_interval()
    }

    /// The environment variable that holds the credential encryption key, when
    /// the deployment keeps that key in a secret instead of a key file.
    pub(crate) fn credential_key_env(&self) -> Result<Option<String>, AppError> {
        self.parse()?.credentials.key_env()
    }

    /// Validates all deployment sections after the persistent-state fence.
    /// Secrets are resolved later, only for the surfaces that use them. A
    /// `credentials.key` file beside `credentials.key_env` is refused when the
    /// vault opens, not here: the file is state on the volume, not a setting,
    /// so the configuration alone cannot prove the key has one source.
    pub(crate) fn validate(&self) -> Result<(), AppError> {
        let config = self.parse()?;
        config.runtime.resolve()?;
        config.search.reconcile_interval()?;
        config.credentials.key_env()?;
        self.auth_settings()?;
        crate::telemetry::TelemetryConfig::from_toml(&self.raw)?;
        Ok(())
    }

    /// Resolves the `[runtime]` section: the Program runtime limits.
    pub(crate) fn runtime_limits(&self) -> Result<coral_run::RuntimeLimits, AppError> {
        self.parse()?.runtime.resolve()
    }

    /// Resolves the mandatory MCP listener and optional OIDC authentication.
    ///
    /// This returns validated configuration and resolved key material only. The
    /// caller constructs the services and owns their lifecycle, so resolving
    /// configuration never produces a live service or its state.
    pub(crate) fn serve_settings(
        &self,
        defaults: coral_outbound::DestinationPolicy,
        launcher: &Launcher,
    ) -> Result<ServeSettings, AppError> {
        let config = self.parse()?;
        let runtime_limits = config.runtime.resolve()?;
        let listeners = config.listeners;
        let mcp_settings = listeners.mcp;
        let mcp_oauth_client_bootstrap = config.outbound.mcp_oauth_client_bootstrap();
        let Some(auth_settings) = self.auth_settings()? else {
            if !is_loopback_ip(listeners.grpc.bind.ip()) {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled listeners.grpc.bind must be loopback".to_string(),
                ));
            }
            let auth_bind_addr = listeners.auth.bind.unwrap_or_else(|| {
                let port = if launcher.desktop {
                    DESKTOP_AUTH_PORT
                } else {
                    0
                };
                SocketAddr::from((Ipv4Addr::LOCALHOST, port))
            });
            return Ok(ServeSettings {
                outbound_policy: config.outbound.policy(defaults),
                mcp_oauth_client_bootstrap,
                runtime_limits,
                mcp_http: mcp_settings.resolve(None)?,
                auth_bind_addr,
                session_auth: None,
                human_interface: human_interface(
                    listeners.web,
                    false,
                    launcher.local_web_url.clone(),
                )?,
            });
        };
        let allowed_audiences = auth_settings.allowed_audiences().to_vec();
        let web_public_url = auth_settings.web_public_url().map(str::to_owned);
        let (auth_settings, session) = auth_settings
            .resolve_runtime_dependencies(&self.config_path, &|name| {
                AppEnvironment::env_var(name).map_err(|error| match error {
                    std::env::VarError::NotPresent => {
                        "environment variable is not present".to_string()
                    }
                    std::env::VarError::NotUnicode(_) => {
                        "environment variable is not valid Unicode".to_string()
                    }
                })
            })
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        let auth_bind_addr = auth_settings.bind_addr();
        // The issuer accessor lives on the resolved settings, so read it after
        // runtime resolution rather than before.
        let authorization_server = required_oauth_url(
            "listeners.auth.public_url",
            Some(auth_settings.authorization_server().issuer()),
        )?;
        let mcp_http = mcp_settings.resolve(Some(&authorization_server))?;
        let base = required_oauth_url(
            "listeners.mcp.public_url",
            mcp_settings.public_url.as_deref(),
        )?;
        let mcp_workspace_urls =
            WorkspaceMcpUrls::new(CanonicalOauthUrl::parse(&base).map_err(|error| {
                AppError::FailedPrecondition(format!("listeners.mcp.public_url {error}"))
            })?)
            .map_err(|error| {
                AppError::FailedPrecondition(format!("listeners.mcp.public_url {error}"))
            })?;
        let public_audiences = public_surface_audiences(
            &mcp_workspace_urls,
            &allowed_audiences,
            web_public_url.as_deref(),
        )?;
        let human_interface = human_interface(listeners.web, true, launcher.local_web_url.clone())?;
        Ok(ServeSettings {
            outbound_policy: config.outbound.policy(defaults),
            mcp_oauth_client_bootstrap,
            runtime_limits,
            mcp_http,
            auth_bind_addr,
            human_interface,
            session_auth: Some(SessionAuthSettings {
                settings: auth_settings,
                session_tokens: session,
                public_audiences,
                mcp_workspace_urls,
                web_public_url,
            }),
        })
    }

    fn auth_settings(&self) -> Result<Option<AuthSettings>, AppError> {
        AuthSettings::from_toml(&self.raw)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))
    }

    fn parse(&self) -> Result<ServerConfigFile, AppError> {
        crate::config_toml::decode(&self.raw).map_err(|error| {
            AppError::FailedPrecondition(format!("server configuration is invalid: {error}"))
        })
    }
}

fn human_interface(
    web: Option<WebSettings>,
    authenticated: bool,
    local: Option<coral_mcp::HumanInterfaceUrl>,
) -> Result<coral_mcp::HumanInterfaceUrl, AppError> {
    match (web, local) {
        (_, Some(_)) if authenticated => Err(AppError::FailedPrecondition(
            "CORAL_LOCAL_WEB_URL is local-only; configure listeners.web.public_url".into(),
        )),
        (Some(_), Some(_)) => Err(AppError::FailedPrecondition(
            "configure only one of listeners.web.public_url or CORAL_LOCAL_WEB_URL".into(),
        )),
        (Some(web), None) => Ok(web.public_url),
        (None, Some(local)) => Ok(local),
        (None, None) => Err(AppError::FailedPrecondition(
            "listeners.web.public_url is required for human handoff; local launchers may supply CORAL_LOCAL_WEB_URL".into(),
        )),
    }
}

/// Default overrides chosen by the local CLI/Desktop composition, never by domain code.
pub const LOCAL_OUTBOUND_DEFAULTS: coral_outbound::DestinationPolicy =
    coral_outbound::DestinationPolicy::new(
        coral_outbound::NetworkAccess::HttpAndHttps,
        coral_outbound::NetworkAccess::HttpAndHttps,
    );

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum NetworkAccessSetting {
    Deny,
    Https,
    HttpAndHttps,
}

impl From<NetworkAccessSetting> for coral_outbound::NetworkAccess {
    fn from(value: NetworkAccessSetting) -> Self {
        match value {
            NetworkAccessSetting::Deny => Self::Deny,
            NetworkAccessSetting::Https => Self::Https,
            NetworkAccessSetting::HttpAndHttps => Self::HttpAndHttps,
        }
    }
}

/// Operator spelling of [`coral_provider::account::ClientBootstrapPolicy`]. Only
/// MCP discovery reports document support and a registration endpoint, so the
/// policy decides nothing for other Provider kinds.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum McpOAuthClientBootstrapSetting {
    /// Assess the document's addresses here; prefer it only when they are public.
    #[default]
    Automatic,
    /// Prefer the document whenever the server supports it, whatever the local
    /// resolver answers: split DNS, or a server with approved internal access.
    PreferClientIdMetadataDocument,
    /// Register dynamically or not at all; never present the document.
    DynamicClientRegistrationOnly,
}

impl From<McpOAuthClientBootstrapSetting> for coral_provider::account::ClientBootstrapPolicy {
    fn from(value: McpOAuthClientBootstrapSetting) -> Self {
        match value {
            McpOAuthClientBootstrapSetting::Automatic => Self::Automatic,
            McpOAuthClientBootstrapSetting::PreferClientIdMetadataDocument => {
                Self::PreferClientIdMetadataDocument
            }
            McpOAuthClientBootstrapSetting::DynamicClientRegistrationOnly => {
                Self::DynamicClientRegistrationOnly
            }
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OutboundSettings {
    loopback: Option<NetworkAccessSetting>,
    private_networks: Option<NetworkAccessSetting>,
    /// How Coral identifies itself to an upstream MCP authorization server when
    /// nobody enters a client ID: the identity it presents, not what it may reach.
    mcp_oauth_client_bootstrap: McpOAuthClientBootstrapSetting,
}

impl OutboundSettings {
    fn mcp_oauth_client_bootstrap(&self) -> coral_provider::account::ClientBootstrapPolicy {
        self.mcp_oauth_client_bootstrap.into()
    }

    fn policy(
        self,
        mut defaults: coral_outbound::DestinationPolicy,
    ) -> coral_outbound::DestinationPolicy {
        if let Some(access) = self.loopback {
            defaults = defaults.with_loopback(access.into());
        }
        if let Some(access) = self.private_networks {
            defaults = defaults.with_private_networks(access.into());
        }
        defaults
    }
}

/// Where the host credential encryption key comes from.
///
/// Without this section the key is the `credentials.key` file in the config
/// directory, which the runtime creates on first start. A cloud deployment
/// whose secrets live in a secret manager names the environment variable that
/// carries the key instead, so the runtime never writes a key file.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CredentialSettings {
    key_env: Option<String>,
}

impl CredentialSettings {
    fn key_env(&self) -> Result<Option<String>, AppError> {
        self.key_env
            .as_deref()
            .map(|configured| {
                super::env_var_name("credentials.key_env", configured)
                    .map_err(AppError::FailedPrecondition)
            })
            .transpose()
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RuntimeSettings {
    execution_heap_bytes: usize,
    compiler_heap_bytes: usize,
    /// Program output a Run Owner may hold applied but not yet recorded before
    /// Program requests wait for their chunk to commit. At least twice the
    /// chunk bytes cap; 8 MiB by default.
    #[serde(rename = "applied_output_cap_bytes")]
    applied_output_cap: usize,
    /// The Fan-out Limit: Invocations of one Run that may be `dispatching`
    /// at once. A ready call above it waits, `pending`, for an earlier one to
    /// end. At least 1; 32 by default.
    fan_out_limit: usize,
    /// The Shutdown Drain: seconds a shutting-down process lets its live
    /// Runs end on their own before it interrupts the rest. 0 interrupts at
    /// once; 30 by default.
    shutdown_drain_seconds: u64,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        let limits = coral_run::IsolateLimits::default();
        Self {
            execution_heap_bytes: limits.execution_heap_bytes(),
            compiler_heap_bytes: limits.compiler_heap_bytes(),
            applied_output_cap: coral_run::AppliedOutputCap::DEFAULT_BYTES,
            fan_out_limit: coral_run::FanOutLimit::DEFAULT_INVOCATIONS,
            shutdown_drain_seconds: coral_run::ShutdownDrain::DEFAULT_DURATION.as_secs(),
        }
    }
}

impl RuntimeSettings {
    fn resolve(self) -> Result<coral_run::RuntimeLimits, AppError> {
        let isolate_limits =
            coral_run::IsolateLimits::new(self.execution_heap_bytes, self.compiler_heap_bytes)
                .map_err(|error| AppError::FailedPrecondition(format!("runtime: {error}")))?;
        let applied_output_cap = coral_run::AppliedOutputCap::new(self.applied_output_cap)
            .map_err(|error| {
                AppError::FailedPrecondition(format!("runtime.applied_output_cap_bytes: {error}"))
            })?;
        let fan_out_limit = coral_run::FanOutLimit::new(self.fan_out_limit).map_err(|error| {
            AppError::FailedPrecondition(format!("runtime.fan_out_limit: {error}"))
        })?;
        Ok(coral_run::RuntimeLimits::new(
            isolate_limits,
            applied_output_cap,
            fan_out_limit,
            coral_run::ShutdownDrain::new(std::time::Duration::from_secs(
                self.shutdown_drain_seconds,
            )),
        ))
    }
}

/// Universal Search's settings.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SearchSettings {
    /// Seconds between the end of one reconcile pass and the start of the
    /// next.
    reconcile_interval_seconds: u64,
}

impl Default for SearchSettings {
    fn default() -> Self {
        Self {
            reconcile_interval_seconds: 5 * 60,
        }
    }
}

impl SearchSettings {
    fn reconcile_interval(&self) -> Result<std::time::Duration, AppError> {
        if self.reconcile_interval_seconds == 0 {
            return Err(AppError::FailedPrecondition(
                "search.reconcile_interval_seconds must be greater than 0".to_string(),
            ));
        }
        Ok(std::time::Duration::from_secs(
            self.reconcile_interval_seconds,
        ))
    }
}

/// Peer listener settings, not an extensible listener registry.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ListenerSettings {
    grpc: GrpcSettings,
    mcp: RawMcpHttpSettings,
    auth: AuthListenerSettings,
    web: Option<WebSettings>,
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct GrpcSettings {
    pub(crate) bind: SocketAddr,
}

impl Default for GrpcSettings {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct RawMcpHttpSettings {
    bind: SocketAddr,
    public_url: Option<String>,
}

impl Default for RawMcpHttpSettings {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            public_url: None,
        }
    }
}

impl RawMcpHttpSettings {
    fn resolve(&self, authorization_server: Option<&str>) -> Result<McpHttpServeConfig, AppError> {
        let Some(authorization_server) = authorization_server else {
            if !is_loopback_ip(self.bind.ip()) {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled listeners.mcp.bind must be loopback".to_string(),
                ));
            }
            if self.public_url.is_some() {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled listeners.mcp.public_url must be unset; it advertises OAuth metadata"
                        .to_string(),
                ));
            }
            return Ok(McpHttpServeConfig::AuthDisabled {
                bind_addr: self.bind,
            });
        };

        let public_url =
            required_oauth_url("listeners.mcp.public_url", self.public_url.as_deref())?;
        let authorization_server = authorization_server.to_string();

        Ok(McpHttpServeConfig::Authenticated {
            bind_addr: self.bind,
            public_url,
            authorization_server,
        })
    }
}

/// Resolved MCP HTTP configuration prepared independently from gRPC startup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpHttpServeConfig {
    /// MCP HTTP backed by an unauthenticated local client.
    ///
    /// Always loopback-only. The former non-loopback opt-out is rejected.
    /// The listener accepts only the loopback Host values.
    AuthDisabled {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
    },
    /// Session-authenticated MCP HTTP backed by per-session clients.
    Authenticated {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
        /// Canonical public origin of the per-Workspace MCP URLs, without a path.
        ///
        /// Each Workspace is served at `<public_url>/workspaces/<workspace_id>/mcp`, and
        /// that full per-workspace URL — never this base — is the OAuth
        /// resource and JWT audience of the sessions it admits. The URL names
        /// the stable Workspace identity, so no Workspace is configured here.
        public_url: String,
        /// OAuth authorization server advertised to MCP clients.
        authorization_server: String,
    },
}

impl McpHttpServeConfig {
    /// Returns the configured MCP HTTP bind address.
    #[must_use]
    pub fn bind_addr(&self) -> SocketAddr {
        match self {
            Self::AuthDisabled { bind_addr, .. } | Self::Authenticated { bind_addr, .. } => {
                *bind_addr
            }
        }
    }
}

/// Validated settings for the mandatory MCP listener and optional OIDC authentication.
///
/// Nothing here is live. The gRPC server's bootstrap resolves configuration;
/// the process composition call site turns these settings into services and
/// owns their lifecycle, so a server builder never constructs a transport it
/// does not run.
pub struct ServeSettings {
    outbound_policy: coral_outbound::DestinationPolicy,
    mcp_oauth_client_bootstrap: coral_provider::account::ClientBootstrapPolicy,
    runtime_limits: coral_run::RuntimeLimits,
    pub(super) mcp_http: McpHttpServeConfig,
    auth_bind_addr: SocketAddr,
    pub(super) session_auth: Option<SessionAuthSettings>,
    human_interface: coral_mcp::HumanInterfaceUrl,
}

impl ServeSettings {
    /// Trusted human destination, required before any MCP work is accepted.
    #[must_use]
    pub fn human_interface(&self) -> &coral_mcp::HumanInterfaceUrl {
        &self.human_interface
    }

    /// Returns the destination permissions selected at process bootstrap.
    #[must_use]
    pub fn outbound_policy(&self) -> coral_outbound::DestinationPolicy {
        self.outbound_policy
    }

    /// Returns the policy for new automatic MCP Provider OAuth clients.
    #[must_use]
    pub fn mcp_oauth_client_bootstrap(&self) -> coral_provider::account::ClientBootstrapPolicy {
        self.mcp_oauth_client_bootstrap
    }

    /// Returns the Program runtime limits selected at process bootstrap.
    #[must_use]
    pub fn runtime_limits(&self) -> coral_run::RuntimeLimits {
        self.runtime_limits
    }

    /// Returns the mandatory MCP HTTP listener configuration.
    #[must_use]
    pub fn mcp_http(&self) -> &McpHttpServeConfig {
        &self.mcp_http
    }

    /// Returns the auth-support HTTP bind in either local or authenticated mode.
    #[must_use]
    pub fn auth_bind_addr(&self) -> SocketAddr {
        self.auth_bind_addr
    }

    /// Takes the resolved session authentication, when `[auth]` is configured.
    #[must_use]
    pub fn take_session_auth(&mut self) -> Option<SessionAuthSettings> {
        self.session_auth.take()
    }
}

/// Resolved session authentication for one authenticated instance.
///
/// Holding one is proof the signing key and provider secret were fetched from
/// their configured sources.
pub struct SessionAuthSettings {
    pub(super) settings: ResolvedAuthSettings,
    pub(super) session_tokens: SessionTokenIssuer,
    pub(super) public_audiences: Vec<String>,
    pub(super) mcp_workspace_urls: WorkspaceMcpUrls,
    web_public_url: Option<String>,
}

impl SessionAuthSettings {
    /// Reef's configured public HTTPS origin, when one is configured.
    #[must_use]
    pub fn web_public_url(&self) -> Option<&str> {
        self.web_public_url.as_deref()
    }

    pub(crate) fn principal_authentication(
        &self,
        database: std::sync::Arc<coral_db::Database>,
    ) -> coral_auth::PrincipalAuthentication {
        coral_auth::PrincipalAuthentication::new(
            &self.session_tokens,
            coral_workspace::OrganizationModule::new(database.clone()),
            coral_workspace::WorkspaceModule::new(database),
        )
    }

    /// Builds public Workspace credential verification over the current domain authority.
    #[must_use]
    pub fn workspace_authentication(
        &self,
        database: std::sync::Arc<coral_db::Database>,
    ) -> crate::WorkspaceAuthentication {
        crate::WorkspaceAuthentication::new(
            self.principal_authentication(database),
            self.mcp_workspace_urls.clone(),
            self.settings.authorization_server().issuer().to_owned(),
        )
    }

    /// The instance's explicitly registered public audiences, canonicalized.
    ///
    /// MCP HTTP audiences are the per-workspace resource
    /// family under [`Self::mcp_workspace_urls`] — a set that cannot be
    /// enumerated here, so it is deliberately not in this list.
    #[must_use]
    pub fn public_audiences(&self) -> &[String] {
        &self.public_audiences
    }

    /// The mandatory per-workspace MCP resource family.
    #[must_use]
    pub fn mcp_workspace_urls(&self) -> &WorkspaceMcpUrls {
        &self.mcp_workspace_urls
    }

    /// Consumes these settings into the authorization server for the instance.
    ///
    /// Every explicitly registered public audience becomes an authorization
    /// resource clients may request a token for, and the per-workspace MCP
    /// family is always registered as a template rather
    /// than an enumeration.
    ///
    /// The server records its sign-ins in `database`, the instance's
    /// initialized deployment database.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the server cannot be built from these settings.
    pub fn into_authorization_server(
        self,
        policy: coral_outbound::DestinationPolicy,
        database: std::sync::Arc<coral_db::Database>,
    ) -> Result<CoralAuthorizationServer, AppError> {
        // Operator-configured HTTP issuer support is a separate auth contract.
        // It must neither narrow explicit Provider permissions nor broaden the
        // policy passed to Provider/Run modules.
        let policy = if self
            .settings
            .authorization_server()
            .issuer()
            .starts_with("http://")
        {
            policy.with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
        } else {
            policy
        };
        let mut server = CoralAuthorizationServer::from_resolved_settings(
            self.settings,
            self.session_tokens,
            policy,
            database,
        )
        .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        for audience in &self.public_audiences {
            server = server
                .with_authorization_resource(audience)
                .map_err(AppError::FailedPrecondition)?;
        }
        Ok(server.with_workspace_resource_family(self.mcp_workspace_urls))
    }
}

/// Collects the explicitly registered resource identifiers that front the
/// instance's private API, beside the per-workspace MCP family.
///
/// Each identifier is both the audience of tokens minted for a public surface
/// and the authorization resource clients name when requesting one. The MCP
/// surface's audiences are not here: they are the per-workspace resources
/// under the configured public base, which cannot be enumerated and whose base
/// is itself not an audience.
///
/// MCP is always present. This registry identifies explicit public resources
/// eligible for first-party Reef token issuance, not private method reachability.
/// Private methods validate the resulting mcp/reef audience independently.
///
/// An identifier names a surface, not an actor: either kind of caller can arrive
/// through any of them, so nothing here says what kind a caller is. Actor kind
/// comes from the authenticated principal instead.
fn public_surface_audiences(
    mcp_workspace_urls: &WorkspaceMcpUrls,
    allowed_audiences: &[String],
    web_public_url: Option<&str>,
) -> Result<Vec<String>, AppError> {
    let mut audiences = Vec::new();
    for (index, configured) in allowed_audiences.iter().enumerate() {
        let label = format!("auth.allowed_audiences[{index}]");
        let audience = required_oauth_url(&label, Some(configured))?;
        if audiences.iter().any(|existing| existing == &audience) {
            return Err(AppError::FailedPrecondition(format!(
                "{label} duplicates another configured public surface audience"
            )));
        }
        // A web interface can explicitly occupy the same public origin as MCP
        // behind a reverse proxy. Its root is then a web resource; only the
        // /workspaces/{id}/mcp paths are MCP resources.
        if mcp_workspace_urls.is_base_audience(&audience)
            && web_public_url != Some(audience.as_str())
        {
            return Err(AppError::FailedPrecondition(format!(
                "{label} duplicates listeners.mcp.public_url, which is the base of the \
                 per-workspace MCP resources and not an audience of its own"
            )));
        }
        // A token for `/mcp` must carry the Workspace its User picked; an
        // allowed audience would issue it one that names no Workspace.
        if mcp_workspace_urls.parse_endpoint(&audience)
            == Some(coral_mcp::McpEndpoint::Organization)
        {
            return Err(AppError::FailedPrecondition(format!(
                "{label} is the Organization MCP Endpoint, whose tokens name the workspace \
                 picked at sign-in; it is not an audience of its own"
            )));
        }
        audiences.push(audience);
    }
    Ok(audiences)
}

fn required_oauth_url(label: &str, value: Option<&str>) -> Result<String, AppError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(AppError::FailedPrecondition(format!(
            "configured [auth] requires {label}"
        )));
    };
    CanonicalOauthUrl::parse(value)
        .map(CanonicalOauthUrl::into_identifier)
        .map_err(|error| AppError::FailedPrecondition(format!("{label} {error}")))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use tempfile::TempDir;

    use super::{GrpcSettings, Launcher, LoadedServerConfig, McpHttpServeConfig, ServeSettings};
    use crate::bootstrap::AppError;
    use crate::state::AppStateLayout;

    const WEB: &str = "\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n";

    /// A config directory holding `raw` as its config file, beside the session
    /// signing key that [`authenticated`] configurations name.
    fn deployment(raw: &str) -> (TempDir, AppStateLayout) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().to_path_buf())).expect("layout");
        fs::write(layout.config_file(), raw).expect("config file");
        fs::write(
            layout.config_dir().join("session.key"),
            coral_test_support::p256_pkcs8(),
        )
        .expect("session key");
        (temp, layout)
    }

    /// Captures `raw` as the deployment configuration.
    fn load(raw: &str) -> (TempDir, LoadedServerConfig) {
        let (temp, layout) = deployment(raw);
        (temp, LoadedServerConfig::load(&layout).expect("load"))
    }

    /// Resolves the serving settings `raw` configures, with deny/deny outbound defaults.
    fn serve(raw: &str) -> Result<ServeSettings, AppError> {
        serve_launched(raw, &Launcher::default())
    }

    fn serve_launched(raw: &str, launcher: &Launcher) -> Result<ServeSettings, AppError> {
        let (_temp, config) = load(raw);
        config.serve_settings(coral_outbound::DestinationPolicy::default(), launcher)
    }

    fn serve_error(raw: &str) -> String {
        match serve(raw) {
            Ok(_) => panic!("serving must fail for {raw}"),
            Err(error) => error.to_string(),
        }
    }

    fn mcp_http(raw: &str) -> Result<McpHttpServeConfig, AppError> {
        serve(raw).map(|settings| settings.mcp_http)
    }

    fn grpc_settings(raw: &str) -> Result<GrpcSettings, AppError> {
        load(raw).1.grpc_settings()
    }

    /// Starts the authorization server `auth` composes and reports, for each
    /// resource, whether an authorize request naming it passes the resource
    /// check instead of being refused `invalid_target`.
    async fn accepted_resources(auth: super::SessionAuthSettings, resources: &[&str]) -> Vec<bool> {
        const CALLBACK: &str = "https://client.example.test/callback";
        let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("client database");
        let server = auth
            .into_authorization_server(
                coral_outbound::DestinationPolicy::default(),
                fixture.database(),
            )
            .expect("authorization server")
            .start()
            .await
            .expect("start authorization server");
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("HTTP client");
        // A registered client needs no metadata fetch, so the request reaches
        // the resource check.
        let registered: serde_json::Value = http
            .post(format!("{}/oauth/register", server.endpoint_uri()))
            .json(&serde_json::json!({ "redirect_uris": [CALLBACK] }))
            .send()
            .await
            .expect("register client")
            .json()
            .await
            .expect("registration");
        let client_id = registered
            .get("client_id")
            .and_then(serde_json::Value::as_str)
            .expect("client ID");
        let mut accepted = Vec::new();
        for resource in resources {
            let response = http
                .get(format!("{}/oauth/authorize", server.endpoint_uri()))
                .query(&[
                    ("client_id", client_id),
                    ("redirect_uri", CALLBACK),
                    ("response_type", "code"),
                    ("code_challenge_method", "S256"),
                    (
                        "code_challenge",
                        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ",
                    ),
                    ("resource", resource),
                ])
                .send()
                .await
                .expect("authorize");
            let refused = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|location| location.to_str().ok())
                .is_some_and(|location| location.contains("error=invalid_target"));
            assert!(
                refused || response.status() == reqwest::StatusCode::OK,
                "{resource}: {}",
                response.status()
            );
            accepted.push(!refused);
        }
        server.shutdown().await.expect("shutdown");
        accepted
    }

    /// An `[auth]` deployment around `mcp_http`, with `auth_fields` in `[auth]`.
    fn authenticated(mcp_http: &str, auth_fields: &str) -> String {
        let web = if mcp_http.contains("[listeners.web]") {
            ""
        } else {
            WEB
        };
        format!(
            "{mcp_http}
{web}
[auth]
{auth_fields}

[listeners.auth]
public_url = 'https://AUTH.example.test/'

[auth.session]
signing_key_file = 'session.key'

[auth.provider]
issuer = 'https://accounts.example.test'
client_id = 'upstream-client'
client_secret = 'test-secret'
"
        )
    }

    #[test]
    fn the_outbound_section_selects_the_mcp_oauth_client_bootstrap_policy() {
        use coral_provider::account::ClientBootstrapPolicy;
        for authenticated_deployment in [false, true] {
            for (field, expected) in [
                ("", ClientBootstrapPolicy::Automatic),
                (
                    "[outbound]\nmcp_oauth_client_bootstrap = 'automatic'\n",
                    ClientBootstrapPolicy::Automatic,
                ),
                (
                    "[outbound]\nmcp_oauth_client_bootstrap = 'prefer-client-id-metadata-document'\n",
                    ClientBootstrapPolicy::PreferClientIdMetadataDocument,
                ),
                (
                    "[outbound]\nmcp_oauth_client_bootstrap = 'dynamic-client-registration-only'\n",
                    ClientBootstrapPolicy::DynamicClientRegistrationOnly,
                ),
            ] {
                let raw = if authenticated_deployment {
                    authenticated(
                        &format!(
                            "{field}[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n"
                        ),
                        "",
                    )
                } else {
                    format!("{field}{WEB}")
                };
                let policy = serve(&raw)
                    .expect("serving settings")
                    .mcp_oauth_client_bootstrap();
                assert_eq!(
                    policy, expected,
                    "authenticated={authenticated_deployment} {field:?}"
                );
            }
        }
        let error = serve_error(&format!(
            "[outbound]\nmcp_oauth_client_bootstrap = 'cimd'\n{WEB}"
        ));
        assert!(error.contains("mcp_oauth_client_bootstrap"), "{error}");
    }

    #[test]
    fn serving_requires_a_human_interface_before_accepting_work() {
        let error = serve_error("");
        assert!(
            error.contains("listeners.web.public_url is required"),
            "{error}"
        );
    }

    #[test]
    fn launcher_destination_is_explicit_local_only_and_cannot_conflict_with_web_configuration() {
        let origin = "http://127.0.0.1:49123";
        let launcher = Launcher {
            local_web_url: Some(coral_mcp::HumanInterfaceUrl::parse(origin).unwrap()),
            ..Launcher::default()
        };
        let settings = serve_launched("", &launcher).unwrap();
        assert_eq!(settings.human_interface().as_str(), origin);
        for (raw, expected) in [
            (
                WEB.to_owned(),
                "configure only one of listeners.web.public_url or CORAL_LOCAL_WEB_URL",
            ),
            (
                authenticated(
                    "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                    "",
                ),
                "CORAL_LOCAL_WEB_URL is local-only; configure listeners.web.public_url",
            ),
        ] {
            let Err(error) = serve_launched(&raw, &launcher) else {
                panic!("unsafe launcher configuration accepted: {raw}");
            };
            assert!(error.to_string().contains(expected), "{error}");
        }
    }

    #[test]
    fn human_handoff_uses_the_explicit_web_origin_and_rejects_unsafe_destinations() {
        for (origin, expected) in [
            ("https://REEF.example.test/", "https://reef.example.test"),
            ("http://127.0.0.1:49152", "http://127.0.0.1:49152"),
        ] {
            let settings = serve(&format!("[listeners.web]\npublic_url = '{origin}'")).unwrap();
            assert_eq!(settings.human_interface().as_str(), expected);
        }
        for origin in [
            "/activity",
            "http://0.0.0.0:5173",
            "https://0.0.0.0:5173",
            "https://[::]:5173",
            "https://reef.example.test/run",
            "https://user:secret@reef.example.test",
            "https://reef.example.test?token=secret",
            "https://reef.example.test#fragment",
            "http://127.0.0.1:0",
        ] {
            assert!(
                serve(&format!("[listeners.web]\npublic_url = '{origin}'")).is_err(),
                "{origin}"
            );
        }
    }

    #[test]
    fn outbound_fields_override_composition_defaults_independently_of_identity() {
        use coral_outbound::{DestinationPolicy, NetworkAccess};
        for authenticated_deployment in [false, true] {
            for defaults in [DestinationPolicy::default(), super::LOCAL_OUTBOUND_DEFAULTS] {
                for (raw, loopback, private) in [
                    ("", None, None),
                    ("[outbound]\nloopback = 'deny'", Some(false), None),
                    ("[outbound]\nprivate_networks = 'https'", None, Some(false)),
                    (
                        "[outbound]\nloopback = 'http-and-https'\nprivate_networks = 'deny'",
                        Some(true),
                        Some(false),
                    ),
                ] {
                    let (_temp, layout) = deployment(&if authenticated_deployment {
                        authenticated(
                            &format!(
                                "{raw}\n[listeners.mcp]\npublic_url = 'https://mcp.example.test'"
                            ),
                            "",
                        )
                    } else {
                        format!("{raw}{WEB}")
                    });
                    let config = LoadedServerConfig::load(&layout).unwrap();
                    config.validate().unwrap();
                    let policy = config.outbound_policy(defaults).unwrap();
                    assert_eq!(
                        config
                            .serve_settings(defaults, &Launcher::default())
                            .unwrap()
                            .outbound_policy(),
                        policy
                    );
                    let permissive = defaults == super::LOCAL_OUTBOUND_DEFAULTS;
                    for (url, allowed) in [
                        ("http://localhost/", loopback.unwrap_or(permissive)),
                        ("http://10.0.0.1/", private.unwrap_or(permissive)),
                        ("https://169.254.169.254/", false),
                    ] {
                        assert_eq!(
                            policy.validate(&url.parse().unwrap()).is_ok(),
                            allowed,
                            "{raw}: {url}"
                        );
                    }
                    fs::write(layout.config_file(), "").unwrap();
                    assert_eq!(config.outbound_policy(defaults).unwrap(), policy);
                    assert_eq!(
                        LoadedServerConfig::load(&layout)
                            .unwrap()
                            .outbound_policy(defaults)
                            .unwrap(),
                        defaults
                    );
                }
            }
        }
        assert_eq!(
            DestinationPolicy::default(),
            DestinationPolicy::new(NetworkAccess::Deny, NetworkAccess::Deny)
        );
    }

    #[test]
    fn the_search_reconcile_interval_defaults_to_five_minutes() {
        for raw in ["", "[search]"] {
            assert_eq!(
                load(raw).1.search_reconcile_interval().expect(raw),
                std::time::Duration::from_mins(5),
                "{raw:?}"
            );
        }
        assert_eq!(
            load("[search]\nreconcile_interval_seconds = 30")
                .1
                .search_reconcile_interval()
                .unwrap(),
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn invalid_runtime_settings_fail_closed() {
        for (raw, expected) in [
            ("[runtime]\nfan_out_limit = 0", "runtime.fan_out_limit:"),
            (
                "[runtime]\napplied_output_cap_bytes = 1",
                "runtime.applied_output_cap_bytes:",
            ),
            ("[runtime]\nfan_out = 4", "unknown field `fan_out`"),
        ] {
            let error = load(raw).1.runtime_limits().expect_err(raw);
            assert!(
                matches!(&error, AppError::FailedPrecondition(message) if message.contains(expected)),
                "{raw}: {error}"
            );
        }
        assert_eq!(
            load("[runtime]\nfan_out_limit = 4\n")
                .1
                .runtime_limits()
                .expect("a limit of four")
                .fan_out_limit()
                .invocations(),
            4
        );
        for (raw, drain) in [
            ("", coral_run::ShutdownDrain::DEFAULT_DURATION),
            (
                "[runtime]\nshutdown_drain_seconds = 0\n",
                std::time::Duration::ZERO,
            ),
            (
                "[runtime]\nshutdown_drain_seconds = 90\n",
                std::time::Duration::from_secs(90),
            ),
        ] {
            assert_eq!(
                load(raw)
                    .1
                    .runtime_limits()
                    .expect("a drain")
                    .shutdown_drain()
                    .duration(),
                drain,
                "{raw:?}"
            );
        }
    }

    #[test]
    fn invalid_search_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[search]\nreconcile_interval = 30",
                "unknown field `reconcile_interval`",
            ),
            (
                "[search]\nreconcile_interval_seconds = 0",
                "search.reconcile_interval_seconds must be greater than 0",
            ),
            (
                "[search]\nreconcile_interval_seconds = -1",
                "search.reconcile_interval_seconds",
            ),
        ] {
            let error = load(raw).1.validate().expect_err(raw);
            assert!(
                matches!(&error, AppError::FailedPrecondition(message) if message.contains(expected)),
                "{raw}: {error}"
            );
        }
    }

    #[test]
    fn the_credential_key_env_is_named_only_when_configured() {
        for raw in ["", "[credentials]"] {
            assert_eq!(
                load(raw).1.credential_key_env().expect(raw),
                None,
                "{raw:?}"
            );
        }
        assert_eq!(
            load("[credentials]\nkey_env = \" CORAL_CREDENTIALS_KEY \"")
                .1
                .credential_key_env()
                .unwrap()
                .as_deref(),
            Some("CORAL_CREDENTIALS_KEY")
        );
    }

    #[test]
    fn invalid_credential_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[credentials]\nkey_env = \"\"",
                "credentials.key_env must not be empty",
            ),
            (
                "[credentials]\nkey_env = \" \"",
                "credentials.key_env must not be empty",
            ),
            (
                "[credentials]\nkey = \"CORAL_CREDENTIALS_KEY\"",
                "unknown field `key`",
            ),
            (
                "[credentials]\nkey_env = 1",
                "credentials.key_env: unsupported value",
            ),
        ] {
            let error = load(raw).1.validate().expect_err(raw);
            assert!(
                matches!(&error, AppError::FailedPrecondition(message) if message.contains(expected)),
                "{raw}: {error}"
            );
        }
    }

    #[test]
    fn invalid_outbound_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[outbound]\nallow_private_networks = 'true'",
                "unknown field `allow_private_networks`",
            ),
            (
                "[outbound]\nallow_private_networks = 1",
                "unknown field `allow_private_networks`",
            ),
            ("[outbound]\nunknown = true", "unknown field `unknown`"),
            (
                "[outbound]\nallow_private_networks = true",
                "unknown field `allow_private_networks`",
            ),
            (
                "[outbound]\nallow_loopback = true",
                "unknown field `allow_loopback`",
            ),
            (
                "[outbound]\nloopback = 'https-only'",
                "outbound.loopback: unsupported value",
            ),
            (
                "[outbound]\nprivate_networks = false",
                "outbound.private_networks: wanted string or table",
            ),
        ] {
            let error = load(raw).1.validate().expect_err(raw);
            assert!(
                matches!(&error, AppError::FailedPrecondition(message)
                    if message.starts_with("server configuration is invalid: ")
                        && message.contains(expected)),
                "{raw}: {error}"
            );
        }
    }

    /// Operators paste secrets under the wrong key, so every section's decode
    /// error names the key path and never echoes the rejected value.
    #[tokio::test]
    async fn decode_errors_name_the_key_path_without_the_value() {
        const SECRET: &str = "sk-live-0123456789abcdef";
        for (section, path) in [
            (
                format!("[outbound]\nloopback = '{SECRET}'"),
                "outbound.loopback",
            ),
            (
                format!("[listeners.mcp]\nbind = '{SECRET}'"),
                "listeners.mcp.bind",
            ),
            (
                format!("[auth]\nallowed_audiences = '{SECRET}'"),
                "auth.allowed_audiences",
            ),
            (format!("[otel]\nheaders = ['{SECRET}']"), "otel.headers"),
            (
                format!("[database]\nbackend = 'postgres://coral:{SECRET}@db.internal/coral'"),
                "database.backend",
            ),
        ] {
            let temp = TempDir::new().unwrap();
            fs::write(temp.path().join("config.toml"), format!("{section}{WEB}")).unwrap();
            let Err(error) = crate::ServerBuilder::new()
                .with_config_dir(temp.path())
                .prepare()
                .await
            else {
                panic!("{section}: startup must reject the configuration");
            };
            let error = error.to_string();
            assert!(error.contains(&format!("{path}: ")), "{section}: {error}");
            assert!(!error.contains(SECRET), "{section}: {error}");
        }
    }

    #[tokio::test]
    async fn fresh_deployments_accept_outbound_configuration() {
        for access in ["deny", "https", "http-and-https"] {
            let temp = TempDir::new().unwrap();
            fs::write(
                temp.path().join("config.toml"),
                format!("[outbound]\nloopback = '{access}'\nprivate_networks = '{access}'\n{WEB}"),
            )
            .unwrap();
            let server = crate::ServerBuilder::new()
                .with_config_dir(temp.path())
                .start()
                .await
                .unwrap();
            server.shutdown().await.unwrap();
        }
    }

    #[test]
    fn desktop_callback_default_preserves_explicit_binds_and_hosted_defaults() {
        for desktop in [false, true] {
            let launcher = Launcher {
                desktop,
                ..Launcher::default()
            };
            let default_bind = if desktop {
                "127.0.0.1:8779"
            } else {
                "127.0.0.1:0"
            };
            for (source, expected) in [
                (WEB.to_owned(), default_bind),
                (
                    format!("[listeners.grpc]\nbind = '127.0.0.1:0'\n{WEB}"),
                    default_bind,
                ),
                (
                    "listeners = { auth = {}, web = { public_url = 'http://127.0.0.1:5173' } }\n"
                        .to_owned(),
                    default_bind,
                ),
                (
                    format!("[listeners.auth]\nbind = '127.0.0.1:18879'\n{WEB}"),
                    "127.0.0.1:18879",
                ),
                (
                    format!("[listeners.auth]\nbind = '[::1]:18879'\n{WEB}"),
                    "[::1]:18879",
                ),
                (
                    format!("[listeners.auth]\nbind = '127.0.0.1:0'\n{WEB}"),
                    "127.0.0.1:0",
                ),
            ] {
                assert_eq!(
                    serve_launched(&source, &launcher).unwrap().auth_bind_addr(),
                    expected.parse::<SocketAddr>().unwrap(),
                    "desktop {desktop}, config: {source}"
                );
            }

            // Hosted deployments keep their ephemeral default under Desktop too.
            let mut settings = serve_launched(
                &authenticated(
                    "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                    "",
                ),
                &launcher,
            )
            .unwrap();
            assert_eq!(settings.auth_bind_addr(), "127.0.0.1:0".parse().unwrap());
            assert!(settings.take_session_auth().is_some());
        }
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_reports_config_metadata_errors() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_temp, layout) = deployment("");
        let permissions = fs::metadata(layout.config_dir()).unwrap().permissions();
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o000))
            .expect("hide config dir");
        let result = LoadedServerConfig::load(&layout);
        fs::set_permissions(layout.config_dir(), permissions).expect("restore config dir");
        assert!(matches!(result, Err(AppError::Io(_))));
    }

    #[test]
    fn defaults_both_listener_binds_to_ephemeral_loopback_with_explicit_web_destination() {
        let settings = grpc_settings(WEB).expect("gRPC settings");
        assert_eq!(settings.bind, SocketAddr::from((Ipv4Addr::LOCALHOST, 0)));
        assert_eq!(
            mcp_http(WEB).expect("MCP config"),
            McpHttpServeConfig::AuthDisabled {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            }
        );
    }

    #[test]
    fn loads_bind_address_alongside_supported_sections() {
        let settings = grpc_settings(
            "[listeners.grpc]\nbind = '127.0.0.2:14555'\n\n[listeners.mcp]\nbind = '[::1]:14556'\n\n[otel]\nservice_name = 'coral'\n",
        )
        .expect("gRPC settings");
        assert_eq!(settings.bind.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
        assert_eq!(settings.bind.port(), 14555);
    }

    #[test]
    fn loads_local_mcp_listener_without_an_enable_switch() {
        let McpHttpServeConfig::AuthDisabled { bind_addr } = mcp_http(&format!(
            "[listeners.grpc]\nbind = '127.0.0.1:0'\n\n[listeners.mcp]\nbind = '[::1]:14556'\n{WEB}"
        ))
        .expect("MCP HTTP config") else {
            panic!("loopback MCP must be explicitly auth-disabled");
        };
        assert_eq!(bind_addr, SocketAddr::from((Ipv6Addr::LOCALHOST, 14556)));
    }

    #[test]
    fn auth_disabled_mcp_http_rejects_public_bind() {
        let error = mcp_http("[listeners.mcp]\nbind = '0.0.0.0:14556'\n")
            .expect_err("public auth-disabled bind must fail");
        assert!(error.to_string().contains("must be loopback"));
    }

    #[test]
    fn auth_disabled_mcp_http_rejects_public_url() {
        let error = mcp_http("[listeners.mcp]\npublic_url = 'https://coral.example/mcp'\n")
            .expect_err("auth-disabled MCP must not advertise an authenticated public URL");
        assert!(
            error.to_string().contains("listeners.mcp.public_url"),
            "{error}"
        );
    }

    #[test]
    fn authenticated_listeners_canonicalize_public_audiences_in_surface_order() {
        let companions = serve(&authenticated(
            "[listeners.mcp]\nbind = '127.0.0.1:0'\npublic_url = 'https://MCP.example.test/'\n",
            "allowed_audiences = ['https://CORAL-WEB.example.test/']",
        ))
        .expect("listener settings");
        assert_eq!(
            companions.mcp_http,
            McpHttpServeConfig::Authenticated {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                public_url: "https://mcp.example.test".to_string(),
                authorization_server: "https://auth.example.test".to_string(),
            }
        );
        // Resolution reports that session auth is configured; constructing the
        // providers and the authorization server from it is the composition
        // root's job, covered in `bootstrap::server`.
        let session_auth = companions.session_auth.expect("session auth");
        // The MCP surface's audiences are the per-workspace family under its
        // base, not an enumerable entry, so the explicit audience list carries
        // only the configured extras.
        assert_eq!(
            session_auth.public_audiences,
            ["https://coral-web.example.test".to_string()]
        );
        assert_eq!(
            session_auth.mcp_workspace_urls.base().identifier(),
            "https://mcp.example.test"
        );
    }

    /// MCP needs no extra audience list: its workspace family is always
    /// registered, but the base URL is not an exact audience of its own.
    #[tokio::test]
    async fn authenticated_mcp_only_config_serves_only_the_workspace_family() {
        let companions = serve(&authenticated(
            "[listeners.mcp]\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test/'\n",
            "",
        ))
        .expect("MCP settings");
        let session_auth = companions.session_auth.expect("session auth");
        assert!(session_auth.public_audiences.is_empty());

        // The family must be registered even though it enumerates to nothing,
        // or every real MCP login is refused invalid_target. The base is not a
        // resource at all.
        assert_eq!(
            accepted_resources(
                session_auth,
                &[
                    "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp",
                    "https://mcp.example.test",
                ],
            )
            .await,
            [true, false]
        );
    }

    #[test]
    fn authenticated_mcp_rejects_non_origin_public_urls() {
        for public_url in [
            "https://mcp.example.test/mcp",
            "https://mcp.example.test/prefix/",
        ] {
            let error = serve_error(&authenticated(
                &format!("[listeners.mcp]\npublic_url = '{public_url}'\n"),
                "",
            ));
            assert!(
                error.contains("listeners.mcp.public_url must be an origin URL without a path"),
                "{error}"
            );
        }
    }

    /// Configured `[auth]` always needs `listeners.mcp.public_url`: no extra
    /// audience replaces it, and omitting the section or giving only a bind
    /// cannot disable the listener or bypass the requirement.
    #[test]
    fn configured_auth_requires_the_mcp_public_url() {
        for (mcp_http, auth_fields) in [
            (
                "",
                "allowed_audiences = ['https://CORAL-WEB.example.test/']",
            ),
            ("", "allowed_audiences = []"),
            ("[listeners.mcp]\nbind = '127.0.0.1:0'\n", ""),
        ] {
            let error = serve_error(&authenticated(mcp_http, auth_fields));
            assert!(
                error.contains("configured [auth] requires listeners.mcp.public_url"),
                "{mcp_http}{auth_fields}: {error}"
            );
        }
    }

    #[tokio::test]
    async fn fresh_web_configuration_registers_the_web_resource_even_on_the_mcp_origin() {
        for web_origin in ["https://web.example.test", "https://mcp.example.test"] {
            let (_temp, layout) = deployment(&authenticated(
                &format!(
                    "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n\
                     [listeners.web]\npublic_url = '{web_origin}'"
                ),
                "",
            ));
            // This crosses the unmarked-state fence too: [listeners.web] is bootstrap
            // configuration, not retired state that needs manual removal.
            let prepared = crate::ServerBuilder::new()
                .with_config_dir(layout.config_dir())
                .prepare()
                .await
                .expect("fresh hosted configuration");
            let mut serving = prepared.serve_settings().expect("serve settings");
            let auth = serving.take_session_auth().expect("authenticated");
            assert_eq!(auth.public_audiences(), [web_origin]);
            assert_eq!(
                accepted_resources(
                    auth,
                    &[
                        web_origin,
                        "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp",
                    ],
                )
                .await,
                [true, true],
                "{web_origin}"
            );
        }
    }

    #[test]
    fn allowed_audiences_reject_invalid_and_duplicate_entries() {
        let cases = [
            (
                "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                "allowed_audiences = ['https://coral-web.example.test/?tenant=one']",
                "auth.allowed_audiences[0] must not include a query",
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://MCP.example.test/'\n",
                "allowed_audiences = ['https://mcp.example.test']",
                "auth.allowed_audiences[0] duplicates listeners.mcp.public_url",
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                "allowed_audiences = ['https://MCP.example.test/mcp']",
                "auth.allowed_audiences[0] is the Organization MCP Endpoint",
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                "allowed_audiences = ['https://CORAL-WEB.example.test/', 'https://coral-web.example.test']",
                "auth.allowed_audiences[1] duplicates another configured public surface audience",
            ),
        ];

        for (mcp_http, auth_fields, expected) in cases {
            let error = serve_error(&authenticated(mcp_http, auth_fields));
            assert!(error.contains(expected), "error: {error}");
        }
    }

    /// Parsing `[auth]` validates the section without touching what it points
    /// at, so an instance whose signing key is missing loads and only fails when
    /// the settings behind it are actually resolved.
    #[test]
    fn resolving_serve_settings_fails_on_a_missing_signing_key() {
        let (_temp, layout) = deployment(&authenticated("", ""));
        let key_path = layout.config_dir().join("session.key");
        fs::remove_file(&key_path).expect("remove session key");

        let config = LoadedServerConfig::load(&layout).expect("load");

        // `ServeSettings` carries key material and so implements no `Debug`,
        // which rules out `expect_err` here.
        let Err(error) = config.serve_settings(
            coral_outbound::DestinationPolicy::default(),
            &Launcher::default(),
        ) else {
            panic!("resolution must still fail on the missing signing key");
        };
        let message = error.to_string();
        assert!(
            message.contains("failed to read signing_key_file")
                && message.contains(&key_path.display().to_string()),
            "{message}"
        );
    }

    /// Binding a public surface off loopback is the operator's call — the risk is
    /// documented rather than gated — so this must resolve without ceremony. The
    /// auth-disabled loopback restriction is a separate rule and still applies.
    #[test]
    fn authenticated_mcp_http_accepts_a_remote_bind() {
        assert!(matches!(
            mcp_http(&authenticated(
                "[listeners.mcp]\nbind = '0.0.0.0:14556'\npublic_url = 'https://mcp.example.test'\n",
                "",
            )),
            Ok(McpHttpServeConfig::Authenticated { .. })
        ));
    }
}
