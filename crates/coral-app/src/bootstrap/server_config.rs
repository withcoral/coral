//! Configuration owned by the long-running Coral server surface.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

use super::env::AppEnvironment;
use super::{AppError, is_loopback_ip};
use crate::auth::session::SessionTokenIssuer;
use crate::auth::{AuthSettings, CoralAuthorizationServer, ResolvedAuthSettings};
use crate::oauth_resource::CanonicalOauthUrl;
use crate::request_auth::SessionPrincipalProvider;
use crate::state::AppStateLayout;
use crate::workspace_mcp_urls::WorkspaceMcpUrls;

#[derive(Debug, Default, Deserialize)]
struct GrpcConfigFile {
    #[serde(default)]
    server: ServerSettings,
}

#[derive(Debug, Default, Deserialize)]
struct McpHttpConfigFile {
    #[serde(default)]
    server: McpHttpServerSettings,
}

#[derive(Debug, Default, Deserialize)]
struct RemovedAuthConfigFile {
    #[serde(default)]
    server: RemovedAuthServerSettings,
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct RemovedAuthServerSettings {
    #[serde(rename = "auth")]
    removed_auth: Option<toml::Value>,
}

/// One fail-closed snapshot of the configuration used to prepare `coral serve`.
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

    pub(crate) fn grpc_settings(&self) -> Result<ServerSettings, AppError> {
        let settings = self.parse::<GrpcConfigFile>()?.server;
        reject_removed_auth(settings.removed_auth.as_ref())?;
        Ok(settings)
    }

    pub(crate) fn reject_removed_auth(&self) -> Result<(), AppError> {
        let settings = self.parse::<RemovedAuthConfigFile>()?.server;
        reject_removed_auth(settings.removed_auth.as_ref())
    }

    /// Resolves the settings `coral serve`'s companions are built from.
    ///
    /// This returns validated configuration and resolved key material only. The
    /// caller constructs the services and owns their lifecycle, so resolving
    /// configuration never produces a live service or its state.
    pub(crate) fn companion_settings(&self) -> Result<ServeSettings, AppError> {
        let Some(auth_settings) = self.auth_settings()? else {
            return Ok(ServeSettings {
                mcp_http: self.resolve_mcp_http(None)?,
                session_auth: None,
            });
        };
        let allowed_audiences = auth_settings.allowed_audiences().to_vec();
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
        // The issuer accessor lives on the resolved settings, so read it after
        // runtime resolution rather than before.
        let authorization_server = required_oauth_url(
            "auth.authorization_server.issuer",
            Some(auth_settings.authorization_server().issuer()),
        )?;
        let mcp_http = self.resolve_mcp_http(Some(&authorization_server))?;
        let mcp_workspace_urls = match &mcp_http {
            Some(McpHttpServeConfig::Authenticated { public_url, .. }) => {
                // The variant carries the canonical identifier, and parsing is
                // idempotent on canonical output, so this cannot fail — but a
                // panic here would turn a config bug into a crash, so it maps
                // to the same configuration error every other bad URL gets.
                let base = CanonicalOauthUrl::parse(public_url).map_err(|error| {
                    AppError::FailedPrecondition(format!("server.mcp_http.public_url {error}"))
                })?;
                Some(WorkspaceMcpUrls::new(base))
            }
            _ => None,
        };
        let public_audiences =
            public_surface_audiences(mcp_workspace_urls.as_ref(), &allowed_audiences)?;
        Ok(ServeSettings {
            mcp_http,
            session_auth: Some(SessionAuthSettings {
                settings: auth_settings,
                session_tokens: session,
                public_audiences,
                mcp_workspace_urls,
            }),
        })
    }

    fn auth_settings(&self) -> Result<Option<AuthSettings>, AppError> {
        AuthSettings::from_toml(&self.raw)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))
    }

    fn resolve_mcp_http(
        &self,
        authorization_server: Option<&str>,
    ) -> Result<Option<McpHttpServeConfig>, AppError> {
        let settings = self.parse::<McpHttpConfigFile>()?.server;
        reject_removed_auth(settings.removed_auth.as_ref())?;
        settings.mcp_http.resolve(authorization_server)
    }

    fn parse<T>(&self) -> Result<T, AppError>
    where
        T: DeserializeOwned + Default,
    {
        toml::from_str(&self.raw).map_err(|error| {
            AppError::FailedPrecondition(format!(
                "server configuration is invalid: {}",
                error.message()
            ))
        })
    }
}

/// Settings loaded from the gRPC-owned fields under `[server]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(default)]
pub(crate) struct ServerSettings {
    pub(crate) bind_addr: SocketAddr,
    #[serde(rename = "auth")]
    removed_auth: Option<toml::Value>,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            removed_auth: None,
        }
    }
}

impl ServerSettings {
    /// Loads only the gRPC-owned fields from `[server]`.
    #[cfg(test)]
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        LoadedServerConfig::load(layout)?.grpc_settings()
    }
}

#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct McpHttpServerSettings {
    mcp_http: RawMcpHttpSettings,
    #[serde(rename = "auth")]
    removed_auth: Option<toml::Value>,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct RawMcpHttpSettings {
    enabled: bool,
    bind: SocketAddr,
    public_url: Option<String>,
    allow_unauthenticated_non_loopback: bool,
    allowed_hosts: Vec<String>,
}

impl Default for RawMcpHttpSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            public_url: None,
            allow_unauthenticated_non_loopback: false,
            allowed_hosts: Vec::new(),
        }
    }
}

impl RawMcpHttpSettings {
    fn resolve(
        &self,
        authorization_server: Option<&str>,
    ) -> Result<Option<McpHttpServeConfig>, AppError> {
        if !self.enabled {
            return Ok(None);
        }

        let Some(authorization_server) = authorization_server else {
            let expose_non_loopback = !is_loopback_ip(self.bind.ip());
            if expose_non_loopback && !self.allow_unauthenticated_non_loopback {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http.bind must be loopback; set \
                     server.mcp_http.allow_unauthenticated_non_loopback = true to expose \
                     the unauthenticated listener deliberately"
                        .to_string(),
                ));
            }
            if self.public_url.is_some() {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http must not advertise OAuth metadata".to_string(),
                ));
            }
            return Ok(Some(McpHttpServeConfig::AuthDisabled {
                bind_addr: self.bind,
                expose_non_loopback,
                allowed_hosts: validated_mcp_allowed_hosts(&self.allowed_hosts)?,
            }));
        };

        if self.allow_unauthenticated_non_loopback {
            return Err(AppError::FailedPrecondition(
                "server.mcp_http.allow_unauthenticated_non_loopback has no effect with [auth] \
                 configured; remove it"
                    .to_string(),
            ));
        }
        if !self.allowed_hosts.is_empty() {
            return Err(AppError::FailedPrecondition(
                "server.mcp_http.allowed_hosts is only supported without [auth]; the \
                 authenticated listener derives its accepted hosts from public_url"
                    .to_string(),
            ));
        }
        let public_url =
            required_oauth_url("server.mcp_http.public_url", self.public_url.as_deref())?;
        let authorization_server = authorization_server.to_string();

        Ok(Some(McpHttpServeConfig::Authenticated {
            bind_addr: self.bind,
            public_url,
            authorization_server,
        }))
    }
}

/// Validates operator-supplied MCP HTTP Host allowlist entries.
///
/// Entries are Host header values (`host` or `host:port`), so a scheme, path,
/// userinfo, or embedded whitespace means the operator pasted a URL — reject it
/// with the field name rather than letting the listener 403 the requests the
/// entry was meant to admit.
fn validated_mcp_allowed_hosts(hosts: &[String]) -> Result<Vec<String>, AppError> {
    for host in hosts {
        let valid = !host.is_empty()
            && !host.contains(|character: char| {
                character.is_whitespace() || matches!(character, '/' | '@')
            });
        if !valid {
            return Err(AppError::FailedPrecondition(format!(
                "server.mcp_http.allowed_hosts entry {host:?} is not a bare host or host:port"
            )));
        }
    }
    Ok(hosts.to_vec())
}

/// Resolved MCP HTTP configuration prepared independently from gRPC startup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpHttpServeConfig {
    /// MCP HTTP backed by an unauthenticated local client.
    ///
    /// Loopback-only unless the operator opted into deliberate exposure with
    /// `server.mcp_http.allow_unauthenticated_non_loopback`.
    AuthDisabled {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
        /// Operator consent to serve unauthenticated off loopback.
        ///
        /// Set only when `bind_addr` is non-loopback and the config opted in;
        /// a loopback bind never carries consent it does not need.
        expose_non_loopback: bool,
        /// Extra Host header values accepted beside the loopback defaults,
        /// e.g. a Docker Compose service name.
        allowed_hosts: Vec<String>,
    },
    /// Session-authenticated MCP HTTP backed by per-session clients.
    Authenticated {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
        /// Canonical public base of the per-workspace MCP URLs.
        ///
        /// Each workspace is served at `<public_url>/workspace/<name>`, and
        /// that full per-workspace URL — never this base — is the OAuth
        /// resource and JWT audience of the sessions it admits. The URL names
        /// the workspace, so no workspace is configured here.
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

    #[cfg(test)]
    fn load(layout: &AppStateLayout) -> Result<Option<Self>, AppError> {
        Ok(LoadedServerConfig::load(layout)?
            .companion_settings()?
            .mcp_http)
    }
}

/// Validated settings for the companions `coral serve` composes beside gRPC.
///
/// Nothing here is live. The gRPC server's bootstrap resolves configuration; the
/// call site turns these settings into services and owns their lifecycle, so a
/// server builder never constructs a transport it does not run. `coral serve`
/// composes them in `coral-cli`'s `serve::compose_session_policies`.
pub struct ServeSettings {
    pub(super) mcp_http: Option<McpHttpServeConfig>,
    pub(super) session_auth: Option<SessionAuthSettings>,
}

impl ServeSettings {
    /// Returns the resolved MCP HTTP configuration, when enabled.
    #[must_use]
    pub fn mcp_http(&self) -> Option<&McpHttpServeConfig> {
        self.mcp_http.as_ref()
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
    pub(super) mcp_workspace_urls: Option<WorkspaceMcpUrls>,
}

impl SessionAuthSettings {
    /// The instance's explicitly registered public audiences, canonicalized.
    ///
    /// When MCP HTTP is served, its audiences are the per-workspace resource
    /// family under [`Self::mcp_workspace_urls`] — a set that cannot be
    /// enumerated here, so it is deliberately not in this list.
    #[must_use]
    pub fn public_audiences(&self) -> &[String] {
        &self.public_audiences
    }

    /// The per-workspace MCP resource family, when MCP HTTP is served.
    #[must_use]
    pub fn mcp_workspace_urls(&self) -> Option<&WorkspaceMcpUrls> {
        self.mcp_workspace_urls.as_ref()
    }

    /// Builds a provider admitting session tokens for exactly `audiences`.
    ///
    /// The caller chooses the allowlist because it depends on the surface: a
    /// public surface admits only its own audience, while the private gRPC API
    /// admits every audience that fronts it.
    #[must_use]
    pub fn principal_provider(
        &self,
        audiences: impl IntoIterator<Item = String>,
    ) -> Arc<SessionPrincipalProvider> {
        Arc::new(SessionPrincipalProvider::new(
            self.session_tokens.verifier(),
            audiences,
        ))
    }

    /// Builds the private gRPC API's provider.
    ///
    /// The private API is reached through the public surfaces that front it,
    /// so it admits every explicitly registered audience plus, when MCP HTTP
    /// is served, every per-workspace MCP resource — bearer-forwarded backend
    /// calls arrive under exactly those audiences.
    #[must_use]
    pub fn private_api_provider(&self) -> Arc<SessionPrincipalProvider> {
        match &self.mcp_workspace_urls {
            Some(urls) => Arc::new(SessionPrincipalProvider::with_workspace_family(
                self.session_tokens.verifier(),
                self.public_audiences.clone(),
                Arc::new(urls.clone()),
            )),
            None => self.principal_provider(self.public_audiences.clone()),
        }
    }

    /// Builds the authenticator the MCP HTTP surface checks bearers with.
    ///
    /// The MCP surface's audience varies by route — each workspace URL is its
    /// own resource — so this provider is used exclusively through
    /// [`SessionPrincipalProvider::principal_for_bearer_with_audience`], with
    /// the expected audience supplied per request. It carries no standing
    /// allowlist of its own.
    #[must_use]
    pub fn mcp_route_authenticator(&self) -> Arc<SessionPrincipalProvider> {
        Arc::new(SessionPrincipalProvider::new(
            self.session_tokens.verifier(),
            [],
        ))
    }

    /// Consumes these settings into the authorization server for the instance.
    ///
    /// Every explicitly registered public audience becomes an authorization
    /// resource clients may request a token for, and the per-workspace MCP
    /// family — when MCP HTTP is served — is registered as a template rather
    /// than an enumeration.
    ///
    /// The server returned here has no database attached and so fails every
    /// login closed. `ServerBuilder::with_session_auth` is the path that
    /// attaches the migrated app database to it.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the server cannot be built from these settings.
    pub fn into_authorization_server(self) -> Result<CoralAuthorizationServer, AppError> {
        let mut server =
            CoralAuthorizationServer::from_resolved_settings(self.settings, self.session_tokens)
                .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        for audience in &self.public_audiences {
            server = server
                .with_authorization_resource(audience)
                .map_err(AppError::FailedPrecondition)?;
        }
        if let Some(urls) = self.mcp_workspace_urls {
            server = server.with_workspace_resource_family(urls);
        }
        Ok(server)
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
/// The gRPC API has no resource identity of its own and instead accepts every
/// identifier returned here plus the MCP family. At least one public surface
/// is therefore required: with none, no token can be minted for anything and
/// every login would fail at authorization.
///
/// An identifier names a surface, not an actor: either kind of caller can arrive
/// through any of them, so nothing here says what kind a caller is. Actor kind
/// comes from the authenticated principal instead.
fn public_surface_audiences(
    mcp_workspace_urls: Option<&WorkspaceMcpUrls>,
    allowed_audiences: &[String],
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
        // The MCP base is deliberately not an audience of its own — it is not
        // an MCP endpoint any more — so an explicit entry naming it would
        // resurrect exactly the audience the per-workspace family replaced.
        // Both spellings are rejected: a trailing-slash `public_url` leaves the
        // canonical base and the normalized base identifier differing by one
        // slash, and either would otherwise slip through as a gRPC audience.
        if mcp_workspace_urls.is_some_and(|urls| urls.is_base_audience(&audience)) {
            return Err(AppError::FailedPrecondition(format!(
                "{label} duplicates server.mcp_http.public_url, which is the base of the \
                 per-workspace MCP resources and not an audience of its own"
            )));
        }
        audiences.push(audience);
    }
    if audiences.is_empty() && mcp_workspace_urls.is_none() {
        return Err(AppError::FailedPrecondition(
            "configured [auth] requires at least one public surface: an enabled server.mcp_http with a public_url, or a non-empty auth.allowed_audiences"
                .to_string(),
        ));
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

fn reject_removed_auth(value: Option<&toml::Value>) -> Result<(), AppError> {
    if value.is_some() {
        return Err(AppError::FailedPrecondition(
            "[server.auth] is no longer supported".to_string(),
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use tempfile::TempDir;

    use super::{LoadedServerConfig, McpHttpServeConfig, ServerSettings};
    use crate::auth::session::test_signing_key;
    use crate::state::AppStateLayout;

    fn write_authenticated_config(layout: &AppStateLayout, mcp_http: &str) {
        write_authenticated_config_with_auth(layout, mcp_http, "");
    }

    fn write_authenticated_config_with_auth(
        layout: &AppStateLayout,
        mcp_http: &str,
        auth_fields: &str,
    ) {
        layout.ensure().expect("config dir");
        fs::write(layout.config_dir().join("session.key"), test_signing_key())
            .expect("session key");
        fs::write(
            layout.config_file(),
            format!(
                "{mcp_http}
[auth]
{auth_fields}

[auth.authorization_server]
issuer = 'https://AUTH.example.test/'

[auth.session]
signing_key_file = 'session.key'

[auth.provider]
issuer = 'https://accounts.example.test'
client_id = 'upstream-client'
client_secret = 'test-secret'
redirect_uri = 'https://auth.example.test/auth/oidc/callback'
"
            ),
        )
        .expect("config file");
    }

    #[test]
    fn defaults_to_ephemeral_ipv4_loopback_without_server_section() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");

        let settings = ServerSettings::load(&layout).expect("server settings");

        assert_eq!(
            settings.bind_addr,
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
        );
        assert!(
            McpHttpServeConfig::load(&layout)
                .expect("MCP HTTP config")
                .is_none()
        );
    }

    #[test]
    fn loads_bind_address_without_claiming_other_sections() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server]\nbind_addr = '127.0.0.2:14555'\n\n[server.mcp_http]\nenabled = true\nbind = 'invalid'\n\n[future]\nvalue = true\n",
        )
        .expect("config file");

        let settings = ServerSettings::load(&layout).expect("server settings");

        assert_eq!(
            settings.bind_addr.ip(),
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2))
        );
        assert_eq!(settings.bind_addr.port(), 14555);
    }

    #[test]
    fn rejects_invalid_bind_address() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server]\nbind_addr = 'localhost:14555'\n",
        )
        .expect("config file");

        ServerSettings::load(&layout).expect_err("bind address must be an IP socket address");
    }

    #[test]
    fn loads_enabled_auth_disabled_mcp_http_without_parsing_grpc_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server]\nbind_addr = 'not-an-address'\n\n[server.mcp_http]\nenabled = true\nbind = '[::1]:14556'\n",
        )
        .expect("config file");

        let Some(McpHttpServeConfig::AuthDisabled {
            bind_addr,
            expose_non_loopback,
            allowed_hosts,
        }) = McpHttpServeConfig::load(&layout).expect("MCP HTTP config")
        else {
            panic!("loopback MCP must be explicitly auth-disabled");
        };
        assert_eq!(bind_addr, SocketAddr::from((Ipv6Addr::LOCALHOST, 14556)));
        assert!(!expose_non_loopback);
        assert!(allowed_hosts.is_empty());
    }

    #[test]
    fn auth_disabled_mcp_http_rejects_public_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\n",
        )
        .expect("config file");

        let error =
            McpHttpServeConfig::load(&layout).expect_err("public auth-disabled bind must fail");

        assert!(error.to_string().contains("must be loopback"));
        // The rejection is also where the operator learns the deliberate way out.
        assert!(
            error
                .to_string()
                .contains("allow_unauthenticated_non_loopback")
        );
    }

    #[test]
    fn opted_in_auth_disabled_mcp_http_accepts_a_public_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\n\
             allow_unauthenticated_non_loopback = true\nallowed_hosts = ['coral']\n",
        )
        .expect("config file");

        let Some(McpHttpServeConfig::AuthDisabled {
            bind_addr,
            expose_non_loopback,
            allowed_hosts,
            ..
        }) = McpHttpServeConfig::load(&layout).expect("MCP HTTP config")
        else {
            panic!("opted-in non-loopback MCP must stay auth-disabled");
        };
        assert_eq!(bind_addr, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 14556)));
        assert!(expose_non_loopback);
        assert_eq!(allowed_hosts, vec!["coral".to_string()]);
    }

    #[test]
    fn opt_in_on_a_loopback_bind_carries_no_exposure() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:14556'\n\
             allow_unauthenticated_non_loopback = true\n",
        )
        .expect("config file");

        let Some(McpHttpServeConfig::AuthDisabled {
            expose_non_loopback,
            ..
        }) = McpHttpServeConfig::load(&layout).expect("MCP HTTP config")
        else {
            panic!("loopback MCP must resolve auth-disabled");
        };
        assert!(
            !expose_non_loopback,
            "a loopback bind must not carry consent it does not need"
        );
    }

    #[test]
    fn opted_in_auth_disabled_mcp_http_still_rejects_public_url() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\n\
             allow_unauthenticated_non_loopback = true\n\
             public_url = 'https://coral.example/mcp'\n",
        )
        .expect("config file");

        let error = McpHttpServeConfig::load(&layout)
            .expect_err("exposure consent must not unlock OAuth metadata");
        assert!(error.to_string().contains("OAuth metadata"));
    }

    #[test]
    fn mcp_http_exposure_opt_in_conflicts_with_auth() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\n\
             public_url = 'https://coral.example/mcp'\n\
             allow_unauthenticated_non_loopback = true\n",
        );

        let error = McpHttpServeConfig::load(&layout)
            .expect_err("a stale unauthenticated opt-in must not survive enabling [auth]");
        assert!(
            error
                .to_string()
                .contains("allow_unauthenticated_non_loopback has no effect")
        );
    }

    #[test]
    fn mcp_http_allowed_hosts_conflict_with_auth() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\n\
             public_url = 'https://coral.example/mcp'\nallowed_hosts = ['coral']\n",
        );

        let error = McpHttpServeConfig::load(&layout)
            .expect_err("authenticated hosts derive from public_url");
        assert!(error.to_string().contains("allowed_hosts"));
    }

    #[test]
    fn mcp_http_allowed_hosts_reject_pasted_urls() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nallowed_hosts = ['http://coral:14556']\n",
        )
        .expect("config file");

        let error =
            McpHttpServeConfig::load(&layout).expect_err("a URL is not a Host header value");
        assert!(error.to_string().contains("not a bare host"));
    }

    #[test]
    fn auth_disabled_mcp_http_rejects_public_url() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\npublic_url = 'https://coral.example/mcp'\n",
        )
        .expect("config file");

        McpHttpServeConfig::load(&layout)
            .expect_err("auth-disabled MCP must not advertise an authenticated public URL");
    }

    #[test]
    fn authenticated_companions_canonicalize_public_audiences_in_surface_order() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config_with_auth(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\npublic_url = 'https://MCP.example.test/'\n",
            "allowed_audiences = ['https://CORAL-UI.example.test/']",
        );

        let companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_settings()
            .expect("companions");
        assert_eq!(
            companions.mcp_http.as_ref(),
            Some(&McpHttpServeConfig::Authenticated {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                public_url: "https://mcp.example.test".to_string(),
                authorization_server: "https://auth.example.test".to_string(),
            })
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
            ["https://coral-ui.example.test".to_string()]
        );
        assert_eq!(
            session_auth
                .mcp_workspace_urls
                .as_ref()
                .expect("MCP workspace family")
                .base()
                .identifier(),
            "https://mcp.example.test"
        );
    }

    /// With MCP HTTP as the only public surface, the workspace family alone
    /// satisfies the at-least-one-surface requirement, and the base URL is not
    /// registered as an exact audience of its own.
    #[test]
    fn authenticated_mcp_only_config_serves_only_the_workspace_family() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test/'\n",
        );

        let companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_settings()
            .expect("MCP-only companions");
        let session_auth = companions.session_auth.expect("session auth");
        assert!(session_auth.public_audiences.is_empty());
        assert!(session_auth.mcp_workspace_urls.is_some());

        let authorization_server = session_auth
            .into_authorization_server()
            .expect("authorization server");
        assert!(
            authorization_server.authorization_resources().is_empty(),
            "the family is a template, not an enumerated resource — and the \
             base is not a resource at all"
        );
        assert!(
            authorization_server.serves_workspace_resource_family(),
            "the family must be registered even though it enumerates to nothing, \
             or every real MCP login is refused invalid_target"
        );
    }

    #[test]
    fn authenticated_coral_ui_only_config_registers_its_allowed_audience() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config_with_auth(
            &layout,
            "",
            "allowed_audiences = ['https://CORAL-UI.example.test/']",
        );

        let companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_settings()
            .expect("Coral UI-only companions");
        assert!(companions.mcp_http.is_none());
        let session_auth = companions.session_auth.expect("session auth");
        assert_eq!(
            session_auth.public_audiences,
            ["https://coral-ui.example.test".to_string()]
        );

        let authorization_server = session_auth
            .into_authorization_server()
            .expect("authorization server");
        assert_eq!(
            authorization_server.authorization_resources(),
            &["https://coral-ui.example.test".to_string()].into()
        );
    }

    #[test]
    fn allowed_audiences_reject_invalid_and_duplicate_entries() {
        let cases = [
            (
                "",
                "allowed_audiences = ['https://coral-ui.example.test/?tenant=one']",
                "auth.allowed_audiences[0] must not include a query",
            ),
            (
                "[server.mcp_http]\nenabled = true\npublic_url = 'https://MCP.example.test/'\n",
                "allowed_audiences = ['https://mcp.example.test']",
                "auth.allowed_audiences[0] duplicates server.mcp_http.public_url",
            ),
            // A non-root base with a trailing slash keeps that slash through
            // canonicalization, so the normalized base identifier every
            // per-workspace resource extends differs from the canonical base by
            // one slash. Naming that normalized form must be rejected too, or it
            // resurrects the base as a private-gRPC audience.
            (
                "[server.mcp_http]\nenabled = true\npublic_url = 'https://coral.example/mcp/'\n",
                "allowed_audiences = ['https://coral.example/mcp']",
                "auth.allowed_audiences[0] duplicates server.mcp_http.public_url",
            ),
            (
                "",
                "allowed_audiences = ['https://CORAL-UI.example.test/', 'https://coral-ui.example.test']",
                "auth.allowed_audiences[1] duplicates another configured public surface audience",
            ),
        ];

        for (mcp_http, auth_fields, expected) in cases {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
            write_authenticated_config_with_auth(&layout, mcp_http, auth_fields);

            let error = LoadedServerConfig::load(&layout)
                .expect("load")
                .companion_settings()
                .err()
                .expect("invalid audience must fail");
            assert!(error.to_string().contains(expected), "error: {error}");
        }
    }

    /// Parsing `[auth]` validates the section without touching what it points
    /// at, so an instance whose signing key is missing loads and only fails when
    /// the settings behind it are actually resolved.
    #[test]
    fn resolving_companion_settings_fails_on_a_missing_signing_key() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(&layout, "");
        fs::remove_file(layout.config_dir().join("session.key")).expect("remove session key");

        let config = LoadedServerConfig::load(&layout).expect("load");

        // `ServeSettings` carries key material and so implements no `Debug`,
        // which rules out `expect_err` here.
        assert!(
            config.companion_settings().is_err(),
            "resolution must still fail on the missing signing key"
        );
    }

    /// The private gRPC API has no resource identity of its own, so an
    /// authenticated instance needs at least one public surface for anything to
    /// be minted for. An empty allowlist behaves exactly like an absent one.
    #[test]
    fn configured_auth_requires_a_public_surface() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config_with_auth(&layout, "", "allowed_audiences = []");
        let error = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_settings()
            .err()
            .expect("auth without a public surface must fail");
        assert!(
            error
                .to_string()
                .contains("requires at least one public surface"),
            "error: {error}"
        );
    }

    #[test]
    fn authenticated_mcp_http_requires_a_public_url() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n",
        );
        let error = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_settings()
            .err()
            .expect("missing public URL must fail");
        assert!(error.to_string().contains("server.mcp_http.public_url"));
    }

    /// Binding a public surface off loopback is the operator's call — the risk is
    /// documented rather than gated — so this must resolve without ceremony. The
    /// auth-disabled loopback restriction is a separate rule and still applies.
    #[test]
    fn authenticated_mcp_http_accepts_a_remote_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\npublic_url = 'https://mcp.example.test/mcp'\n",
        );
        let companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_settings()
            .expect("a remote MCP bind is the operator's choice");
        assert!(matches!(
            companions.mcp_http.as_ref(),
            Some(&McpHttpServeConfig::Authenticated { .. })
        ));
    }

    #[test]
    fn legacy_mcp_oauth_keys_are_rejected() {
        for field in ["resource_url", "authorization_server", "scope"] {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
            layout.ensure().expect("config dir");
            fs::write(
                layout.config_file(),
                format!("[server.mcp_http]\nenabled = true\n{field} = 'legacy'\n"),
            )
            .expect("config file");
            McpHttpServeConfig::load(&layout).expect_err("legacy key must fail");
        }
    }

    #[test]
    fn stale_static_auth_config_is_rejected_by_both_views() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.auth]\ntoken_env = 'CORAL_SERVER_AUTH_TOKEN'\n",
        )
        .expect("config file");

        ServerSettings::load(&layout).expect_err("removed static auth config must fail");
        McpHttpServeConfig::load(&layout).expect_err("removed static auth config must fail");
    }
}
