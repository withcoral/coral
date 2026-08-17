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
use crate::workspaces::WorkspaceName;

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
        let public_audiences = public_surface_audiences(mcp_http.as_ref(), &allowed_audiences)?;
        Ok(ServeSettings {
            mcp_http,
            session_auth: Some(SessionAuthSettings {
                settings: auth_settings,
                session_tokens: session,
                public_audiences,
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
    workspace: Option<String>,
}

impl Default for RawMcpHttpSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            public_url: None,
            allow_unauthenticated_non_loopback: false,
            allowed_hosts: Vec::new(),
            workspace: None,
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
                workspace: self.workspace()?,
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
            workspace: self.workspace()?,
        }))
    }

    /// Normalizes the configured workspace name without asking whether it
    /// exists.
    ///
    /// Existence is a request-time question answered against the caller's
    /// memberships, so checking it here would make startup depend on workspace
    /// state that may legitimately be created later.
    fn workspace(&self) -> Result<Option<String>, AppError> {
        self.workspace
            .as_deref()
            .map(|name| {
                WorkspaceName::parse(name)
                    .map(|name| name.as_str().to_string())
                    .map_err(|error| {
                        AppError::FailedPrecondition(format!("server.mcp_http.workspace: {error}"))
                    })
            })
            .transpose()
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
        /// Validated name of the workspace this local surface serves.
        ///
        /// `None` means the operator named none, which is distinct from any
        /// name they could have written: no default is substituted here, and
        /// the adapter resolves the sole local workspace instead. The name is
        /// only checked for shape — whether a workspace by that name exists is
        /// answered when a request needs it.
        workspace: Option<String>,
    },
    /// Session-authenticated MCP HTTP backed by per-session clients.
    Authenticated {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
        /// Canonical public MCP URL advertised to clients and used as JWT audience.
        public_url: String,
        /// OAuth authorization server advertised to MCP clients.
        authorization_server: String,
        /// Validated name of the shared workspace every session is bound to.
        ///
        /// `None` means the operator named none, which stays a valid server
        /// configuration: whether a session may proceed is answered per session
        /// against the caller's memberships, so refusing to start would deny an
        /// instance that is otherwise serving its other surfaces. Only the name's
        /// shape is checked here — existence is a request-time question.
        workspace: Option<String>,
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
}

impl SessionAuthSettings {
    /// The instance's public surfaces, canonicalized.
    #[must_use]
    pub fn public_audiences(&self) -> &[String] {
        &self.public_audiences
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

    /// Consumes these settings into the authorization server for the instance.
    ///
    /// Every public surface is registered as an authorization resource clients
    /// may request a token for.
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
        Ok(server)
    }
}

/// Collects the resource identifiers that front the instance's private API.
///
/// Each identifier is both the audience of tokens minted for a public surface
/// and the authorization resource clients name when requesting one. Some are
/// derived from a surface Coral serves, while others are explicitly registered
/// for external fronting surfaces such as a hosted UI BFF.
///
/// The gRPC API has no resource identity of its own and instead accepts every
/// identifier returned here. At least one is therefore required: with none, no
/// token can be minted for anything and every login would fail at authorization.
///
/// An identifier names a surface, not an actor: either kind of caller can arrive
/// through any of them, so nothing here says what kind a caller is. Actor kind
/// comes from the authenticated principal instead.
fn public_surface_audiences(
    mcp_http: Option<&McpHttpServeConfig>,
    allowed_audiences: &[String],
) -> Result<Vec<String>, AppError> {
    let mut audiences = match mcp_http {
        Some(McpHttpServeConfig::Authenticated { public_url, .. }) => vec![public_url.clone()],
        _ => Vec::new(),
    };
    for (index, configured) in allowed_audiences.iter().enumerate() {
        let label = format!("auth.allowed_audiences[{index}]");
        let audience = required_oauth_url(&label, Some(configured))?;
        if audiences.iter().any(|existing| existing == &audience) {
            return Err(AppError::FailedPrecondition(format!(
                "{label} duplicates another configured public surface audience"
            )));
        }
        audiences.push(audience);
    }
    if audiences.is_empty() {
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
            workspace,
        }) = McpHttpServeConfig::load(&layout).expect("MCP HTTP config")
        else {
            panic!("loopback MCP must be explicitly auth-disabled");
        };
        assert_eq!(bind_addr, SocketAddr::from((Ipv6Addr::LOCALHOST, 14556)));
        assert!(!expose_non_loopback);
        assert!(allowed_hosts.is_empty());
        assert_eq!(workspace, None);
    }

    /// An explicitly named workspace is normalized and carried through, and an
    /// absent one stays absent: no name is invented for the operator who wrote
    /// none, so the adapter can still tell "unnamed" from every real name.
    #[test]
    fn auth_disabled_workspace_carries_an_explicit_name_and_nothing_otherwise() {
        for (workspace_field, expected) in [
            ("", None),
            ("workspace = 'analytics'\n", Some("analytics")),
            // Whitespace is trimmed by the same parser every other workspace
            // name goes through, so configuration cannot name a workspace the
            // rest of the app could never match.
            ("workspace = '  analytics  '\n", Some("analytics")),
            // `default` carries no reserved status; it is an ordinary name that
            // resolves only if such a workspace actually exists.
            ("workspace = 'default'\n", Some("default")),
        ] {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
            layout.ensure().expect("config dir");
            fs::write(
                layout.config_file(),
                format!("[server.mcp_http]\nenabled = true\n{workspace_field}"),
            )
            .expect("config file");

            let Some(McpHttpServeConfig::AuthDisabled { workspace, .. }) =
                McpHttpServeConfig::load(&layout).expect("MCP HTTP config")
            else {
                panic!("loopback MCP must be explicitly auth-disabled");
            };
            assert_eq!(workspace.as_deref(), expected, "config: {workspace_field}");
        }
    }

    #[test]
    fn auth_disabled_workspace_rejects_an_unusable_name() {
        for invalid in ["", "   ", "..", "team/analytics"] {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
            layout.ensure().expect("config dir");
            fs::write(
                layout.config_file(),
                format!("[server.mcp_http]\nenabled = true\nworkspace = '{invalid}'\n"),
            )
            .expect("config file");

            let error = McpHttpServeConfig::load(&layout)
                .expect_err("an unusable workspace name must fail");
            assert!(
                error.to_string().contains("server.mcp_http.workspace"),
                "error: {error}"
            );
        }
    }

    /// An authenticated surface serves the one workspace its configuration
    /// names, so the name is carried through both modes by the same parser, and
    /// naming none stays absent rather than becoming a substituted default.
    #[test]
    fn authenticated_workspace_carries_an_explicit_name_and_nothing_otherwise() {
        for (workspace_field, expected) in [
            ("", None),
            ("workspace = 'analytics'\n", Some("analytics")),
            ("workspace = '  analytics  '\n", Some("analytics")),
            // `default` carries no reserved status on this surface either.
            ("workspace = 'default'\n", Some("default")),
        ] {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
            write_authenticated_config(
                &layout,
                &format!(
                    "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test/'\n{workspace_field}"
                ),
            );

            let companions = LoadedServerConfig::load(&layout)
                .expect("load")
                .companion_settings()
                .expect("a configured workspace is not a startup precondition");
            let Some(McpHttpServeConfig::Authenticated { workspace, .. }) =
                companions.mcp_http.as_ref()
            else {
                panic!("configured [auth] must produce an authenticated MCP surface");
            };
            assert_eq!(workspace.as_deref(), expected, "config: {workspace_field}");
        }
    }

    /// A name nothing could ever match is the operator's mistake, so it fails at
    /// startup in both modes — unlike a well-formed name for a workspace that
    /// does not exist yet, which stays a per-session question.
    #[test]
    fn authenticated_workspace_rejects_an_unusable_name() {
        for invalid in ["", "   ", "..", "team/analytics"] {
            let temp = TempDir::new().expect("temp dir");
            let layout =
                AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
            write_authenticated_config(
                &layout,
                &format!(
                    "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test/'\nworkspace = '{invalid}'\n"
                ),
            );

            let error = LoadedServerConfig::load(&layout)
                .expect("load")
                .companion_settings()
                .err()
                .expect("an unusable workspace name must fail");
            assert!(
                error.to_string().contains("server.mcp_http.workspace"),
                "error: {error}"
            );
        }
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
                workspace: None,
            })
        );
        // Resolution reports that session auth is configured; constructing the
        // providers and the authorization server from it is the composition
        // root's job, covered in `bootstrap::server`.
        let session_auth = companions.session_auth.expect("session auth");
        // Both surfaces front the private API, so both audiences are admitted;
        // neither says anything about what kind of actor arrives through it.
        assert_eq!(
            session_auth.public_audiences,
            [
                "https://mcp.example.test".to_string(),
                "https://coral-ui.example.test".to_string(),
            ]
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
                "auth.allowed_audiences[0] duplicates another configured public surface audience",
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
