//! Configuration owned by the long-running Coral server surface.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use zeroize::Zeroizing;

use super::env::AppEnvironment;
use super::{AppError, is_loopback_ip};
use crate::auth::{AuthSettings, CoralAuthorizationServer};
use crate::identity::{BearerAuthenticator, PrincipalProvider};
use crate::oauth_resource::CanonicalOauthUrl;
use crate::request_auth::SessionPrincipalProvider;
use crate::state::AppStateLayout;

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

    pub(crate) fn reject_unprepared_auth(&self) -> Result<(), AppError> {
        if AuthSettings::from_toml(&self.raw)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?
            .is_some()
        {
            return Err(AppError::FailedPrecondition(
                "configured [auth] requires ServerBuilder::prepare_for_serve before starting standalone gRPC"
                    .to_string(),
            ));
        }
        Ok(())
    }

    pub(crate) fn companion_config(&self) -> Result<ServeCompanionConfig, AppError> {
        let auth_settings = AuthSettings::from_toml(&self.raw)
            .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        let Some(auth_settings) = auth_settings else {
            return Ok(ServeCompanionConfig {
                mcp_http: self.resolve_mcp_http(None)?,
                authorization_server: None,
                grpc_principal_provider: None,
                mcp_principal_provider: None,
            });
        };
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
        let grpc = self.grpc_settings()?;
        grpc.reject_unacknowledged_authenticated_bind()?;
        let mcp_http = self.resolve_mcp_http(Some(&authorization_server))?;
        let audience = authenticated_resource_identifier(&grpc, mcp_http.as_ref())?;
        // Two independently constructed providers that enforce the same policy
        // today. They are kept separate so the private gRPC API and MCP HTTP can
        // diverge later without one surface silently inheriting the other's
        // change.
        let grpc_principal_provider = Some(Arc::new(SessionPrincipalProvider::new(
            session.verifier(),
            audience.clone(),
        )) as Arc<dyn PrincipalProvider>);
        let mcp_principal_provider = Some(Arc::new(SessionPrincipalProvider::new(
            session.verifier(),
            audience.clone(),
        )) as Arc<dyn BearerAuthenticator>);
        let mut authorization_server =
            CoralAuthorizationServer::from_resolved_settings(auth_settings, session)
                .map_err(|error| AppError::FailedPrecondition(error.to_string()))?;
        authorization_server = authorization_server
            .with_authorization_resource(audience)
            .map_err(AppError::FailedPrecondition)?;
        let authorization_server = Some(authorization_server);
        Ok(ServeCompanionConfig {
            mcp_http,
            authorization_server,
            grpc_principal_provider,
            mcp_principal_provider,
        })
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
    /// Acknowledges that a non-loopback bind serves cleartext authenticated gRPC.
    allow_insecure_remote_grpc_bind: bool,
    /// Public URL identifying the gRPC API as an OAuth protected resource.
    public_url: Option<String>,
    #[serde(rename = "auth")]
    removed_auth: Option<toml::Value>,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            allow_insecure_remote_grpc_bind: false,
            public_url: None,
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

    /// Rejects an authenticated gRPC listener that would serve bearer tokens
    /// over cleartext without the operator acknowledging it.
    ///
    /// Coral terminates no TLS, so a non-loopback bind is plaintext h2c and the
    /// tokens on it can be read off the wire. This mirrors the acknowledgements
    /// the MCP HTTP and OAuth listeners already require.
    fn reject_unacknowledged_authenticated_bind(&self) -> Result<(), AppError> {
        if !is_loopback_ip(self.bind_addr.ip()) && !self.allow_insecure_remote_grpc_bind {
            return Err(AppError::FailedPrecondition(
                "non-loopback server.bind_addr serves cleartext authenticated gRPC and requires server.allow_insecure_remote_grpc_bind = true"
                    .to_string(),
            ));
        }
        Ok(())
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
    allow_insecure_remote_http_bind: bool,
    public_url: Option<String>,
}

impl Default for RawMcpHttpSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            allow_insecure_remote_http_bind: false,
            public_url: None,
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
            if !is_loopback_ip(self.bind.ip()) {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http.bind must be loopback".to_string(),
                ));
            }
            if self.public_url.is_some() {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http must not advertise OAuth metadata".to_string(),
                ));
            }
            return Ok(Some(McpHttpServeConfig::AuthDisabled {
                bind_addr: self.bind,
            }));
        };

        if !is_loopback_ip(self.bind.ip()) && !self.allow_insecure_remote_http_bind {
            return Err(AppError::FailedPrecondition(
                "non-loopback server.mcp_http.bind serves cleartext authenticated endpoints and requires server.mcp_http.allow_insecure_remote_http_bind = true"
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

/// Resolved MCP HTTP configuration prepared independently from gRPC startup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpHttpServeConfig {
    /// Loopback-only MCP HTTP backed by an unauthenticated local client.
    AuthDisabled {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
    },
    /// Session-authenticated MCP HTTP backed by per-session clients.
    Authenticated {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
        /// Canonical public MCP URL advertised to clients and used as JWT audience.
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
            Self::AuthDisabled { bind_addr } | Self::Authenticated { bind_addr, .. } => *bind_addr,
        }
    }

    #[cfg(test)]
    fn load(layout: &AppStateLayout) -> Result<Option<Self>, AppError> {
        Ok(LoadedServerConfig::load(layout)?
            .companion_config()?
            .mcp_http)
    }
}

/// Configuration for companions that `coral serve` starts beside gRPC.
pub struct ServeCompanionConfig {
    mcp_http: Option<McpHttpServeConfig>,
    authorization_server: Option<CoralAuthorizationServer>,
    grpc_principal_provider: Option<Arc<dyn PrincipalProvider>>,
    mcp_principal_provider: Option<Arc<dyn BearerAuthenticator>>,
}

impl ServeCompanionConfig {
    /// Returns the resolved MCP HTTP configuration, when enabled.
    #[must_use]
    pub fn mcp_http(&self) -> Option<&McpHttpServeConfig> {
        self.mcp_http.as_ref()
    }

    /// Takes the prepared Coral authorization server, when enabled.
    #[must_use]
    pub fn take_authorization_server(&mut self) -> Option<CoralAuthorizationServer> {
        self.authorization_server.take()
    }

    /// Returns the session policy the private gRPC API authenticates with.
    ///
    /// This is a separate value from [`Self::mcp_principal_provider`] that
    /// currently enforces the same audience; see `companion_config`.
    #[must_use]
    pub fn grpc_principal_provider(&self) -> Option<Arc<dyn PrincipalProvider>> {
        self.grpc_principal_provider.as_ref().map(Arc::clone)
    }

    /// Returns the session policy authenticated MCP HTTP admits tokens with.
    ///
    /// MCP HTTP holds the bearer token itself rather than gRPC metadata, so this
    /// is a [`BearerAuthenticator`] instead of a [`PrincipalProvider`].
    #[must_use]
    pub fn mcp_principal_provider(&self) -> Option<Arc<dyn BearerAuthenticator>> {
        self.mcp_principal_provider.as_ref().map(Arc::clone)
    }
}

/// Resolves the one resource identifier an authenticated instance uses.
///
/// The identifier is both the token audience the served surfaces accept and the
/// authorization resource the authorization server issues for. Configuring
/// `[auth]` protects gRPC whether or not MCP HTTP runs, so the gRPC API carries
/// its own `[server].public_url` for the MCP-disabled case, mirroring
/// `[server.mcp_http].public_url`. When MCP HTTP is enabled its public URL is
/// the identifier, so a config that sets both is a conflict rather than a silent
/// preference: one instance has one audience.
fn authenticated_resource_identifier(
    grpc: &ServerSettings,
    mcp_http: Option<&McpHttpServeConfig>,
) -> Result<String, AppError> {
    let mcp_audience = match mcp_http {
        Some(McpHttpServeConfig::Authenticated { public_url, .. }) => Some(public_url.clone()),
        _ => None,
    };
    match (grpc.public_url.as_deref(), mcp_audience) {
        (Some(_), Some(_)) => Err(AppError::FailedPrecondition(
            "configure only one of server.public_url or server.mcp_http.public_url: one instance has one token audience"
                .to_string(),
        )),
        (None, Some(mcp_audience)) => Ok(mcp_audience),
        (grpc_public_url @ Some(_), None) => required_oauth_url("server.public_url", grpc_public_url),
        (None, None) => Err(AppError::FailedPrecondition(
            "configured [auth] requires server.public_url, or an enabled server.mcp_http with public_url"
                .to_string(),
        )),
    }
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
        layout.ensure().expect("config dir");
        fs::write(layout.config_dir().join("session.key"), test_signing_key())
            .expect("session key");
        fs::write(
            layout.config_file(),
            format!(
                "{mcp_http}
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

        let Some(McpHttpServeConfig::AuthDisabled { bind_addr }) =
            McpHttpServeConfig::load(&layout).expect("MCP HTTP config")
        else {
            panic!("loopback MCP must be explicitly auth-disabled");
        };
        assert_eq!(bind_addr, SocketAddr::from((Ipv6Addr::LOCALHOST, 14556)));
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
    fn authenticated_companions_share_one_canonical_mcp_audience() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\npublic_url = 'https://MCP.example.test/'\n",
        );

        let mut companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .expect("companions");
        assert_eq!(
            companions.mcp_http(),
            Some(&McpHttpServeConfig::Authenticated {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                public_url: "https://mcp.example.test".to_string(),
                authorization_server: "https://auth.example.test".to_string(),
            })
        );
        // Two independently constructed providers enforcing the same audience
        // today, kept separate so the two surfaces can diverge later.
        assert!(companions.grpc_principal_provider().is_some());
        assert!(companions.mcp_principal_provider().is_some());
        assert!(companions.take_authorization_server().is_some());
    }

    #[test]
    fn authenticated_grpc_resolves_without_any_mcp_http_surface() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server]\npublic_url = 'https://GRPC.example.test/'\n",
        );

        let mut companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .expect("gRPC-only authentication must resolve");

        assert!(companions.mcp_http().is_none());
        assert!(companions.grpc_principal_provider().is_some());
        assert!(companions.take_authorization_server().is_some());
    }

    #[test]
    fn configured_auth_requires_exactly_one_resource_identifier() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(&layout, "");
        let error = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .err()
            .expect("auth without any resource identifier must fail");
        assert!(error.to_string().contains("requires server.public_url"));

        write_authenticated_config(
            &layout,
            "[server]\npublic_url = 'https://grpc.example.test'\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test'\n",
        );
        let error = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .err()
            .expect("two resource identifiers must fail");
        assert!(
            error
                .to_string()
                .contains("configure only one of server.public_url")
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
            .companion_config()
            .err()
            .expect("missing public URL must fail");
        assert!(error.to_string().contains("server.mcp_http.public_url"));
    }

    #[test]
    fn authenticated_mcp_http_requires_acknowledging_a_remote_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\npublic_url = 'https://mcp.example.test/mcp'\n",
        );
        let error = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .err()
            .expect("cleartext remote MCP bind must fail");
        assert!(
            error
                .to_string()
                .contains("server.mcp_http.allow_insecure_remote_http_bind = true"),
            "error: {error}"
        );

        write_authenticated_config(
            &layout,
            "[server.mcp_http]\nenabled = true\nbind = '0.0.0.0:14556'\nallow_insecure_remote_http_bind = true\npublic_url = 'https://mcp.example.test/mcp'\n",
        );
        let companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .expect("acknowledged remote MCP bind must resolve");
        assert!(matches!(
            companions.mcp_http(),
            Some(&McpHttpServeConfig::Authenticated { .. })
        ));
    }

    #[test]
    fn authenticated_grpc_requires_acknowledging_a_remote_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        write_authenticated_config(
            &layout,
            "[server]\nbind_addr = '0.0.0.0:14555'\npublic_url = 'https://grpc.example.test'\n",
        );
        let error = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .err()
            .expect("cleartext remote gRPC bind must fail");
        assert!(
            error
                .to_string()
                .contains("server.allow_insecure_remote_grpc_bind = true"),
            "error: {error}"
        );

        write_authenticated_config(
            &layout,
            "[server]\nbind_addr = '0.0.0.0:14555'\nallow_insecure_remote_grpc_bind = true\npublic_url = 'https://grpc.example.test'\n",
        );
        let companions = LoadedServerConfig::load(&layout)
            .expect("load")
            .companion_config()
            .expect("acknowledged remote gRPC bind must resolve");
        assert!(companions.grpc_principal_provider().is_some());
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
