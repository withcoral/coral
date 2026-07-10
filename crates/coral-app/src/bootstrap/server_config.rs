//! Configuration owned by the long-running Coral server surface.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;

use serde::Deserialize;
use serde::de::DeserializeOwned;
use url::{Host, Url};
use zeroize::Zeroizing;

use super::AppError;
use super::env::AppEnvironment;
use crate::auth::OidcAuthConfig;
use crate::auth::session::SessionTokenConfig;
use crate::identity::UserPrincipalProvider;
use crate::request_auth::SessionUserPrincipalProvider;
use crate::state::AppStateLayout;

const DEFAULT_MCP_SCOPE: &str = "coral:mcp";

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

    pub(crate) fn companion_config(&self) -> Result<ServeCompanionConfig, AppError> {
        let session = SessionTokenConfig::from_config_raw(&self.config_path, &self.raw)
            .map_err(AppError::FailedPrecondition)?;
        let oidc_auth =
            OidcAuthConfig::from_config_raw(&self.raw, session.clone(), &AppEnvironment::env_var)
                .map_err(AppError::FailedPrecondition)?;
        let session_principal_provider = session.clone().map(|session| {
            Arc::new(SessionUserPrincipalProvider::new(session)) as Arc<dyn UserPrincipalProvider>
        });
        let mcp_http = self.resolve_mcp_http(session.as_ref(), oidc_auth.as_ref())?;
        Ok(ServeCompanionConfig {
            mcp_http,
            oidc_auth,
            session_principal_provider,
        })
    }

    fn resolve_mcp_http(
        &self,
        session: Option<&SessionTokenConfig>,
        oauth: Option<&OidcAuthConfig>,
    ) -> Result<Option<McpHttpServeConfig>, AppError> {
        let settings = self.parse::<McpHttpConfigFile>()?.server;
        reject_removed_auth(settings.removed_auth.as_ref())?;
        settings.mcp_http.resolve(session, oauth)
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
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        LoadedServerConfig::load(layout)?.grpc_settings()
    }

    pub(crate) fn reject_removed_auth(layout: &AppStateLayout) -> Result<(), AppError> {
        LoadedServerConfig::load(layout)?.reject_removed_auth()
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
    resource_url: Option<String>,
    authorization_server: Option<String>,
    scope: String,
}

impl Default for RawMcpHttpSettings {
    fn default() -> Self {
        Self {
            enabled: false,
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            allow_insecure_remote_http_bind: false,
            resource_url: None,
            authorization_server: None,
            scope: DEFAULT_MCP_SCOPE.to_string(),
        }
    }
}

impl RawMcpHttpSettings {
    fn resolve(
        &self,
        session: Option<&SessionTokenConfig>,
        oauth: Option<&OidcAuthConfig>,
    ) -> Result<Option<McpHttpServeConfig>, AppError> {
        if !self.enabled {
            return Ok(None);
        }

        let Some(session) = session else {
            if !is_loopback(self.bind.ip()) {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http.bind must be loopback".to_string(),
                ));
            }
            if self.resource_url.is_some() || self.authorization_server.is_some() {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http must not advertise OAuth metadata".to_string(),
                ));
            }
            return Ok(Some(McpHttpServeConfig::AuthDisabled {
                bind_addr: self.bind,
            }));
        };

        if !is_loopback(self.bind.ip()) && !self.allow_insecure_remote_http_bind {
            return Err(AppError::FailedPrecondition(
                "non-loopback server.mcp_http.bind serves cleartext authenticated endpoints and requires server.mcp_http.allow_insecure_remote_http_bind = true"
                    .to_string(),
            ));
        }
        let resource_url =
            required_oauth_url("server.mcp_http.resource_url", self.resource_url.as_deref())?;
        let authorization_server = required_oauth_url(
            "server.mcp_http.authorization_server",
            self.authorization_server.as_deref(),
        )?;
        if self.scope.is_empty()
            || !self
                .scope
                .bytes()
                .all(|byte| matches!(byte, 33 | 35..=91 | 93..=126))
        {
            return Err(AppError::FailedPrecondition(
                "server.mcp_http.scope must be one printable OAuth scope token".to_string(),
            ));
        }

        require_url_match(
            "server.mcp_http.authorization_server",
            &authorization_server,
            session.issuer(),
        )?;
        require_url_match(
            "server.mcp_http.resource_url",
            &resource_url,
            session.audience(),
        )?;
        if let Some(oauth) = oauth {
            require_url_match(
                "server.mcp_http.authorization_server",
                &authorization_server,
                oauth.issuer(),
            )?;
            require_url_match(
                "server.mcp_http.resource_url",
                &resource_url,
                oauth.resource(),
            )?;
            if self.scope != oauth.scope() {
                return Err(AppError::FailedPrecondition(
                    "server.mcp_http.scope must match configured auth.oauth.scope".to_string(),
                ));
            }
        }

        Ok(Some(McpHttpServeConfig::Authenticated {
            bind_addr: self.bind,
            resource_url,
            authorization_server,
            scope: self.scope.clone(),
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
        /// OAuth protected-resource URL advertised to MCP clients.
        resource_url: String,
        /// OAuth authorization server advertised to MCP clients.
        authorization_server: String,
        /// One RFC 6749 scope token required by this resource.
        scope: String,
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

/// Configuration for companions that `coral-serve` starts beside gRPC.
#[derive(Clone)]
pub struct ServeCompanionConfig {
    mcp_http: Option<McpHttpServeConfig>,
    oidc_auth: Option<OidcAuthConfig>,
    session_principal_provider: Option<Arc<dyn UserPrincipalProvider>>,
}

impl ServeCompanionConfig {
    /// Returns the resolved MCP HTTP configuration, when enabled.
    #[must_use]
    pub fn mcp_http(&self) -> Option<&McpHttpServeConfig> {
        self.mcp_http.as_ref()
    }

    /// Returns the resolved OAuth authorization-server configuration, when enabled.
    #[must_use]
    pub fn oidc_auth(&self) -> Option<&OidcAuthConfig> {
        self.oidc_auth.as_ref()
    }

    /// Returns the session principal provider shared with the prepared gRPC server.
    #[must_use]
    pub fn session_principal_provider(&self) -> Option<Arc<dyn UserPrincipalProvider>> {
        self.session_principal_provider.as_ref().map(Arc::clone)
    }
}

fn require_url_match(label: &str, actual: &str, expected: &str) -> Result<(), AppError> {
    if Url::parse(actual).ok() != Url::parse(expected).ok() {
        return Err(AppError::FailedPrecondition(format!(
            "{label} must match configured served authentication"
        )));
    }
    Ok(())
}

fn required_oauth_url(label: &str, value: Option<&str>) -> Result<String, AppError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(AppError::FailedPrecondition(format!(
            "authenticated MCP HTTP requires {label}"
        )));
    };
    let url = Url::parse(value).map_err(|_error| unsafe_oauth_url(label))?;
    let loopback = match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => ip.is_loopback(),
        Some(Host::Ipv6(ip)) => is_loopback(IpAddr::V6(ip)),
        None => false,
    };
    if !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || (url.scheme() != "https" && !(url.scheme() == "http" && loopback))
    {
        return Err(unsafe_oauth_url(label));
    }
    Ok(url.to_string())
}

fn unsafe_oauth_url(label: &str) -> AppError {
    AppError::FailedPrecondition(format!(
        "{label} must be an absolute HTTPS or loopback HTTP URL without credentials, query, or fragment"
    ))
}

fn reject_removed_auth(value: Option<&toml::Value>) -> Result<(), AppError> {
    if value.is_some() {
        return Err(AppError::FailedPrecondition(
            "[server.auth] is no longer supported".to_string(),
        ));
    }
    Ok(())
}

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use tempfile::TempDir;

    use super::{McpHttpServeConfig, ServerSettings};
    use crate::state::AppStateLayout;

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
    fn auth_disabled_mcp_http_rejects_oauth_metadata() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nresource_url = 'https://coral.example/mcp'\n",
        )
        .expect("config file");

        McpHttpServeConfig::load(&layout)
            .expect_err("auth-disabled MCP must not advertise OAuth metadata");
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
