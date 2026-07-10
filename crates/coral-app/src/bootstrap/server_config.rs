//! Configuration owned by the long-running Coral server surface.

use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use serde::Deserialize;
use url::{Host, Url};

use super::AppError;
use crate::state::AppStateLayout;

const DEFAULT_MCP_SCOPE: &str = "coral:mcp";

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    server: ServerSettings,
}

/// Settings loaded from the top-level `[server]` section of `config.toml`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct ServerSettings {
    pub(crate) bind_addr: SocketAddr,
    mcp_http: ServerMcpHttpSettings,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            mcp_http: ServerMcpHttpSettings::default(),
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct ServerMcpHttpSettings {
    enabled: bool,
    bind: SocketAddr,
    allow_insecure_remote_http_bind: bool,
    resource_url: Option<String>,
    authorization_server: Option<String>,
    scope: String,
}

impl Default for ServerMcpHttpSettings {
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

#[derive(Debug)]
pub(crate) enum ResolvedMcpHttpSettings {
    AuthDisabled {
        bind: SocketAddr,
    },
    Authenticated {
        bind: SocketAddr,
        resource_url: String,
        authorization_server: String,
        scope: String,
    },
}

impl ServerSettings {
    /// Loads only the `[server]` section, leaving other config ownership intact.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        if !layout.config_file().try_exists()? {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(layout.config_file())?;
        Ok(toml::from_str::<ConfigFile>(&raw)?.server)
    }

    pub(crate) fn mcp_http(
        &self,
        authenticated: bool,
    ) -> Result<Option<ResolvedMcpHttpSettings>, AppError> {
        if !self.mcp_http.enabled {
            return Ok(None);
        }

        let settings = &self.mcp_http;
        if !authenticated {
            if !is_loopback(settings.bind.ip()) {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http.bind must be loopback".to_string(),
                ));
            }
            if settings.resource_url.is_some() || settings.authorization_server.is_some() {
                return Err(AppError::FailedPrecondition(
                    "auth-disabled server.mcp_http must not advertise OAuth metadata".to_string(),
                ));
            }
            return Ok(Some(ResolvedMcpHttpSettings::AuthDisabled {
                bind: settings.bind,
            }));
        }

        if !is_loopback(settings.bind.ip()) && !settings.allow_insecure_remote_http_bind {
            return Err(AppError::FailedPrecondition(
                "non-loopback server.mcp_http.bind serves cleartext authenticated endpoints and requires server.mcp_http.allow_insecure_remote_http_bind = true"
                    .to_string(),
            ));
        }
        let resource_url = required_oauth_url(
            "server.mcp_http.resource_url",
            settings.resource_url.as_deref(),
        )?;
        let authorization_server = required_oauth_url(
            "server.mcp_http.authorization_server",
            settings.authorization_server.as_deref(),
        )?;
        let scope = settings.scope.as_str();
        if scope.is_empty()
            || !scope
                .bytes()
                .all(|byte| matches!(byte, 33 | 35..=91 | 93..=126))
        {
            return Err(AppError::FailedPrecondition(
                "server.mcp_http.scope must be one printable OAuth scope token".to_string(),
            ));
        }

        Ok(Some(ResolvedMcpHttpSettings::Authenticated {
            bind: settings.bind,
            resource_url,
            authorization_server,
            scope: scope.to_string(),
        }))
    }
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

fn is_loopback(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use tempfile::TempDir;

    use super::{ResolvedMcpHttpSettings, ServerSettings};
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
        assert!(settings.mcp_http(false).expect("MCP config").is_none());
    }

    #[test]
    fn loads_bind_address_without_claiming_other_sections() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server]\nbind_addr = '127.0.0.2:14555'\n\n[future]\nvalue = true\n",
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
    fn loads_enabled_auth_disabled_mcp_http_on_loopback() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.mcp_http]\nenabled = true\nbind = '[::1]:14556'\n",
        )
        .expect("config file");

        let settings = ServerSettings::load(&layout).expect("server settings");
        let Some(ResolvedMcpHttpSettings::AuthDisabled { bind }) =
            settings.mcp_http(false).expect("MCP config")
        else {
            panic!("loopback MCP must be explicitly auth-disabled");
        };
        assert_eq!(bind, SocketAddr::from((Ipv6Addr::LOCALHOST, 14556)));
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

        let settings = ServerSettings::load(&layout).expect("server settings");
        let error = settings
            .mcp_http(false)
            .expect_err("public auth-disabled bind must fail");
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

        let settings = ServerSettings::load(&layout).expect("server settings");
        let error = settings
            .mcp_http(false)
            .expect_err("auth-disabled MCP must not advertise OAuth metadata");
        assert!(error.to_string().contains("must not advertise"));
    }

    #[test]
    fn authenticated_mcp_http_requires_acknowledgement_for_public_bind() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            r"
[server.mcp_http]
enabled = true
bind = '0.0.0.0:14556'
resource_url = 'https://coral.example/mcp'
authorization_server = 'https://auth.example'
scope = 'coral:mcp'
",
        )
        .expect("config file");

        let mut settings = ServerSettings::load(&layout).expect("server settings");
        let error = settings
            .mcp_http(true)
            .expect_err("public cleartext bind must require acknowledgement");
        assert!(
            error
                .to_string()
                .contains("allow_insecure_remote_http_bind")
        );
        settings.mcp_http.allow_insecure_remote_http_bind = true;
        let Some(ResolvedMcpHttpSettings::Authenticated {
            bind,
            resource_url,
            authorization_server,
            scope,
        }) = settings.mcp_http(true).expect("MCP config")
        else {
            panic!("authenticated MCP settings must resolve");
        };
        assert_eq!(bind, SocketAddr::from((Ipv4Addr::UNSPECIFIED, 14556)));
        assert_eq!(resource_url, "https://coral.example/mcp");
        assert_eq!(authorization_server, "https://auth.example/");
        assert_eq!(scope, "coral:mcp");
    }

    #[test]
    fn authenticated_mcp_http_rejects_unsafe_metadata_urls_and_scopes() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            r"
[server.mcp_http]
enabled = true
resource_url = 'http://coral.example/mcp'
authorization_server = 'https://auth.example'
scope = 'two scopes'
",
        )
        .expect("config file");

        let settings = ServerSettings::load(&layout).expect("server settings");
        let error = settings
            .mcp_http(true)
            .expect_err("public HTTP OAuth metadata must fail");
        assert!(error.to_string().contains("absolute HTTPS"));
    }

    #[test]
    fn stale_static_auth_config_is_rejected() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.auth]\ntoken_env = 'CORAL_SERVER_AUTH_TOKEN'\n",
        )
        .expect("config file");

        ServerSettings::load(&layout).expect_err("removed static auth config must fail closed");
    }
}
