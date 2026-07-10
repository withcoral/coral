//! Configuration owned by the long-running Coral server surface.

use std::net::{Ipv4Addr, SocketAddr};
use std::sync::Arc;

use serde::Deserialize;
use zeroize::Zeroizing;

use super::AppError;
use super::env::AppEnvironment;
use crate::request_auth::{AuthValidator, StaticTokenValidator};
use crate::state::AppStateLayout;

const DEFAULT_TOKEN_ENV: &str = "CORAL_SERVER_AUTH_TOKEN";

#[derive(Debug, Default, Deserialize)]
struct ConfigFile {
    #[serde(default)]
    server: ServerSettings,
}

/// Settings loaded from the top-level `[server]` section of `config.toml`.
#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub(crate) struct ServerSettings {
    pub(crate) bind_addr: SocketAddr,
    auth: Option<ServerAuthSettings>,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            auth: None,
        }
    }
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct ServerAuthSettings {
    token_env: String,
}

impl Default for ServerAuthSettings {
    fn default() -> Self {
        Self {
            token_env: DEFAULT_TOKEN_ENV.to_string(),
        }
    }
}

impl ServerSettings {
    /// Loads only the `[server]` section, leaving other config ownership intact.
    pub(crate) fn load(layout: &AppStateLayout) -> Result<Self, AppError> {
        if !layout.config_file().try_exists()? {
            return Ok(Self::default());
        }
        let raw = std::fs::read_to_string(layout.config_file())?;
        toml::from_str::<ConfigFile>(&raw)
            .map(|config| config.server)
            .map_err(|error| {
                AppError::FailedPrecondition(format!(
                    "server configuration is invalid: {}",
                    error.message()
                ))
            })
    }
    pub(crate) fn auth_validator(&self) -> Result<Option<Arc<dyn AuthValidator>>, AppError> {
        self.auth_validator_with(AppEnvironment::env_var)
    }

    fn auth_validator_with(
        &self,
        read_env: impl FnOnce(&str) -> Result<Option<String>, std::env::VarError>,
    ) -> Result<Option<Arc<dyn AuthValidator>>, AppError> {
        let Some(auth) = &self.auth else {
            return Ok(None);
        };
        let token_env = auth.token_env.trim();
        if token_env.is_empty() || token_env.bytes().any(|byte| matches!(byte, b'=' | b'\0')) {
            return Err(AppError::FailedPrecondition(
                "server.auth.token_env must name a valid environment variable".to_string(),
            ));
        }
        let token = Zeroizing::new(
            read_env(token_env)
                .map_err(|_error| missing_token_error(token_env))?
                .ok_or_else(|| missing_token_error(token_env))?,
        );
        let validator = StaticTokenValidator::new(token.trim())
            .ok_or_else(|| missing_token_error(token_env))?;
        Ok(Some(Arc::new(validator)))
    }
}

fn missing_token_error(token_env: &str) -> AppError {
    AppError::FailedPrecondition(format!(
        "server authentication requires environment variable `{token_env}` to contain a nonempty printable ASCII token"
    ))
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use tempfile::TempDir;

    use super::{DEFAULT_TOKEN_ENV, ServerSettings};
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
    fn config_decode_errors_do_not_echo_secret_source_lines() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(
            layout.config_file(),
            "[server.auth]\ntoken = 'must-not-leak'\n",
        )
        .expect("config file");

        let error = ServerSettings::load(&layout).expect_err("malformed config must fail");
        assert!(!error.to_string().contains("must-not-leak"));
    }

    #[test]
    fn auth_section_defaults_token_env_and_builds_a_redacted_validator() {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().join("config"))).expect("layout");
        layout.ensure().expect("config dir");
        fs::write(layout.config_file(), "[server.auth]\n").expect("config file");
        let settings = ServerSettings::load(&layout).expect("server settings");

        let validator = settings
            .auth_validator_with(|name| {
                assert_eq!(name, DEFAULT_TOKEN_ENV);
                Ok(Some("  secret-._~+/=: \t".to_string()))
            })
            .expect("auth config")
            .expect("validator");

        assert!(validator.accepts_bearer("secret-._~+/=:"));
        assert!(!validator.accepts_bearer("other-value"));
    }

    #[test]
    fn invalid_auth_configuration_fails_startup_resolution_without_disclosure() {
        let settings = ServerSettings {
            bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            auth: Some(super::ServerAuthSettings {
                token_env: "  CUSTOM_TOKEN  ".to_string(),
            }),
        };

        for token_env in ["", "BAD=NAME", "BAD\0NAME"] {
            let mut invalid = settings.clone();
            invalid.auth.as_mut().expect("auth").token_env = token_env.to_string();
            let Err(error) = invalid.auth_validator_with(|_| panic!("must not read env")) else {
                panic!("invalid environment name must fail");
            };
            assert_eq!(
                error.to_string(),
                "failed precondition: server.auth.token_env must name a valid environment variable"
            );
        }

        let Err(error) = settings.auth_validator_with(|name| {
            assert_eq!(name, "CUSTOM_TOKEN");
            Err(std::env::VarError::NotUnicode("must-not-leak".into()))
        }) else {
            panic!("non-UTF8 token must fail");
        };
        assert!(!error.to_string().contains("must-not-leak"));

        for token in [" \t ", "two words", "tökén"] {
            let Err(error) = settings.auth_validator_with(|_| Ok(Some(token.to_string()))) else {
                panic!("invalid token must fail");
            };
            assert!(!error.to_string().contains(token));
        }
    }
}
