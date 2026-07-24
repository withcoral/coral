//! Parsing and validation for Coral authentication settings.

use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use url::{Host, Url};
use zeroize::Zeroizing;

use super::session::{SessionTokenError, SessionTokenIssuer};

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
const DEFAULT_SIGNING_KEY_ENV: &str = "CORAL_SESSION_SIGNING_KEY";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_hours(720);

/// Settings for the top-level `[auth]` configuration section.
///
/// [`AuthSettings::from_toml`] deserializes this type directly and validates it
/// before returning.
/// [`CoralAuthorizationServer::from_settings`](super::CoralAuthorizationServer::from_settings)
/// revalidates these settings at its own boundary. This type performs no
/// `config.toml` filesystem reads.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AuthSettings {
    #[serde(
        default = "default_bind_addr",
        deserialize_with = "deserialize_bind_addr"
    )]
    http_bind_addr: SocketAddr,
    #[serde(default)]
    allow_insecure_remote_http_bind: bool,
    session: SessionTokenSettings,
    authorization_server: AuthorizationServerSettings,
    #[serde(default)]
    providers: BTreeMap<String, OidcProviderSettings>,
}

impl AuthSettings {
    /// Deserializes and validates the auth settings from a whole `config.toml` snapshot.
    ///
    /// # Errors
    ///
    /// Returns an error when an auth-owned TOML field or cross-section
    /// relationship is invalid.
    pub fn from_toml(raw: &str) -> Result<Option<Self>, String> {
        let document: AuthConfigDocument =
            toml::from_str(raw).map_err(|error| config_error(error.message()))?;
        let Some(mut settings) = document.auth else {
            return Ok(None);
        };
        settings.validate()?;
        Ok(Some(settings))
    }

    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.http_bind_addr
    }

    pub(crate) fn authorization_server(&self) -> &AuthorizationServerSettings {
        &self.authorization_server
    }

    pub(super) fn resolve_session_token_issuer(
        mut self,
        config_path: &Path,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<(Self, SessionTokenIssuer), SessionTokenError> {
        self.validate()?;
        let key = self.session.load_signing_key(config_path, get_var)?;
        let issuer = SessionTokenIssuer::new(
            Some(&self.authorization_server.issuer),
            key.as_slice(),
            Duration::from_secs(self.session.access_token_ttl_seconds),
        )?;
        Ok((self, issuer))
    }

    pub(super) fn matches_session_token_issuer(&self, issuer: &SessionTokenIssuer) -> bool {
        self.authorization_server.issuer == issuer.issuer
            && Duration::from_secs(self.session.access_token_ttl_seconds) == issuer.access_token_ttl
    }

    pub(super) fn validate(&mut self) -> Result<(), String> {
        self.session.validate()?;
        self.authorization_server.validate()?;
        if self.providers.is_empty() {
            return Err(config_error(
                "auth.providers must configure at least one OIDC provider",
            ));
        }
        for (name, provider) in &mut self.providers {
            provider.validate(name)?;
        }
        if !is_loopback_ip(self.http_bind_addr.ip()) && !self.allow_insecure_remote_http_bind {
            return Err(config_error(
                "non-loopback auth.http_bind_addr serves cleartext OAuth endpoints and requires auth.allow_insecure_remote_http_bind = true",
            ));
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct AuthConfigDocument {
    auth: Option<AuthSettings>,
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SessionTokenSettings {
    signing_key_env: Option<String>,
    signing_key_file: Option<PathBuf>,
    access_token_ttl_seconds: u64,
}

impl Default for SessionTokenSettings {
    fn default() -> Self {
        Self {
            signing_key_env: None,
            signing_key_file: None,
            access_token_ttl_seconds: DEFAULT_TOKEN_TTL.as_secs(),
        }
    }
}

impl SessionTokenSettings {
    fn validate(&mut self) -> Result<(), String> {
        if self.access_token_ttl_seconds == 0 {
            return Err(session_config_error(
                "access_token_ttl_seconds must be greater than 0",
            ));
        }
        match (&mut self.signing_key_env, &self.signing_key_file) {
            (Some(_), Some(_)) => Err(session_config_error(
                "configure only one of signing_key_env or signing_key_file",
            )),
            (Some(env_name), None) => {
                *env_name = env_name.trim().to_string();
                if env_name.is_empty() {
                    Err(session_config_error("signing_key_env must not be empty"))
                } else {
                    Ok(())
                }
            }
            (None, _) => Ok(()),
        }
    }

    fn load_signing_key(
        &self,
        config_path: &Path,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<Zeroizing<Vec<u8>>, SessionTokenError> {
        match (&self.signing_key_env, &self.signing_key_file) {
            (Some(_), Some(_)) => Err(session_config_error(
                "configure only one of signing_key_env or signing_key_file",
            )),
            (None, Some(path)) => {
                let path = config_path.parent().unwrap_or(Path::new(".")).join(path);
                std::fs::read(&path)
                    .map(Zeroizing::new)
                    .map_err(|error| session_file_error("read signing_key_file", &path, &error))
            }
            (env_name, None) => {
                let env_name = env_name
                    .as_deref()
                    .unwrap_or(DEFAULT_SIGNING_KEY_ENV)
                    .trim();
                let value = get_var(env_name)
                    .map_err(|error| {
                        session_config_error(format!("failed to read `{env_name}`: {error}"))
                    })?
                    .ok_or_else(|| {
                        session_config_error(format!("env var `{env_name}` is not set"))
                    })?;
                let value = Zeroizing::new(value);
                BASE64_STANDARD
                    .decode(value.trim())
                    .map(Zeroizing::new)
                    .map_err(|_error| {
                        session_config_error(format!(
                            "env var `{env_name}` must contain a base64-encoded PKCS#8 P-256 private key"
                        ))
                    })
            }
        }
    }
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AuthorizationServerSettings {
    pub(super) issuer: String,
}

impl AuthorizationServerSettings {
    fn validate(&mut self) -> Result<(), String> {
        self.issuer = required("auth.authorization_server.issuer", &self.issuer)?;
        self.issuer = validate_issuer("auth.authorization_server.issuer", &self.issuer, true)?;
        Ok(())
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OidcProviderSettings {
    issuer: String,
    client_id: String,
    redirect_uri: String,
    client_secret: Option<SecretMarker>,
    client_secret_env: Option<SecretMarker>,
}

impl OidcProviderSettings {
    fn validate(&mut self, name: &str) -> Result<(), String> {
        if !valid_path_segment(name) {
            return Err(config_error(
                "auth.providers keys must be non-empty path segments",
            ));
        }
        if self.client_secret.is_some() == self.client_secret_env.is_some() {
            return Err(config_error(format!(
                "auth.providers.{name} must configure exactly one of client_secret or client_secret_env"
            )));
        }
        self.issuer = required(&format!("auth.providers.{name}.issuer"), &self.issuer)?;
        self.issuer = validate_issuer("OIDC provider issuer", &self.issuer, false)?;
        self.client_id = required(&format!("auth.providers.{name}.client_id"), &self.client_id)?;
        self.redirect_uri = required(
            &format!("auth.providers.{name}.redirect_uri"),
            &self.redirect_uri,
        )?;
        validate_endpoint("OIDC provider redirect URI", &self.redirect_uri)?;
        Ok(())
    }
}

struct SecretMarker;

impl<'de> Deserialize<'de> for SecretMarker {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || D::Error::custom("provider secret must be a nonempty string");
        let value = String::deserialize(deserializer)
            .map(Zeroizing::new)
            .map_err(|_error| invalid())?;
        (!value.trim().is_empty())
            .then_some(Self)
            .ok_or_else(invalid)
    }
}

fn default_bind_addr() -> SocketAddr {
    DEFAULT_BIND_ADDR
}

fn deserialize_bind_addr<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<SocketAddr, D::Error> {
    let value = String::deserialize(deserializer)?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(DEFAULT_BIND_ADDR);
    }
    value
        .parse()
        .map_err(|error| D::Error::custom(format!("auth.http_bind_addr is invalid: {error}")))
}

fn required(label: &str, value: &str) -> Result<String, String> {
    let value = value.trim();
    if value.is_empty() {
        Err(config_error(format!("{label} is required")))
    } else {
        Ok(value.to_string())
    }
}

fn valid_path_segment(value: &str) -> bool {
    !value.is_empty()
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'~'))
}

fn validate_issuer(label: &str, raw: &str, root_only: bool) -> Result<String, String> {
    let url = validate_endpoint(label, raw)?;
    if url.query().is_some() {
        return Err(config_error(format!("{label} must not include a query")));
    }
    if root_only && !matches!(url.path(), "" | "/") {
        return Err(config_error(format!("{label} must mount at the root")));
    }
    Ok(url.as_str().trim_end_matches('/').to_string())
}

fn validate_endpoint(label: &str, raw: &str) -> Result<Url, String> {
    let url = Url::parse(raw.trim())
        .map_err(|error| config_error(format!("{label} is not a valid URL: {error}")))?;
    if !url.username().is_empty() || url.password().is_some() {
        return Err(config_error(format!(
            "{label} must not include credentials"
        )));
    }
    if url.fragment().is_some() {
        return Err(config_error(format!("{label} must not include a fragment")));
    }
    let loopback = match url.host() {
        Some(Host::Domain(host)) => host.eq_ignore_ascii_case("localhost"),
        Some(Host::Ipv4(ip)) => is_loopback_ip(ip.into()),
        Some(Host::Ipv6(ip)) => is_loopback_ip(ip.into()),
        None => return Err(config_error(format!("{label} must include a host"))),
    };
    if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
        return Err(config_error(format!(
            "{label} requires https or loopback http"
        )));
    }
    Ok(url)
}

fn is_loopback_ip(ip: IpAddr) -> bool {
    ip.is_loopback()
        || matches!(ip, IpAddr::V6(ip) if ip.to_ipv4_mapped().is_some_and(|ip| ip.is_loopback()))
}

fn config_error(message: impl AsRef<str>) -> String {
    format!("invalid auth configuration: {}", message.as_ref())
}

fn session_config_error(message: impl AsRef<str>) -> String {
    format!("invalid auth.session configuration: {}", message.as_ref())
}

fn session_file_error(action: &str, path: &Path, error: &std::io::Error) -> String {
    session_config_error(format!("failed to {action} {}: {error}", path.display()))
}

pub(super) fn signing_key_env_error(error: &std::env::VarError) -> String {
    match error {
        std::env::VarError::NotPresent => "environment variable is not present".to_string(),
        std::env::VarError::NotUnicode(_) => "environment value is not valid UTF-8".to_string(),
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::OsString;
    use std::fs;

    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use ring::rand::SystemRandom;
    use ring::signature::{ECDSA_P256_SHA256_FIXED_SIGNING, EcdsaKeyPair};

    use super::*;

    const SESSION: &str = "[auth.session]\nsigning_key_file = 'session.key'\n";
    const AUTHORIZATION_SERVER: &str =
        "[auth.authorization_server]\nissuer = 'http://localhost:9080/'\n";
    const PROVIDER: &str = "[auth.providers.test]\nissuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\nclient_secret_env = 'UNREAD_ENV'\nredirect_uri = 'http://localhost:9080/auth/oidc/test/callback'\n";

    fn valid(extra: &str) -> String {
        format!("[auth]\n{SESSION}{AUTHORIZATION_SERVER}{extra}\n{PROVIDER}")
    }

    fn reject(raw: &str) -> String {
        match AuthSettings::from_toml(raw) {
            Err(error) => error,
            Ok(_) => panic!("expected invalid config"),
        }
    }

    #[test]
    fn directly_deserializes_and_validates_auth_settings() {
        let settings = AuthSettings::from_toml(&valid(""))
            .expect("valid config")
            .expect("auth settings");
        assert_eq!(settings.bind_addr(), DEFAULT_BIND_ADDR);
        assert_eq!(
            settings.authorization_server().issuer,
            "http://localhost:9080"
        );

        assert!(AuthSettings::from_toml("unowned = true").unwrap().is_none());
    }

    #[test]
    fn rejects_invalid_auth_section_relationships() {
        let cases = vec![
            ("[auth]\n".to_string(), "session"),
            (
                format!("[auth]\n{SESSION}{PROVIDER}"),
                "authorization_server",
            ),
            (
                format!("[auth]\n{SESSION}{AUTHORIZATION_SERVER}"),
                "must configure at least one",
            ),
            (
                valid("").replace(
                    "issuer = 'http://localhost:9080/'",
                    "issuer = 'http://localhost:9080/nested'",
                ),
                "must mount at the root",
            ),
            (
                valid("").replace("[auth.authorization_server]", "[auth.oauth]"),
                "oauth",
            ),
            (
                valid("").replace(
                    "signing_key_file = 'session.key'",
                    "signing_key_file = 'session.key'\nissuer = 'removed'",
                ),
                "issuer",
            ),
            (
                valid("").replace(
                    "signing_key_file = 'session.key'",
                    "signing_key_file = 'session.key'\naudience = 'removed'",
                ),
                "audience",
            ),
            (
                valid("").replace(
                    "client_secret_env = 'UNREAD_ENV'",
                    "client_secret_env = 'UNREAD_ENV'\nclient_secret = 'inline'",
                ),
                "exactly one",
            ),
            (
                valid("").replace("client_secret_env = 'UNREAD_ENV'\n", ""),
                "exactly one",
            ),
        ];
        for (raw, expected) in cases {
            assert!(
                reject(&raw).contains(expected),
                "expected `{expected}` for {raw}"
            );
        }
    }

    #[test]
    fn resolves_session_key_sources_after_parsing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("config.toml");
        let key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        fs::write(temp.path().join("session.key"), key.as_ref()).expect("session key");
        let settings = AuthSettings::from_toml(&valid(""))
            .expect("valid config")
            .expect("auth settings");
        let (settings, issuer) = settings
            .resolve_session_token_issuer(&config_path, &|_| Ok(None))
            .expect("file key");
        assert!(settings.matches_session_token_issuer(&issuer));
        assert_eq!(issuer.issuer, "http://localhost:9080");

        let encoded = BASE64_STANDARD.encode(key.as_ref());
        let raw = valid("").replace(
            "signing_key_file = 'session.key'",
            "signing_key_env = 'SESSION_KEY'",
        );
        let settings = AuthSettings::from_toml(&raw)
            .expect("valid config")
            .expect("auth settings");
        settings
            .resolve_session_token_issuer(&config_path, &|name| {
                Ok((name == "SESSION_KEY").then(|| encoded.clone()))
            })
            .expect("environment key");
    }

    #[test]
    fn session_key_sources_fail_closed_without_leaking_secrets() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("config.toml");
        let key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        let encoded_key = BASE64_STANDARD.encode(key.as_ref());

        let default_env = valid("").replace("signing_key_file = 'session.key'\n", "");
        let settings = AuthSettings::from_toml(&default_env)
            .expect("valid config")
            .expect("auth settings");
        settings
            .resolve_session_token_issuer(&config_path, &|name| {
                Ok((name == DEFAULT_SIGNING_KEY_ENV).then(|| encoded_key.clone()))
            })
            .expect("default environment key");

        for (replacement, expected) in [
            (
                "signing_key_file = 'session.key'\nsigning_key_env = 'SESSION_KEY'",
                "configure only one",
            ),
            ("signing_key_env = ''", "signing_key_env must not be empty"),
            ("access_token_ttl_seconds = 0", "greater than 0"),
        ] {
            let raw = valid("").replace("signing_key_file = 'session.key'", replacement);
            assert!(reject(&raw).contains(expected));
        }

        let Err(missing_file) = AuthSettings::from_toml(&valid(""))
            .expect("valid config")
            .expect("auth settings")
            .resolve_session_token_issuer(&config_path, &|_| Ok(None))
        else {
            panic!("expected missing signing key file");
        };
        assert!(missing_file.contains(&temp.path().join("session.key").display().to_string()));

        let env_config = valid("").replace(
            "signing_key_file = 'session.key'",
            "signing_key_env = 'SESSION_KEY'",
        );
        let settings = AuthSettings::from_toml(&env_config)
            .expect("valid config")
            .expect("auth settings");
        let Err(invalid_base64) = settings.resolve_session_token_issuer(&config_path, &|_| {
            Ok(Some("visible-secret".to_string()))
        }) else {
            panic!("expected invalid environment key");
        };
        assert!(!invalid_base64.contains("visible-secret"));

        let inline_key = valid("").replace(
            "signing_key_file = 'session.key'",
            "signing_key = 'visible-secret'",
        );
        assert!(!reject(&inline_key).contains("visible-secret"));
    }

    #[test]
    fn configuration_errors_do_not_leak_secrets() {
        for secret in ["client_secret_env = ''", "client_secret = 42"] {
            let raw = valid("").replace("client_secret_env = 'UNREAD_ENV'", secret);
            let error = reject(&raw);
            assert!(error.contains("provider secret must be a nonempty string"));
            assert!(!error.contains("42"));
        }
        let malformed = "[auth]\nclient_secret = 'SUPER_SECRET' trailing-garbage\n";
        assert!(!reject(malformed).contains("SUPER_SECRET"));
        validate_endpoint("test URL", "http://[::ffff:127.0.0.1]/").expect("mapped loopback");
    }

    #[cfg(unix)]
    #[test]
    fn non_unicode_environment_errors_do_not_leak_values() {
        use std::os::unix::ffi::OsStringExt as _;

        let secret = b"visible-secret-\xff-tail".to_vec();
        let error = signing_key_env_error(&std::env::VarError::NotUnicode(OsString::from_vec(
            secret.clone(),
        )));
        assert!(!error.contains("visible-secret"));
        assert_eq!(error, "environment value is not valid UTF-8");
    }
}
