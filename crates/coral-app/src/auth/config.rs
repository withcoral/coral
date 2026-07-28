//! Parsing and validation for Coral authentication settings.

use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use url::Url;
use zeroize::Zeroizing;

use super::error::AuthServerError;
use super::session::SessionTokenIssuer;
use crate::bootstrap::is_loopback_ip;
use crate::outbound_url_policy::ConfiguredEndpointUrl;

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
const DEFAULT_SIGNING_KEY_ENV: &str = "CORAL_SESSION_SIGNING_KEY";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_hours(720);
const MAX_TOKEN_TTL: Duration = Duration::from_hours(24 * 365);
const CONFLICTING_KEY_SOURCES: &str = "configure only one of signing_key_env or signing_key_file";

/// Validated settings for the top-level `[auth]` configuration section.
///
/// [`AuthSettings::from_toml`] is the only constructor: the deserialize target
/// is the private `RawAuthSettings`, so an unvalidated value of this type
/// cannot be built outside this module — by the rest of the crate or by
/// external callers. Holding one is proof that validation ran, which is why
/// nothing downstream revalidates. This type performs no `config.toml`
/// filesystem reads.
///
/// The settings are read through [`ResolvedAuthSettings`], which
/// [`AuthSettings::resolve_runtime_dependencies`] produces once the secrets
/// the config only points at have actually been fetched.
pub struct AuthSettings(RawAuthSettings);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuthSettings {
    /// Address the authorization server's HTTP listener binds to.
    ///
    /// This address must serve [`AuthorizationServerSettings::issuer`]'s
    /// origin, either directly or through a TLS-terminating reverse proxy.
    /// Published metadata derives `authorization_endpoint` and `token_endpoint`
    /// from the issuer rather than from the bound address, so a bind that does
    /// not serve the issuer origin advertises endpoints nothing answers on.
    /// The relationship is deliberately not validated, because proxied
    /// deployments legitimately bind an address that shares no host or port
    /// with the issuer. Note that the default binds an ephemeral loopback port,
    /// so a fixed issuer can only be served through it in flows that discover
    /// the assigned port at runtime.
    #[serde(
        default = "default_bind_addr",
        deserialize_with = "deserialize_bind_addr"
    )]
    http_bind_addr: SocketAddr,
    #[serde(default)]
    allow_insecure_remote_http_bind: bool,
    session: SessionTokenSettings,
    authorization_server: AuthorizationServerSettings,
    provider: OidcProviderSettings,
}

impl AuthSettings {
    /// Deserializes and validates the auth settings from a whole `config.toml` snapshot.
    ///
    /// # Errors
    ///
    /// Returns [`AuthServerError::Config`] when an auth-owned TOML field or
    /// cross-section relationship is invalid.
    pub fn from_toml(raw: &str) -> Result<Option<Self>, AuthServerError> {
        let document: AuthConfigDocument =
            toml::from_str(raw).map_err(|error| config_error(error.message()))?;
        let Some(mut settings) = document.auth else {
            return Ok(None);
        };
        settings.validate()?;
        Ok(Some(Self(settings)))
    }

    /// Fetches the secrets the config only points at — the provider client
    /// secret and the session signing key — and builds the session-token
    /// issuer keyed by the latter.
    ///
    /// # Errors
    ///
    /// Returns [`AuthServerError::Config`] when a secret source cannot be read
    /// or the session key material is unusable.
    pub(super) fn resolve_runtime_dependencies(
        self,
        config_path: &Path,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<(ResolvedAuthSettings, SessionTokenIssuer), AuthServerError> {
        let client_secret = self.0.provider.resolve_secret(get_var)?;
        let key = self.0.session.load_signing_key(config_path, get_var)?;
        let access_token_ttl = Duration::from_secs(self.0.session.access_token_ttl_seconds);
        let session_tokens = SessionTokenIssuer::new(
            Some(&self.0.authorization_server.issuer),
            key.as_slice(),
            access_token_ttl,
        )
        .map_err(AuthServerError::Config)?;
        Ok((
            ResolvedAuthSettings {
                http_bind_addr: self.0.http_bind_addr,
                authorization_server: self.0.authorization_server,
                access_token_ttl,
                provider: ResolvedOidcProvider {
                    client_secret,
                    settings: self.0.provider,
                },
            },
            session_tokens,
        ))
    }
}

/// Validated auth settings whose runtime dependencies have been resolved.
///
/// [`AuthSettings::resolve_runtime_dependencies`] is the only constructor, so
/// holding one is proof that the provider secret was fetched from its source —
/// which is why reading it cannot fail and nothing rechecks runtime readiness.
pub(crate) struct ResolvedAuthSettings {
    http_bind_addr: SocketAddr,
    authorization_server: AuthorizationServerSettings,
    access_token_ttl: Duration,
    provider: ResolvedOidcProvider,
}

impl ResolvedAuthSettings {
    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.http_bind_addr
    }

    pub(crate) fn authorization_server(&self) -> &AuthorizationServerSettings {
        &self.authorization_server
    }

    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the OIDC federation descendant")
    )]
    pub(super) fn provider(&self) -> &ResolvedOidcProvider {
        &self.provider
    }

    pub(super) fn matches_session_token_issuer(&self, issuer: &SessionTokenIssuer) -> bool {
        self.authorization_server.issuer == issuer.issuer
            && self.access_token_ttl == issuer.access_token_ttl
    }
}

impl RawAuthSettings {
    fn validate(&mut self) -> Result<(), AuthServerError> {
        self.session.validate()?;
        self.authorization_server.validate()?;
        self.provider.validate(&self.authorization_server.issuer)?;
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
    auth: Option<RawAuthSettings>,
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
    fn validate(&mut self) -> Result<(), AuthServerError> {
        if self.access_token_ttl_seconds == 0 {
            return Err(session_config_error(
                "access_token_ttl_seconds must be greater than 0",
            ));
        }
        if self.access_token_ttl_seconds > MAX_TOKEN_TTL.as_secs() {
            return Err(session_config_error(format!(
                "access_token_ttl_seconds must not exceed {}",
                MAX_TOKEN_TTL.as_secs()
            )));
        }
        match (&mut self.signing_key_env, &self.signing_key_file) {
            (Some(_), Some(_)) => Err(session_config_error(CONFLICTING_KEY_SOURCES)),
            (Some(env_name), None) => {
                *env_name = env_name.trim().to_string();
                if env_name.is_empty() {
                    Err(session_config_error("signing_key_env must not be empty"))
                } else {
                    Ok(())
                }
            }
            // An empty path would otherwise reach `load_signing_key`, where
            // joining it onto the config directory yields that directory and
            // the read fails with EISDIR — naming the directory rather than
            // the blank setting that caused it.
            (None, Some(path)) if path.as_os_str().is_empty() => {
                Err(session_config_error("signing_key_file must not be empty"))
            }
            (None, _) => Ok(()),
        }
    }

    fn load_signing_key(
        &self,
        config_path: &Path,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<Zeroizing<Vec<u8>>, AuthServerError> {
        // Defense in depth: `validate` rejects this pairing before any call
        // reaches here, so this arm only guards a future caller that skips it.
        match (&self.signing_key_env, &self.signing_key_file) {
            (Some(_), Some(_)) => Err(session_config_error(CONFLICTING_KEY_SOURCES)),
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
                // Decode into a zeroizing buffer rather than taking the `Vec`
                // that `decode` allocates: a malformed value fails partway
                // through, and that buffer would be freed still holding the
                // leading plaintext bytes of the private key.
                let mut decoded = Zeroizing::new(Vec::new());
                BASE64_STANDARD
                    .decode_vec(value.trim(), &mut decoded)
                    .map_err(|_error| {
                        session_config_error(format!(
                            "env var `{env_name}` must contain a base64-encoded PKCS#8 P-256 private key"
                        ))
                    })?;
                Ok(decoded)
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
    fn validate(&mut self) -> Result<(), AuthServerError> {
        self.issuer = required("auth.authorization_server.issuer", &self.issuer)?;
        self.issuer = validate_issuer("auth.authorization_server.issuer", &self.issuer, true)?;
        Ok(())
    }
}

const DEFAULT_PROVIDER_SCOPES: &[&str] = &["openid", "email", "profile"];
const RESERVED_PROVIDER_AUTH_PARAMS: &[&str] = &[
    "response_type",
    // `fragment` and `form_post` both stop the authorization code from ever
    // reaching the GET callback route, so a login started with either simply
    // never completes and leaves nothing in this server's logs.
    "response_mode",
    "client_id",
    "redirect_uri",
    "scope",
    "state",
    "nonce",
    "code_challenge",
    "code_challenge_method",
];

/// Validated settings for one upstream OIDC provider.
///
/// The derived `Debug` is safe to print: the only secret-bearing field is a
/// [`ProviderSecret`], which redacts itself.
#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(super) struct OidcProviderSettings {
    #[serde(rename = "type")]
    provider_type: Option<String>,
    pub(super) issuer: String,
    pub(super) client_id: String,
    client_secret: Option<ProviderSecret>,
    client_secret_env: Option<String>,
    pub(super) redirect_uri: String,
    pub(super) scopes: Vec<String>,
    pub(super) principal_claim: String,
    pub(super) display_name_claim: String,
    pub(super) auth_params: BTreeMap<String, String>,
    pub(super) required_claims: BTreeMap<String, Value>,
}

impl OidcProviderSettings {
    fn validate(&mut self, authorization_server_issuer: &str) -> Result<(), AuthServerError> {
        if self.client_secret.is_some() == self.client_secret_env.is_some() {
            return Err(config_error(
                "auth.provider must configure exactly one of client_secret or client_secret_env",
            ));
        }
        if let Some(provider_type) = &mut self.provider_type {
            *provider_type = provider_type.trim().to_string();
            if provider_type != "oidc" {
                return Err(invalid_provider("type must be `oidc`"));
            }
        }

        self.issuer = provider_required("issuer", &self.issuer)?;
        let issuer = provider_endpoint("issuer", &self.issuer)?;
        if issuer.as_url().query().is_some() {
            return Err(invalid_provider("issuer must not include a query"));
        }
        validate_canonical_issuer(&self.issuer, &issuer)?;
        self.client_id = provider_required("client_id", &self.client_id)?;
        self.redirect_uri = provider_required("redirect_uri", &self.redirect_uri)?;
        let redirect_uri = provider_endpoint("redirect_uri", &self.redirect_uri)?;
        // The upstream IdP sends the browser back to a callback route under
        // this server's origin, so a redirect URI pointing anywhere else can
        // only fail later, at the IdP or in the browser — far from Coral's own
        // logs.
        let served_origin = Url::parse(authorization_server_issuer)
            .map_err(|error| {
                config_error(format!(
                    "auth.authorization_server.issuer is not a valid URL: {error}"
                ))
            })?
            .origin();
        if redirect_uri.as_url().origin() != served_origin {
            return Err(invalid_provider(format!(
                "redirect_uri must share the origin of auth.authorization_server.issuer ({})",
                served_origin.ascii_serialization()
            )));
        }

        if let Some(secret) = &mut self.client_secret {
            *secret = ProviderSecret::from_trimmed("client_secret", secret.as_str())?;
        }
        if let Some(env_name) = &mut self.client_secret_env {
            *env_name = env_name.trim().to_string();
            if env_name.is_empty() {
                return Err(invalid_provider(
                    "client_secret_env must be a nonempty string",
                ));
            }
            if env_name.bytes().any(|byte| matches!(byte, b'=' | b'\0')) {
                return Err(invalid_provider("client_secret_env is invalid"));
            }
        }

        if self.scopes.is_empty() {
            self.scopes = DEFAULT_PROVIDER_SCOPES
                .iter()
                .map(ToString::to_string)
                .collect();
        }
        if self.scopes.iter().any(|scope| !valid_scope_token(scope)) {
            return Err(invalid_provider(
                "scopes must contain valid OAuth scope tokens",
            ));
        }
        if !self.scopes.iter().any(|scope| scope == "openid") {
            return Err(invalid_provider("scopes must include `openid`"));
        }

        self.principal_claim = provider_claim("principal_claim", &self.principal_claim, "sub")?;
        self.display_name_claim =
            provider_claim("display_name_claim", &self.display_name_claim, "email")?;
        for key in self.required_claims.keys() {
            validate_provider_key("required_claims", key)?;
        }
        for key in self.auth_params.keys() {
            validate_provider_key("auth_params", key)?;
            if RESERVED_PROVIDER_AUTH_PARAMS
                .iter()
                .any(|reserved| key.eq_ignore_ascii_case(reserved))
            {
                return Err(invalid_provider(format!(
                    "auth_params must not include reserved parameter `{key}`"
                )));
            }
        }
        Ok(())
    }

    /// Reads the client secret from whichever source the config names.
    fn resolve_secret(
        &self,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<ProviderSecret, AuthServerError> {
        match (&self.client_secret, &self.client_secret_env) {
            (Some(secret), _) => Ok(secret.clone()),
            (None, Some(env_name)) => {
                let value = Zeroizing::new(
                    get_var(env_name)
                        .map_err(|_error| invalid_provider("client_secret_env could not be read"))?
                        .ok_or_else(|| {
                            invalid_provider(format!(
                                "client_secret_env names `{env_name}`, which is not set"
                            ))
                        })?,
                );
                ProviderSecret::from_trimmed("client_secret_env", &value)
            }
            // Defense in depth: `validate` requires exactly one source before
            // any call reaches here, so this arm only guards a future caller
            // that skips it.
            (None, None) => Err(invalid_provider(
                "client_secret or client_secret_env is required",
            )),
        }
    }
}

/// Provider settings whose client secret has been read from its source.
///
/// [`OidcProviderSettings::resolve_secret`] is the only way to build one, so
/// holding one is proof the secret is present. The configured fields are
/// reached through [`Deref`]; only the secret needs an accessor, because it is
/// the one value the config may merely have pointed at.
#[derive(Clone, Debug)]
pub(super) struct ResolvedOidcProvider {
    settings: OidcProviderSettings,
    client_secret: ProviderSecret,
}

impl ResolvedOidcProvider {
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "used by the OIDC federation descendant")
    )]
    pub(super) fn client_secret(&self) -> &str {
        self.client_secret.as_str()
    }
}

impl Deref for ResolvedOidcProvider {
    type Target = OidcProviderSettings;

    fn deref(&self) -> &Self::Target {
        &self.settings
    }
}

#[derive(Clone)]
struct ProviderSecret(Arc<Zeroizing<String>>);

impl ProviderSecret {
    /// Trims a secret read from `field`, which names it in any error.
    fn from_trimmed(field: &str, value: &str) -> Result<Self, AuthServerError> {
        let value = value.trim();
        if value.is_empty() {
            Err(invalid_provider(format!("{field} must not be empty")))
        } else {
            Ok(Self(Arc::new(Zeroizing::new(value.to_string()))))
        }
    }

    fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for ProviderSecret {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let invalid = || D::Error::custom("provider secret must be a nonempty string");
        let value = String::deserialize(deserializer)
            .map(Zeroizing::new)
            .map_err(|_error| invalid())?;
        if value.trim().is_empty() {
            return Err(invalid());
        }
        Ok(Self(Arc::new(value)))
    }
}

impl fmt::Debug for ProviderSecret {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("<redacted>")
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

fn required(label: &str, value: &str) -> Result<String, AuthServerError> {
    let value = value.trim();
    if value.is_empty() {
        Err(config_error(format!("{label} is required")))
    } else {
        Ok(value.to_string())
    }
}

fn provider_required(field: &str, value: &str) -> Result<String, AuthServerError> {
    let value = value.trim();
    if value.is_empty() {
        Err(invalid_provider(format!("{field} is required")))
    } else {
        Ok(value.to_string())
    }
}

fn provider_endpoint(field: &str, value: &str) -> Result<ConfiguredEndpointUrl, AuthServerError> {
    ConfiguredEndpointUrl::parse(value)
        .map_err(|error| invalid_provider(format!("{field} is invalid: {error}")))
}

/// Rejects a provider issuer the URL parser would rewrite.
///
/// The issuer is retained exactly as configured, because an `OpenID` Connect
/// discovery document's `issuer` must equal the configured one byte for byte.
/// Providers publish issuers both with a trailing slash and without one, so
/// both are accepted — the parser appends one only to a root-path URL. Any
/// other divergence from the parser's canonical serialization (a mixed-case
/// host, an explicit default port, dot segments, non-canonical percent
/// encoding) is a form no provider can publish, so accepting it would trade a
/// startup error for a discovery mismatch on every login.
fn validate_canonical_issuer(
    configured: &str,
    parsed: &ConfiguredEndpointUrl,
) -> Result<(), AuthServerError> {
    let canonical = parsed.as_url().as_str();
    if configured == canonical || format!("{configured}/") == canonical {
        return Ok(());
    }
    Err(invalid_provider(format!(
        "issuer must be copied exactly as the provider publishes it; `{configured}` is not a canonical URL (its canonical form is `{canonical}`)"
    )))
}

fn valid_scope_token(scope: &str) -> bool {
    !scope.is_empty()
        && scope
            .bytes()
            .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
}

fn provider_claim(field: &str, value: &str, default: &str) -> Result<String, AuthServerError> {
    let value = if value.trim().is_empty() {
        default
    } else {
        value.trim()
    };
    validate_provider_key(field, value)?;
    Ok(value.to_string())
}

/// Rejects a key that cannot name a claim or an authorization query parameter.
///
/// Interior whitespace matters as much as surrounding whitespace: a claim name
/// carrying it can never be found in an ID token, and validating it here is
/// what makes `provider_claim`'s startup guarantee real rather than reporting
/// it as a missing claim on every login.
fn validate_provider_key(field: &str, key: &str) -> Result<(), AuthServerError> {
    if key.is_empty() || key.chars().any(char::is_whitespace) {
        return Err(invalid_provider(format!(
            "{field} keys must be nonempty and contain no whitespace"
        )));
    }
    Ok(())
}

fn invalid_provider(message: impl fmt::Display) -> AuthServerError {
    AuthServerError::Config(format!(
        "invalid auth configuration: auth.provider.{message}"
    ))
}

fn validate_issuer(label: &str, raw: &str, root_only: bool) -> Result<String, AuthServerError> {
    let url = validate_endpoint(label, raw)?;
    if url.as_url().query().is_some() {
        return Err(config_error(format!("{label} must not include a query")));
    }
    if root_only && !matches!(url.as_url().path(), "" | "/") {
        return Err(config_error(format!("{label} must mount at the root")));
    }
    Ok(url.as_url().as_str().trim_end_matches('/').to_string())
}

fn validate_endpoint(label: &str, raw: &str) -> Result<ConfiguredEndpointUrl, AuthServerError> {
    ConfiguredEndpointUrl::parse(raw.trim())
        .map_err(|error| config_error(format!("{label} is invalid: {error}")))
}

fn config_error(message: impl AsRef<str>) -> AuthServerError {
    AuthServerError::Config(format!("invalid auth configuration: {}", message.as_ref()))
}

fn session_config_error(message: impl AsRef<str>) -> AuthServerError {
    AuthServerError::Config(format!(
        "invalid auth.session configuration: {}",
        message.as_ref()
    ))
}

fn session_file_error(action: &str, path: &Path, error: &std::io::Error) -> AuthServerError {
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
    use serde_json::json;

    use super::*;

    use crate::auth::test_config::{PROVIDER, SESSION};

    /// Local override of the shared fixture: the trailing slash proves
    /// `validate_issuer` normalizes it away, and the rejection table rewrites
    /// this exact string to build its nested-path case.
    const AUTHORIZATION_SERVER: &str =
        "[auth.authorization_server]\nissuer = 'http://localhost:9080/'\n";

    fn valid(extra: &str) -> String {
        format!("[auth]\n{SESSION}{AUTHORIZATION_SERVER}{extra}\n{PROVIDER}")
    }

    fn provider_issuer(issuer: &str) -> String {
        valid("").replace(
            "issuer = 'https://accounts.example.test'",
            &format!("issuer = '{issuer}'"),
        )
    }

    fn reject(raw: &str) -> String {
        match AuthSettings::from_toml(raw) {
            Err(error) => error.to_string(),
            Ok(_) => panic!("expected invalid config"),
        }
    }

    /// A config directory holding the session signing key the fixture names.
    fn signing_key_dir() -> tempfile::TempDir {
        let temp = tempfile::tempdir().expect("tempdir");
        let key =
            EcdsaKeyPair::generate_pkcs8(&ECDSA_P256_SHA256_FIXED_SIGNING, &SystemRandom::new())
                .expect("P-256 signing key");
        fs::write(temp.path().join("session.key"), key.as_ref()).expect("session key");
        temp
    }

    fn parse(raw: &str) -> AuthSettings {
        AuthSettings::from_toml(raw)
            .expect("valid config")
            .expect("auth settings")
    }

    fn resolve(
        raw: &str,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> (ResolvedAuthSettings, SessionTokenIssuer) {
        let temp = signing_key_dir();
        parse(raw)
            .resolve_runtime_dependencies(&temp.path().join("config.toml"), get_var)
            .expect("resolved runtime dependencies")
    }

    fn resolve_error(
        raw: &str,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> String {
        let temp = signing_key_dir();
        let Err(error) =
            parse(raw).resolve_runtime_dependencies(&temp.path().join("config.toml"), get_var)
        else {
            panic!("expected unresolvable runtime dependencies");
        };
        error.to_string()
    }

    fn resolved(raw: &str) -> ResolvedAuthSettings {
        resolve(raw, &|_| Ok(None)).0
    }

    #[test]
    fn directly_deserializes_and_validates_auth_settings() {
        let settings = resolved(&valid(""));
        assert_eq!(settings.bind_addr(), DEFAULT_BIND_ADDR);
        assert_eq!(
            settings.authorization_server().issuer,
            "http://localhost:9080"
        );

        assert!(AuthSettings::from_toml("unowned = true").unwrap().is_none());
    }

    #[test]
    fn retains_validated_provider_values_and_secure_defaults() {
        let raw = valid("")
            .replace(
                "issuer = 'https://accounts.example.test'",
                "issuer = ' https://accounts.example.test/tenant/ '",
            )
            .replace("client_id = 'upstream-client'", "client_id = ' client-id '")
            .replace(
                "client_secret = 'provider-secret'",
                "client_secret = ' inline-secret '",
            )
            .replace(
                "redirect_uri = 'http://localhost:9080/auth/oidc/callback'",
                "redirect_uri = ' http://localhost:9080/callback '\nprincipal_claim = ' '\ndisplay_name_claim = ' email '",
            );
        let settings = resolved(&raw);
        let provider = settings.provider();
        assert_eq!(provider.issuer, "https://accounts.example.test/tenant/");
        assert_eq!(provider.client_id, "client-id");
        assert_eq!(provider.client_secret(), "inline-secret");
        assert_eq!(provider.redirect_uri, "http://localhost:9080/callback");
        assert_eq!(provider.scopes, ["openid", "email", "profile"]);
        assert_eq!(provider.principal_claim, "sub");
        assert_eq!(provider.display_name_claim, "email");
        let debug = format!("{provider:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("inline-secret"));
    }

    #[test]
    fn resolves_provider_env_secret_and_retains_options() {
        let raw = valid("")
            .replace(
                "client_secret = 'provider-secret'",
                "client_secret_env = ' PROVIDER_SECRET '",
            )
            .replace(
                "redirect_uri = 'http://localhost:9080/auth/oidc/callback'",
                "redirect_uri = 'http://localhost:9080/auth/oidc/callback'\nscopes = ['openid', 'groups']\nprincipal_claim = 'uid'\ndisplay_name_claim = 'name'",
            )
            + "\n[auth.provider.auth_params]\nprompt = 'select_account'\n\
               [auth.provider.required_claims]\nhd = 'example.test'\n";
        let (settings, _issuer) = resolve(&raw, &|name| {
            Ok((name == "PROVIDER_SECRET").then(|| " env-secret ".to_string()))
        });
        let provider = settings.provider();
        assert_eq!(provider.client_secret(), "env-secret");
        assert_eq!(provider.scopes, ["openid", "groups"]);
        assert_eq!(provider.principal_claim, "uid");
        assert_eq!(provider.display_name_claim, "name");
        assert_eq!(
            provider.auth_params.get("prompt").map(String::as_str),
            Some("select_account")
        );
        assert_eq!(
            provider.required_claims.get("hd"),
            Some(&json!("example.test"))
        );
        let debug = format!("{provider:?}");
        assert!(!debug.contains("env-secret"));
    }

    #[test]
    fn reports_provider_secret_sources_that_cannot_be_read() {
        let env_config = valid("").replace(
            "client_secret = 'provider-secret'",
            "client_secret_env = 'PROVIDER_SECRET'",
        );
        let unset = resolve_error(&env_config, &|_| Ok(None));
        assert!(
            unset.contains("client_secret_env names `PROVIDER_SECRET`, which is not set"),
            "{unset}"
        );
        let blank = resolve_error(&env_config, &|_| Ok(Some("   ".to_string())));
        assert!(
            blank.contains("client_secret_env must not be empty"),
            "{blank}"
        );
        let unreadable = resolve_error(&env_config, &|_| Err("host detail".to_string()));
        assert!(
            unreadable.contains("client_secret_env could not be read"),
            "{unreadable}"
        );
        assert!(!unreadable.contains("host detail"), "{unreadable}");

        let invalid_name = valid("").replace(
            "client_secret = 'provider-secret'",
            "client_secret_env = 'PROVIDER=SECRET'",
        );
        assert!(reject(&invalid_name).contains("client_secret_env is invalid"));
    }

    /// A provider issuer is compared byte for byte against the issuer in the
    /// discovery document, so every form a provider can publish has to survive
    /// validation unchanged — including both trailing-slash spellings.
    #[test]
    fn retains_every_issuer_spelling_a_provider_can_publish() {
        for issuer in [
            "https://accounts.example.test",
            "https://accounts.example.test/",
            "https://accounts.example.test/tenant",
            "https://accounts.example.test/tenant/",
            "http://127.0.0.1:9080/tenant/",
        ] {
            assert_eq!(resolved(&provider_issuer(issuer)).provider().issuer, issuer);
        }
    }

    #[test]
    fn rejects_provider_issuers_no_provider_could_publish() {
        for (issuer, canonical) in [
            (
                "https://ACCOUNTS.example.test/tenant",
                "https://accounts.example.test/tenant",
            ),
            (
                "https://accounts.example.test:443/tenant",
                "https://accounts.example.test/tenant",
            ),
            (
                "https://accounts.example.test/tenant/../other",
                "https://accounts.example.test/other",
            ),
        ] {
            let error = reject(&provider_issuer(issuer));
            assert!(
                error.contains("copied exactly as the provider publishes it"),
                "{error}"
            );
            assert!(error.contains(canonical), "{error}");
        }
    }

    #[test]
    fn rejects_invalid_provider_fields() {
        let cases = vec![
            (
                valid("").replace(
                    "issuer = 'https://accounts.example.test'",
                    "type = 'oauth'\nissuer = 'https://accounts.example.test'",
                ),
                "type must be `oidc`",
            ),
            (
                valid("").replace(
                    "https://accounts.example.test",
                    "http://accounts.example.test",
                ),
                "issuer is invalid: configured endpoint must use HTTPS",
            ),
            (
                valid("").replace(
                    "https://accounts.example.test",
                    "https://accounts.example.test?q=1",
                ),
                "issuer must not include a query",
            ),
            (
                valid("").replace(
                    "http://localhost:9080/auth/oidc/callback",
                    "https://remote.test/callback",
                ),
                "must share the origin",
            ),
            (
                valid("").replace(
                    "redirect_uri = 'http://localhost:9080/auth/oidc/callback'",
                    "redirect_uri = 'http://localhost:9080/auth/oidc/callback'\nscopes = ['email']",
                ),
                "scopes must include `openid`",
            ),
            (
                valid("").replace(
                    "redirect_uri = 'http://localhost:9080/auth/oidc/callback'",
                    "redirect_uri = 'http://localhost:9080/auth/oidc/callback'\nscopes = ['openid', 'two scopes']",
                ),
                "scopes must contain valid OAuth scope tokens",
            ),
            (
                valid("").replace(
                    "client_id = 'upstream-client'",
                    "client_id = 'upstream-client'\nprincipal_claim = 'user id'",
                ),
                "principal_claim keys must be nonempty and contain no whitespace",
            ),
            (
                valid("") + "\n[auth.provider.auth_params]\nCLIENT_ID = 'override'\n",
                "reserved parameter `CLIENT_ID`",
            ),
            (
                valid("") + "\n[auth.provider.auth_params]\nresponse_mode = 'fragment'\n",
                "reserved parameter `response_mode`",
            ),
            (
                valid("") + "\n[auth.provider.required_claims]\n' spaced ' = true\n",
                "required_claims keys must be nonempty and contain no whitespace",
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
    fn rejects_invalid_auth_section_relationships() {
        let cases = vec![
            ("[auth]\n".to_string(), "session"),
            (
                format!("[auth]\n{SESSION}{PROVIDER}"),
                "authorization_server",
            ),
            (
                format!("[auth]\n{SESSION}{AUTHORIZATION_SERVER}"),
                "missing field `provider`",
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
                valid("").replace("[auth.provider]", "[auth.providers.test]"),
                "unknown field `providers`",
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
                    "client_secret = 'provider-secret'",
                    "client_secret = 'provider-secret'\nclient_secret_env = 'PROVIDER_SECRET'",
                ),
                "exactly one",
            ),
            (
                valid("").replace("client_secret = 'provider-secret'\n", ""),
                "exactly one",
            ),
            (
                valid("").replace(
                    "redirect_uri = 'http://localhost:9080/auth/oidc/callback'",
                    "redirect_uri = 'https://elsewhere.example/auth/oidc/callback'",
                ),
                "must share the origin",
            ),
            (
                valid("").replace(
                    "redirect_uri = 'http://localhost:9080/auth/oidc/callback'",
                    "redirect_uri = 'http://localhost:9081/auth/oidc/callback'",
                ),
                "must share the origin",
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
            .resolve_runtime_dependencies(&config_path, &|_| Ok(None))
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
            .resolve_runtime_dependencies(&config_path, &|name| {
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
            .resolve_runtime_dependencies(&config_path, &|name| {
                Ok((name == DEFAULT_SIGNING_KEY_ENV).then(|| encoded_key.clone()))
            })
            .expect("default environment key");

        for (replacement, expected) in [
            (
                "signing_key_file = 'session.key'\nsigning_key_env = 'SESSION_KEY'",
                "configure only one",
            ),
            ("signing_key_env = ''", "signing_key_env must not be empty"),
            (
                "signing_key_file = ''",
                "signing_key_file must not be empty",
            ),
            ("access_token_ttl_seconds = 0", "greater than 0"),
            (
                "access_token_ttl_seconds = 18446744073709551615",
                "must not exceed",
            ),
            ("access_token_ttl_seconds = 31536001", "must not exceed"),
        ] {
            let raw = valid("").replace("signing_key_file = 'session.key'", replacement);
            assert!(reject(&raw).contains(expected));
        }

        let Err(missing_file) = AuthSettings::from_toml(&valid(""))
            .expect("valid config")
            .expect("auth settings")
            .resolve_runtime_dependencies(&config_path, &|_| Ok(None))
        else {
            panic!("expected missing signing key file");
        };
        assert!(
            missing_file
                .to_string()
                .contains(&temp.path().join("session.key").display().to_string())
        );

        let env_config = valid("").replace(
            "signing_key_file = 'session.key'",
            "signing_key_env = 'SESSION_KEY'",
        );
        let settings = AuthSettings::from_toml(&env_config)
            .expect("valid config")
            .expect("auth settings");
        let Err(invalid_base64) = settings.resolve_runtime_dependencies(&config_path, &|_| {
            Ok(Some("visible-secret".to_string()))
        }) else {
            panic!("expected invalid environment key");
        };
        assert!(!invalid_base64.to_string().contains("visible-secret"));

        let inline_key = valid("").replace(
            "signing_key_file = 'session.key'",
            "signing_key = 'visible-secret'",
        );
        assert!(!reject(&inline_key).contains("visible-secret"));
    }

    #[test]
    fn configuration_errors_do_not_leak_secrets() {
        for (secret, expected) in [
            (
                "client_secret_env = ''",
                "client_secret_env must be a nonempty string",
            ),
            (
                "client_secret = 42",
                "provider secret must be a nonempty string",
            ),
        ] {
            let raw = valid("").replace("client_secret = 'provider-secret'", secret);
            let error = reject(&raw);
            assert!(error.contains(expected), "{error}");
            assert!(!error.contains("42"), "{error}");
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
