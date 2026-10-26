//! Parsing and validation for Coral authentication settings.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
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

use super::OIDC_CALLBACK_PATH;
use super::error::AuthServerError;
use super::sealed_handshake::HandshakeKeys;
use crate::oauth_resource::CanonicalOauthUrl;
use coral_auth::SessionTokenIssuer;
use coral_outbound::{Configured, EndpointUrl};

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
const DEFAULT_SIGNING_KEY_ENV: &str = "CORAL_SESSION_SIGNING_KEY";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_hours(720);
const MAX_TOKEN_TTL: Duration = Duration::from_hours(24 * 365);
const CONFLICTING_KEY_SOURCES: &str = "configure only one of signing_key_env or signing_key_file";

/// Validated authentication policy and its auth/web listener identities.
///
/// [`AuthSettings::from_toml`] is the only constructor: the deserialize target
/// is the private `RawAuthSettings`, so an unvalidated value of this type
/// cannot be built outside this module — by the rest of the crate or by
/// external callers. Holding one is proof that validation ran, which is why
/// nothing downstream revalidates. This type performs no `config.toml`
/// filesystem reads.
///
/// The settings are read through the crate-internal `ResolvedAuthSettings`,
/// which `resolve_runtime_dependencies` produces once the secrets the config
/// only points at have actually been fetched.
pub struct AuthSettings {
    auth: RawAuthSettings,
    bind_addr: SocketAddr,
    authorization_server: AuthorizationServerSettings,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuthSettings {
    /// Additional public-surface resource identifiers registered with the
    /// authorization server, independent of which clients may skip consent.
    #[serde(default)]
    allowed_audiences: Vec<String>,
    #[serde(default)]
    trusted_clients: Vec<String>,
    session: SessionTokenSettings,
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
            crate::config_toml::decode(raw).map_err(|error| config_error(error.to_string()))?;
        let issuer = document.listeners.auth.validate(document.auth.is_some())?;
        let web_client = document
            .listeners
            .web
            .as_ref()
            .and_then(WebClient::from_settings);
        let Some(mut auth) = document.auth else {
            return Ok(None);
        };
        auth.validate()?;
        let issuer =
            issuer.ok_or_else(|| config_error("[auth] requires listeners.auth.public_url"))?;
        if let Some(web) = &web_client {
            // Do not count the derived registration as a duplicate of an
            // explicitly listed audience. Explicit duplicates still fail in
            // server bootstrap, and other audiences gain no client trust.
            let registered = auth.allowed_audiences.iter().any(|value| {
                CanonicalOauthUrl::parse(value).is_ok_and(|url| url.identifier() == web.public_url)
            });
            if !registered {
                auth.allowed_audiences.push(web.public_url.clone());
            }
        }
        let authorization_server = AuthorizationServerSettings {
            issuer,
            trusted_clients: std::mem::take(&mut auth.trusted_clients),
            web_client,
        };
        Ok(Some(Self {
            auth,
            bind_addr: document.listeners.auth.bind.unwrap_or(DEFAULT_BIND_ADDR),
            authorization_server,
        }))
    }

    /// Returns additional public-surface audiences, including the derived web
    /// resource when configured. Server bootstrap canonicalizes explicit entries
    /// alongside the other public-surface identifiers it composes.
    pub(crate) fn allowed_audiences(&self) -> &[String] {
        &self.auth.allowed_audiences
    }

    pub(crate) fn web_public_url(&self) -> Option<&str> {
        self.authorization_server
            .web_client
            .as_ref()
            .map(|web| web.public_url.as_str())
    }

    /// Fetches the secrets the config only points at — the provider client
    /// secret and the session signing key — and builds the session-token
    /// issuer keyed by the latter.
    ///
    /// # Errors
    ///
    /// Returns [`AuthServerError::Config`] when a secret source cannot be read
    /// or the session key material is unusable.
    pub(crate) fn resolve_runtime_dependencies(
        self,
        config_path: &Path,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<(ResolvedAuthSettings, SessionTokenIssuer), AuthServerError> {
        let provider = self
            .auth
            .provider
            .resolve(&self.authorization_server.issuer, get_var)?;
        let key = self.auth.session.load_signing_key(config_path, get_var)?;
        let access_token_ttl = Duration::from_secs(self.auth.session.access_token_ttl_seconds);
        let session_tokens = SessionTokenIssuer::new(
            Some(&self.authorization_server.issuer),
            key.as_slice(),
            access_token_ttl,
        )
        .map_err(AuthServerError::Config)?;
        let handshake_keys = HandshakeKeys::derive(key.as_slice())
            .map_err(|error| session_config_error(error.to_string()))?;
        Ok((
            ResolvedAuthSettings {
                bind_addr: self.bind_addr,
                authorization_server: self.authorization_server,
                access_token_ttl,
                provider,
                handshake_keys: Arc::new(handshake_keys),
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
    bind_addr: SocketAddr,
    authorization_server: AuthorizationServerSettings,
    access_token_ttl: Duration,
    provider: ResolvedOidcProvider,
    /// Seals sign-in state into browsers; derived from the session signing key.
    handshake_keys: Arc<HandshakeKeys>,
}

impl ResolvedAuthSettings {
    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    pub(crate) fn authorization_server(&self) -> &AuthorizationServerSettings {
        &self.authorization_server
    }

    pub(super) fn provider(&self) -> &ResolvedOidcProvider {
        &self.provider
    }

    pub(super) fn handshake_keys(&self) -> &HandshakeKeys {
        &self.handshake_keys
    }

    pub(crate) fn bootstrap_owner(&self) -> Option<(&str, &str)> {
        self.provider
            .bootstrap_owner_subject
            .as_deref()
            .map(|subject| (self.provider.issuer.as_str(), subject))
    }

    pub(super) fn matches_session_token_issuer(&self, issuer: &SessionTokenIssuer) -> bool {
        self.authorization_server.issuer == issuer.issuer()
            && self.access_token_ttl == issuer.access_token_ttl()
    }
}

impl RawAuthSettings {
    fn validate(&mut self) -> Result<(), AuthServerError> {
        self.session.validate()?;
        self.provider.validate()?;
        for client_id in &mut self.trusted_clients {
            *client_id = validate_trusted_client(client_id)?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
struct AuthConfigDocument {
    auth: Option<RawAuthSettings>,
    #[serde(default)]
    listeners: AuthOwnedListeners,
}

// Bootstrap validates the listener names and owns grpc/mcp configuration.
#[derive(Default, Deserialize)]
#[serde(default)]
struct AuthOwnedListeners {
    auth: AuthListenerSettings,
    web: Option<WebSettings>,
}

/// Auth-support bind and external identity. Bootstrap selects a local default
/// when the bind is absent; authenticated instances default to ephemeral loopback
/// and must supply a public URL. TLS can terminate at a proxy, so the public
/// origin need not match the bind address.
#[derive(Clone, Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct AuthListenerSettings {
    #[serde(deserialize_with = "deserialize_bind_addr")]
    pub(crate) bind: Option<SocketAddr>,
    public_url: Option<String>,
}

impl AuthListenerSettings {
    fn validate(&self, authenticated: bool) -> Result<Option<String>, AuthServerError> {
        if !authenticated {
            if self
                .bind
                .is_some_and(|bind| !crate::bootstrap::is_loopback_ip(bind.ip()))
            {
                return Err(config_error(
                    "auth-disabled listeners.auth.bind must be loopback",
                ));
            }
            if self.public_url.is_some() {
                return Err(config_error("listeners.auth.public_url requires [auth]"));
            }
            return Ok(None);
        }
        let issuer = required(
            "listeners.auth.public_url",
            self.public_url.as_deref().unwrap_or(""),
        )?;
        validate_issuer("listeners.auth.public_url", &issuer, true).map(Some)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct WebSettings {
    #[serde(deserialize_with = "deserialize_web_url")]
    pub(crate) public_url: coral_mcp::HumanInterfaceUrl,
}

fn deserialize_web_url<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<coral_mcp::HumanInterfaceUrl, D::Error> {
    let value = String::deserialize(deserializer)?;
    coral_mcp::HumanInterfaceUrl::parse(&value).map_err(D::Error::custom)
}

/// One operator-designated hosted web client, not a native Desktop client or
/// a caller-supplied client registration. HTTPS authenticates its endpoint;
/// plain loopback HTTP must not silently acquire consent-bypass privileges.
#[derive(Clone)]
pub(super) struct WebClient {
    pub(super) public_url: String,
    client_id: String,
    login_redirect_uri: String,
}

impl WebClient {
    fn from_settings(settings: &WebSettings) -> Option<Self> {
        // The typed destination already establishes URL safety and canonical
        // origin shape. Only HTTPS adds metadata identity and client trust;
        // loopback HTTP remains navigation with explicit OAuth registration.
        let public_url = settings.public_url.as_str();
        if !public_url.starts_with("https://") {
            return None;
        }
        let public_url = public_url.to_owned();
        Some(Self {
            client_id: format!("{public_url}/.well-known/oauth-client"),
            login_redirect_uri: format!("{public_url}/auth/callback"),
            public_url,
        })
    }
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
                *env_name = crate::bootstrap::env_var_name("signing_key_env", env_name)
                    .map_err(session_config_error)?;
                Ok(())
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

#[derive(Clone)]
pub(crate) struct AuthorizationServerSettings {
    pub(super) issuer: String,
    /// Client IDs whose authorization requests skip the approval page.
    ///
    /// A listed client's sign-in redirects straight to the upstream provider
    /// once it passes every other check — client metadata resolution, redirect
    /// registration, PKCE, and resource validation are unchanged. What an
    /// entry removes is the one step where a person sees who is asking before
    /// anything else happens, so it is an operator statement that this exact
    /// client needs no per-login confirmation. That statement should only be
    /// made for a client whose registered redirect URIs nothing else can
    /// serve: a loopback redirect is claimable by any local process, and the
    /// approval page is the only thing that would have shown the user which
    /// port the authorization code is about to be handed to.
    trusted_clients: Vec<String>,
    web_client: Option<WebClient>,
}

impl AuthorizationServerSettings {
    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Reports whether this request may skip the approval page: the web
    /// client signing a user in, or an explicitly trusted `client_id`.
    ///
    /// The comparison is exact string equality: a request's client ID is only
    /// accepted when it is the canonical serialization of the URL it names, and
    /// validation holds the configured entries to the same spelling, so one
    /// string is the whole identity on both sides.
    pub(super) fn is_trusted_client(&self, client_id: &str, redirect_uri: &str) -> bool {
        self.web_login(client_id, redirect_uri).is_some()
            || self
                .trusted_clients
                .iter()
                .any(|trusted| trusted == client_id)
    }

    /// The web identity signing a user in: its client ID returning to its
    /// login callback. Only that pairing skips consent, so the server pins its
    /// resource itself, independently of what the client metadata document
    /// advertises. The same identity returning anywhere else is an ordinary
    /// client request, validated against its metadata document and confirmed
    /// by the user: Reef presents it with its Provider Setup callback when a
    /// workspace adds another workspace's MCP server on the same deployment.
    pub(super) fn web_login(&self, client_id: &str, redirect_uri: &str) -> Option<&WebClient> {
        self.web_client
            .as_ref()
            .filter(|web| web.client_id == client_id && web.login_redirect_uri == redirect_uri)
    }

    /// The HTTPS origin of the web interface, when one is configured.
    pub(super) fn web_public_url(&self) -> Option<&str> {
        self.web_client.as_ref().map(|web| web.public_url.as_str())
    }

    /// Returns the trusted client IDs for the topology check at server
    /// construction, which is where the full client-ID policy can first run.
    pub(super) fn trusted_clients(&self) -> &[String] {
        &self.trusted_clients
    }
}

/// Rejects a trusted-client entry that could never match a request.
///
/// The metadata resolver only accepts a client ID that is the canonical
/// serialization of the URL it names, and the trusted match is exact string
/// equality against that canonical string. An entry the URL parser would
/// rewrite therefore cannot match any request this server accepts; failing at
/// startup names the misspelling, where the runtime symptom — the approval
/// page still appearing — points nowhere near it.
///
/// This owns only the spelling half of that guarantee. Whether a canonical
/// entry names a client the request-time policy can accept also depends on
/// the served topology — the issuer's scheme and the loopback IDs derived
/// from registered resources — and the resource set only exists at server
/// construction, so the resolver re-checks every entry under the real policy
/// there.
fn validate_trusted_client(configured: &str) -> Result<String, AuthServerError> {
    let entry_error = |message: String| config_error(format!("auth.trusted_clients {message}"));
    let value = configured.trim();
    if value.is_empty() {
        return Err(entry_error("entries must not be empty".to_string()));
    }
    let url = Url::parse(value)
        .map_err(|error| entry_error(format!("entry `{value}` is not a valid URL: {error}")))?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err(entry_error(format!(
            "entry `{value}` must use HTTP or HTTPS"
        )));
    }
    if url.as_str() != value {
        return Err(entry_error(format!(
            "entries must be canonical client IDs; `{value}` canonicalizes to `{}`",
            url.as_str()
        )));
    }
    Ok(value.to_string())
}

const DEFAULT_PROVIDER_SCOPES: &[&str] = &["openid", "email", "profile"];

/// Query parameters reserved to the authorization request.
///
/// No outside input may pre-set one. Operator-supplied `auth_params` are
/// checked against this list here, and the provider client checks the
/// `authorization_endpoint` it reads out of the discovery document against the
/// same list, so the two paths cannot drift apart and leave the less trusted
/// source — the discovery document — the more permissive one.
pub(super) const RESERVED_PROVIDER_AUTH_PARAMS: &[&str] = &[
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

/// Deserialize-and-validate target for the `[auth.provider]` section.
///
/// This shape exists only between parsing and
/// [`resolve`](Self::resolve): it is the one place a client secret and a
/// name pointing at one coexist, and it does not outlive the resolution that
/// settles which of them the provider actually uses.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OidcProviderSettings {
    issuer: String,
    client_id: String,
    client_secret: Option<ProviderSecret>,
    client_secret_env: Option<String>,
    scopes: Vec<String>,
    display_name_claim: String,
    name_claim: String,
    email_claim: String,
    email_verified_claim: String,
    bootstrap_owner_subject: Option<String>,
    auth_params: BTreeMap<String, String>,
    required_claims: BTreeMap<String, Value>,
    access_token: Option<AccessTokenAdmission>,
}

/// Optional login admission evidence from an upstream RS256 JWT access token.
/// It supplements, never replaces, the verified OIDC identity. The audience is
/// explicit because a provider's access-token audience need not be its OIDC
/// client ID. Keys come only from that provider's validated discovery JWKS.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct AccessTokenAdmission {
    pub(super) audience: String,
    #[serde(default)]
    pub(super) required_claims: BTreeMap<String, Value>,
}

impl OidcProviderSettings {
    fn validate(&mut self) -> Result<(), AuthServerError> {
        if self.client_secret.is_some() == self.client_secret_env.is_some() {
            return Err(config_error(
                "auth.provider must configure exactly one of client_secret or client_secret_env",
            ));
        }
        self.issuer = provider_required("issuer", &self.issuer)?;
        let issuer = provider_literal_endpoint("issuer", &self.issuer)?;
        if issuer.as_url().query().is_some() {
            return Err(invalid_provider("issuer must not include a query"));
        }
        validate_canonical_issuer(&self.issuer, &issuer)?;
        self.client_id = provider_required("client_id", &self.client_id)?;

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

        // An omitted `scopes` and an explicit `scopes = []` both land here, and
        // both take the defaults. Separating them would only let an empty list
        // report `scopes must include openid` instead, which is the same
        // startup failure for a request that could never authenticate.
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

        self.validate_identity_policy()?;
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

    fn validate_identity_policy(&mut self) -> Result<(), AuthServerError> {
        self.display_name_claim =
            provider_claim("display_name_claim", &self.display_name_claim, "email")?;
        self.name_claim = provider_claim("name_claim", &self.name_claim, "name")?;
        self.email_claim = provider_claim("email_claim", &self.email_claim, "email")?;
        self.email_verified_claim = provider_claim(
            "email_verified_claim",
            &self.email_verified_claim,
            "email_verified",
        )?;
        if self
            .bootstrap_owner_subject
            .as_deref()
            .is_some_and(|subject| {
                subject.len() > 255
                    || !subject.is_ascii()
                    || coral_workspace::UserId::parse(subject).is_err()
                    || subject
                        == coral_workspace::local_organization_owner()
                            .user_id()
                            .as_str()
            })
        {
            return Err(invalid_provider(
                "bootstrap_owner_subject must be a valid non-local OIDC subject",
            ));
        }
        for (key, value) in &self.required_claims {
            validate_provider_key("required_claims", key)?;
            validate_required_claim_value(key, value)?;
        }
        if let Some(admission) = &mut self.access_token {
            admission.audience = provider_required("access_token.audience", &admission.audience)?;
            for (key, value) in &admission.required_claims {
                validate_provider_key("access_token.required_claims", key)?;
                validate_required_claim_value(key, value)?;
            }
        }
        Ok(())
    }

    /// Reads the client secret from whichever source the config names and
    /// hands the validated fields to the provider that will serve logins.
    ///
    /// The fields move rather than copy, so the two ways of naming a secret do
    /// not survive into the resolved value alongside the secret itself.
    fn resolve(
        self,
        authorization_server_issuer: &str,
        get_var: &impl Fn(&str) -> Result<Option<String>, String>,
    ) -> Result<ResolvedOidcProvider, AuthServerError> {
        let client_secret = match (self.client_secret, self.client_secret_env) {
            (Some(secret), _) => secret,
            (None, Some(env_name)) => {
                let value = Zeroizing::new(
                    get_var(&env_name)
                        .map_err(|_error| invalid_provider("client_secret_env could not be read"))?
                        .ok_or_else(|| {
                            invalid_provider(format!(
                                "client_secret_env names `{env_name}`, which is not set"
                            ))
                        })?,
                );
                ProviderSecret::from_trimmed("client_secret_env", &value)?
            }
            // Defense in depth: `validate` requires exactly one source before
            // any call reaches here, so this arm only guards a future caller
            // that skips it.
            (None, None) => {
                return Err(invalid_provider(
                    "client_secret or client_secret_env is required",
                ));
            }
        };
        Ok(ResolvedOidcProvider {
            issuer: self.issuer,
            client_id: self.client_id,
            // The validated issuer is a canonical origin with no trailing slash.
            redirect_uri: format!("{authorization_server_issuer}{OIDC_CALLBACK_PATH}"),
            scopes: self.scopes,
            bootstrap_owner_subject: self.bootstrap_owner_subject,
            display_name_claim: self.display_name_claim,
            name_claim: self.name_claim,
            email_claim: self.email_claim,
            email_verified_claim: self.email_verified_claim,
            auth_params: self.auth_params,
            required_claims: self.required_claims,
            access_token: self.access_token,
            client_secret,
        })
    }
}

/// One upstream OIDC provider, validated and ready to serve logins.
///
/// [`OidcProviderSettings::resolve`] is the only way to build one, and it moves
/// the validated fields out of the configured shape, so this type carries no
/// `client_secret_env` still pointing at a secret and no empty `client_secret`
/// to disagree with the one it holds. Holding one is therefore proof the secret
/// was fetched, which is why reading it cannot fail.
///
/// The derived `Debug` is safe to print: the only secret-bearing field is a
/// [`ProviderSecret`], which redacts itself.
#[derive(Clone, Debug)]
pub(super) struct ResolvedOidcProvider {
    pub(super) issuer: String,
    pub(super) client_id: String,
    pub(super) redirect_uri: String,
    pub(super) scopes: Vec<String>,
    bootstrap_owner_subject: Option<String>,
    pub(super) display_name_claim: String,
    pub(super) name_claim: String,
    pub(super) email_claim: String,
    pub(super) email_verified_claim: String,
    pub(super) auth_params: BTreeMap<String, String>,
    pub(super) required_claims: BTreeMap<String, Value>,
    pub(super) access_token: Option<AccessTokenAdmission>,
    client_secret: ProviderSecret,
}

impl ResolvedOidcProvider {
    pub(super) fn client_secret(&self) -> &str {
        self.client_secret.as_str()
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

fn deserialize_bind_addr<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<SocketAddr>, D::Error> {
    let value = String::deserialize(deserializer)?;
    let value = value.trim();
    if value.is_empty() {
        return Ok(Some(DEFAULT_BIND_ADDR));
    }
    value.parse().map(Some).map_err(D::Error::custom)
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

/// Parses the provider issuer without silently repairing its configured spelling.
/// Discovery compares it byte for byte; syntax violations must fail at startup.
fn provider_literal_endpoint(
    field: &str,
    value: &str,
) -> Result<EndpointUrl<Configured>, AuthServerError> {
    let syntax_violation = Cell::new(None);
    let record_violation = |violation| syntax_violation.set(Some(violation));
    let url = Url::options()
        .syntax_violation_callback(Some(&record_violation))
        .parse(value)
        .map_err(|error| invalid_provider(format!("{field} is invalid: {error}")))?;
    if let Some(violation) = syntax_violation.get() {
        return Err(invalid_provider(format!("{field} is invalid: {violation}")));
    }
    EndpointUrl::<Configured>::from_parsed(url)
        .map_err(|error| invalid_provider(format!("{field} is invalid: {error}")))
}

/// Rejects a provider issuer the URL parser would rewrite.
///
/// The issuer is retained exactly as configured, because an `OpenID` Connect
/// discovery document's `issuer` must equal the configured one byte for byte.
/// `OpenID` Connect Discovery 1.0 §3 and RFC 8414 §2 both require a published
/// issuer to be a normalized URL, so a spelling the parser rewrites is one no
/// compliant provider can publish. Accepting it would not make it work: it
/// would trade a startup error for a discovery mismatch on every login.
///
/// Providers publish issuers both with a trailing slash and without one, so
/// both are accepted — the parser appends one only to a root-path URL.
fn validate_canonical_issuer(
    configured: &str,
    parsed: &EndpointUrl<Configured>,
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

/// Rejects a `required_claims` value no ID token could carry.
///
/// These values are held as JSON so the check against a decoded ID token is a
/// plain equality, but they are written in TOML, and the two languages do not
/// describe the same values. TOML has a datetime that JSON does not, and `toml`
/// hands one over as an object carrying a private marker key — accepted in
/// silence today, then unmatchable against every ID token a provider can send.
/// A date belongs in quotes, where it compares as the string the provider
/// actually sends.
///
/// Numbers are accepted as written and compared as written: `serde_json` holds
/// integers and floats apart, so `3` does not equal `3.0`. That distinction is
/// the provider's to make, not something this can settle at startup — the value
/// has to be spelled the way the ID token spells it.
fn validate_required_claim_value(key: &str, value: &Value) -> Result<(), AuthServerError> {
    let is_scalar =
        |value: &Value| matches!(value, Value::String(_) | Value::Bool(_) | Value::Number(_));
    let supported = match value {
        Value::Array(values) => values.iter().all(is_scalar),
        value => is_scalar(value),
    };
    if supported {
        Ok(())
    } else {
        Err(invalid_provider(format!(
            "required_claims `{key}` must be a string, boolean, number, or an array of those; quote a date or time so it is compared as the string the provider sends"
        )))
    }
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

fn validate_endpoint(label: &str, raw: &str) -> Result<EndpointUrl<Configured>, AuthServerError> {
    EndpointUrl::<Configured>::parse(raw.trim())
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

    use serde_json::json;

    use super::*;

    use crate::auth::test_config::{PROVIDER, SESSION};

    /// Local override of the shared fixture: the trailing slash proves
    /// `validate_issuer` normalizes it away, and the rejection table rewrites
    /// this exact string to build its nested-path case.
    const AUTHORIZATION_SERVER: &str = "[listeners.auth]\npublic_url = 'http://localhost:9080/'\n";

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
        let key = coral_test_support::p256_pkcs8();
        fs::write(temp.path().join("session.key"), &key).expect("session key");
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
    fn auth_and_web_listener_addresses_derive_oauth_identity() {
        let raw = format!(
            "{SESSION}{PROVIDER}\n\
            [listeners.auth]\nbind = '127.0.0.1:9081'\npublic_url = 'https://LOGIN.example.test:443/'\n\
            [listeners.web]\npublic_url = 'https://web.example.test'\n"
        );
        let settings = resolved(&raw);
        assert_eq!(settings.bind_addr(), "127.0.0.1:9081".parse().unwrap());
        assert_eq!(
            settings.authorization_server().issuer(),
            "https://login.example.test"
        );
        assert_eq!(
            settings.provider().redirect_uri,
            "https://login.example.test/auth/oidc/callback"
        );
        assert!(settings.authorization_server().is_trusted_client(
            "https://web.example.test/.well-known/oauth-client",
            "https://web.example.test/auth/callback"
        ));
    }

    #[test]
    fn local_auth_listener_requires_loopback_and_no_public_oauth_identity() {
        for (raw, expected) in [
            (
                "[listeners.auth]\nbind = '0.0.0.0:9080'",
                "auth-disabled listeners.auth.bind must be loopback",
            ),
            (
                "[listeners.auth]\npublic_url = 'https://login.example.test'",
                "listeners.auth.public_url requires [auth]",
            ),
            (
                "[listeners.auth]\nbind = 'not-an-address'",
                "listeners.auth.bind: invalid socket address syntax",
            ),
        ] {
            assert_eq!(
                reject(raw),
                format!("invalid auth configuration: {expected}"),
                "{raw}"
            );
        }
        assert!(
            AuthSettings::from_toml("[listeners.auth]\nbind = '127.0.0.1:9080'")
                .expect("local bind")
                .is_none()
        );
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
                "client_secret = ' inline-secret '",
                "client_secret = ' inline-secret '\ndisplay_name_claim = ' email '",
            );
        let settings = resolved(&raw);
        let provider = settings.provider();
        assert_eq!(provider.issuer, "https://accounts.example.test/tenant/");
        assert_eq!(provider.client_id, "client-id");
        assert_eq!(provider.client_secret(), "inline-secret");
        assert_eq!(
            provider.redirect_uri,
            "http://localhost:9080/auth/oidc/callback"
        );
        assert_eq!(provider.scopes, ["openid", "email", "profile"]);
        assert_eq!(provider.display_name_claim, "email");
        let debug = format!("{provider:?}");
        assert!(debug.contains("<redacted>"));
        assert!(!debug.contains("inline-secret"));
    }

    #[test]
    fn web_origin_derives_one_registered_and_trusted_first_party_client() {
        const WEB_CLIENT: &str = "https://coral.example.test/.well-known/oauth-client";
        const LOGIN_CALLBACK: &str = "https://coral.example.test/auth/callback";
        let raw = valid("") + "\n[listeners.web]\npublic_url = 'https://CORAL.example.test:443/'\n";
        let settings = parse(&raw);
        assert_eq!(settings.allowed_audiences(), ["https://coral.example.test"]);
        let resolved = resolved(&raw);
        let authorization = resolved.authorization_server();
        assert!(authorization.is_trusted_client(WEB_CLIENT, LOGIN_CALLBACK));
        assert_eq!(
            authorization
                .web_login(WEB_CLIENT, LOGIN_CALLBACK)
                .map(|web| web.public_url.as_str()),
            Some("https://coral.example.test")
        );
        for untrusted in [
            "https://coral.example.test/other-client",
            "https://coral.example.test/.well-known/oauth-client?other=1",
            "https://coral.example.test:8443/.well-known/oauth-client",
            "https://coral.example.test.attacker.test/.well-known/oauth-client",
            "http://localhost:9080/.well-known/oauth-client",
        ] {
            assert!(
                !authorization.is_trusted_client(untrusted, LOGIN_CALLBACK),
                "{untrusted}"
            );
        }
        // Trust is the sign-in pairing, not the client ID alone: the same
        // identity returning to its Provider Setup callback, or to any other
        // callback, is an ordinary client request.
        for callback in [
            "https://coral.example.test/provider-setup/oauth/callback",
            "https://coral.example.test/auth/callback/",
            "https://coral.example.test/auth/callback?next=1",
            "https://attacker.test/auth/callback",
        ] {
            assert!(
                !authorization.is_trusted_client(WEB_CLIENT, callback),
                "{callback}"
            );
            assert!(
                authorization.web_login(WEB_CLIENT, callback).is_none(),
                "{callback}"
            );
        }
    }

    #[test]
    fn web_configuration_rejects_ambiguous_or_non_loopback_plaintext_origins() {
        for public_url in [
            "",
            "http://coral.example.test",
            "coral-app://app",
            "https://coral.example.test/path",
            "https://coral.example.test?x=1",
            "https://coral.example.test/#fragment",
            "https://user:secret@coral.example.test",
        ] {
            let raw = format!("[listeners.web]\npublic_url = '{public_url}'");
            // Validate the table even without [auth]; Desktop simply omits it.
            let error = reject(&raw);
            assert!(
                error.starts_with(
                    "invalid auth configuration: listeners.web.public_url: must be a concrete HTTPS \
                     or explicit loopback HTTP origin"
                ),
                "{public_url}: {error}"
            );
            assert!(!error.contains("secret"), "{error}");
        }
        for (raw, expected) in [
            ("[listeners.web]", "missing field `public_url`"),
            (
                "[listeners.web]\npublic_url = 'https://coral.example.test'\nunknown = true",
                "unknown field `unknown`",
            ),
        ] {
            let error = reject(raw);
            assert!(error.contains(expected), "{raw}: {error}");
        }
        assert!(
            AuthSettings::from_toml("[listeners.grpc]\nbind = '127.0.0.1:0'")
                .expect("Desktop config")
                .is_none()
        );
    }

    #[test]
    fn loopback_web_navigation_does_not_register_an_audience_or_trust_a_client() {
        for origin in ["http://localhost:5173", "http://127.0.0.1:49152"] {
            let raw = valid("") + &format!("\n[listeners.web]\npublic_url = '{origin}'\n");
            assert!(parse(&raw).allowed_audiences().is_empty());
            assert!(!resolved(&raw).authorization_server().is_trusted_client(
                &format!("{origin}/.well-known/oauth-client"),
                &format!("{origin}/auth/callback")
            ));
        }
    }

    #[test]
    fn web_registration_deduplicates_only_its_derived_audience_and_trusts_no_others() {
        let raw = valid("").replace("[auth]\n", "[auth]\nallowed_audiences = ['https://CORAL.example.test/', 'https://other.example.test']\n")
            + "\n[listeners.web]\npublic_url = 'https://coral.example.test'\n";
        assert_eq!(parse(&raw).allowed_audiences().len(), 2);
        let settings = resolved(&raw);
        assert!(!settings.authorization_server().is_trusted_client(
            "https://other.example.test/.well-known/oauth-client",
            "https://other.example.test/auth/callback"
        ));
        assert!(!settings.authorization_server().is_trusted_client(
            "http://localhost:9080/.well-known/oauth-client",
            "http://localhost:9080/auth/callback"
        ));
    }

    #[test]
    fn trusted_clients_match_exactly_and_default_to_none() {
        // A listed client is trusted for whichever registered callback it
        // names; only the derived web client is held to one.
        const CALLBACKS: [&str; 2] = [
            "https://client.example.test/callback",
            "https://client.example.test/other/callback",
        ];
        let settings = resolved(&valid(""));
        for callback in CALLBACKS {
            assert!(
                !settings
                    .authorization_server()
                    .is_trusted_client("https://client.example.test/oauth/client.json", callback),
                "{callback}"
            );
        }

        let raw = valid("").replace(
            "[auth]\n",
            "[auth]\ntrusted_clients = [' https://client.example.test/oauth/client.json ', 'http://127.0.0.1:1457/.well-known/oauth-client']\n",
        );
        let settings = resolved(&raw);
        let authorization_server = settings.authorization_server();
        for (client, trusted) in [
            ("https://client.example.test/oauth/client.json", true),
            ("http://127.0.0.1:1457/.well-known/oauth-client", true),
            ("https://client.example.test/oauth/other.json", false),
        ] {
            for callback in CALLBACKS {
                assert_eq!(
                    authorization_server.is_trusted_client(client, callback),
                    trusted,
                    "{client} {callback}"
                );
            }
        }
    }

    /// A request's client ID is accepted only in its canonical spelling, so an
    /// entry the parser would rewrite can never match one — it has to fail at
    /// startup, where the error names the entry, rather than show the approval
    /// page forever.
    #[test]
    fn rejects_trusted_clients_that_could_never_match_a_request() {
        let with_trusted = |entry: &str| {
            valid("").replace(
                "[auth]\n",
                &format!("[auth]\ntrusted_clients = [{entry}]\n"),
            )
        };
        reject_all(vec![
            (
                with_trusted("''"),
                "trusted_clients entries must not be empty",
            ),
            (with_trusted("'not a url'"), "is not a valid URL"),
            (
                with_trusted("'https://Client.example.test/oauth/client.json'"),
                "canonicalizes to `https://client.example.test/oauth/client.json`",
            ),
            (
                with_trusted("'https://client.example.test:443/oauth/client.json'"),
                "canonicalizes to",
            ),
            (
                with_trusted(r"'https://client.example.test/oauth\client.json'"),
                "canonicalizes to",
            ),
            (
                with_trusted("'https://client.example.test'"),
                "canonicalizes to `https://client.example.test/`",
            ),
            (
                with_trusted("'file:///oauth/client.json'"),
                "must use HTTP or HTTPS",
            ),
        ]);
    }

    #[test]
    fn derives_callback_from_the_canonical_authorization_server_issuer() {
        for (issuer, callback) in [
            (
                "http://localhost:9080/",
                "http://localhost:9080/auth/oidc/callback",
            ),
            (
                "https://AUTH.example.test:443/",
                "https://auth.example.test/auth/oidc/callback",
            ),
            ("http://[::1]:9080", "http://[::1]:9080/auth/oidc/callback"),
        ] {
            let raw = valid("").replace("http://localhost:9080/", issuer);
            assert_eq!(resolved(&raw).provider().redirect_uri, callback);
        }
    }

    #[test]
    fn resolves_provider_env_secret_and_retains_options() {
        let raw = valid("")
            .replace(
                "client_secret = 'provider-secret'",
                "client_secret_env = ' PROVIDER_SECRET '",
            )
            .replace(
                "client_id = 'upstream-client'",
                "client_id = 'upstream-client'\nscopes = ['openid', 'groups']\ndisplay_name_claim = 'name'",
            )
            + "\n[auth.provider.auth_params]\nprompt = 'select_account'\n\
               [auth.provider.required_claims]\nhd = 'example.test'\n";
        let (settings, _issuer) = resolve(&raw, &|name| {
            Ok((name == "PROVIDER_SECRET").then(|| " env-secret ".to_string()))
        });
        let provider = settings.provider();
        assert_eq!(provider.client_secret(), "env-secret");
        assert_eq!(provider.scopes, ["openid", "groups"]);
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

    /// Discovery 1.0 §3 and RFC 8414 §2 require a published issuer to be a
    /// normalized URL, so these spellings can only ever mismatch the discovery
    /// document. Rejecting them at startup costs nothing a login would have
    /// kept.
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

    fn reject_all(cases: Vec<(String, &str)>) {
        for (raw, expected) in cases {
            assert!(
                reject(&raw).contains(expected),
                "expected `{expected}` for {raw}"
            );
        }
    }

    /// The provider issuer is compared exactly against discovery metadata.
    #[test]
    fn rejects_invalid_provider_endpoints() {
        reject_all(vec![
            (
                valid("").replace(
                    "https://accounts.example.test",
                    "http://accounts.example.test",
                ),
                "issuer is invalid: configured endpoint must use HTTPS",
            ),
            (
                provider_issuer(r"https://accounts.example.test\tenant"),
                "issuer is invalid: backslash",
            ),
            (
                provider_issuer("https://accounts.example.test/^"),
                "issuer is invalid: non-URL code point",
            ),
            (
                provider_issuer("https://accounts.example.test/%1"),
                "issuer is invalid: expected 2 hex digits after %",
            ),
            (
                valid("").replace(
                    "https://accounts.example.test",
                    "https://accounts.example.test?q=1",
                ),
                "issuer must not include a query",
            ),
        ]);
    }

    #[test]
    fn rejects_invalid_provider_fields() {
        reject_all(vec![
            (
                valid("").replace(
                    "client_id = 'upstream-client'",
                    "client_id = 'upstream-client'\nscopes = ['email']",
                ),
                "scopes must include `openid`",
            ),
            (
                valid("").replace(
                    "client_id = 'upstream-client'",
                    "client_id = 'upstream-client'\nscopes = ['openid', 'two scopes']",
                ),
                "scopes must contain valid OAuth scope tokens",
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
        ]);
    }

    /// A TOML datetime deserializes into a marker object rather than failing,
    /// so without this guard `expires = 2030-01-01` starts the server and then
    /// fails to match on every login.
    #[test]
    fn access_token_admission_is_opt_in_and_requires_an_explicit_audience() {
        assert!(resolved(&valid("")).provider().access_token.is_none());
        let raw = valid("")
            + "\n[auth.provider.access_token]\naudience = ' environment-client '\n\
            [auth.provider.access_token.required_claims]\norg_id = 'org_allowed'\n";
        let settings = resolved(&raw);
        let policy = settings
            .provider()
            .access_token
            .as_ref()
            .expect("access token policy");
        assert_eq!(policy.audience, "environment-client");
        assert_eq!(
            policy.required_claims.get("org_id"),
            Some(&Value::String("org_allowed".into()))
        );
        for suffix in [
            "[auth.provider.access_token]",
            "[auth.provider.access_token]\naudience = ''",
            "[auth.provider.access_token]\naudience = 'env'\nunknown = true",
            "[auth.provider.access_token]\naudience = 'env'\n[auth.provider.access_token.required_claims]\n' bad ' = true",
            "[auth.provider.access_token]\naudience = 'env'\n[auth.provider.access_token.required_claims]\norg_id = { value = 'org_allowed' }",
        ] {
            assert!(
                AuthSettings::from_toml(&(valid("") + "\n" + suffix)).is_err(),
                "{suffix}"
            );
        }
    }

    #[test]
    fn rejects_required_claims_no_id_token_could_carry() {
        reject_all(vec![
            (
                valid("") + "\n[auth.provider.required_claims]\nexpires = 2030-01-01\n",
                "required_claims `expires` must be a string, boolean, number, or an array",
            ),
            (
                valid("") + "\n[auth.provider.required_claims]\nissued = 09:30:00\n",
                "required_claims `issued` must be a string, boolean, number, or an array",
            ),
            (
                valid("") + "\n[auth.provider.required_claims]\nnested = { tier = 'gold' }\n",
                "required_claims `nested` must be a string, boolean, number, or an array",
            ),
            (
                valid("") + "\n[auth.provider.required_claims]\nwindows = [2030-01-01]\n",
                "required_claims `windows` must be a string, boolean, number, or an array",
            ),
        ]);
    }

    #[test]
    fn retains_every_required_claim_shape_an_id_token_can_carry() {
        let raw = valid("")
            + "\n[auth.provider.required_claims]\nhd = 'example.test'\nverified = true\n\
               level = 3\nratio = 1.5\ngroups = ['staff', 'admin']\n";
        let settings = resolved(&raw);
        let claims = &settings.provider().required_claims;
        assert_eq!(claims.get("hd"), Some(&json!("example.test")));
        assert_eq!(claims.get("verified"), Some(&json!(true)));
        assert_eq!(claims.get("level"), Some(&json!(3)));
        assert_eq!(claims.get("ratio"), Some(&json!(1.5)));
        assert_eq!(claims.get("groups"), Some(&json!(["staff", "admin"])));
    }

    #[test]
    fn rejects_invalid_auth_section_relationships() {
        let cases = vec![
            ("[auth]\n".to_string(), "session"),
            (
                format!("[auth]\n{SESSION}{PROVIDER}"),
                "listeners.auth.public_url",
            ),
            (
                format!("[auth]\n{SESSION}{AUTHORIZATION_SERVER}"),
                "missing field `provider`",
            ),
            (
                valid("").replace(
                    "public_url = 'http://localhost:9080/'",
                    "public_url = 'http://localhost:9080/nested'",
                ),
                "must mount at the root",
            ),
            (
                valid("").replace("[listeners.auth]", "[auth.oauth]"),
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
        let key = coral_test_support::p256_pkcs8();
        fs::write(temp.path().join("session.key"), &key).expect("session key");
        let settings = AuthSettings::from_toml(&valid(""))
            .expect("valid config")
            .expect("auth settings");
        let (settings, issuer) = settings
            .resolve_runtime_dependencies(&config_path, &|_| Ok(None))
            .expect("file key");
        assert!(settings.matches_session_token_issuer(&issuer));
        assert_eq!(issuer.issuer(), "http://localhost:9080");

        let encoded = BASE64_STANDARD.encode(&key);
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
        let key = coral_test_support::p256_pkcs8();
        let encoded_key = BASE64_STANDARD.encode(&key);

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
