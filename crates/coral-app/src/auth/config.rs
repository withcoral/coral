//! Parsing and validation for Coral authentication settings.

use std::fmt;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use serde::de::Error as _;
use serde::{Deserialize, Deserializer};
use url::Url;
use zeroize::Zeroizing;

use super::error::AuthServerError;
use super::oidc_mode::{self, OidcMode, OidcProviderSettings, ResolvedOidcProvider};
use super::sealed_handshake::HandshakeKeys;
use crate::oauth_resource::CanonicalOauthUrl;
use coral_auth::SessionTokenIssuer;
use coral_outbound::{Configured, EndpointUrl};
use coral_sign_in::{
    ModeEnvironment, ModeName, ModeSettings, SettingError, SignInMode, SignInModes,
};

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);
const DEFAULT_SIGNING_KEY_ENV: &str = "CORAL_SESSION_SIGNING_KEY";
const DEFAULT_TOKEN_TTL: Duration = Duration::from_hours(720);
const MAX_TOKEN_TTL: Duration = Duration::from_hours(24 * 365);
const DEFAULT_REFRESH_TOKEN_TTL: Duration = coral_auth::user_tokens::DEFAULT_REFRESH_GRANT_LIFETIME;
const CONFLICTING_KEY_SOURCES: &str = "configure only one of signing_key_env or signing_key_file";

/// Validated authentication policy and its auth/web listener identities.
///
/// [`AuthSettings::from_sections`] is the only constructor: the deserialize
/// target is the private `RawAuthSettings`, so an unvalidated value of this
/// type cannot be built outside this module — by the rest of the crate or by
/// external callers. Holding one is proof that validation ran, which is why
/// nothing downstream revalidates. This type performs no `config.toml`
/// filesystem reads.
///
/// The settings are read through the crate-internal `ResolvedAuthSettings`,
/// which `resolve_runtime_dependencies` produces once the secrets the config
/// only points at have actually been fetched. A clone is the same validated
/// policy, so each resolution starts from one.
#[derive(Clone)]
pub struct AuthSettings {
    auth: RawAuthSettings,
    sign_in: SignIn<OidcProviderSettings>,
    bind_addr: SocketAddr,
    authorization_server: AuthorizationServerSettings,
}

/// The `[auth]` table as bootstrap decodes it from `config.toml`, before
/// [`AuthSettings::from_sections`] validates it.
#[derive(Deserialize)]
#[serde(transparent)]
pub(crate) struct AuthSection(RawAuthSettings);

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawAuthSettings {
    /// Additional public-surface resource identifiers registered with the
    /// authorization server, independent of which clients may skip consent.
    #[serde(default)]
    allowed_audiences: Vec<String>,
    #[serde(default)]
    trusted_clients: Vec<String>,
    session: SessionTokenSettings,
    /// Read by the mode that its `mode` names: [`provider_choice`].
    provider: toml::Table,
}

impl AuthSettings {
    /// Validates the auth settings bootstrap decoded from `config.toml`: the
    /// `[auth]` table, and the auth and web listeners whose addresses give
    /// the authorization server its identity. Without `[auth]` there are no
    /// settings, and the auth listener must then stay local.
    ///
    /// # Errors
    ///
    /// Returns [`AuthServerError::Config`] when an auth-owned field or
    /// cross-section relationship is invalid, or when `[auth.provider]`
    /// names a sign-in mode that `modes` lacks.
    pub(crate) fn from_sections(
        auth: Option<AuthSection>,
        listener: &AuthListenerSettings,
        web: Option<&WebSettings>,
        modes: &SignInModes,
    ) -> Result<Option<Self>, AuthServerError> {
        let issuer = listener.validate(auth.is_some())?;
        let web_client = web.and_then(WebClient::from_settings);
        // Any web interface, `http` on loopback too, begins a sign-in.
        let fresh_sign_in_url = web.map(|web| web.public_url.page_url(FRESH_SIGN_IN_PATH));
        let Some(AuthSection(mut auth)) = auth else {
            return Ok(None);
        };
        auth.validate()?;
        let issuer =
            issuer.ok_or_else(|| config_error("[auth] requires listeners.auth.public_url"))?;
        let sign_in = provider_choice(std::mem::take(&mut auth.provider), &issuer, modes)?;
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
            fresh_sign_in_url,
        };
        Ok(Some(Self {
            auth,
            sign_in,
            bind_addr: listener.bind.unwrap_or(DEFAULT_BIND_ADDR),
            authorization_server,
        }))
    }

    /// Where the authorization server listens.
    pub(crate) fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    /// The authorization server's issuer, from `listeners.auth.public_url`.
    pub(crate) fn issuer(&self) -> &str {
        &self.authorization_server.issuer
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

    /// Fetches the secrets the config only points at — the OIDC provider's
    /// client secret, when the mode is OIDC, and the session signing key —
    /// and builds the session-token issuer keyed by the latter. A registered
    /// mode read its own settings when it was built.
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
        let sign_in = match self.sign_in {
            SignIn::Oidc(provider) => SignIn::Oidc(Box::new(
                provider.resolve(&self.authorization_server.issuer, get_var)?,
            )),
            SignIn::Mode {
                name,
                mode,
                feature,
            } => SignIn::Mode {
                name,
                mode,
                feature,
            },
        };
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
                refresh_token_ttl: Duration::from_secs(self.auth.session.refresh_token_ttl_seconds),
                sign_in,
                handshake_keys: Arc::new(handshake_keys),
            },
            session_tokens,
        ))
    }
}

/// Validated auth settings whose runtime dependencies have been resolved.
///
/// [`AuthSettings::resolve_runtime_dependencies`] is the only constructor, so
/// holding one is proof that the session signing key, and for OIDC the
/// provider secret, were fetched from their sources — which is why reading
/// them cannot fail and nothing rechecks runtime readiness.
pub(crate) struct ResolvedAuthSettings {
    bind_addr: SocketAddr,
    authorization_server: AuthorizationServerSettings,
    access_token_ttl: Duration,
    refresh_token_ttl: Duration,
    sign_in: SignIn<ResolvedOidcProvider>,
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

    /// How long a Refresh Grant can refresh after its sign-in.
    pub(super) fn refresh_token_ttl(&self) -> Duration {
        self.refresh_token_ttl
    }

    /// The sign-in mode that `[auth.provider]` configures, and the name its
    /// URLs are served under: the registered mode the settings hold, or the
    /// OIDC mode of their provider, which reaches it under `policy`.
    ///
    /// # Errors
    /// Returns [`AuthServerError::ProviderClient`] when the OIDC mode's HTTP
    /// client cannot be built.
    pub(super) fn sign_in_mode(
        &self,
        policy: coral_outbound::DestinationPolicy,
    ) -> Result<(ModeName, Arc<dyn SignInMode>), AuthServerError> {
        let provider = match &self.sign_in {
            SignIn::Oidc(provider) => provider,
            SignIn::Mode { name, mode, .. } => return Ok((*name, Arc::clone(mode))),
        };
        let mode = OidcMode::new(
            (**provider).clone(),
            Arc::clone(&self.handshake_keys),
            policy,
        )
        .map_err(|error| AuthServerError::ProviderClient(error.to_string()))?;
        Ok((oidc_mode::NAME, Arc::new(mode)))
    }

    /// The Enterprise Feature that the configured sign-in mode is, or `None`
    /// for a mode of every Edition, as OIDC is.
    pub(super) fn sign_in_feature(&self) -> Option<&coral_edition::EnterpriseFeature> {
        match &self.sign_in {
            SignIn::Oidc(_) => None,
            SignIn::Mode { feature, .. } => feature.as_ref(),
        }
    }

    pub(super) fn handshake_keys(&self) -> &Arc<HandshakeKeys> {
        &self.handshake_keys
    }

    pub(super) fn matches_session_token_issuer(&self, issuer: &SessionTokenIssuer) -> bool {
        self.authorization_server.issuer == issuer.issuer()
            && self.access_token_ttl == issuer.access_token_ttl()
    }
}

impl RawAuthSettings {
    fn validate(&mut self) -> Result<(), AuthServerError> {
        self.session.validate()?;
        for client_id in &mut self.trusted_clients {
            *client_id = validate_trusted_client(client_id)?;
        }
        Ok(())
    }
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

#[derive(Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SessionTokenSettings {
    signing_key_env: Option<String>,
    signing_key_file: Option<PathBuf>,
    access_token_ttl_seconds: u64,
    /// How long a client may refresh after one sign-in. Absolute: rotation
    /// never extends it.
    refresh_token_ttl_seconds: u64,
}

impl Default for SessionTokenSettings {
    fn default() -> Self {
        Self {
            signing_key_env: None,
            signing_key_file: None,
            access_token_ttl_seconds: DEFAULT_TOKEN_TTL.as_secs(),
            refresh_token_ttl_seconds: DEFAULT_REFRESH_TOKEN_TTL.as_secs(),
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
        if self.refresh_token_ttl_seconds == 0 {
            return Err(session_config_error(
                "refresh_token_ttl_seconds must be greater than 0",
            ));
        }
        if self.refresh_token_ttl_seconds > MAX_TOKEN_TTL.as_secs() {
            return Err(session_config_error(format!(
                "refresh_token_ttl_seconds must not exceed {}",
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
    /// The web interface's sign-in page, with the mark that asks it to begin
    /// a sign-in although the browser holds a session.
    fresh_sign_in_url: Option<String>,
}

impl AuthorizationServerSettings {
    pub(crate) fn issuer(&self) -> &str {
        &self.issuer
    }

    /// The web interface's sign-in page, with the mark that asks it to begin
    /// a sign-in although the browser holds a session, or `None` in a
    /// deployment with no web interface.
    pub(super) fn fresh_sign_in_url(&self) -> Option<&str> {
        self.fresh_sign_in_url.as_deref()
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

/// The web interface's sign-in page, with the mark that makes it begin a
/// sign-in although the browser holds a session.
const FRESH_SIGN_IN_PATH: &str = "/login?fresh=1";

/// The sign-in mode that `[auth.provider]` selects, with its settings
/// checked: OIDC with its provider `P`, which the server builds into a mode
/// when it starts, or a mode the build registered, built already, with the
/// Enterprise Feature its factory names.
#[derive(Clone)]
enum SignIn<P> {
    Oidc(Box<P>),
    Mode {
        name: ModeName,
        mode: Arc<dyn SignInMode>,
        feature: Option<coral_edition::EnterpriseFeature>,
    },
}

/// Reads `mode` from `[auth.provider]` and gives the rest of the table to
/// that mode, with Coral's URL `coral_url`. No `mode` means `oidc`, which
/// every build has and no registered mode replaces. A key that the mode does
/// not read is refused with the mode's name.
fn provider_choice(
    mut table: toml::Table,
    coral_url: &str,
    modes: &SignInModes,
) -> Result<SignIn<OidcProviderSettings>, AuthServerError> {
    let mode = match table.remove("mode") {
        None => None,
        Some(toml::Value::String(mode)) => Some(mode),
        Some(_) => return Err(missing_mode(None, modes)),
    };
    // `oidc` is the mode every build has, and a missing `mode` selects it.
    let oidc = oidc_mode::NAME.as_str();
    let name = match mode.as_deref() {
        Some(name) if name != oidc => name,
        _ => {
            let mut provider: OidcProviderSettings =
                ModeSettings::new(table).decode().map_err(|error| {
                    not_a_setting(oidc, &error).unwrap_or_else(|| {
                        // The text that a decode error of the whole file has.
                        AuthServerError::Config(format!(
                            "server configuration is invalid: {}",
                            at_provider(&error)
                        ))
                    })
                })?;
            provider.validate()?;
            return Ok(SignIn::Oidc(Box::new(provider)));
        }
    };
    let Some(factory) = modes.get(name) else {
        return Err(missing_mode(Some(name), modes));
    };
    let coral_url = Url::parse(coral_url)
        .map_err(|_error| config_error("listeners.auth.public_url is not a URL"))?;
    let mode = factory
        .build(&ModeSettings::new(table), &ModeEnvironment::new(coral_url))
        .map_err(|error| {
            not_a_setting(name, &error).unwrap_or_else(|| config_error(at_provider(&error)))
        })?;
    Ok(SignIn::Mode {
        name: factory.name(),
        mode,
        feature: factory.feature(),
    })
}

/// The refusal of a key of `[auth.provider]` that the mode `mode` does not
/// read, or `None` when `error` is about another setting.
fn not_a_setting(mode: &str, error: &SettingError) -> Option<AuthServerError> {
    error.is_unknown().then(|| {
        invalid_provider(format!(
            "{}: not a setting of sign-in mode `{mode}`",
            error.field()
        ))
    })
}

/// Where in `[auth.provider]` a setting is unusable, and why.
fn at_provider(error: &SettingError) -> String {
    if error.field().is_empty() {
        format!("auth.provider: {}", error.reason())
    } else {
        format!("auth.provider.{error}")
    }
}

/// The one refusal for every `mode` this build lacks, `value` being the
/// string it holds, if any. It repeats the value only when it has the form of
/// a mode's name, since an operator can paste a secret under the wrong key.
fn missing_mode(value: Option<&str>, modes: &SignInModes) -> AuthServerError {
    let oidc = oidc_mode::NAME.as_str();
    let mut names: Vec<&str> = modes.names().filter(|name| *name != oidc).collect();
    names.sort_unstable();
    let names = std::iter::once(oidc)
        .chain(names)
        .map(|name| format!("`{name}`"))
        .collect::<Vec<_>>()
        .join(", ");
    let mode = match value.filter(|value| ModeName::is_name(value)) {
        Some(value) => format!("sign-in mode `{value}`"),
        None => "such sign-in mode".to_owned(),
    };
    invalid_provider(format!(
        "mode: this build has no {mode}. The modes of this build: {names}"
    ))
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

pub(super) fn invalid_provider(message: impl fmt::Display) -> AuthServerError {
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

pub(super) fn config_error(message: impl AsRef<str>) -> AuthServerError {
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

    use serde_json::{Value, json};

    use super::*;

    use crate::auth::test_config::{PROVIDER, SESSION, test_modes};
    use crate::bootstrap::{auth_settings, auth_settings_with};

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
        match auth_settings(raw) {
            Err(error) => error,
            Ok(_) => panic!("expected invalid config"),
        }
    }

    impl ResolvedAuthSettings {
        /// The OIDC provider that the settings configure.
        pub(in crate::auth) fn provider(&self) -> &ResolvedOidcProvider {
            match &self.sign_in {
                SignIn::Oidc(provider) => provider,
                SignIn::Mode { .. } => panic!("the settings configure another sign-in mode"),
            }
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
        auth_settings(raw)
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
                "invalid auth configuration: auth-disabled listeners.auth.bind must be loopback",
            ),
            (
                "[listeners.auth]\npublic_url = 'https://login.example.test'",
                "invalid auth configuration: listeners.auth.public_url requires [auth]",
            ),
            (
                "[listeners.auth]\nbind = 'not-an-address'",
                "server configuration is invalid: listeners.auth.bind: invalid socket address syntax",
            ),
        ] {
            assert_eq!(reject(raw), expected, "{raw}");
        }
        assert!(
            auth_settings("[listeners.auth]\nbind = '127.0.0.1:9080'")
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

        // A document without `[auth]` configures no sign-in.
        assert!(
            auth_settings("[otel]\nservice_name = 'coral'")
                .unwrap()
                .is_none()
        );
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
                    "server configuration is invalid: listeners.web.public_url: must be a concrete HTTPS \
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
            auth_settings("[listeners.grpc]\nbind = '127.0.0.1:0'")
                .expect("Desktop config")
                .is_none()
        );
    }

    #[test]
    fn loopback_web_navigation_does_not_register_an_audience_or_trust_a_client() {
        for origin in ["http://localhost:5173", "http://127.0.0.1:49152"] {
            let raw = valid("") + &format!("\n[listeners.web]\npublic_url = '{origin}'\n");
            assert_eq!(parse(&raw).allowed_audiences(), [] as [String; 0]);
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
                auth_settings(&(valid("") + "\n" + suffix)).is_err(),
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

    /// `[auth.provider]` with `mode`, then `settings`.
    fn with_mode(mode: &str, settings: &str) -> String {
        format!(
            "[auth]\n{SESSION}{AUTHORIZATION_SERVER}\n[auth.provider]\nmode = {mode}\n{settings}"
        )
    }

    /// The fixture with `mode = 'oidc'`.
    fn valid_with_oidc_mode() -> String {
        valid("").replace("[auth.provider]\n", "[auth.provider]\nmode = 'oidc'\n")
    }

    /// A mode that a launcher registers as `oidc`. Its `build` fails, so a
    /// start that builds it is refused.
    struct Impostor;

    impl coral_sign_in::SignInModeFactory for Impostor {
        fn name(&self) -> ModeName {
            oidc_mode::NAME
        }

        fn feature(&self) -> Option<coral_edition::EnterpriseFeature> {
            None
        }

        fn build(
            &self,
            _settings: &ModeSettings,
            _environment: &ModeEnvironment,
        ) -> Result<Arc<dyn SignInMode>, SettingError> {
            Err(SettingError::new("", "a mode registered as `oidc` built"))
        }
    }

    fn impostor_modes() -> SignInModes {
        SignInModes::default().with(Arc::new(Impostor))
    }

    /// A file of today has no `mode`, and means `oidc`. `oidc` is the
    /// build's own: a mode that a launcher registers under that name never
    /// builds.
    #[test]
    fn a_file_with_no_mode_or_mode_oidc_signs_in_by_oidc() {
        let today = format!("{:?}", resolved(&valid("")).provider());
        assert_eq!(
            format!("{:?}", resolved(&valid_with_oidc_mode()).provider()),
            today
        );
        // A build with other modes reads such a file the same way.
        for modes in [test_modes(), impostor_modes()] {
            for raw in [valid(""), valid_with_oidc_mode()] {
                assert!(
                    matches!(auth_settings_with(&raw, &modes), Ok(Some(_))),
                    "{raw}"
                );
            }
        }
    }

    /// One message serves every `mode` the build lacks. It lists the modes
    /// of the build, and repeats only a value that has the form of a name.
    #[test]
    fn a_mode_this_build_lacks_refuses_the_start() {
        const SECRET: &str = "Sk_Live_0123456789abcdef";
        let prefix = "invalid auth configuration: auth.provider.mode: ";
        for (mode, modes, expected) in [
            (
                "'nope'",
                SignInModes::default(),
                "this build has no sign-in mode `nope`. The modes of this build: `oidc`",
            ),
            (
                "'nope'",
                test_modes(),
                "this build has no sign-in mode `nope`. The modes of this build: `oidc`, `test`",
            ),
            (
                "'nope'",
                impostor_modes(),
                "this build has no sign-in mode `nope`. The modes of this build: `oidc`",
            ),
            (
                &format!("'{SECRET}'"),
                SignInModes::default(),
                "this build has no such sign-in mode. The modes of this build: `oidc`",
            ),
            (
                "7",
                SignInModes::default(),
                "this build has no such sign-in mode. The modes of this build: `oidc`",
            ),
        ] {
            let raw = with_mode(mode, "");
            let Err(error) = auth_settings_with(&raw, &modes) else {
                panic!("{mode}: expected a refusal");
            };
            assert_eq!(error, format!("{prefix}{expected}"), "{mode}");
        }
    }

    /// A key that the configured mode does not read names the key and the
    /// mode, never the value. A nested key keeps its path.
    #[test]
    fn an_unknown_setting_names_the_mode() {
        const SECRET: &str = "Sk_Live_0123456789abcdef";
        for (raw, modes, expected) in [
            (
                valid("") + &format!("unknown = '{SECRET}'\n"),
                SignInModes::default(),
                "unknown: not a setting of sign-in mode `oidc`",
            ),
            (
                valid_with_oidc_mode() + "sign_in_url = 'https://idp.example.test/start'\n",
                test_modes(),
                "sign_in_url: not a setting of sign-in mode `oidc`",
            ),
            (
                valid("")
                    + &format!(
                        "\n[auth.provider.access_token]\naudience = 'env'\nunknown = '{SECRET}'\n"
                    ),
                SignInModes::default(),
                "access_token.unknown: not a setting of sign-in mode `oidc`",
            ),
        ] {
            let Err(error) = auth_settings_with(&raw, &modes) else {
                panic!("{raw}: expected a refusal");
            };
            assert_eq!(
                error,
                format!("invalid auth configuration: auth.provider.{expected}"),
                "{raw}"
            );
        }
    }

    /// The mode gets the rest of `[auth.provider]`, without `mode`, and
    /// Coral's URL. Its settings resolve with no client secret.
    #[tokio::test]
    async fn a_registered_mode_gets_the_rest_of_the_section_and_coral_url() {
        let prefix = "invalid auth configuration: auth.provider";
        let start = "sign_in_url = 'https://idp.example.test/start'";
        let settings = auth_settings_with(&with_mode("'test'", start), &test_modes())
            .expect("the test mode starts")
            .expect("auth settings");
        let dir = signing_key_dir();
        let (resolved, _tokens) = settings
            .resolve_runtime_dependencies(&dir.path().join("config.toml"), &|_| Ok(None))
            .expect("no client secret to read");
        let (name, _mode) = resolved
            .sign_in_mode(crate::auth::test_config::loopback_policy())
            .expect("the registered mode");
        assert_eq!(name.as_str(), "test");
        let unknown = format!("{start}\nissuer = 'https://accounts.example.test'");
        let Err(error) = auth_settings_with(&with_mode("'test'", &unknown), &test_modes()) else {
            panic!("{unknown}: expected a refusal");
        };
        assert_eq!(
            error,
            format!("{prefix}.issuer: not a setting of sign-in mode `test`")
        );

        let table = toml::from_str(&format!("mode = 'test'\n{start}")).expect("TOML");
        let Ok(SignIn::Mode { name, mode, .. }) =
            provider_choice(table, "http://localhost:9080", &test_modes())
        else {
            panic!("the test mode builds");
        };
        assert_eq!(name.as_str(), "test");
        let begin = mode
            .begin(
                &coral_sign_in::SignInValue::new("value".to_owned()),
                coral_db::UnixNanos::new(0),
            )
            .await
            .expect("begin");
        assert!(
            begin
                .as_str()
                .ends_with("&return_to=http%3A%2F%2Flocalhost%3A9080%2F"),
            "{begin}"
        );
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

    /// Coral's URL is `https`, or `http` on a loopback host only, whatever
    /// the sign-in mode.
    #[test]
    fn refuses_an_http_public_url_off_loopback() {
        let at = |public_url: &str| {
            valid("").replace(
                "public_url = 'http://localhost:9080/'",
                &format!("public_url = '{public_url}'"),
            )
        };
        let refusal = "invalid auth configuration: listeners.auth.public_url is invalid: \
                       configured endpoint must use HTTPS or explicit loopback HTTP";
        assert_eq!(reject(&at("http://auth.example.test/")), refusal);
        // A registered mode gets no say: core refuses before it builds one.
        let registered = at("http://auth.example.test/").replace(
            "issuer = 'https://accounts.example.test'\nclient_id = 'upstream-client'\n\
             client_secret = 'provider-secret'\n",
            "mode = 'test'\nsign_in_url = 'https://idp.example.test/start'\n",
        );
        assert!(registered.contains("mode = 'test'"), "{registered}");
        let Err(error) = auth_settings_with(&registered, &test_modes()) else {
            panic!("the test mode starts behind an http public URL");
        };
        assert_eq!(error, refusal);
        for public_url in [
            "https://auth.example.test/",
            "http://127.0.0.1:9080/",
            "http://[::1]:9080/",
        ] {
            if let Err(error) = auth_settings(&at(public_url)) {
                panic!("{public_url}: {error}");
            }
        }
    }

    #[test]
    fn resolves_session_key_sources_after_parsing() {
        let temp = tempfile::tempdir().expect("tempdir");
        let config_path = temp.path().join("config.toml");
        let key = coral_test_support::p256_pkcs8();
        fs::write(temp.path().join("session.key"), &key).expect("session key");
        let settings = auth_settings(&valid(""))
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
        let settings = auth_settings(&raw)
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
        let settings = auth_settings(&default_env)
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
            (
                "signing_key_file = 'session.key'\nrefresh_token_ttl_seconds = 0",
                "refresh_token_ttl_seconds must be greater than 0",
            ),
            (
                "signing_key_file = 'session.key'\nrefresh_token_ttl_seconds = 31536001",
                "refresh_token_ttl_seconds must not exceed",
            ),
        ] {
            let raw = valid("").replace("signing_key_file = 'session.key'", replacement);
            assert!(reject(&raw).contains(expected));
        }

        let Err(missing_file) = auth_settings(&valid(""))
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
        let settings = auth_settings(&env_config)
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
