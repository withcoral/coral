//! Configuration owned by the long-running Coral server surface.

use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use serde::Deserialize;
use serde::de::IgnoredAny;
use zeroize::Zeroizing;

use super::env::AppEnvironment;
use super::mcp_interface::ConfiguredMcpInterface;
use super::secret_store_config::SecretStoreDefinitions;
use super::{AppError, is_loopback_ip};
use crate::auth::{
    AuthListenerSettings, AuthSection, AuthServerError, AuthSettings, CoralAuthorizationServer,
    ResolvedAuthSettings, WebSettings,
};
use crate::config_toml::Document;
use crate::oauth_resource::CanonicalOauthUrl;
use crate::state::AppStateLayout;
use crate::telemetry::TelemetryConfig;
use crate::telemetry::config::OtlpConfig;
use crate::workspace_mcp_urls::WorkspaceMcpUrls;
use coral_auth::SessionTokenIssuer;

/// Bootstrap owns the document's section names; section parsers own their fields.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ServerConfigFile {
    listeners: ListenerSettings,
    runtime: RuntimeSettings,
    outbound: OutboundSettings,
    search: SearchSettings,
    credentials: CredentialSettings,
    updates: UpdateSettings,
    license: Option<LicenseSettings>,
    /// Decoded entry by entry in `secret_store_config`, so a failure can name the store.
    secret_stores: Vec<toml::Value>,
    auth: Option<AuthSection>,
    otel: OtlpConfig,
    /// The state fence reads this section before anything here is decoded.
    #[serde(rename = "database")]
    _database: IgnoredAny,
}

impl ServerConfigFile {
    /// Decodes what is left of the document. A failure names the key path
    /// and never the value.
    fn decode(document: Document<'_>) -> Result<Self, AppError> {
        document.decode().map_err(AppError::ConfigDecode)
    }

    /// Validates the auth settings: the `[auth]` table, and the auth and web
    /// listeners whose addresses give the authorization server its identity.
    /// None without `[auth]`.
    fn auth_settings(&mut self) -> Result<Option<AuthSettings>, AppError> {
        AuthSettings::from_sections(
            self.auth.take(),
            &self.listeners.auth,
            self.listeners.web.as_ref(),
        )
        .map_err(AppError::Auth)
    }
}

/// Refuses `setting` with what `problem` says about it.
fn refused(setting: &str, problem: impl std::fmt::Display) -> AppError {
    AppError::config(setting, format!("{setting} {problem}"))
}

/// Refuses `setting` in a deployment without `[auth]`.
fn refused_without_auth(setting: &str, problem: &str) -> AppError {
    AppError::config(setting, format!("auth-disabled {setting} {problem}"))
}

/// What the launching process supplies beside the configuration file.
///
/// These are not settings: the process that launches Coral reads them at its
/// own bootstrap and passes them in through `ServerBuilder`.
#[derive(Clone, Default)]
pub(crate) struct Launcher {
    /// Desktop owns this launch, so local auth support defaults to its fixed port.
    pub(crate) desktop: bool,
    /// A local launcher's web origin for human handoff, used only without `[auth]`.
    pub(crate) local_web_url: Option<coral_mcp::HumanInterfaceUrl>,
    /// The launching binary's version, for telemetry's `service.version`.
    pub(crate) service_version: Option<&'static str>,
    /// The variables that stop this process from sending usage data. A
    /// launch that supplies none sends nothing.
    pub(crate) usage_environment: Option<coral_usage::SendEnvironment>,
    /// What the launching binary adds to Coral, such as its Edition check.
    pub(crate) extensions: super::runtime::Extensions,
    /// Builds the tool surface MCP serves in place of core's.
    pub(crate) mcp_interface_factory: Option<std::sync::Arc<dyn super::McpInterfaceFactory>>,
    /// Compile in this process: a test that starts Coral in its own process.
    pub(crate) compile_in_process: bool,
    /// Where startup reports each database migration step it applies.
    pub(crate) migration_report: Option<super::runtime::MigrationReport>,
    /// The clock a test drives. Without one, the host's clock.
    pub(crate) clock: Option<std::sync::Arc<dyn coral_db::Clock>>,
}

impl Launcher {
    /// The one clock of this Coral Instance: the host's, unless a test
    /// drives its own. Everything that issues or verifies a credential reads
    /// it, so a token is dated and checked by the same clock.
    pub(crate) fn clock(&self) -> std::sync::Arc<dyn coral_db::Clock> {
        self.clock
            .clone()
            .unwrap_or_else(|| std::sync::Arc::new(coral_db::SystemClock))
    }
}

/// Local auth support's default port under Desktop, which registers it as a
/// fixed Provider OAuth callback.
const DESKTOP_AUTH_PORT: u16 = 8779;

/// The configuration file as captured, shared by state preparation and
/// serving so that neither reads the file again.
///
/// Capture does not parse settings or resolve secrets; state initialization must
/// reject retired state before bootstrap interprets deployment settings.
/// [`Self::validate`] then decodes the document once.
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

    pub(crate) fn raw(&self) -> &str {
        &self.raw
    }

    /// Decodes the document and checks every rule that needs no secret, after
    /// the persistent-state fence and before the first write.
    ///
    /// `launcher` is what the launching process supplies beside the file.
    /// `grpc_bind` is the private gRPC address the composition selects itself;
    /// without one, `listeners.grpc.bind` is the address.
    ///
    /// This is the one place a rule is checked, so a deployment whose
    /// configuration cannot be served leaves a fresh state root unchanged.
    /// Secrets are read later, only for the surfaces that use them. A
    /// `credentials.key` file beside `credentials.key_env` is refused when the
    /// vault opens, not here: the file is state on the volume, not a setting,
    /// so the configuration alone cannot prove the key has one source.
    pub(crate) fn validate(
        &self,
        launcher: &Launcher,
        grpc_bind: Option<SocketAddr>,
    ) -> Result<DeploymentConfig, AppError> {
        let mut document = Document::parse(&self.raw).map_err(AppError::ConfigDecode)?;
        let mcp_interface = match &launcher.mcp_interface_factory {
            Some(factory) => ConfiguredMcpInterface::take(factory, &mut document)
                .map_err(AppError::ConfigDecode)?,
            None => None,
        };
        // A section that no factory took is an unknown field here.
        let mut file = ServerConfigFile::decode(document)?;
        let auth = file.auth_settings()?;
        let ServerConfigFile {
            listeners,
            runtime,
            outbound,
            search,
            credentials,
            updates,
            license,
            secret_stores,
            auth: _,
            otel,
            _database: _,
        } = file;
        let local_run_executors =
            runtime.local_run_executors(listeners.run_executor.token_env.is_some())?;
        let runtime_limits = runtime.resolve()?;
        let search_reconcile_interval = search.reconcile_interval()?;
        let credential_key_env = credentials.key_env()?;
        let license = license.map(LicenseSettings::validate).transpose()?;
        let secret_stores = credentials
            .defined_secret_stores(&secret_stores)?
            .map(|defined| {
                SecretStoreDefinitions::validate(
                    &self.raw,
                    defined.to_vec(),
                    &launcher.extensions.secret_store_kinds,
                )
            })
            .transpose()?;
        let (mcp_http, auth_bind_addr, session_auth) = match auth {
            None => {
                if !is_loopback_ip(listeners.grpc.bind.ip()) {
                    return Err(refused_without_auth(
                        "listeners.grpc.bind",
                        "must be loopback",
                    ));
                }
                // The composition's own address, which no setting names.
                if grpc_bind.is_some_and(|bind| !is_loopback_ip(bind.ip())) {
                    return Err(refused_without_auth(
                        "private gRPC bind",
                        "must be loopback",
                    ));
                }
                let auth_bind_addr = listeners.auth.bind.unwrap_or_else(|| {
                    let port = if launcher.desktop {
                        DESKTOP_AUTH_PORT
                    } else {
                        0
                    };
                    SocketAddr::from((Ipv4Addr::LOCALHOST, port))
                });
                (listeners.mcp.resolve(None)?, auth_bind_addr, None)
            }
            Some(settings) => {
                let auth_bind_addr = settings.bind_addr();
                let (mcp_http, session_auth) =
                    SessionAuthConfig::validate(settings, &listeners.mcp)?;
                (mcp_http, auth_bind_addr, Some(session_auth))
            }
        };
        let run_executor = listeners.run_executor.validate(session_auth.is_some())?;
        let human_interface = human_interface(
            listeners.web,
            session_auth.is_some(),
            launcher.local_web_url.clone(),
        )?;
        Ok(DeploymentConfig {
            config_path: self.config_path.clone(),
            grpc_bind: grpc_bind.unwrap_or(listeners.grpc.bind),
            mcp_http,
            auth_bind_addr,
            human_interface,
            run_executor,
            local_run_executors,
            runtime_limits,
            outbound,
            search_reconcile_interval,
            semantic_search_enabled: search.semantic_search_enabled,
            update_check: updates.check,
            credential_key_env,
            license,
            secret_stores,
            mcp_interface,
            session_auth,
            telemetry: TelemetryConfig { otel },
        })
    }
}

/// The deployment's configuration: `config.toml` decoded once, with every
/// rule checked that needs no secret.
///
/// [`LoadedServerConfig::validate`] is the only constructor, so holding one is
/// proof that the deployment can be served as configured once its secrets are
/// read. Each setting is a field; nothing here reads the file or decodes the
/// document again. What reads a secret says so: [`Self::serve_settings`],
/// [`Self::run_executor_listener`] and [`Self::secret_stores`].
pub(crate) struct DeploymentConfig {
    /// Where the file is, to find a signing key file named beside it.
    config_path: PathBuf,
    grpc_bind: SocketAddr,
    mcp_http: McpHttpServeConfig,
    auth_bind_addr: SocketAddr,
    human_interface: coral_mcp::HumanInterfaceUrl,
    run_executor: RunExecutorListenerConfig,
    local_run_executors: Option<LocalRunExecutorSettings>,
    runtime_limits: coral_run::RuntimeLimits,
    outbound: OutboundSettings,
    search_reconcile_interval: std::time::Duration,
    semantic_search_enabled: bool,
    update_check: bool,
    credential_key_env: Option<String>,
    /// Where the License is. None without `[license]`.
    license: Option<LicenseSource>,
    /// None when the deployment turned Secret Stores off.
    secret_stores: Option<SecretStoreDefinitions>,
    mcp_interface: Option<ConfiguredMcpInterface>,
    session_auth: Option<SessionAuthConfig>,
    telemetry: TelemetryConfig,
}

/// Validated session authentication, before its secrets are read.
struct SessionAuthConfig {
    settings: AuthSettings,
    public_audiences: Vec<String>,
    mcp_workspace_urls: WorkspaceMcpUrls,
    web_public_url: Option<String>,
}

impl SessionAuthConfig {
    /// Checks the public identities an `[auth]` deployment serves under: the
    /// MCP listener's public origin and the audiences beside it.
    fn validate(
        settings: AuthSettings,
        mcp: &RawMcpHttpSettings,
    ) -> Result<(McpHttpServeConfig, Self), AppError> {
        let authorization_server =
            required_oauth_url("listeners.auth.public_url", Some(settings.issuer()))?;
        let mcp_http = mcp.resolve(Some(&authorization_server))?;
        let base = required_oauth_url(MCP_PUBLIC_URL, mcp.public_url.as_deref())?;
        let mcp_workspace_urls = WorkspaceMcpUrls::new(
            CanonicalOauthUrl::parse(&base).map_err(|error| refused(MCP_PUBLIC_URL, error))?,
        )
        .map_err(|error| refused(MCP_PUBLIC_URL, error))?;
        let public_audiences = public_surface_audiences(
            &mcp_workspace_urls,
            settings.allowed_audiences(),
            settings.web_public_url(),
        )?;
        let web_public_url = settings.web_public_url().map(str::to_owned);
        Ok((
            mcp_http,
            Self {
                settings,
                public_audiences,
                mcp_workspace_urls,
                web_public_url,
            },
        ))
    }
}

/// The settings can hold a secret an operator put under the wrong key, so
/// this names the type and prints nothing of it.
impl std::fmt::Debug for DeploymentConfig {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DeploymentConfig")
            .finish_non_exhaustive()
    }
}

impl DeploymentConfig {
    /// Where the private gRPC listener binds.
    pub(crate) fn grpc_bind(&self) -> SocketAddr {
        self.grpc_bind
    }

    /// Whether `[auth]` is configured, so every listener authenticates.
    pub(crate) fn authenticated(&self) -> bool {
        self.session_auth.is_some()
    }

    /// The operator's outbound permissions over the composition's `defaults`.
    pub(crate) fn outbound_policy(
        &self,
        defaults: coral_outbound::DestinationPolicy,
    ) -> coral_outbound::DestinationPolicy {
        self.outbound.policy(defaults)
    }

    /// The policy for new automatic MCP Provider OAuth clients.
    pub(crate) fn mcp_oauth_client_bootstrap(
        &self,
    ) -> coral_provider::account::ClientBootstrapPolicy {
        self.outbound.mcp_oauth_client_bootstrap()
    }

    /// How often the search index's reconcile task runs a pass.
    pub(crate) fn search_reconcile_interval(&self) -> std::time::Duration {
        self.search_reconcile_interval
    }

    /// Whether `[search] semantic_search_enabled` turns Universal Search's
    /// vector side on.
    pub(crate) fn semantic_search_enabled(&self) -> bool {
        self.semantic_search_enabled
    }

    /// Universal Search's vector side: vectors of the model manifest's
    /// dimension when `[search] semantic_search_enabled` is true, otherwise
    /// off.
    pub(crate) fn vector_storage(&self) -> coral_search::VectorStorage {
        if self.semantic_search_enabled {
            coral_search::VectorStorage::On {
                dimension: coral_embed::MANIFEST.dimension,
            }
        } else {
            coral_search::VectorStorage::Off
        }
    }

    /// Whether the runtime reads the release feed to tell Organization Owners
    /// about a newer release.
    pub(crate) fn update_check(&self) -> bool {
        self.update_check
    }

    /// The environment variable that holds the credential encryption key, when
    /// the deployment keeps that key in a secret instead of a key file.
    pub(crate) fn credential_key_env(&self) -> Option<&str> {
        self.credential_key_env.as_deref()
    }

    /// The License the deployment configures, trimmed, or None without
    /// `[license]`. `read_env` reads a `key_env` variable, which startup
    /// reads once: a new License applies at the next restart.
    ///
    /// # Errors
    ///
    /// Returns what `read_env` refuses, or a refusal of a variable that holds
    /// no License or one that is not a single line.
    pub(crate) fn license(
        &self,
        read_env: &dyn Fn(&str, &str) -> Result<String, AppError>,
    ) -> Result<Option<coral_credentials::Secret>, AppError> {
        let Some(source) = &self.license else {
            return Ok(None);
        };
        match source {
            LicenseSource::Inline(text) => license_secret(text.expose())
                .map(Some)
                .map_err(|message| refused("license.key", message)),
            LicenseSource::Env(name) => {
                let value = read_env(LICENSE_KEY_ENV, name)?;
                license_secret(&value).map(Some).map_err(|message| {
                    AppError::invalid_secret(
                        LICENSE_KEY_ENV,
                        name,
                        format!("{LICENSE_KEY_ENV} names `{name}`, which {message}"),
                    )
                })
            }
        }
    }

    /// The `[otel]` section.
    pub(crate) fn telemetry(&self) -> &TelemetryConfig {
        &self.telemetry
    }

    /// The local Run Executors of `[runtime]`: none when
    /// `local_run_executors` is false, which `[listeners.run_executor]`
    /// then makes up for with a `token_env`.
    pub(crate) fn local_run_executors(&self) -> Option<LocalRunExecutorSettings> {
        self.local_run_executors
    }

    /// The `[runtime]` section: the Program runtime limits.
    pub(crate) fn runtime_limits(&self) -> coral_run::RuntimeLimits {
        self.runtime_limits
    }

    /// The Secret Stores this deployment defines, or the disabled set when it
    /// turned them off. Reads each store's secret ID from the environment and
    /// sends nothing; an `env` store keeps the reader for later reads.
    pub(crate) fn secret_stores(
        &self,
        policy: coral_outbound::DestinationPolicy,
    ) -> Result<coral_secret_store::SecretStores, AppError> {
        self.secret_stores_from(
            policy,
            &(std::sync::Arc::new(AppEnvironment::env_var) as coral_secret_store::EnvReader),
        )
    }

    pub(super) fn secret_stores_from(
        &self,
        policy: coral_outbound::DestinationPolicy,
        env: &coral_secret_store::EnvReader,
    ) -> Result<coral_secret_store::SecretStores, AppError> {
        match &self.secret_stores {
            Some(defined) => defined.build(policy, env),
            None => Ok(coral_secret_store::SecretStores::disabled()),
        }
    }

    /// The Run Executor listener of `[listeners.run_executor]`, reading each
    /// operator token through `env`, [`AppEnvironment::required_env_var`] at
    /// bootstrap.
    pub(crate) fn run_executor_listener(
        &self,
        env: &dyn Fn(&str, &str) -> Result<String, AppError>,
    ) -> Result<RunExecutorListener, AppError> {
        self.run_executor.resolve(env)
    }

    /// The settings of the mandatory MCP listener and of OIDC authentication
    /// when `[auth]` is configured, whose signing key and provider secret
    /// this reads from their sources.
    ///
    /// This returns configuration and resolved key material only. The
    /// caller constructs the services and owns their lifecycle, so resolving
    /// configuration never produces a live service or its state.
    pub(crate) fn serve_settings(
        &self,
        defaults: coral_outbound::DestinationPolicy,
    ) -> Result<ServeSettings, AppError> {
        Ok(ServeSettings {
            outbound_policy: self.outbound_policy(defaults),
            mcp_oauth_client_bootstrap: self.mcp_oauth_client_bootstrap(),
            mcp_http: self.mcp_http.clone(),
            mcp_interface: self.mcp_interface.clone(),
            auth_bind_addr: self.auth_bind_addr,
            session_auth: self.resolve_session_auth()?,
            human_interface: self.human_interface.clone(),
        })
    }

    fn resolve_session_auth(&self) -> Result<Option<SessionAuthSettings>, AppError> {
        let Some(auth) = &self.session_auth else {
            return Ok(None);
        };
        let (settings, session_tokens) = auth
            .settings
            .clone()
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
            .map_err(AppError::Auth)?;
        Ok(Some(SessionAuthSettings {
            settings,
            session_tokens,
            public_audiences: auth.public_audiences.clone(),
            mcp_workspace_urls: auth.mcp_workspace_urls.clone(),
            web_public_url: auth.web_public_url.clone(),
        }))
    }
}

fn human_interface(
    web: Option<WebSettings>,
    authenticated: bool,
    local: Option<coral_mcp::HumanInterfaceUrl>,
) -> Result<coral_mcp::HumanInterfaceUrl, AppError> {
    const WEB_PUBLIC_URL: &str = "listeners.web.public_url";
    const LOCAL_WEB_URL: &str = "CORAL_LOCAL_WEB_URL";
    match (web, local) {
        (_, Some(_)) if authenticated => Err(refused(
            LOCAL_WEB_URL,
            format_args!("is local-only; configure {WEB_PUBLIC_URL}"),
        )),
        (Some(_), Some(_)) => Err(AppError::config(
            LOCAL_WEB_URL,
            format!("configure only one of {WEB_PUBLIC_URL} or {LOCAL_WEB_URL}"),
        )),
        (Some(web), None) => Ok(web.public_url),
        (None, Some(local)) => Ok(local),
        (None, None) => Err(refused(
            WEB_PUBLIC_URL,
            format_args!(
                "is required for human handoff; local launchers may supply {LOCAL_WEB_URL}"
            ),
        )),
    }
}

/// Default overrides chosen by the local CLI/Desktop composition, never by domain code.
pub const LOCAL_OUTBOUND_DEFAULTS: coral_outbound::DestinationPolicy =
    coral_outbound::DestinationPolicy::new(
        coral_outbound::NetworkAccess::HttpAndHttps,
        coral_outbound::NetworkAccess::HttpAndHttps,
    );

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum NetworkAccessSetting {
    Deny,
    Https,
    HttpAndHttps,
}

impl From<NetworkAccessSetting> for coral_outbound::NetworkAccess {
    fn from(value: NetworkAccessSetting) -> Self {
        match value {
            NetworkAccessSetting::Deny => Self::Deny,
            NetworkAccessSetting::Https => Self::Https,
            NetworkAccessSetting::HttpAndHttps => Self::HttpAndHttps,
        }
    }
}

/// Operator spelling of [`coral_provider::account::ClientBootstrapPolicy`]. Only
/// MCP discovery reports document support and a registration endpoint, so the
/// policy decides nothing for other Provider kinds.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
enum McpOAuthClientBootstrapSetting {
    /// Assess the document's addresses here; prefer it only when they are public.
    #[default]
    Automatic,
    /// Prefer the document whenever the server supports it, whatever the local
    /// resolver answers: split DNS, or a server with approved internal access.
    PreferClientIdMetadataDocument,
    /// Register dynamically or not at all; never present the document.
    DynamicClientRegistrationOnly,
}

impl From<McpOAuthClientBootstrapSetting> for coral_provider::account::ClientBootstrapPolicy {
    fn from(value: McpOAuthClientBootstrapSetting) -> Self {
        match value {
            McpOAuthClientBootstrapSetting::Automatic => Self::Automatic,
            McpOAuthClientBootstrapSetting::PreferClientIdMetadataDocument => {
                Self::PreferClientIdMetadataDocument
            }
            McpOAuthClientBootstrapSetting::DynamicClientRegistrationOnly => {
                Self::DynamicClientRegistrationOnly
            }
        }
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct OutboundSettings {
    loopback: Option<NetworkAccessSetting>,
    private_networks: Option<NetworkAccessSetting>,
    /// How Coral identifies itself to an upstream MCP authorization server when
    /// nobody enters a client ID: the identity it presents, not what it may reach.
    mcp_oauth_client_bootstrap: McpOAuthClientBootstrapSetting,
}

impl OutboundSettings {
    fn mcp_oauth_client_bootstrap(&self) -> coral_provider::account::ClientBootstrapPolicy {
        self.mcp_oauth_client_bootstrap.into()
    }

    fn policy(
        &self,
        mut defaults: coral_outbound::DestinationPolicy,
    ) -> coral_outbound::DestinationPolicy {
        if let Some(access) = self.loopback {
            defaults = defaults.with_loopback(access.into());
        }
        if let Some(access) = self.private_networks {
            defaults = defaults.with_private_networks(access.into());
        }
        defaults
    }
}

/// Where the host credential encryption key comes from.
///
/// Without this section the key is the `credentials.key` file in the config
/// directory, which the runtime creates on first start. A cloud deployment
/// whose secrets live in a secret manager names the environment variable that
/// carries the key instead, so the runtime never writes a key file.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct CredentialSettings {
    key_env: Option<String>,
    /// Turns Secret Stores off, for a deployment that cannot offer them, such
    /// as Desktop, whose generated config no operator edits. Reef then hides
    /// them instead of showing how to define one.
    disable_secret_stores: bool,
}

impl CredentialSettings {
    /// The `[[secret_stores]]` tables to build, or `None` when the deployment
    /// turned Secret Stores off. Refuses a config that does both, so the file
    /// cannot say both.
    fn defined_secret_stores<'a>(
        &self,
        secret_stores: &'a [toml::Value],
    ) -> Result<Option<&'a [toml::Value]>, AppError> {
        match (self.disable_secret_stores, secret_stores.is_empty()) {
            (false, _) => Ok(Some(secret_stores)),
            (true, true) => Ok(None),
            (true, false) => Err(refused(
                "credentials.disable_secret_stores",
                "is set, so remove the [[secret_stores]] tables",
            )),
        }
    }

    fn key_env(&self) -> Result<Option<String>, AppError> {
        self.key_env
            .as_deref()
            .map(|configured| {
                const SETTING: &str = "credentials.key_env";
                super::env_var_name(SETTING, configured)
                    .map_err(|message| AppError::config(SETTING, message))
            })
            .transpose()
    }
}

/// The `[license]` section: one License, given inline or by the name of the
/// environment variable that holds it (ADR 0038).
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LicenseSettings {
    key: Option<String>,
    key_env: Option<String>,
}

const LICENSE_KEY_ENV: &str = "license.key_env";

/// Where the configured License is.
enum LicenseSource {
    Inline(coral_credentials::Secret),
    /// The environment variable that holds it.
    Env(String),
}

/// `text`, trimmed, as a secret: a leaked License unlocks Enterprise
/// (ADR 0038), so it never reaches a log. Refuses what holds no License or
/// is not one line.
fn license_secret(text: &str) -> Result<coral_credentials::Secret, &'static str> {
    let text = text.trim();
    if text.is_empty() {
        return Err("holds no License");
    }
    coral_credentials::Secret::new(text.to_owned())
        .map_err(|_not_one_line| "holds no License: a License is one line")
}

impl LicenseSettings {
    fn validate(self) -> Result<LicenseSource, AppError> {
        match (self.key, self.key_env) {
            (Some(key), None) => license_secret(&key)
                .map(LicenseSource::Inline)
                .map_err(|message| refused("license.key", message)),
            (None, Some(name)) => super::env_var_name(LICENSE_KEY_ENV, &name)
                .map(LicenseSource::Env)
                .map_err(|message| AppError::config(LICENSE_KEY_ENV, message)),
            (Some(_), Some(_)) => Err(refused(
                "license",
                "takes one License: set key or key_env, not both",
            )),
            (None, None) => Err(refused("license", "must set key or key_env, or be removed")),
        }
    }
}

#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RuntimeSettings {
    execution_heap_bytes: usize,
    compiler_heap_bytes: usize,
    /// Program output a Run Owner may hold applied but not yet recorded before
    /// Program requests wait for their chunk to commit. At least twice the
    /// chunk bytes cap; 8 MiB by default.
    #[serde(rename = "applied_output_cap_bytes")]
    applied_output_cap: usize,
    /// The Fan-out Limit: Invocations of one Run that may be `dispatching`
    /// at once. A ready call above it waits, `pending`, for an earlier one to
    /// end. At least 1; 32 by default.
    fan_out_limit: usize,
    /// The Shutdown Drain: seconds a shutting-down process lets its live
    /// Runs end on their own before it interrupts the rest. 0 interrupts at
    /// once; 30 by default.
    shutdown_drain_seconds: u64,
    /// The Run CPU Budget: CPU seconds one Run's Program may use. A Run's
    /// budget is the lower of it and the Run's time limit. Unset by default;
    /// refused on Windows.
    execution_cpu_seconds: Option<u64>,
    /// Whether Coral starts Run Executors as its own child processes. True
    /// by default.
    local_run_executors: bool,
    /// The most local Run Executors at once; past it work waits. At least
    /// 1; 4 by default.
    local_run_executor_limit: usize,
    /// The most Runs one local Run Executor holds at once. At least 1; 128
    /// by default.
    run_executor_max_runs_held: u32,
}

/// The resolved local Run Executors: how many, and how many Runs each holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct LocalRunExecutorSettings {
    pub(crate) limit: usize,
    pub(crate) max_runs_held: u32,
}

impl Default for RuntimeSettings {
    fn default() -> Self {
        let limits = coral_run::IsolateLimits::default();
        Self {
            execution_heap_bytes: limits.execution_heap_bytes(),
            compiler_heap_bytes: limits.compiler_heap_bytes(),
            applied_output_cap: coral_run::AppliedOutputCap::DEFAULT_BYTES,
            fan_out_limit: coral_run::FanOutLimit::DEFAULT_INVOCATIONS,
            shutdown_drain_seconds: coral_run::ShutdownDrain::DEFAULT_DURATION.as_secs(),
            execution_cpu_seconds: None,
            local_run_executors: true,
            local_run_executor_limit: 4,
            run_executor_max_runs_held: 128,
        }
    }
}

impl RuntimeSettings {
    /// The local Run Executors, or none when they are off. Off, and with no
    /// operator token (`dialed` false) to admit another, nothing could run
    /// a Program: that refuses to boot.
    fn local_run_executors(
        &self,
        dialed: bool,
    ) -> Result<Option<LocalRunExecutorSettings>, AppError> {
        if self.local_run_executor_limit == 0 {
            return Err(refused(
                "runtime.local_run_executor_limit",
                "must be at least 1",
            ));
        }
        if self.run_executor_max_runs_held == 0 {
            return Err(refused(
                "runtime.run_executor_max_runs_held",
                "must be at least 1",
            ));
        }
        match (self.local_run_executors, dialed) {
            (true, _) => Ok(Some(LocalRunExecutorSettings {
                limit: self.local_run_executor_limit,
                max_runs_held: self.run_executor_max_runs_held,
            })),
            (false, true) => Ok(None),
            (false, false) => Err(refused(
                "runtime.local_run_executors",
                format_args!(
                    "= false requires {RUN_EXECUTOR_TOKEN_ENV}: \
                     with neither, no Run Executor could run a Program"
                ),
            )),
        }
    }

    fn resolve(self) -> Result<coral_run::RuntimeLimits, AppError> {
        fn refuse(setting: &str, error: impl std::fmt::Display) -> AppError {
            AppError::config(setting, format!("{setting}: {error}"))
        }
        // The two heap sizes are judged together.
        let isolate_limits =
            coral_run::IsolateLimits::new(self.execution_heap_bytes, self.compiler_heap_bytes)
                .map_err(|error| refuse("runtime", error))?;
        let applied_output_cap = coral_run::AppliedOutputCap::new(self.applied_output_cap)
            .map_err(|error| refuse("runtime.applied_output_cap_bytes", error))?;
        let fan_out_limit = coral_run::FanOutLimit::new(self.fan_out_limit)
            .map_err(|error| refuse("runtime.fan_out_limit", error))?;
        let limits = coral_run::RuntimeLimits::new(
            isolate_limits,
            applied_output_cap,
            fan_out_limit,
            coral_run::ShutdownDrain::new(std::time::Duration::from_secs(
                self.shutdown_drain_seconds,
            )),
        );
        let Some(seconds) = self.execution_cpu_seconds else {
            return Ok(limits);
        };
        let budget = coral_run::RunCpuBudget::new(std::time::Duration::from_secs(seconds))
            .map_err(|error| refuse("runtime.execution_cpu_seconds", error))?;
        Ok(limits.with_cpu_budget(budget))
    }
}

/// The release check. Off unless the operator turns it on: it reads a
/// public feed on GitHub, which an air-gapped or private deployment may not
/// want to reach.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct UpdateSettings {
    check: bool,
}

/// Universal Search's settings.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct SearchSettings {
    /// Seconds between the end of one reconcile pass and the start of the
    /// next.
    reconcile_interval_seconds: u64,
    /// Whether Coral runs semantic search. On by default (ADR 0035). Off,
    /// Coral needs no pgvector, loads and downloads no model, makes no
    /// vectors, and leaves a vector table from an earlier start as it is.
    semantic_search_enabled: bool,
}

impl Default for SearchSettings {
    fn default() -> Self {
        Self {
            reconcile_interval_seconds: 5 * 60,
            semantic_search_enabled: true,
        }
    }
}

impl SearchSettings {
    fn reconcile_interval(&self) -> Result<std::time::Duration, AppError> {
        if self.reconcile_interval_seconds == 0 {
            return Err(refused(
                "search.reconcile_interval_seconds",
                "must be greater than 0",
            ));
        }
        Ok(std::time::Duration::from_secs(
            self.reconcile_interval_seconds,
        ))
    }
}

/// Peer listener settings, not an extensible listener registry.
#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ListenerSettings {
    grpc: GrpcSettings,
    mcp: RawMcpHttpSettings,
    auth: AuthListenerSettings,
    web: Option<WebSettings>,
    run_executor: RunExecutorListenerSettings,
}

/// The shortest operator token the Run Executor listener accepts.
const MIN_RUN_EXECUTOR_TOKEN_CHARS: usize = 32;

/// `[listeners.run_executor]`: where Run Executors dial, over plaintext h2c,
/// and the variables that hold the operator tokens they present.
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct RunExecutorListenerSettings {
    bind: SocketAddr,
    token_env: Option<TokenEnv>,
}

impl Default for RunExecutorListenerSettings {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            token_env: None,
        }
    }
}

/// One variable, or two during a rotation.
#[derive(Deserialize)]
#[serde(untagged)]
enum TokenEnv {
    One(String),
    Rotation(Vec<String>),
}

/// The resolved Run Executor listener.
pub(crate) struct RunExecutorListener {
    pub(crate) bind: SocketAddr,
    pub(crate) tokens: Vec<coral_run_executor::Token>,
}

/// The validated Run Executor listener: where it binds, and the variables
/// that hold its operator tokens.
struct RunExecutorListenerConfig {
    bind: SocketAddr,
    token_env: Vec<String>,
}

const RUN_EXECUTOR_TOKEN_ENV: &str = "listeners.run_executor.token_env";

impl RunExecutorListenerSettings {
    /// Checks the section without reading a token. Without `[auth]`
    /// (`authenticated` false) its bind must be loopback.
    fn validate(self, authenticated: bool) -> Result<RunExecutorListenerConfig, AppError> {
        const SETTING: &str = RUN_EXECUTOR_TOKEN_ENV;
        const BIND: &str = "listeners.run_executor.bind";
        let names = match self.token_env {
            None => Vec::new(),
            Some(TokenEnv::One(name)) => vec![name],
            Some(TokenEnv::Rotation(names)) if (1..=2).contains(&names.len()) => names,
            Some(TokenEnv::Rotation(_)) => {
                return Err(refused(
                    SETTING,
                    "must name one environment variable, or two during a rotation",
                ));
            }
        };
        if !is_loopback_ip(self.bind.ip()) {
            if !authenticated {
                return Err(refused_without_auth(BIND, "must be loopback"));
            }
            if names.is_empty() {
                return Err(refused(
                    BIND,
                    format_args!("off loopback requires {SETTING}"),
                ));
            }
        }
        let token_env = names
            .iter()
            .map(|configured| {
                super::env_var_name(SETTING, configured)
                    .map_err(|message| AppError::config(SETTING, message))
            })
            .collect::<Result<_, _>>()?;
        Ok(RunExecutorListenerConfig {
            bind: self.bind,
            token_env,
        })
    }
}

impl RunExecutorListenerConfig {
    /// Reads each operator token through `env`.
    fn resolve(
        &self,
        env: &dyn Fn(&str, &str) -> Result<String, AppError>,
    ) -> Result<RunExecutorListener, AppError> {
        const SETTING: &str = RUN_EXECUTOR_TOKEN_ENV;
        let refuse = |name: &str, message: String| AppError::invalid_secret(SETTING, name, message);
        let tokens = self
            .token_env
            .iter()
            .map(|name| {
                let value = env(SETTING, name)?;
                if value.chars().count() < MIN_RUN_EXECUTOR_TOKEN_CHARS {
                    return Err(refuse(
                        name,
                        format!(
                            "{SETTING} names `{name}`, whose value is under {MIN_RUN_EXECUTOR_TOKEN_CHARS} characters"
                        ),
                    ));
                }
                coral_run_executor::Token::new(value).map_err(|invalid| {
                    refuse(name, format!("{SETTING} names `{name}`: {invalid}"))
                })
            })
            .collect::<Result<_, _>>()?;
        Ok(RunExecutorListener {
            bind: self.bind,
            tokens,
        })
    }
}

#[derive(Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct GrpcSettings {
    pub(crate) bind: SocketAddr,
}

impl Default for GrpcSettings {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
        }
    }
}

/// The key path of the MCP listener's public origin.
const MCP_PUBLIC_URL: &str = "listeners.mcp.public_url";

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
struct RawMcpHttpSettings {
    bind: SocketAddr,
    public_url: Option<String>,
}

impl Default for RawMcpHttpSettings {
    fn default() -> Self {
        Self {
            bind: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            public_url: None,
        }
    }
}

impl RawMcpHttpSettings {
    fn resolve(&self, authorization_server: Option<&str>) -> Result<McpHttpServeConfig, AppError> {
        let Some(authorization_server) = authorization_server else {
            if !is_loopback_ip(self.bind.ip()) {
                return Err(refused_without_auth(
                    "listeners.mcp.bind",
                    "must be loopback",
                ));
            }
            if self.public_url.is_some() {
                return Err(refused_without_auth(
                    MCP_PUBLIC_URL,
                    "must be unset; it advertises OAuth metadata",
                ));
            }
            return Ok(McpHttpServeConfig::AuthDisabled {
                bind_addr: self.bind,
            });
        };

        let public_url = required_oauth_url(MCP_PUBLIC_URL, self.public_url.as_deref())?;
        let authorization_server = authorization_server.to_string();

        Ok(McpHttpServeConfig::Authenticated {
            bind_addr: self.bind,
            public_url,
            authorization_server,
        })
    }
}

/// Resolved MCP HTTP configuration prepared independently from gRPC startup.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum McpHttpServeConfig {
    /// MCP HTTP backed by an unauthenticated local client.
    ///
    /// Always loopback-only. The former non-loopback opt-out is rejected.
    /// The listener accepts only the loopback Host values.
    AuthDisabled {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
    },
    /// Session-authenticated MCP HTTP backed by per-session clients.
    Authenticated {
        /// Address for the MCP HTTP listener.
        bind_addr: SocketAddr,
        /// Canonical public origin of the per-Workspace MCP URLs, without a path.
        ///
        /// Each Workspace is served at `<public_url>/workspaces/<workspace_id>/mcp`, and
        /// that full per-workspace URL — never this base — is the OAuth
        /// resource and JWT audience of the sessions it admits. The URL names
        /// the stable Workspace identity, so no Workspace is configured here.
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
}

/// Validated settings for the mandatory MCP listener and optional OIDC authentication.
///
/// Nothing here is live. The gRPC server's bootstrap resolves configuration;
/// the process composition call site turns these settings into services and
/// owns their lifecycle, so a server builder never constructs a transport it
/// does not run.
pub struct ServeSettings {
    outbound_policy: coral_outbound::DestinationPolicy,
    mcp_oauth_client_bootstrap: coral_provider::account::ClientBootstrapPolicy,
    pub(super) mcp_http: McpHttpServeConfig,
    pub(super) mcp_interface: Option<ConfiguredMcpInterface>,
    auth_bind_addr: SocketAddr,
    pub(super) session_auth: Option<SessionAuthSettings>,
    human_interface: coral_mcp::HumanInterfaceUrl,
}

impl ServeSettings {
    /// Trusted human destination, required before any MCP work is accepted.
    #[must_use]
    pub fn human_interface(&self) -> &coral_mcp::HumanInterfaceUrl {
        &self.human_interface
    }

    /// Returns the destination permissions selected at process bootstrap.
    #[must_use]
    pub fn outbound_policy(&self) -> coral_outbound::DestinationPolicy {
        self.outbound_policy
    }

    /// Returns the policy for new automatic MCP Provider OAuth clients.
    #[must_use]
    pub fn mcp_oauth_client_bootstrap(&self) -> coral_provider::account::ClientBootstrapPolicy {
        self.mcp_oauth_client_bootstrap
    }

    /// Returns the mandatory MCP HTTP listener configuration.
    #[must_use]
    pub fn mcp_http(&self) -> &McpHttpServeConfig {
        &self.mcp_http
    }

    /// Returns the auth-support HTTP bind in either local or authenticated mode.
    #[must_use]
    pub fn auth_bind_addr(&self) -> SocketAddr {
        self.auth_bind_addr
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
    pub(super) mcp_workspace_urls: WorkspaceMcpUrls,
    web_public_url: Option<String>,
}

impl SessionAuthSettings {
    /// Reef's configured public HTTPS origin, when one is configured.
    #[must_use]
    pub fn web_public_url(&self) -> Option<&str> {
        self.web_public_url.as_deref()
    }

    /// Builds the verification of this instance's credentials. `clock` is
    /// the one the authorization server issues them by.
    pub(crate) fn principal_authentication(
        &self,
        database: std::sync::Arc<coral_db::Database>,
        clock: std::sync::Arc<dyn coral_db::Clock>,
    ) -> coral_auth::PrincipalAuthentication {
        coral_auth::PrincipalAuthentication::new(
            &self.session_tokens,
            coral_workspace::OrganizationModule::new(database.clone()),
            coral_workspace::WorkspaceModule::new(database),
            clock,
        )
    }

    /// Builds public Workspace credential verification over the current domain authority.
    /// `clock` is the one the authorization server issues credentials by.
    #[must_use]
    pub fn workspace_authentication(
        &self,
        database: std::sync::Arc<coral_db::Database>,
        clock: std::sync::Arc<dyn coral_db::Clock>,
    ) -> crate::WorkspaceAuthentication {
        crate::WorkspaceAuthentication::new(
            self.principal_authentication(database, clock),
            self.mcp_workspace_urls.clone(),
            self.settings.authorization_server().issuer().to_owned(),
        )
    }

    /// The instance's explicitly registered public audiences, canonicalized.
    ///
    /// MCP HTTP audiences are the per-workspace resource
    /// family under [`Self::mcp_workspace_urls`] — a set that cannot be
    /// enumerated here, so it is deliberately not in this list.
    #[must_use]
    pub fn public_audiences(&self) -> &[String] {
        &self.public_audiences
    }

    /// The mandatory per-workspace MCP resource family.
    #[must_use]
    pub fn mcp_workspace_urls(&self) -> &WorkspaceMcpUrls {
        &self.mcp_workspace_urls
    }

    /// Consumes these settings into the authorization server for the instance.
    ///
    /// Every explicitly registered public audience becomes an authorization
    /// resource clients may request a token for, and the per-workspace MCP
    /// family is always registered as a template rather
    /// than an enumeration.
    ///
    /// The server records its sign-ins in `database`, the instance's
    /// initialized deployment database, and dates them by `clock`, the
    /// instance's one clock.
    ///
    /// # Errors
    ///
    /// Returns [`AppError`] when the server cannot be built from these settings.
    pub fn into_authorization_server(
        self,
        policy: coral_outbound::DestinationPolicy,
        database: std::sync::Arc<coral_db::Database>,
        clock: std::sync::Arc<dyn coral_db::Clock>,
    ) -> Result<CoralAuthorizationServer, AppError> {
        // Operator-configured HTTP issuer support is a separate auth contract.
        // It must neither narrow explicit Provider permissions nor broaden the
        // policy passed to Provider/Run modules.
        let policy = if self
            .settings
            .authorization_server()
            .issuer()
            .starts_with("http://")
        {
            policy.with_loopback(coral_outbound::NetworkAccess::HttpAndHttps)
        } else {
            policy
        };
        let mut server = CoralAuthorizationServer::from_resolved_settings(
            self.settings,
            self.session_tokens,
            policy,
            database,
            clock,
        )
        .map_err(AppError::Auth)?;
        for audience in &self.public_audiences {
            server = server
                .with_authorization_resource(audience)
                .map_err(|message| AppError::Auth(AuthServerError::Config(message)))?;
        }
        Ok(server.with_workspace_resource_family(self.mcp_workspace_urls))
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
/// MCP is always present. This registry identifies explicit public resources
/// eligible for first-party Reef token issuance, not private method reachability.
/// Private methods validate the resulting mcp/reef audience independently.
///
/// An identifier names a surface, not an actor: either kind of caller can arrive
/// through any of them, so nothing here says what kind a caller is. Actor kind
/// comes from the authenticated principal instead.
fn public_surface_audiences(
    mcp_workspace_urls: &WorkspaceMcpUrls,
    allowed_audiences: &[String],
    web_public_url: Option<&str>,
) -> Result<Vec<String>, AppError> {
    let mut audiences = Vec::new();
    for (index, configured) in allowed_audiences.iter().enumerate() {
        let label = format!("auth.allowed_audiences[{index}]");
        let audience = required_oauth_url(&label, Some(configured))?;
        if audiences.iter().any(|existing| existing == &audience) {
            return Err(refused(
                &label,
                "duplicates another configured public surface audience",
            ));
        }
        // A web interface can explicitly occupy the same public origin as MCP
        // behind a reverse proxy. Its root is then a web resource; only the
        // /workspaces/{id}/mcp paths are MCP resources.
        if mcp_workspace_urls.is_base_audience(&audience)
            && web_public_url != Some(audience.as_str())
        {
            return Err(refused(
                &label,
                format_args!(
                    "duplicates {MCP_PUBLIC_URL}, which is the base of the \
                     per-workspace MCP resources and not an audience of its own"
                ),
            ));
        }
        // A token for `/mcp` must carry the Workspace its User picked; an
        // allowed audience would issue it one that names no Workspace.
        if mcp_workspace_urls.parse_endpoint(&audience)
            == Some(coral_mcp::McpEndpoint::Organization)
        {
            return Err(refused(
                &label,
                "is the Organization MCP Endpoint, whose tokens name the workspace \
                 picked at sign-in; it is not an audience of its own",
            ));
        }
        audiences.push(audience);
    }
    Ok(audiences)
}

fn required_oauth_url(label: &str, value: Option<&str>) -> Result<String, AppError> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(AppError::config(
            label,
            format!("configured [auth] requires {label}"),
        ));
    };
    CanonicalOauthUrl::parse(value)
        .map(CanonicalOauthUrl::into_identifier)
        .map_err(|error| refused(label, error))
}

#[cfg(test)]
pub(super) mod tests {
    use std::fs;
    use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

    use tempfile::TempDir;

    use super::{
        DeploymentConfig, Launcher, LoadedServerConfig, McpHttpServeConfig, ServeSettings,
    };
    use crate::auth::AuthServerError;
    use crate::bootstrap::AppError;
    use crate::state::AppStateLayout;

    const WEB: &str = "\n[listeners.web]\npublic_url = 'http://127.0.0.1:5173'\n";

    /// A config directory holding `raw` as its config file, beside the session
    /// signing key that [`authenticated`] configurations name.
    fn deployment(raw: &str) -> (TempDir, AppStateLayout) {
        let temp = TempDir::new().expect("temp dir");
        let layout = AppStateLayout::discover(Some(temp.path().to_path_buf())).expect("layout");
        fs::write(layout.config_file(), raw).expect("config file");
        fs::write(
            layout.config_dir().join("session.key"),
            coral_test_support::p256_pkcs8(),
        )
        .expect("session key");
        (temp, layout)
    }

    /// Captures `raw` as the deployment configuration.
    fn load(raw: &str) -> (TempDir, LoadedServerConfig) {
        let (temp, layout) = deployment(raw);
        (temp, LoadedServerConfig::load(&layout).expect("load"))
    }

    /// Validates `raw` as the whole configuration of a launch with `launcher`.
    fn validated_launched(raw: &str, launcher: &Launcher) -> Result<DeploymentConfig, AppError> {
        load(raw).1.validate(launcher, None)
    }

    /// Validates `raw` as the whole configuration.
    fn validated(raw: &str) -> Result<DeploymentConfig, AppError> {
        validated_launched(raw, &Launcher::default())
    }

    /// Validates `raw` beside the web origin every deployment names, so a
    /// test can give one section alone.
    fn section(raw: &str) -> Result<DeploymentConfig, AppError> {
        validated(&format!("{raw}{WEB}"))
    }

    /// Validates `raw` beside the web origin, in a launch whose binary adds
    /// `kinds` to the Secret Store kinds every build has.
    pub(in crate::bootstrap) fn validated_with(
        raw: &str,
        kinds: &coral_secret_store::SecretStoreKinds,
    ) -> Result<DeploymentConfig, AppError> {
        let launcher = Launcher {
            extensions: crate::Extensions {
                secret_store_kinds: kinds.clone(),
                ..crate::Extensions::default()
            },
            ..Launcher::default()
        };
        validated_launched(&format!("{raw}{WEB}"), &launcher)
    }

    /// Decodes `raw` and validates its auth settings as startup does, for
    /// the auth module's tests. A refusal is the message alone.
    pub(crate) fn auth_settings(raw: &str) -> Result<Option<crate::auth::AuthSettings>, String> {
        super::Document::parse(raw)
            .map_err(AppError::ConfigDecode)
            .and_then(super::ServerConfigFile::decode)
            .and_then(|mut file| file.auth_settings())
            .map_err(|error| match error {
                AppError::Auth(error) => error.to_string(),
                AppError::ConfigDecode(error) => {
                    format!("server configuration is invalid: {error}")
                }
                other => other.to_string(),
            })
    }

    /// The `[otel]` settings startup reads from `raw`, beside the web origin
    /// every deployment names, for the telemetry module's tests.
    pub(crate) fn telemetry(raw: &str) -> Result<crate::telemetry::TelemetryConfig, AppError> {
        section(raw).map(|config| config.telemetry().clone())
    }

    /// Resolves the serving settings `raw` configures, with deny/deny outbound defaults.
    fn serve(raw: &str) -> Result<ServeSettings, AppError> {
        serve_launched(raw, &Launcher::default())
    }

    fn serve_launched(raw: &str, launcher: &Launcher) -> Result<ServeSettings, AppError> {
        // The directory holds the signing key that resolution reads.
        let (_temp, config) = load(raw);
        config
            .validate(launcher, None)?
            .serve_settings(coral_outbound::DestinationPolicy::default())
    }

    /// Why the serving settings `raw` configures were refused.
    fn serve_refusal(raw: &str) -> AppError {
        match serve(raw) {
            Ok(_) => panic!("serving must fail for {raw}"),
            Err(error) => error,
        }
    }

    /// The refusal a test expects.
    #[derive(Debug, Clone, Copy)]
    enum Refused {
        /// A rule refused this setting.
        Setting(&'static str),
        /// The document did not decode, and the failure starts with this:
        /// the key path, then the reason.
        Decode(&'static str),
        /// The value of this environment variable was refused.
        Secret(&'static str),
    }

    #[track_caller]
    fn assert_refused(error: &AppError, expected: Refused, case: &str) {
        let refused = match (error, expected) {
            (AppError::Config { setting, .. }, Refused::Setting(expected)) => setting == expected,
            (AppError::ConfigDecode(decode), Refused::Decode(expected)) => {
                decode.to_string().starts_with(expected)
            }
            (AppError::InvalidSecret { variable, .. }, Refused::Secret(expected)) => {
                variable == expected
            }
            _ => false,
        };
        assert!(refused, "{case}: expected {expected:?}, got {error:?}");
    }

    fn mcp_http(raw: &str) -> Result<McpHttpServeConfig, AppError> {
        serve(raw).map(|settings| settings.mcp_http)
    }

    fn grpc_bind(raw: &str) -> Result<SocketAddr, AppError> {
        validated(raw).map(|config| config.grpc_bind())
    }

    /// Starts the authorization server `auth` composes and reports, for each
    /// resource, whether an authorize request naming it passes the resource
    /// check instead of being refused `invalid_target`.
    async fn accepted_resources(auth: super::SessionAuthSettings, resources: &[&str]) -> Vec<bool> {
        const CALLBACK: &str = "https://client.example.test/callback";
        let fixture = coral_test_support::TestDatabase::new(coral_db::migration_plan())
            .await
            .expect("client database");
        let server = auth
            .into_authorization_server(
                coral_outbound::DestinationPolicy::default(),
                fixture.database(),
                std::sync::Arc::new(coral_db::SystemClock),
            )
            .expect("authorization server")
            .start()
            .await
            .expect("start authorization server");
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .expect("HTTP client");
        // A registered client needs no metadata fetch, so the request reaches
        // the resource check.
        let registered: serde_json::Value = http
            .post(format!("{}/oauth/register", server.endpoint_uri()))
            .json(&serde_json::json!({ "redirect_uris": [CALLBACK] }))
            .send()
            .await
            .expect("register client")
            .json()
            .await
            .expect("registration");
        let client_id = registered
            .get("client_id")
            .and_then(serde_json::Value::as_str)
            .expect("client ID");
        let mut accepted = Vec::new();
        for resource in resources {
            let response = http
                .get(format!("{}/oauth/authorize", server.endpoint_uri()))
                .query(&[
                    ("client_id", client_id),
                    ("redirect_uri", CALLBACK),
                    ("response_type", "code"),
                    ("code_challenge_method", "S256"),
                    (
                        "code_challenge",
                        "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ",
                    ),
                    ("resource", resource),
                ])
                .send()
                .await
                .expect("authorize");
            let refused = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|location| location.to_str().ok())
                .is_some_and(|location| location.contains("error=invalid_target"));
            assert!(
                refused || response.status() == reqwest::StatusCode::OK,
                "{resource}: {}",
                response.status()
            );
            accepted.push(!refused);
        }
        server.shutdown().await.expect("shutdown");
        accepted
    }

    /// An `[auth]` deployment around `mcp_http`, with `auth_fields` in `[auth]`.
    fn authenticated(mcp_http: &str, auth_fields: &str) -> String {
        let web = if mcp_http.contains("[listeners.web]") {
            ""
        } else {
            WEB
        };
        format!(
            "{mcp_http}
{web}
[auth]
{auth_fields}

[listeners.auth]
public_url = 'https://AUTH.example.test/'

[auth.session]
signing_key_file = 'session.key'

[auth.provider]
issuer = 'https://accounts.example.test'
client_id = 'upstream-client'
client_secret = 'test-secret'
"
        )
    }

    #[test]
    fn the_outbound_section_selects_the_mcp_oauth_client_bootstrap_policy() {
        use coral_provider::account::ClientBootstrapPolicy;
        for authenticated_deployment in [false, true] {
            for (field, expected) in [
                ("", ClientBootstrapPolicy::Automatic),
                (
                    "[outbound]\nmcp_oauth_client_bootstrap = 'automatic'\n",
                    ClientBootstrapPolicy::Automatic,
                ),
                (
                    "[outbound]\nmcp_oauth_client_bootstrap = 'prefer-client-id-metadata-document'\n",
                    ClientBootstrapPolicy::PreferClientIdMetadataDocument,
                ),
                (
                    "[outbound]\nmcp_oauth_client_bootstrap = 'dynamic-client-registration-only'\n",
                    ClientBootstrapPolicy::DynamicClientRegistrationOnly,
                ),
            ] {
                let raw = if authenticated_deployment {
                    authenticated(
                        &format!(
                            "{field}[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n"
                        ),
                        "",
                    )
                } else {
                    format!("{field}{WEB}")
                };
                let policy = serve(&raw)
                    .expect("serving settings")
                    .mcp_oauth_client_bootstrap();
                assert_eq!(
                    policy, expected,
                    "authenticated={authenticated_deployment} {field:?}"
                );
            }
        }
        let error = serve_refusal(&format!(
            "[outbound]\nmcp_oauth_client_bootstrap = 'cimd'\n{WEB}"
        ));
        assert_refused(
            &error,
            Refused::Decode("outbound.mcp_oauth_client_bootstrap: unsupported value"),
            "an unknown policy",
        );
    }

    #[test]
    fn serving_requires_a_human_interface_before_accepting_work() {
        assert_refused(
            &serve_refusal(""),
            Refused::Setting("listeners.web.public_url"),
            "no web origin",
        );
    }

    #[test]
    fn launcher_destination_is_explicit_local_only_and_cannot_conflict_with_web_configuration() {
        let origin = "http://127.0.0.1:49123";
        let launcher = Launcher {
            local_web_url: Some(coral_mcp::HumanInterfaceUrl::parse(origin).unwrap()),
            ..Launcher::default()
        };
        let settings = serve_launched("", &launcher).unwrap();
        assert_eq!(settings.human_interface().as_str(), origin);
        for (raw, expected) in [
            (WEB.to_owned(), Refused::Setting("CORAL_LOCAL_WEB_URL")),
            (
                authenticated(
                    "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                    "",
                ),
                Refused::Setting("CORAL_LOCAL_WEB_URL"),
            ),
        ] {
            let Err(error) = serve_launched(&raw, &launcher) else {
                panic!("unsafe launcher configuration accepted: {raw}");
            };
            assert_refused(&error, expected, &raw);
        }
    }

    #[test]
    fn human_handoff_uses_the_explicit_web_origin_and_rejects_unsafe_destinations() {
        for (origin, expected) in [
            ("https://REEF.example.test/", "https://reef.example.test"),
            ("http://127.0.0.1:49152", "http://127.0.0.1:49152"),
        ] {
            let settings = serve(&format!("[listeners.web]\npublic_url = '{origin}'")).unwrap();
            assert_eq!(settings.human_interface().as_str(), expected);
        }
        for origin in [
            "/activity",
            "http://0.0.0.0:5173",
            "https://0.0.0.0:5173",
            "https://[::]:5173",
            "https://reef.example.test/run",
            "https://user:secret@reef.example.test",
            "https://reef.example.test?token=secret",
            "https://reef.example.test#fragment",
            "http://127.0.0.1:0",
        ] {
            assert!(
                serve(&format!("[listeners.web]\npublic_url = '{origin}'")).is_err(),
                "{origin}"
            );
        }
    }

    #[test]
    fn outbound_fields_override_composition_defaults_independently_of_identity() {
        use coral_outbound::{DestinationPolicy, NetworkAccess};
        for authenticated_deployment in [false, true] {
            for defaults in [DestinationPolicy::default(), super::LOCAL_OUTBOUND_DEFAULTS] {
                for (raw, loopback, private) in [
                    ("", None, None),
                    ("[outbound]\nloopback = 'deny'", Some(false), None),
                    ("[outbound]\nprivate_networks = 'https'", None, Some(false)),
                    (
                        "[outbound]\nloopback = 'http-and-https'\nprivate_networks = 'deny'",
                        Some(true),
                        Some(false),
                    ),
                ] {
                    let (_temp, layout) = deployment(&if authenticated_deployment {
                        authenticated(
                            &format!(
                                "{raw}\n[listeners.mcp]\npublic_url = 'https://mcp.example.test'"
                            ),
                            "",
                        )
                    } else {
                        format!("{raw}{WEB}")
                    });
                    let config = LoadedServerConfig::load(&layout)
                        .unwrap()
                        .validate(&Launcher::default(), None)
                        .unwrap();
                    let policy = config.outbound_policy(defaults);
                    assert_eq!(
                        config.serve_settings(defaults).unwrap().outbound_policy(),
                        policy
                    );
                    let permissive = defaults == super::LOCAL_OUTBOUND_DEFAULTS;
                    for (url, allowed) in [
                        ("http://localhost/", loopback.unwrap_or(permissive)),
                        ("http://10.0.0.1/", private.unwrap_or(permissive)),
                        ("https://169.254.169.254/", false),
                    ] {
                        assert_eq!(
                            policy.validate(&url.parse().unwrap()).is_ok(),
                            allowed,
                            "{raw}: {url}"
                        );
                    }
                    // The configuration is a snapshot: a later write to the
                    // file changes only the next capture.
                    fs::write(layout.config_file(), WEB).unwrap();
                    assert_eq!(config.outbound_policy(defaults), policy);
                    assert_eq!(
                        LoadedServerConfig::load(&layout)
                            .unwrap()
                            .validate(&Launcher::default(), None)
                            .unwrap()
                            .outbound_policy(defaults),
                        defaults
                    );
                }
            }
        }
        assert_eq!(
            DestinationPolicy::default(),
            DestinationPolicy::new(NetworkAccess::Deny, NetworkAccess::Deny)
        );
    }

    #[test]
    fn the_search_reconcile_interval_defaults_to_five_minutes() {
        for raw in ["", "[search]"] {
            assert_eq!(
                section(raw).expect(raw).search_reconcile_interval(),
                std::time::Duration::from_mins(5),
                "{raw:?}"
            );
        }
        assert_eq!(
            section("[search]\nreconcile_interval_seconds = 30")
                .unwrap()
                .search_reconcile_interval(),
            std::time::Duration::from_secs(30)
        );
    }

    #[test]
    fn the_vector_side_is_on_unless_the_operator_turns_it_off() {
        let on = coral_search::VectorStorage::On {
            dimension: coral_embed::MANIFEST.dimension,
        };
        let off = coral_search::VectorStorage::Off;
        for (raw, expected) in [
            ("", on),
            ("[search]", on),
            ("[search]\nsemantic_search_enabled = false", off),
            ("[search]\nsemantic_search_enabled = true", on),
        ] {
            assert_eq!(
                section(raw).expect(raw).vector_storage(),
                expected,
                "{raw:?}"
            );
        }
        let error = section("[search]\nsemantic_search_enabled = \"no\"")
            .expect_err("a string is not a boolean");
        assert_refused(
            &error,
            Refused::Decode("search.semantic_search_enabled: unsupported value"),
            "a string",
        );
    }

    #[test]
    fn the_release_check_is_off_unless_the_operator_turns_it_on() {
        for (raw, expected) in [
            ("", false),
            ("[updates]", false),
            ("[updates]\ncheck = false", false),
            ("[updates]\ncheck = true", true),
        ] {
            assert_eq!(section(raw).expect(raw).update_check(), expected, "{raw:?}");
        }
        let error = section("[updates]\nfeed = \"x\"").expect_err("unknown key");
        assert_refused(
            &error,
            Refused::Decode("updates.feed: unknown field"),
            "an unknown key",
        );
    }

    #[test]
    fn local_run_executors_are_on_by_default_and_off_only_beside_an_operator_token() {
        let off = "[runtime]\nlocal_run_executors = false\n";
        let token = "[listeners.run_executor]\ntoken_env = 'TOKEN'\n";
        let local = |limit, max_runs_held| super::LocalRunExecutorSettings {
            limit,
            max_runs_held,
        };
        for (raw, expected) in [
            (String::new(), Ok(Some(local(4, 128)))),
            (token.to_owned(), Ok(Some(local(4, 128)))),
            (
                "[runtime]\nlocal_run_executor_limit = 2\nrun_executor_max_runs_held = 16\n"
                    .to_owned(),
                Ok(Some(local(2, 16))),
            ),
            (format!("{off}{token}"), Ok(None)),
            (
                off.to_owned(),
                Err(Refused::Setting("runtime.local_run_executors")),
            ),
            (
                "[runtime]\nlocal_run_executor_limit = 0\n".to_owned(),
                Err(Refused::Setting("runtime.local_run_executor_limit")),
            ),
            (
                "[runtime]\nrun_executor_max_runs_held = 0\n".to_owned(),
                Err(Refused::Setting("runtime.run_executor_max_runs_held")),
            ),
        ] {
            let resolved = section(&raw).map(|config| config.local_run_executors());
            match (&resolved, expected) {
                (Ok(resolved), Ok(expected)) => assert_eq!(*resolved, expected, "{raw}"),
                (Err(error), Err(expected)) => assert_refused(error, expected, &raw),
                _ => panic!("{raw}: {resolved:?}"),
            }
        }
    }

    #[test]
    fn invalid_runtime_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[runtime]\nfan_out_limit = 0",
                Refused::Setting("runtime.fan_out_limit"),
            ),
            (
                "[runtime]\napplied_output_cap_bytes = 1",
                Refused::Setting("runtime.applied_output_cap_bytes"),
            ),
            (
                "[runtime]\nexecution_heap_bytes = 1",
                Refused::Setting("runtime"),
            ),
            (
                "[runtime]\nfan_out = 4",
                Refused::Decode("runtime.fan_out: unknown field"),
            ),
            (
                "[runtime]\nexecution_cpu_seconds = 0",
                Refused::Setting("runtime.execution_cpu_seconds"),
            ),
            // The compilation deadline is a fixed Program Authoring Limit.
            (
                "[runtime]\ncompilation_timeout_seconds = 30",
                Refused::Decode("runtime.compilation_timeout_seconds: unknown field"),
            ),
        ] {
            assert_refused(&section(raw).expect_err(raw), expected, raw);
        }
        assert_eq!(
            section("[runtime]\nfan_out_limit = 4\n")
                .expect("a limit of four")
                .runtime_limits()
                .fan_out_limit()
                .invocations(),
            4
        );
        assert_eq!(
            section("")
                .expect("default limits")
                .runtime_limits()
                .compilation_timeout(),
            std::time::Duration::from_secs(2),
            "production keeps the two-second compilation deadline"
        );
        for (raw, drain) in [
            ("", coral_run::ShutdownDrain::DEFAULT_DURATION),
            (
                "[runtime]\nshutdown_drain_seconds = 0\n",
                std::time::Duration::ZERO,
            ),
            (
                "[runtime]\nshutdown_drain_seconds = 90\n",
                std::time::Duration::from_secs(90),
            ),
        ] {
            assert_eq!(
                section(raw)
                    .expect("a drain")
                    .runtime_limits()
                    .shutdown_drain()
                    .duration(),
                drain,
                "{raw:?}"
            );
        }
    }

    /// Catches: a default budget that ends Runs, a set budget that is lost,
    /// or one that Windows accepts without a way to measure it.
    #[test]
    fn execution_cpu_seconds_sets_the_run_cpu_budget() {
        let budget = |raw| {
            section(raw).map(|config| {
                config
                    .runtime_limits()
                    .cpu_budget()
                    .map(coral_run::RunCpuBudget::cpu)
            })
        };
        assert_eq!(budget("").expect("default limits"), None);
        let set = budget("[runtime]\nexecution_cpu_seconds = 5\n");
        if cfg!(windows) {
            assert_refused(
                &set.expect_err("Windows measures no CPU time"),
                Refused::Setting("runtime.execution_cpu_seconds"),
                "a budget on Windows",
            );
        } else {
            assert_eq!(
                set.expect("a budget"),
                Some(std::time::Duration::from_secs(5))
            );
        }
    }

    #[test]
    fn invalid_search_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[search]\nreconcile_interval = 30",
                Refused::Decode("search.reconcile_interval: unknown field"),
            ),
            (
                "[search]\nreconcile_interval_seconds = 0",
                Refused::Setting("search.reconcile_interval_seconds"),
            ),
            (
                "[search]\nreconcile_interval_seconds = -1",
                Refused::Decode("search.reconcile_interval_seconds: "),
            ),
        ] {
            assert_refused(&section(raw).expect_err(raw), expected, raw);
        }
    }

    #[test]
    fn the_credential_key_env_is_named_only_when_configured() {
        for raw in ["", "[credentials]"] {
            assert_eq!(
                section(raw).expect(raw).credential_key_env(),
                None,
                "{raw:?}"
            );
        }
        assert_eq!(
            section("[credentials]\nkey_env = \" CORAL_CREDENTIALS_KEY \"")
                .unwrap()
                .credential_key_env(),
            Some("CORAL_CREDENTIALS_KEY")
        );
    }

    // Catches: a License read from the wrong place, or a `[license]` that
    // names two Licenses or none, which startup must refuse.
    #[test]
    fn the_license_is_the_one_key_or_variable_the_section_names() {
        let read_env = |setting: &str, name: &str| -> Result<String, AppError> {
            match name {
                "CORAL_TEST_LICENSE" => Ok(" v4.public.from-env\n".to_owned()),
                _ => Err(AppError::MissingSecret {
                    setting: setting.to_owned(),
                    variable: name.to_owned(),
                }),
            }
        };
        for (raw, expected) in [
            ("", None),
            (
                "[license]\nkey = \" v4.public.inline\\n\"",
                Some("v4.public.inline"),
            ),
            (
                "[license]\nkey_env = 'CORAL_TEST_LICENSE'",
                Some("v4.public.from-env"),
            ),
        ] {
            let license = section(raw).expect(raw).license(&read_env).expect(raw);
            assert_eq!(
                license.as_ref().map(coral_credentials::Secret::expose),
                expected,
                "{raw:?}"
            );
        }
        for (raw, expected) in [
            ("[license]", Refused::Setting("license")),
            (
                "[license]\nkey = 'v4.public.a'\nkey_env = 'CORAL_TEST_LICENSE'",
                Refused::Setting("license"),
            ),
            ("[license]\nkey = ' '", Refused::Setting("license.key")),
            (
                "[license]\nkey = \"v4.public.a\\nv4.public.b\"",
                Refused::Setting("license.key"),
            ),
            (
                "[license]\nkey_env = ''",
                Refused::Setting("license.key_env"),
            ),
            (
                "[license]\npath = 'license.txt'",
                Refused::Decode("license.path: unknown field"),
            ),
        ] {
            assert_refused(&section(raw).expect_err(raw), expected, raw);
        }
        let unset = section("[license]\nkey_env = 'CORAL_UNSET_LICENSE'")
            .expect("valid")
            .license(&read_env)
            .expect_err("unset variable");
        assert!(
            matches!(&unset, AppError::MissingSecret { variable, .. } if variable == "CORAL_UNSET_LICENSE"),
            "{unset:?}"
        );
    }

    #[test]
    fn invalid_credential_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[credentials]\nkey_env = \"\"",
                Refused::Setting("credentials.key_env"),
            ),
            (
                "[credentials]\nkey_env = \" \"",
                Refused::Setting("credentials.key_env"),
            ),
            (
                "[credentials]\nkey = \"CORAL_CREDENTIALS_KEY\"",
                Refused::Decode("credentials.key: unknown field"),
            ),
            (
                "[credentials]\nkey_env = 1",
                Refused::Decode("credentials.key_env: unsupported value"),
            ),
            (
                "[credentials]\ndisable_secret_stores = 'yes'",
                Refused::Decode("credentials.disable_secret_stores: unsupported value"),
            ),
            (
                "[credentials]\ndisable_secret_stores = true\n\
                 [[secret_stores]]\nname = 'env'\nkind = 'env'\nprefix = 'MYCO_'",
                Refused::Setting("credentials.disable_secret_stores"),
            ),
        ] {
            assert_refused(&section(raw).expect_err(raw), expected, raw);
        }
    }

    // Catches: Reef treating a deployment that defines no store yet like one
    // that turned Secret Stores off, which would hide how to define one.
    #[test]
    fn only_disable_secret_stores_turns_them_off() {
        for (raw, disabled) in [
            ("", false),
            ("[credentials]\ndisable_secret_stores = false", false),
            ("[credentials]\ndisable_secret_stores = true", true),
        ] {
            let stores = section(raw)
                .expect(raw)
                .secret_stores_from(
                    coral_outbound::DestinationPolicy::default(),
                    &(std::sync::Arc::new(|_: &str| Ok(None)) as coral_secret_store::EnvReader),
                )
                .expect(raw);
            assert_eq!(stores.is_disabled(), disabled, "{raw:?}");
            assert_eq!(stores.list().count(), 0, "{raw:?}");
        }
    }

    #[test]
    fn invalid_outbound_settings_fail_closed() {
        for (raw, expected) in [
            (
                "[outbound]\nallow_private_networks = 'true'",
                "outbound.allow_private_networks: unknown field",
            ),
            (
                "[outbound]\nallow_private_networks = 1",
                "outbound.allow_private_networks: unknown field",
            ),
            (
                "[outbound]\nunknown = true",
                "outbound.unknown: unknown field",
            ),
            (
                "[outbound]\nallow_private_networks = true",
                "outbound.allow_private_networks: unknown field",
            ),
            (
                "[outbound]\nallow_loopback = true",
                "outbound.allow_loopback: unknown field",
            ),
            (
                "[outbound]\nloopback = 'https-only'",
                "outbound.loopback: unsupported value",
            ),
            (
                "[outbound]\nprivate_networks = false",
                "outbound.private_networks: wanted string or table",
            ),
        ] {
            assert_refused(
                &section(raw).expect_err(raw),
                Refused::Decode(expected),
                raw,
            );
        }
    }

    /// Operators paste secrets under the wrong key, so every section's decode
    /// error names the key path and never echoes the rejected value.
    #[tokio::test]
    async fn decode_errors_name_the_key_path_without_the_value() {
        const SECRET: &str = "sk-live-0123456789abcdef";
        for (section, path) in [
            (
                format!("[outbound]\nloopback = '{SECRET}'"),
                "outbound.loopback",
            ),
            (
                format!("[listeners.mcp]\nbind = '{SECRET}'"),
                "listeners.mcp.bind",
            ),
            (
                format!("[auth]\nallowed_audiences = '{SECRET}'"),
                "auth.allowed_audiences",
            ),
            (format!("[otel]\nheaders = ['{SECRET}']"), "otel.headers"),
            (
                format!("[database]\nbackend = 'postgres://coral:{SECRET}@db.internal/coral'"),
                "database.backend",
            ),
            (format!("secret_stores = '{SECRET}'\n"), "secret_stores"),
        ] {
            let temp = TempDir::new().unwrap();
            fs::write(temp.path().join("config.toml"), format!("{section}{WEB}")).unwrap();
            let Err(error) = crate::ServerBuilder::new()
                .with_config_dir(temp.path())
                .prepare()
                .await
            else {
                panic!("{section}: startup must reject the configuration");
            };
            let error = error.to_string();
            assert!(error.contains(&format!("{path}: ")), "{section}: {error}");
            assert!(!error.contains(SECRET), "{section}: {error}");
        }
    }

    #[tokio::test]
    async fn fresh_deployments_accept_outbound_configuration() {
        for access in ["deny", "https", "http-and-https"] {
            let temp = TempDir::new().unwrap();
            coral_test_support::write_server_config(
                temp.path(),
                &format!("[outbound]\nloopback = '{access}'\nprivate_networks = '{access}'\n{WEB}"),
            );
            let server = crate::ServerBuilder::new()
                .with_config_dir(temp.path())
                .start()
                .await
                .unwrap();
            server.shutdown().await.unwrap();
        }
    }

    #[test]
    fn desktop_callback_default_preserves_explicit_binds_and_hosted_defaults() {
        for desktop in [false, true] {
            let launcher = Launcher {
                desktop,
                ..Launcher::default()
            };
            let default_bind = if desktop {
                "127.0.0.1:8779"
            } else {
                "127.0.0.1:0"
            };
            for (source, expected) in [
                (WEB.to_owned(), default_bind),
                (
                    format!("[listeners.grpc]\nbind = '127.0.0.1:0'\n{WEB}"),
                    default_bind,
                ),
                (
                    "listeners = { auth = {}, web = { public_url = 'http://127.0.0.1:5173' } }\n"
                        .to_owned(),
                    default_bind,
                ),
                (
                    format!("[listeners.auth]\nbind = '127.0.0.1:18879'\n{WEB}"),
                    "127.0.0.1:18879",
                ),
                (
                    format!("[listeners.auth]\nbind = '[::1]:18879'\n{WEB}"),
                    "[::1]:18879",
                ),
                (
                    format!("[listeners.auth]\nbind = '127.0.0.1:0'\n{WEB}"),
                    "127.0.0.1:0",
                ),
            ] {
                assert_eq!(
                    serve_launched(&source, &launcher).unwrap().auth_bind_addr(),
                    expected.parse::<SocketAddr>().unwrap(),
                    "desktop {desktop}, config: {source}"
                );
            }

            // Hosted deployments keep their ephemeral default under Desktop too.
            let mut settings = serve_launched(
                &authenticated(
                    "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                    "",
                ),
                &launcher,
            )
            .unwrap();
            assert_eq!(settings.auth_bind_addr(), "127.0.0.1:0".parse().unwrap());
            assert!(settings.take_session_auth().is_some());
        }
    }

    #[cfg(unix)]
    #[test]
    fn snapshot_reports_config_metadata_errors() {
        use std::os::unix::fs::PermissionsExt as _;

        let (_temp, layout) = deployment("");
        let permissions = fs::metadata(layout.config_dir()).unwrap().permissions();
        fs::set_permissions(layout.config_dir(), fs::Permissions::from_mode(0o000))
            .expect("hide config dir");
        let result = LoadedServerConfig::load(&layout);
        fs::set_permissions(layout.config_dir(), permissions).expect("restore config dir");
        assert!(matches!(result, Err(AppError::Io(_))));
    }

    #[test]
    fn defaults_both_listener_binds_to_ephemeral_loopback_with_explicit_web_destination() {
        assert_eq!(
            grpc_bind(WEB).expect("gRPC bind"),
            SocketAddr::from((Ipv4Addr::LOCALHOST, 0))
        );
        assert_eq!(
            mcp_http(WEB).expect("MCP config"),
            McpHttpServeConfig::AuthDisabled {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
            }
        );
    }

    #[test]
    fn loads_bind_address_alongside_supported_sections() {
        let bind = grpc_bind(&format!(
            "[listeners.grpc]\nbind = '127.0.0.2:14555'\n\n[listeners.mcp]\nbind = '[::1]:14556'\n\n[otel]\nservice_name = 'coral'\n{WEB}"
        ))
        .expect("gRPC bind");
        assert_eq!(bind.ip(), IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
        assert_eq!(bind.port(), 14555);
    }

    #[test]
    fn loads_local_mcp_listener_without_an_enable_switch() {
        let McpHttpServeConfig::AuthDisabled { bind_addr } = mcp_http(&format!(
            "[listeners.grpc]\nbind = '127.0.0.1:0'\n\n[listeners.mcp]\nbind = '[::1]:14556'\n{WEB}"
        ))
        .expect("MCP HTTP config") else {
            panic!("loopback MCP must be explicitly auth-disabled");
        };
        assert_eq!(bind_addr, SocketAddr::from((Ipv6Addr::LOCALHOST, 14556)));
    }

    /// Without `[auth]` the MCP listener stays on loopback and advertises no
    /// authenticated public URL.
    #[test]
    fn auth_disabled_mcp_http_rejects_a_public_bind_and_a_public_url() {
        for (raw, setting) in [
            (
                "[listeners.mcp]\nbind = '0.0.0.0:14556'\n",
                "listeners.mcp.bind",
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://coral.example/mcp'\n",
                "listeners.mcp.public_url",
            ),
        ] {
            assert_refused(
                &mcp_http(raw).expect_err(raw),
                Refused::Setting(setting),
                raw,
            );
        }
    }

    #[test]
    fn authenticated_listeners_canonicalize_public_audiences_in_surface_order() {
        let companions = serve(&authenticated(
            "[listeners.mcp]\nbind = '127.0.0.1:0'\npublic_url = 'https://MCP.example.test/'\n",
            "allowed_audiences = ['https://CORAL-WEB.example.test/']",
        ))
        .expect("listener settings");
        assert_eq!(
            companions.mcp_http,
            McpHttpServeConfig::Authenticated {
                bind_addr: SocketAddr::from((Ipv4Addr::LOCALHOST, 0)),
                public_url: "https://mcp.example.test".to_string(),
                authorization_server: "https://auth.example.test".to_string(),
            }
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
            ["https://coral-web.example.test".to_string()]
        );
        assert_eq!(
            session_auth.mcp_workspace_urls.base().identifier(),
            "https://mcp.example.test"
        );
    }

    /// MCP needs no extra audience list: its workspace family is always
    /// registered, but the base URL is not an exact audience of its own.
    #[tokio::test]
    async fn authenticated_mcp_only_config_serves_only_the_workspace_family() {
        let companions = serve(&authenticated(
            "[listeners.mcp]\nbind = '127.0.0.1:0'\npublic_url = 'https://mcp.example.test/'\n",
            "",
        ))
        .expect("MCP settings");
        let session_auth = companions.session_auth.expect("session auth");
        assert_eq!(
            session_auth.public_audiences,
            [] as [std::string::String; 0]
        );

        // The family must be registered even though it enumerates to nothing,
        // or every real MCP login is refused invalid_target. The base is not a
        // resource at all.
        assert_eq!(
            accepted_resources(
                session_auth,
                &[
                    "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp",
                    "https://mcp.example.test",
                ],
            )
            .await,
            [true, false]
        );
    }

    #[test]
    fn authenticated_mcp_rejects_non_origin_public_urls() {
        for public_url in [
            "https://mcp.example.test/mcp",
            "https://mcp.example.test/prefix/",
        ] {
            let error = serve_refusal(&authenticated(
                &format!("[listeners.mcp]\npublic_url = '{public_url}'\n"),
                "",
            ));
            assert_refused(
                &error,
                Refused::Setting("listeners.mcp.public_url"),
                public_url,
            );
        }
    }

    /// Configured `[auth]` always needs `listeners.mcp.public_url`: no extra
    /// audience replaces it, and omitting the section or giving only a bind
    /// cannot disable the listener or bypass the requirement.
    #[test]
    fn configured_auth_requires_the_mcp_public_url() {
        for (mcp_http, auth_fields) in [
            (
                "",
                "allowed_audiences = ['https://CORAL-WEB.example.test/']",
            ),
            ("", "allowed_audiences = []"),
            ("[listeners.mcp]\nbind = '127.0.0.1:0'\n", ""),
        ] {
            let error = serve_refusal(&authenticated(mcp_http, auth_fields));
            assert_refused(
                &error,
                Refused::Setting("listeners.mcp.public_url"),
                &format!("{mcp_http}{auth_fields}"),
            );
        }
    }

    #[tokio::test]
    async fn fresh_web_configuration_registers_the_web_resource_even_on_the_mcp_origin() {
        for web_origin in ["https://web.example.test", "https://mcp.example.test"] {
            let (_temp, layout) = deployment(&authenticated(
                &format!(
                    "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n\
                     [listeners.web]\npublic_url = '{web_origin}'"
                ),
                "",
            ));
            // This crosses the unmarked-state fence too: [listeners.web] is bootstrap
            // configuration, not retired state that needs manual removal.
            let prepared = crate::ServerBuilder::new()
                .with_config_dir(layout.config_dir())
                .prepare()
                .await
                .expect("fresh hosted configuration");
            let mut serving = prepared.serve_settings().expect("serve settings");
            let auth = serving.take_session_auth().expect("authenticated");
            assert_eq!(auth.public_audiences(), [web_origin]);
            assert_eq!(
                accepted_resources(
                    auth,
                    &[
                        web_origin,
                        "https://mcp.example.test/workspaces/01234567-89ab-4cde-8fab-0123456789ab/mcp",
                    ],
                )
                .await,
                [true, true],
                "{web_origin}"
            );
        }
    }

    #[test]
    fn allowed_audiences_reject_invalid_and_duplicate_entries() {
        let cases = [
            (
                "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                "allowed_audiences = ['https://coral-web.example.test/?tenant=one']",
                Refused::Setting("auth.allowed_audiences[0]"),
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://MCP.example.test/'\n",
                "allowed_audiences = ['https://mcp.example.test']",
                Refused::Setting("auth.allowed_audiences[0]"),
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                "allowed_audiences = ['https://MCP.example.test/mcp']",
                Refused::Setting("auth.allowed_audiences[0]"),
            ),
            (
                "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n",
                "allowed_audiences = ['https://CORAL-WEB.example.test/', 'https://coral-web.example.test']",
                Refused::Setting("auth.allowed_audiences[1]"),
            ),
        ];

        for (mcp_http, auth_fields, expected) in cases {
            let error = serve_refusal(&authenticated(mcp_http, auth_fields));
            assert_refused(&error, expected, auth_fields);
        }
    }

    /// Validation checks `[auth]` without touching what it points at, so an
    /// instance whose signing key is missing validates and only fails when
    /// the settings behind it are actually resolved.
    #[test]
    fn resolving_serve_settings_fails_on_a_missing_signing_key() {
        let (_temp, layout) = deployment(&authenticated(MCP, ""));
        let key_path = layout.config_dir().join("session.key");
        fs::remove_file(&key_path).expect("remove session key");

        let config = LoadedServerConfig::load(&layout)
            .expect("load")
            .validate(&Launcher::default(), None)
            .expect("the configuration is valid without the key it names");

        // `ServeSettings` carries key material and so implements no `Debug`,
        // which rules out `expect_err` here.
        let Err(AppError::Auth(AuthServerError::Config(message))) =
            config.serve_settings(coral_outbound::DestinationPolicy::default())
        else {
            panic!("resolution must still fail on the missing signing key");
        };
        assert!(
            message.contains("failed to read signing_key_file")
                && message.contains(&key_path.display().to_string()),
            "{message}"
        );
    }

    /// Binding a public surface off loopback is the operator's call — the risk is
    /// documented rather than gated — so this must resolve without ceremony. The
    /// auth-disabled loopback restriction is a separate rule and still applies.
    #[test]
    fn authenticated_mcp_http_accepts_a_remote_bind() {
        assert!(matches!(
            mcp_http(&authenticated(
                "[listeners.mcp]\nbind = '0.0.0.0:14556'\npublic_url = 'https://mcp.example.test'\n",
                "",
            )),
            Ok(McpHttpServeConfig::Authenticated { .. })
        ));
    }

    const MCP: &str = "[listeners.mcp]\npublic_url = 'https://mcp.example.test'\n";
    const PUBLIC_RUN_EXECUTORS: &str = "[listeners.run_executor]\nbind = '0.0.0.0:50052'\n";

    /// Resolves `[listeners.run_executor]` from `raw`, with `env` as the
    /// process environment.
    fn run_executor_listener(
        raw: &str,
        env: &[(&str, &str)],
    ) -> Result<super::RunExecutorListener, AppError> {
        let config = if raw.contains("[listeners.web]") {
            validated(raw)
        } else {
            section(raw)
        }?;
        config.run_executor_listener(&|setting, name| {
            let value = env.iter().find(|(key, _)| *key == name);
            value
                .map(|(_, value)| (*value).to_owned())
                .ok_or_else(|| AppError::MissingSecret {
                    setting: setting.to_owned(),
                    variable: name.to_owned(),
                })
        })
    }

    #[test]
    fn the_run_executor_listener_admits_one_operator_token_or_two_during_a_rotation() {
        let token = "t".repeat(32);
        let env = [("TOKEN_A", token.as_str()), ("TOKEN_B", token.as_str())];
        let loopback = SocketAddr::from((Ipv4Addr::LOCALHOST, 0));
        let public = SocketAddr::from((Ipv4Addr::UNSPECIFIED, 50052));
        let remote = authenticated(
            &format!("{MCP}{PUBLIC_RUN_EXECUTORS}token_env = 'TOKEN_A'\n"),
            "",
        );
        for (raw, bind, tokens) in [
            (String::new(), loopback, 0),
            (
                "[listeners.run_executor]\ntoken_env = 'TOKEN_A'\n".to_owned(),
                loopback,
                1,
            ),
            (
                "[listeners.run_executor]\ntoken_env = ['TOKEN_A', 'TOKEN_B']\n".to_owned(),
                loopback,
                2,
            ),
            (remote, public, 1),
        ] {
            let listener = run_executor_listener(&raw, &env).expect(&raw);
            assert_eq!(
                (listener.bind, listener.tokens.len()),
                (bind, tokens),
                "{raw}"
            );
        }
    }

    #[test]
    fn the_run_executor_listener_refuses_to_boot_without_a_usable_token_or_off_loopback_without_auth()
     {
        let short = "t".repeat(31);
        let spaced = format!("{} t", "t".repeat(31));
        let env = [("SHORT", short.as_str()), ("SPACED", spaced.as_str())];
        let section = "[listeners.run_executor]\n";
        for (raw, refusal) in [
            (
                PUBLIC_RUN_EXECUTORS.to_owned(),
                Refused::Setting("listeners.run_executor.bind"),
            ),
            (
                authenticated(&format!("{MCP}{PUBLIC_RUN_EXECUTORS}"), ""),
                Refused::Setting("listeners.run_executor.bind"),
            ),
            (
                format!("{section}token_env = 'SHORT'\n"),
                Refused::Secret("SHORT"),
            ),
            (
                format!("{section}token_env = 'SPACED'\n"),
                Refused::Secret("SPACED"),
            ),
            (
                format!("{section}token_env = []\n"),
                Refused::Setting("listeners.run_executor.token_env"),
            ),
            (
                format!("{section}token_env = ['SHORT', 'SPACED', 'MISSING']\n"),
                Refused::Setting("listeners.run_executor.token_env"),
            ),
        ] {
            let Err(error) = run_executor_listener(&raw, &env) else {
                panic!("{raw}: the listener must not resolve");
            };
            assert_refused(&error, refusal, &raw);
        }
    }
}
