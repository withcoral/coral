//! Secret-free authentication settings and derived credential authority.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// Validates the already-supported public client identifier without normalizing it.
/// # Errors
/// Rejects blank, control-bearing, or oversized identifiers; valid bytes stay exact.
pub fn validate_oauth_client_id(value: &str) -> Result<(), crate::Error> {
    if value.trim().is_empty() || value.len() > 1024 || value.chars().any(char::is_control) {
        return Err(crate::Error::InvalidInput(
            "public OAuth client ID must be nonblank, control-free, and at most 1024 bytes".into(),
        ));
    }
    Ok(())
}

/// Authentication facts, independent of the destinations derived during materialization.
/// MCP issuer/resource facts come from discovery, never editable metadata overrides.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "method", rename_all = "snake_case", deny_unknown_fields)]
pub enum AuthenticationConfiguration {
    /// No upstream credential.
    None,
    /// An opaque HTTP bearer credential.
    Bearer,
    /// Credential Field values, and constants, sent in named HTTP headers.
    HeaderCredentials {
        /// Lowercase header name to what Coral sends in it.
        headers: BTreeMap<String, HeaderBinding>,
    },
    /// HTTP Basic credentials, which Coral encodes so no one enters an encoded pair.
    BasicAuth {
        /// Sent as the username.
        username: CredentialSource,
        /// Sent as the password.
        password: CredentialSource,
    },
    /// Credential Field values, and constants, sent as query parameters. OpenAPI only;
    /// the values join the URL only as the request leaves, never a stored or shown URL.
    QueryCredentials {
        /// Query parameter name to what Coral sends in it.
        parameters: BTreeMap<String, CredentialSource>,
    },
    /// Device authorization with host-owned token polling.
    #[serde(rename = "oauth_device_code")]
    OAuthDeviceCode {
        /// Device authorization endpoint.
        device_authorization_url: String,
        /// Token endpoint.
        token_url: String,
        /// Requested scopes.
        scopes: Vec<String>,
        /// Separator used only when requesting scopes; tokens remain independent.
        #[serde(default)]
        scope_format: OAuthScopeFormat,
        /// Canonical RFC 8707 resource, if discovered for MCP.
        #[serde(default)]
        resource: Option<String>,
        /// Authorization server issuer, if discovered for MCP.
        #[serde(default)]
        issuer: Option<String>,
        /// Public or confidential client settings; PKCE does not apply.
        client: Option<OAuthClientPolicy>,
    },
    /// Authorization-code OAuth with a public client and mandatory S256 PKCE.
    OAuthAuthorizationCode {
        /// Authorization endpoint.
        authorization_url: String,
        /// Token endpoint.
        token_url: String,
        /// Exact sorted requested scope set.
        scopes: Vec<String>,
        /// Separator used only when requesting scopes; tokens remain independent.
        #[serde(default)]
        scope_format: OAuthScopeFormat,
        /// Canonical RFC 8707 resource, if discovered for MCP.
        resource: Option<String>,
        /// Authorization server issuer, if discovered for MCP.
        issuer: Option<String>,
        /// Client ownership evidence.
        client: Option<OAuthClientPolicy>,
    },
}

/// Encoding of a requested scope list, independent of its exact token set.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthScopeFormat {
    /// Standard OAuth space-separated tokens.
    #[default]
    Space,
    /// Service-specific comma-separated tokens.
    Comma,
}

impl OAuthScopeFormat {
    /// Validates and canonicalizes independent scope tokens for this encoding.
    /// # Errors
    /// Rejects invalid tokens, excessive lists, or commas ambiguous in comma encoding.
    pub fn normalize(self, values: &[String]) -> Result<Vec<String>, crate::Error> {
        if values.len() > 100
            || values.iter().any(|scope| {
                scope.is_empty()
                    || scope.len() > 256
                    || !scope
                        .bytes()
                        .all(|byte| matches!(byte, 0x21 | 0x23..=0x5b | 0x5d..=0x7e))
                    || (self == Self::Comma && scope.contains(','))
            })
        {
            return Err(crate::Error::InvalidInput(
                "invalid requested scopes".into(),
            ));
        }
        let mut scopes = values.to_vec();
        scopes.sort();
        scopes.dedup();
        Ok(scopes)
    }

    /// Encodes tokens for an authorization or device authorization request.
    #[must_use]
    pub fn encode(self, scopes: &[String]) -> String {
        scopes.join(match self {
            Self::Space => " ",
            Self::Comma => ",",
        })
    }
}

/// Which authentication method a configuration uses, without its settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AuthenticationKind {
    /// No upstream credential.
    None,
    /// An opaque HTTP bearer credential.
    Bearer,
    /// Credential Field values, and constants, sent in named HTTP headers.
    HeaderCredentials,
    /// A username and password, sent as HTTP Basic.
    BasicAuth,
    /// Credential Field values, and constants, sent as query parameters.
    QueryCredentials,
    /// Device-code OAuth.
    OAuthDeviceCode,
    /// Authorization-code OAuth.
    OAuthAuthorizationCode,
}

impl AuthenticationKind {
    /// The method's name, as stored configuration spells it.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Bearer => "bearer",
            Self::HeaderCredentials => "header_credentials",
            Self::BasicAuth => "basic_auth",
            Self::QueryCredentials => "query_credentials",
            Self::OAuthDeviceCode => "oauth_device_code",
            Self::OAuthAuthorizationCode => "oauth_authorization_code",
        }
    }
}

/// What Coral sends in one credential header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum HeaderBinding {
    /// One Credential Field's value, after an exact prefix such as `Token token=`.
    Field {
        /// Credential Field name.
        field: String,
        /// Literal text sent before the value; empty for none.
        #[serde(default, skip_serializing_if = "String::is_empty")]
        prefix: String,
    },
    /// A constant the Upstream Service requires beside the credentials.
    Literal {
        /// Exact header value.
        value: String,
    },
}

/// Where one part of a credential comes from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "source", rename_all = "snake_case", deny_unknown_fields)]
pub enum CredentialSource {
    /// One Credential Field's value.
    Field {
        /// Credential Field name.
        field: String,
    },
    /// A constant, such as Mailgun's Basic username `api`; may be empty.
    Literal {
        /// Exact value.
        value: String,
    },
}

impl AuthenticationConfiguration {
    /// Whether any credential part is a non-empty literal value rather than a
    /// Credential Field a person fills in.
    #[must_use]
    pub fn has_literal_values(&self) -> bool {
        // An empty literal, such as a Basic password that is absent, carries no value.
        let literal = |source: &CredentialSource| matches!(source, CredentialSource::Literal { value } if !value.is_empty());
        match self {
            Self::HeaderCredentials { headers } => headers.values().any(
                |binding| matches!(binding, HeaderBinding::Literal { value } if !value.is_empty()),
            ),
            Self::BasicAuth { username, password } => literal(username) || literal(password),
            Self::QueryCredentials { parameters } => parameters.values().any(literal),
            Self::None
            | Self::Bearer
            | Self::OAuthDeviceCode { .. }
            | Self::OAuthAuthorizationCode { .. } => false,
        }
    }

    /// Returns the requested tokens and their outgoing encoding for either OAuth flow.
    #[must_use]
    pub fn oauth_scopes(&self) -> Option<(&[String], OAuthScopeFormat)> {
        match self {
            Self::OAuthAuthorizationCode {
                scopes,
                scope_format,
                ..
            }
            | Self::OAuthDeviceCode {
                scopes,
                scope_format,
                ..
            } => Some((scopes, *scope_format)),
            _ => None,
        }
    }

    /// Replaces only the requested scopes, preserving discovered endpoint and resource facts.
    #[must_use]
    pub fn with_oauth_scopes(mut self, values: Vec<String>, format: OAuthScopeFormat) -> Self {
        if let Self::OAuthAuthorizationCode {
            scopes,
            scope_format,
            ..
        }
        | Self::OAuthDeviceCode {
            scopes,
            scope_format,
            ..
        } = &mut self
        {
            *scopes = values;
            *scope_format = format;
        }
        self
    }

    /// The method this configuration uses.
    #[must_use]
    pub const fn kind(&self) -> AuthenticationKind {
        match self {
            Self::None => AuthenticationKind::None,
            Self::Bearer => AuthenticationKind::Bearer,
            Self::HeaderCredentials { .. } => AuthenticationKind::HeaderCredentials,
            Self::BasicAuth { .. } => AuthenticationKind::BasicAuth,
            Self::QueryCredentials { .. } => AuthenticationKind::QueryCredentials,
            Self::OAuthDeviceCode { .. } => AuthenticationKind::OAuthDeviceCode,
            Self::OAuthAuthorizationCode { .. } => AuthenticationKind::OAuthAuthorizationCode,
        }
    }

    /// The Credential Fields an account enters for this method. A bearer token is the
    /// one field `token`; OAuth obtains its tokens rather than asking for them.
    #[must_use]
    pub fn credential_fields(&self) -> BTreeSet<&str> {
        match self {
            Self::None | Self::OAuthDeviceCode { .. } | Self::OAuthAuthorizationCode { .. } => {
                BTreeSet::new()
            }
            Self::Bearer => BTreeSet::from(["token"]),
            Self::HeaderCredentials { headers } => headers
                .values()
                .filter_map(|binding| match binding {
                    HeaderBinding::Field { field, .. } => Some(field.as_str()),
                    HeaderBinding::Literal { .. } => None,
                })
                .collect(),
            Self::BasicAuth { username, password } => [username, password]
                .into_iter()
                .filter_map(|part| match part {
                    CredentialSource::Field { field } => Some(field.as_str()),
                    CredentialSource::Literal { .. } => None,
                })
                .collect(),
            Self::QueryCredentials { parameters } => parameters
                .values()
                .filter_map(|part| match part {
                    CredentialSource::Field { field } => Some(field.as_str()),
                    CredentialSource::Literal { .. } => None,
                })
                .collect(),
        }
    }

    /// What a person is asked for, field by field: the owner's presentation where it
    /// exists, else the field name. A bearer token is one masked field, "Token".
    #[must_use]
    pub fn credential_prompts(
        &self,
        presented: &BTreeMap<String, crate::CredentialField>,
    ) -> BTreeMap<String, crate::CredentialField> {
        self.credential_fields()
            .into_iter()
            .map(|name| {
                let field =
                    presented
                        .get(name)
                        .cloned()
                        .unwrap_or_else(|| crate::CredentialField {
                            label: if name == "token" {
                                "Token".into()
                            } else {
                                name.into()
                            },
                            description: String::new(),
                            sensitive: true,
                        });
                (name.to_owned(), field)
            })
            .collect()
    }
}

/// Exact credential authority retained with a Provider Generation.
/// Construction normalizes the destination set; contracts cannot nest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProviderCredentialContract {
    authentication: AuthenticationConfiguration,
    base_urls: Vec<String>,
}

impl From<AuthenticationConfiguration> for ProviderCredentialContract {
    fn from(authentication: AuthenticationConfiguration) -> Self {
        authentication.at_destinations(Vec::new())
    }
}

impl AuthenticationConfiguration {
    /// Binds authentication to the normalized destinations derived by the input profile.
    /// Anonymous configuration never acquires account requirements.
    #[must_use]
    pub fn at_destinations(self, mut base_urls: Vec<String>) -> ProviderCredentialContract {
        if self == Self::None {
            base_urls.clear();
        }
        base_urls.sort();
        base_urls.dedup();
        ProviderCredentialContract {
            authentication: self,
            base_urls,
        }
    }

    /// Stamps ownership only when an OAuth application has not yet been selected.
    #[must_use]
    pub fn with_oauth_client(self, policy: &OAuthClientPolicy) -> Self {
        if self.oauth_client().is_some() {
            self
        } else {
            self.with_client_policy(Some(policy.clone()))
        }
    }

    /// Replaces OAuth application policy after explicit owner configuration.
    #[must_use]
    pub fn with_client_policy(mut self, policy: Option<OAuthClientPolicy>) -> Self {
        if let Self::OAuthAuthorizationCode { client, .. } | Self::OAuthDeviceCode { client, .. } =
            &mut self
        {
            *client = policy;
        }
        self
    }

    /// Returns the OAuth application's ownership evidence.
    #[must_use]
    pub fn oauth_client(&self) -> Option<&OAuthClientPolicy> {
        match self {
            Self::OAuthAuthorizationCode { client, .. } | Self::OAuthDeviceCode { client, .. } => {
                client.as_ref()
            }
            _ => None,
        }
    }
}

impl ProviderCredentialContract {
    /// Credential-free authority, independent of destination changes.
    pub const NONE: Self = Self {
        authentication: AuthenticationConfiguration::None,
        base_urls: Vec::new(),
    };

    /// Returns the authentication requirements without their derived destinations.
    #[must_use]
    pub const fn authentication(&self) -> &AuthenticationConfiguration {
        &self.authentication
    }

    /// Returns the exact normalized destination set, including base paths.
    #[must_use]
    pub fn destinations(&self) -> &[String] {
        &self.base_urls
    }

    /// Replaces the derived destination set rather than wrapping an existing contract.
    #[must_use]
    pub fn at_destinations(self, base_urls: Vec<String>) -> Self {
        self.authentication.at_destinations(base_urls)
    }

    /// Stamps OAuth application ownership without changing destinations.
    #[must_use]
    pub fn with_oauth_client(self, policy: &OAuthClientPolicy) -> Self {
        Self {
            authentication: self.authentication.with_oauth_client(policy),
            ..self
        }
    }

    /// Replaces OAuth application policy without changing destinations.
    #[must_use]
    pub fn with_client_policy(self, policy: Option<OAuthClientPolicy>) -> Self {
        Self {
            authentication: self.authentication.with_client_policy(policy),
            ..self
        }
    }

    /// Returns the OAuth application's ownership evidence.
    #[must_use]
    pub fn oauth_client(&self) -> Option<&OAuthClientPolicy> {
        self.authentication.oauth_client()
    }

    /// Encodes canonical, secret-free compatibility evidence.
    /// # Errors
    /// Returns a catalog error if encoding fails.
    pub fn to_storage(&self) -> Result<String, crate::Error> {
        if *self == Self::NONE {
            return Ok("none".into());
        }
        serde_json::to_string(self)
            .map_err(|_invalid| crate::Error::CorruptData("invalid credential contract".into()))
    }

    /// Decodes retained compatibility evidence.
    /// # Errors
    /// Returns a catalog error for malformed evidence.
    pub fn from_storage(value: &str) -> Result<Self, crate::Error> {
        if value == "none" {
            return Ok(Self::NONE);
        }
        serde_json::from_str(value)
            .map_err(|_invalid| crate::Error::CorruptData("invalid credential contract".into()))
    }
}

/// How the OAuth client identity was established and which identifier it uses.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthClientPolicy {
    /// Client bootstrap method.
    pub bootstrap: OAuthClientBootstrap,
    /// Exact client identifier the authorization server knows.
    pub client_id: String,
    /// Authentication used at the token endpoint; never contains a secret.
    #[serde(default)]
    pub authentication: OAuthClientAuthenticationMethod,
    /// Authorization-code protection selected by the owner.
    #[serde(default)]
    pub pkce: OAuthPkcePolicy,
    /// Non-secret upstream authorization options.
    #[serde(default)]
    pub authorization_parameters: std::collections::BTreeMap<String, String>,
}

/// Token endpoint authentication for a Provider's application.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthClientAuthenticationMethod {
    /// Public client without a shared secret.
    #[default]
    None,
    /// Form-encoded identifier and secret in the HTTP Basic header.
    ClientSecretBasic,
    /// Identifier and secret in the token request form.
    ClientSecretPost,
}

/// PKCE policy; disabling it requires an HTTP confidential client.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthPkcePolicy {
    /// Generate a fresh S256 challenge for each authorization.
    #[default]
    S256,
    /// Explicit exception for an HTTP Provider using client authentication.
    Disabled,
}

impl OAuthClientPolicy {
    /// Constructs the existing public-client policy with S256 protection.
    #[must_use]
    pub fn public(bootstrap: OAuthClientBootstrap, client_id: String) -> Self {
        Self {
            bootstrap,
            client_id,
            authentication: OAuthClientAuthenticationMethod::None,
            pkce: OAuthPkcePolicy::S256,
            authorization_parameters: std::collections::BTreeMap::new(),
        }
    }

    /// Validates a manually configured device client without authorization-code options.
    /// # Errors
    /// Rejects automatic bootstrap, PKCE, and authorization parameters.
    pub fn validate_device(&self) -> Result<(), crate::Error> {
        validate_oauth_client_id(&self.client_id)?;
        if self.bootstrap != OAuthClientBootstrap::Manual
            || self.pkce != OAuthPkcePolicy::Disabled
            || !self.authorization_parameters.is_empty()
        {
            return Err(crate::Error::InvalidInput(
                "invalid device OAuth application".into(),
            ));
        }
        Ok(())
    }

    /// Validates the application policy against its upstream protocol.
    /// # Errors
    /// Rejects unsupported bootstrap, PKCE exceptions and protocol-field overrides.
    pub fn validate(&self, mcp: bool, endpoint: &str) -> Result<(), crate::Error> {
        validate_oauth_client_id(&self.client_id)?;
        if (self.authentication != OAuthClientAuthenticationMethod::None
            && self.bootstrap != OAuthClientBootstrap::Manual)
            || (self.pkce == OAuthPkcePolicy::Disabled
                && (mcp || self.authentication == OAuthClientAuthenticationMethod::None))
        {
            return Err(crate::Error::InvalidInput(
                "invalid OAuth application policy".into(),
            ));
        }
        let url = url::Url::parse(endpoint).map_err(|_error| {
            crate::Error::InvalidInput("invalid authorization endpoint".into())
        })?;
        let mut names = std::collections::BTreeSet::new();
        let mut size = 0_usize;
        for (name, value) in url.query_pairs().chain(
            self.authorization_parameters
                .iter()
                .map(|(k, v)| (k.as_str().into(), v.as_str().into())),
        ) {
            size = size.saturating_add(name.len()).saturating_add(value.len());
            if name.is_empty()
                || name.len() > 128
                || value.len() > 2048
                || name.chars().any(char::is_control)
                || value.chars().any(char::is_control)
                || oauth_parameter_is_reserved(&name)
                || !names.insert(name.to_ascii_lowercase())
                || names.len() > 32
                || size > 8192
            {
                return Err(crate::Error::InvalidInput(
                    "invalid authorization parameters".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Whether `value` can name an OAuth endpoint: an absolute HTTP(S) URL with a
/// host, and no credentials, fragment, or reserved OAuth parameter.
#[must_use]
pub fn oauth_endpoint_is_valid(value: &str) -> bool {
    if value.len() > 2048 || value.chars().any(char::is_control) || value.contains('\\') {
        return false;
    }
    url::Url::parse(value).is_ok_and(|url| {
        matches!(url.scheme(), "http" | "https")
            && url.host_str().is_some()
            && url.username().is_empty()
            && url.password().is_none()
            && url.fragment().is_none()
            && !url
                .query_pairs()
                .any(|(name, _)| oauth_parameter_is_reserved(&name))
    })
}

/// Protocol fields controlled by Coral, or unsuitable for browser navigation.
#[must_use]
pub fn oauth_parameter_is_reserved(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "client_id"
            | "client_secret"
            | "client_assertion"
            | "client_assertion_type"
            | "response_type"
            | "response_mode"
            | "state"
            | "redirect_uri"
            | "scope"
            | "resource"
            | "code"
            | "code_verifier"
            | "code_challenge"
            | "code_challenge_method"
            | "access_token"
            | "refresh_token"
            | "grant_type"
            | "assertion"
            | "password"
    )
}

/// Client bootstrap methods in the decided order of preference.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OAuthClientBootstrap {
    /// Client ID Metadata Document published at Coral's public HTTPS origin.
    ClientIdMetadataDocument,
    /// Dynamic Client Registration against the issuer's registration endpoint.
    DynamicClientRegistration,
    /// A manually supplied, pre-registered client identifier.
    Manual,
}
