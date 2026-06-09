//! Runtime source input models and OAuth helpers.

use std::collections::BTreeMap;

use url::Url;

use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

/// The kind of interactive input required by one validated source spec.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestInputKind {
    /// A non-secret input persisted in source variables.
    Variable,
    /// A secret input persisted separately from source variables.
    Secret,
}

/// One interactive input extracted from a validated source spec.
///
/// The app and CLI can map this into prompts, persisted variables, or secret
/// collection flows without depending on protobuf-specific types.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestInputSpec {
    /// The source-spec-declared input key.
    pub key: String,
    /// Whether this input is a variable or a secret.
    pub kind: ManifestInputKind,
    /// Whether the user must provide an explicit value.
    pub required: bool,
    /// The source-spec-declared default value, if any.
    pub default_value: String,
    /// Exact non-secret values accepted for this input. Empty means unconstrained.
    pub allowed_values: Vec<String>,
    /// Optional authored hint shown to the user when collecting the input.
    pub hint: Option<String>,
    /// Optional credential retrieval choices for a secret input.
    pub credential: Option<ManifestCredentialSpec>,
}

/// Credential retrieval choices declared for one secret input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestCredentialSpec {
    /// Authored retrieval methods in display order.
    pub methods: Vec<ManifestCredentialMethod>,
}

/// Supported credential retrieval method kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestCredentialMethodKind {
    /// Collect the secret value through the source configuration path.
    SourceConfig,
    /// Run an OAuth authorization-code flow to retrieve the secret value.
    OAuth,
}

/// One credential retrieval method declared on a secret input.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestCredentialMethod {
    /// Method kind.
    pub kind: ManifestCredentialMethodKind,
    /// Optional display label.
    pub label: Option<String>,
    /// Optional display description.
    pub description: Option<String>,
    /// Optional hint describing how to obtain the values this method needs.
    pub hint: Option<String>,
    /// OAuth configuration when `kind` is [`ManifestCredentialMethodKind::OAuth`].
    pub oauth: Option<ManifestOAuthCredentialSpec>,
}

/// OAuth credential retrieval configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthCredentialSpec {
    /// OAuth flow settings.
    pub flow: ManifestOAuthFlowSpec,
    /// Loopback callback URI Coral binds during authorization-code sessions.
    pub redirect_uri: Option<String>,
    /// Whether Coral binds the authored redirect URI port exactly or chooses a free port.
    pub redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode,
    /// Provider authorization endpoint URL template for authorization-code sessions.
    pub authorization_url: Option<String>,
    /// Provider device authorization endpoint URL template for device-code sessions.
    pub device_authorization_url: Option<String>,
    /// Provider token endpoint URL template.
    pub token_url: String,
    /// OAuth client configuration.
    pub client: ManifestOAuthClientSpec,
    /// Optional OAuth scope parameter configuration.
    pub scopes: Option<ManifestOAuthScopesSpec>,
}

/// OAuth provider endpoint URLs rendered with source variables.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthEndpointUrls {
    /// Provider authorization endpoint URL for authorization-code sessions.
    pub authorization_url: Option<String>,
    /// Provider device authorization endpoint URL for device-code sessions.
    pub device_authorization_url: Option<String>,
    /// Provider token endpoint URL.
    pub token_url: String,
}

impl ManifestOAuthCredentialSpec {
    /// Resolve the local listener port behavior for this OAuth redirect URI.
    pub fn redirect_bind_port(&self) -> Result<ManifestOAuthRedirectBindPort> {
        let redirect_uri = self.redirect_uri.as_deref().ok_or_else(|| {
            ManifestError::validation("OAuth redirect URI is missing redirect_uri")
        })?;
        redirect_bind_port(
            redirect_uri,
            self.redirect_uri_port_mode,
            "OAuth redirect URI",
        )
    }

    /// Render provider endpoint URL templates with resolved source variables.
    pub fn endpoint_urls(
        &self,
        source_inputs: &BTreeMap<String, String>,
    ) -> Result<ManifestOAuthEndpointUrls> {
        let authorization_url = self
            .authorization_url
            .as_deref()
            .map(|template| render_oauth_endpoint_url("authorization", template, source_inputs))
            .transpose()?;
        let device_authorization_url = self
            .device_authorization_url
            .as_deref()
            .map(|template| {
                render_oauth_endpoint_url("device authorization", template, source_inputs)
            })
            .transpose()?;
        let token_url = render_oauth_endpoint_url("token", &self.token_url, source_inputs)?;
        Ok(ManifestOAuthEndpointUrls {
            authorization_url,
            device_authorization_url,
            token_url,
        })
    }
}

fn render_oauth_endpoint_url(
    label: &str,
    raw_template: &str,
    source_inputs: &BTreeMap<String, String>,
) -> Result<String> {
    let template = ParsedTemplate::parse(raw_template)?;
    let mut rendered = String::with_capacity(template.raw().len());
    for part in template.parts() {
        match part {
            crate::TemplatePart::Literal(literal) => rendered.push_str(literal),
            crate::TemplatePart::Token(token) => {
                if token.namespace() != &TemplateNamespace::Input {
                    return Err(ManifestError::validation(format!(
                        "unsupported OAuth endpoint template token '{}'",
                        token.raw()
                    )));
                }
                if token.default_value().is_some() {
                    return Err(ManifestError::validation(format!(
                        "OAuth endpoint template token '{}' must declare defaults under top-level inputs",
                        token.raw()
                    )));
                }
                let value = source_inputs.get(token.key()).ok_or_else(|| {
                    ManifestError::validation(format!(
                        "missing source input '{}' for OAuth endpoint template",
                        token.key()
                    ))
                })?;
                rendered.push_str(value);
            }
        }
    }
    let parsed = Url::parse(&rendered).map_err(|error| {
        ManifestError::validation(format!("invalid OAuth {label} URL: {error}"))
    })?;
    if !crate::parsed_url_is_https_or_loopback(&parsed) {
        return Err(ManifestError::validation(format!(
            "OAuth {label} URL must use https, except localhost development URLs"
        )));
    }
    Ok(rendered)
}

/// Supported loopback redirect URI port binding modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOAuthRedirectUriPortMode {
    /// Bind the exact port authored in `redirect_uri`.
    Fixed,
    /// Bind a random free port and use it in OAuth authorization and token exchange.
    Random,
}

/// Resolved loopback listener port behavior for an OAuth redirect URI.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOAuthRedirectBindPort {
    /// Bind the exact fixed port authored in `redirect_uri`.
    Fixed(u16),
    /// Bind port 0 and let the OS choose a free port.
    Random,
}

/// Supported OAuth credential retrieval flow settings.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthFlowSpec {
    /// OAuth flow kind.
    pub kind: ManifestOAuthFlowKind,
    /// PKCE requirement for the flow.
    pub pkce: ManifestOAuthPkceMode,
}

/// Supported OAuth flow kinds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOAuthFlowKind {
    /// OAuth 2.0 authorization-code grant.
    AuthorizationCode,
    /// OAuth 2.0 device authorization grant.
    DeviceCode,
}

/// Supported PKCE modes for OAuth credential retrieval.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOAuthPkceMode {
    /// Require a generated code verifier and S256 challenge.
    Required,
    /// Do not include PKCE parameters.
    Disabled,
}

/// OAuth client configuration for credential retrieval.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthClientSpec {
    /// Client ID resolution configuration.
    pub id: ManifestOAuthClientIdSpec,
    /// Optional confidential-client secret configuration.
    pub secret: Option<ManifestOAuthClientSecretSpec>,
}

/// OAuth client ID resolution configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthClientIdSpec {
    /// Optional manifest-authored default client ID.
    pub default: Option<String>,
    /// Optional credential-retrieval input key for a client ID override.
    pub input: Option<String>,
}

/// OAuth client secret retrieval configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthClientSecretSpec {
    /// Credential-retrieval input key for the client secret.
    pub input: String,
    /// How Coral sends the client secret to the token endpoint.
    pub transport: ManifestOAuthClientSecretTransport,
}

/// Supported confidential-client secret transport modes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOAuthClientSecretTransport {
    /// Send `Authorization: Basic base64(client_id:client_secret)`.
    BasicAuth,
    /// Send `client_secret` in the token request body.
    RequestBody,
}

impl ManifestOAuthClientSecretTransport {
    /// Canonical manifest label for this transport mode.
    pub fn label(self) -> &'static str {
        match self {
            Self::BasicAuth => "basic_auth",
            Self::RequestBody => "request_body",
        }
    }

    /// Parse a canonical manifest transport label.
    pub fn from_label(value: &str) -> Option<Self> {
        match value {
            "basic_auth" => Some(Self::BasicAuth),
            "request_body" => Some(Self::RequestBody),
            _ => None,
        }
    }
}

/// OAuth scope parameter configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthScopesSpec {
    /// The `scope` parameter value definition.
    pub scope: ManifestOAuthScopeSpec,
}

/// OAuth scope parameter values and delimiter.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManifestOAuthScopeSpec {
    /// Delimiter used to join scope values.
    pub delimiter: ManifestOAuthScopeDelimiter,
    /// Authored scope values.
    pub values: Vec<String>,
}

/// Supported OAuth scope delimiters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ManifestOAuthScopeDelimiter {
    /// Join scope values with a single space.
    Space,
    /// Join scope values with a comma.
    Comma,
}

fn redirect_bind_port(
    raw: &str,
    port_mode: ManifestOAuthRedirectUriPortMode,
    context: &str,
) -> Result<ManifestOAuthRedirectBindPort> {
    let url = Url::parse(raw)
        .map_err(|error| ManifestError::validation(format!("{context} is invalid: {error}")))?;
    if url.scheme() != "http" {
        return Err(ManifestError::validation(format!(
            "{context} must use http"
        )));
    }
    let host = url.host_str().unwrap_or_default();
    if host != "127.0.0.1" && host != "localhost" {
        return Err(ManifestError::validation(format!(
            "{context} must use a loopback host"
        )));
    }
    let has_explicit_port = redirect_uri_has_explicit_port(raw);
    match port_mode {
        ManifestOAuthRedirectUriPortMode::Fixed if has_explicit_port => {
            let port = url.port_or_known_default().ok_or_else(|| {
                ManifestError::validation(format!(
                    "{context} must include an explicit non-zero port when redirect_uri_port_mode is fixed"
                ))
            })?;
            if port == 0 {
                return Err(ManifestError::validation(format!(
                    "{context} must include an explicit non-zero port when redirect_uri_port_mode is fixed"
                )));
            }
            Ok(ManifestOAuthRedirectBindPort::Fixed(port))
        }
        ManifestOAuthRedirectUriPortMode::Fixed => Err(ManifestError::validation(format!(
            "{context} must include an explicit non-zero port when redirect_uri_port_mode is fixed"
        ))),
        ManifestOAuthRedirectUriPortMode::Random if !has_explicit_port || url.port() == Some(0) => {
            Ok(ManifestOAuthRedirectBindPort::Random)
        }
        ManifestOAuthRedirectUriPortMode::Random => Err(ManifestError::validation(format!(
            "{context} must omit the port or use port 0 when redirect_uri_port_mode is random"
        ))),
    }
}

fn redirect_uri_has_explicit_port(raw: &str) -> bool {
    let Some((_, after_scheme)) = raw.split_once("://") else {
        return false;
    };
    let authority = after_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or_default();
    let host_and_port = authority
        .rsplit_once('@')
        .map_or(authority, |(_, host_and_port)| host_and_port);
    let Some((_, port)) = host_and_port.rsplit_once(':') else {
        return false;
    };
    !port.is_empty() && port.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::{
        ManifestOAuthClientIdSpec, ManifestOAuthClientSecretTransport, ManifestOAuthClientSpec,
        ManifestOAuthCredentialSpec, ManifestOAuthFlowKind, ManifestOAuthFlowSpec,
        ManifestOAuthPkceMode, ManifestOAuthRedirectBindPort, ManifestOAuthRedirectUriPortMode,
    };
    use std::collections::BTreeMap;

    fn oauth_template(
        authorization_url: Option<&str>,
        token_url: &str,
    ) -> ManifestOAuthCredentialSpec {
        ManifestOAuthCredentialSpec {
            flow: ManifestOAuthFlowSpec {
                kind: ManifestOAuthFlowKind::AuthorizationCode,
                pkce: ManifestOAuthPkceMode::Disabled,
            },
            redirect_uri: Some("http://127.0.0.1:53682/oauth/callback".to_string()),
            redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode::Fixed,
            authorization_url: authorization_url.map(ToString::to_string),
            device_authorization_url: Some(
                "https://login.microsoftonline.com/{{input.TENANT}}/oauth2/v2.0/devicecode"
                    .to_string(),
            ),
            token_url: token_url.to_string(),
            client: ManifestOAuthClientSpec {
                id: ManifestOAuthClientIdSpec {
                    default: Some("default-client".to_string()),
                    input: None,
                },
                secret: None,
            },
            scopes: None,
        }
    }

    #[test]
    fn oauth_client_secret_transport_labels_are_canonical() {
        assert_eq!(
            ManifestOAuthClientSecretTransport::BasicAuth.label(),
            "basic_auth"
        );
        assert_eq!(
            ManifestOAuthClientSecretTransport::RequestBody.label(),
            "request_body"
        );
        assert_eq!(
            ManifestOAuthClientSecretTransport::from_label("basic_auth"),
            Some(ManifestOAuthClientSecretTransport::BasicAuth)
        );
        assert_eq!(
            ManifestOAuthClientSecretTransport::from_label("request_body"),
            Some(ManifestOAuthClientSecretTransport::RequestBody)
        );
        assert_eq!(
            ManifestOAuthClientSecretTransport::from_label("unsupported"),
            None
        );
    }

    #[test]
    fn fixed_redirect_port_requires_explicit_non_zero_loopback_port() {
        let oauth = oauth_template(None, "https://provider.example.com/oauth/token");
        assert_eq!(
            oauth.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Fixed(53682)
        );

        let missing_port = ManifestOAuthCredentialSpec {
            redirect_uri: Some("http://127.0.0.1/oauth/callback".to_string()),
            ..oauth.clone()
        };
        assert!(
            missing_port
                .redirect_bind_port()
                .expect_err("missing port")
                .to_string()
                .contains("explicit non-zero port")
        );

        let zero_port = ManifestOAuthCredentialSpec {
            redirect_uri: Some("http://127.0.0.1:0/oauth/callback".to_string()),
            ..oauth.clone()
        };
        assert!(
            zero_port
                .redirect_bind_port()
                .expect_err("zero fixed port")
                .to_string()
                .contains("explicit non-zero port")
        );
    }

    #[test]
    fn random_redirect_port_accepts_missing_or_zero_port() {
        let oauth = ManifestOAuthCredentialSpec {
            redirect_uri: Some("http://127.0.0.1/oauth/callback".to_string()),
            redirect_uri_port_mode: ManifestOAuthRedirectUriPortMode::Random,
            ..oauth_template(None, "https://provider.example.com/oauth/token")
        };
        assert_eq!(
            oauth.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Random
        );

        let zero_port = ManifestOAuthCredentialSpec {
            redirect_uri: Some("http://localhost:0/oauth/callback".to_string()),
            ..oauth
        };
        assert_eq!(
            zero_port.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Random
        );
    }

    #[test]
    fn redirect_uri_requires_loopback_http() {
        let non_loopback = ManifestOAuthCredentialSpec {
            redirect_uri: Some("http://example.com:53682/oauth/callback".to_string()),
            ..oauth_template(None, "https://provider.example.com/oauth/token")
        };
        assert!(
            non_loopback
                .redirect_bind_port()
                .expect_err("non-loopback redirect")
                .to_string()
                .contains("loopback host")
        );

        let https = ManifestOAuthCredentialSpec {
            redirect_uri: Some("https://127.0.0.1:53682/oauth/callback".to_string()),
            ..oauth_template(None, "https://provider.example.com/oauth/token")
        };
        assert!(
            https
                .redirect_bind_port()
                .expect_err("https redirect")
                .to_string()
                .contains("must use http")
        );
    }

    #[test]
    fn endpoint_urls_reject_inline_template_defaults() {
        let oauth = oauth_template(
            Some(
                "https://login.microsoftonline.com/{{input.TENANT|organizations}}/oauth2/v2.0/authorize",
            ),
            "https://login.microsoftonline.com/{{input.TENANT}}/oauth2/v2.0/token",
        );
        let source_inputs = BTreeMap::from([("TENANT".to_string(), "organizations".to_string())]);
        let error = oauth
            .endpoint_urls(&source_inputs)
            .expect_err("inline endpoint defaults should fail at the public render boundary");

        assert!(
            error
                .to_string()
                .contains("must declare defaults under top-level inputs"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn endpoint_urls_render_source_input_templates() {
        let oauth = oauth_template(
            Some("https://login.microsoftonline.com/{{input.TENANT}}/oauth2/v2.0/authorize"),
            "https://login.microsoftonline.com/{{input.TENANT}}/oauth2/v2.0/token",
        );
        let source_inputs = BTreeMap::from([("TENANT".to_string(), "organizations".to_string())]);
        let endpoints = oauth.endpoint_urls(&source_inputs).expect("endpoint urls");

        assert_eq!(
            endpoints.authorization_url.as_deref(),
            Some("https://login.microsoftonline.com/organizations/oauth2/v2.0/authorize")
        );
        assert_eq!(
            endpoints.device_authorization_url.as_deref(),
            Some("https://login.microsoftonline.com/organizations/oauth2/v2.0/devicecode")
        );
        assert_eq!(
            endpoints.token_url,
            "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"
        );
    }

    #[test]
    fn endpoint_urls_reject_plaintext_non_loopback_urls() {
        let oauth = oauth_template(None, "http://provider.example.com/oauth/token");
        let source_inputs = BTreeMap::from([("TENANT".to_string(), "organizations".to_string())]);
        let error = oauth
            .endpoint_urls(&source_inputs)
            .expect_err("non-loopback plaintext token endpoint");

        assert!(
            error.to_string().contains("must use https"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn endpoint_urls_allow_plaintext_loopback_for_development() {
        let oauth = ManifestOAuthCredentialSpec {
            device_authorization_url: Some("http://localhost:3000/oauth/device".to_string()),
            ..oauth_template(
                Some("http://127.0.0.1:3000/oauth/authorize"),
                "http://[::1]:3000/oauth/token",
            )
        };

        let endpoints = oauth
            .endpoint_urls(&BTreeMap::new())
            .expect("loopback plaintext endpoint URLs");

        assert_eq!(
            endpoints.authorization_url.as_deref(),
            Some("http://127.0.0.1:3000/oauth/authorize")
        );
        assert_eq!(
            endpoints.device_authorization_url.as_deref(),
            Some("http://localhost:3000/oauth/device")
        );
        assert_eq!(endpoints.token_url, "http://[::1]:3000/oauth/token");
    }

    #[test]
    fn endpoint_urls_reject_localhost_lookalikes() {
        let oauth = oauth_template(None, "http://localhost.evil.example/oauth/token");
        let source_inputs = BTreeMap::from([("TENANT".to_string(), "organizations".to_string())]);
        let error = oauth
            .endpoint_urls(&source_inputs)
            .expect_err("localhost lookalike token endpoint");

        assert!(
            error.to_string().contains("must use https"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn endpoint_urls_reject_missing_runtime_inputs() {
        let oauth = oauth_template(
            Some("https://login.microsoftonline.com/{{input.TENANT}}/oauth2/v2.0/authorize"),
            "https://login.microsoftonline.com/{{input.TENANT}}/oauth2/v2.0/token",
        );
        let error = oauth
            .endpoint_urls(&BTreeMap::new())
            .expect_err("missing endpoint input");
        assert!(
            error.to_string().contains("missing source input 'TENANT'"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn endpoint_urls_reject_non_input_tokens() {
        let oauth = oauth_template(
            None,
            "https://provider.example.com/{{filter.tenant}}/oauth/token",
        );
        let source_inputs = BTreeMap::from([("TENANT".to_string(), "organizations".to_string())]);
        let error = oauth
            .endpoint_urls(&source_inputs)
            .expect_err("non-input endpoint token");
        assert!(
            error
                .to_string()
                .contains("unsupported OAuth endpoint template token")
        );
    }
}
