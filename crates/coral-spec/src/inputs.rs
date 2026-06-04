//! Extracts interactive source inputs from source-spec documents.
//!
//! Sources that need interactive configuration declare their inputs under a
//! top-level `inputs` map. Each entry fixes the input's kind (`variable` or
//! `secret`), an optional default, and an optional hint. References elsewhere
//! in the manifest use `{{input.KEY}}` templates, `from: input`, or typed
//! wrappers such as `from: bearer`; the declared kind determines whether the
//! value is resolved from the variable or secret store. Manifests that take no
//! interactive inputs may omit the block entirely.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use url::Url;

use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace, TemplateToken};

const RESERVED_INPUT_KEY_PREFIXES: &[&str] = &["__coral"];

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
    let (rendered, _) = render_oauth_endpoint_template(raw_template, |token| {
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
        Ok(OAuthEndpointTemplateValue::Append(value.clone()))
    })?;
    Url::parse(&rendered).map_err(|error| {
        ManifestError::validation(format!("invalid OAuth {label} URL: {error}"))
    })?;
    Ok(rendered)
}

enum OAuthEndpointTemplateValue {
    Append(String),
    DeferUrlValidation,
}

fn render_oauth_endpoint_template(
    raw_template: &str,
    mut resolve_token: impl FnMut(&TemplateToken) -> Result<OAuthEndpointTemplateValue>,
) -> Result<(String, bool)> {
    let template = ParsedTemplate::parse(raw_template)?;
    let mut rendered = String::with_capacity(template.raw().len());
    let mut has_deferred_url_validation = false;
    for part in template.parts() {
        match part {
            crate::TemplatePart::Literal(literal) => rendered.push_str(literal),
            crate::TemplatePart::Token(token) => match resolve_token(token)? {
                OAuthEndpointTemplateValue::Append(value) => rendered.push_str(&value),
                OAuthEndpointTemplateValue::DeferUrlValidation => {
                    has_deferred_url_validation = true;
                }
            },
        }
    }
    Ok((rendered, has_deferred_url_validation))
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

/// Merge user-provided secrets and variables with manifest defaults into one
/// runtime-ready input map.
#[must_use]
pub fn resolve_inputs(
    declared: &[ManifestInputSpec],
    source_secrets: &BTreeMap<String, String>,
    source_variables: &BTreeMap<String, String>,
) -> BTreeMap<String, String> {
    let mut resolved = BTreeMap::new();
    for input in declared {
        let value = match input.kind {
            ManifestInputKind::Secret => source_secrets.get(&input.key).cloned(),
            ManifestInputKind::Variable => source_variables
                .get(&input.key)
                .cloned()
                .or_else(|| (!input.required).then(|| input.default_value.clone())),
        };
        if let Some(value) = value {
            resolved.insert(input.key.clone(), value);
        }
    }
    resolved
}

/// Collect interactive source inputs from an already-parsed manifest value.
///
/// # Errors
///
/// Returns a [`ManifestError`] when an input is declared incorrectly or the
/// manifest references an input that is not declared under the top-level
/// `inputs` block.
pub(crate) fn collect_source_inputs_value(root: &Value) -> Result<Vec<ManifestInputSpec>> {
    let inputs = collect_declared_inputs(root)?;
    validate_input_references(root, &inputs)?;
    validate_oauth_endpoint_templates(&inputs)?;
    Ok(inputs)
}

pub(crate) fn declared_secret_input_names(inputs: &[ManifestInputSpec]) -> BTreeSet<String> {
    inputs
        .iter()
        .filter(|input| input.kind == ManifestInputKind::Secret)
        .map(|input| input.key.clone())
        .collect()
}

pub(crate) fn required_secret_input_names(inputs: &[ManifestInputSpec]) -> BTreeSet<String> {
    inputs
        .iter()
        .filter(|input| input.kind == ManifestInputKind::Secret && input.required)
        .map(|input| input.key.clone())
        .collect()
}

pub(crate) fn collect_declared_inputs(root: &Value) -> Result<Vec<ManifestInputSpec>> {
    let root = root
        .as_object()
        .ok_or_else(|| ManifestError::validation("manifest must be a mapping"))?;
    let Some(inputs) = root.get("inputs") else {
        return Ok(Vec::new());
    };
    let inputs = inputs.as_object().ok_or_else(|| {
        ManifestError::validation("manifest `inputs` must be declared as a mapping")
    })?;

    let mut ordered = Vec::new();
    for (key, value) in inputs {
        validate_input_key("manifest input key", key)?;
        let input = value.as_object().ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{key}' must be declared as a mapping"
            ))
        })?;
        let kind = match input.get("kind").and_then(Value::as_str) {
            Some("variable") => ManifestInputKind::Variable,
            Some("secret") => ManifestInputKind::Secret,
            Some(other) => {
                return Err(ManifestError::validation(format!(
                    "manifest input '{key}' has unsupported kind '{other}'"
                )));
            }
            None => {
                return Err(ManifestError::validation(format!(
                    "manifest input '{key}' is missing kind"
                )));
            }
        };
        let default_value = input
            .get("default")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if kind == ManifestInputKind::Secret && default_value.is_some() {
            return Err(ManifestError::validation(format!(
                "manifest secret input '{key}' must not declare a default"
            )));
        }
        if kind == ManifestInputKind::Variable && credential_like_input_key(key) {
            return Err(ManifestError::validation(format!(
                "manifest input '{key}' looks credential-like and must use kind: secret"
            )));
        }
        let hint = input
            .get("hint")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        let credential = input
            .get("credential")
            .map(|value| parse_credential(key, value))
            .transpose()?;
        let required = input
            .get("required")
            .map(|value| {
                value.as_bool().ok_or_else(|| {
                    ManifestError::validation(format!(
                        "manifest input '{key}' required must be a boolean"
                    ))
                })
            })
            .transpose()?
            .unwrap_or(default_value.is_none());
        if kind != ManifestInputKind::Secret && credential.is_some() {
            return Err(ManifestError::validation(format!(
                "manifest input '{key}' declares credential methods but is not a secret"
            )));
        }
        ordered.push(ManifestInputSpec {
            key: key.clone(),
            kind,
            required,
            default_value: default_value.unwrap_or_default(),
            hint,
            credential,
        });
    }

    Ok(ordered)
}

fn credential_like_input_key(key: &str) -> bool {
    const MARKERS: &[&str] = &[
        "API_KEY",
        "APPLICATION_KEY",
        "ACCESS_KEY",
        "ACCESS_KEY_ID",
        "ACCESS_TOKEN",
        "ADMIN_KEY",
        "AUTHORIZATION",
        "BEARER_TOKEN",
        "CLIENT_SECRET",
        "PASSWORD",
        "PRIVATE_KEY",
        "READ_KEY",
        "SECRET",
        "TOKEN",
    ];

    let key = key.to_ascii_uppercase();
    MARKERS.iter().any(|marker| {
        key == *marker
            || key.contains(&format!("_{marker}_"))
            || key.ends_with(&format!("_{marker}"))
            || key.starts_with(&format!("{marker}_"))
    })
}

pub(crate) fn validate_oauth_endpoint_templates(inputs: &[ManifestInputSpec]) -> Result<()> {
    validate_oauth_endpoint_templates_with_scope(inputs, "top-level inputs")
}

pub(crate) fn validate_oauth_endpoint_templates_with_scope(
    inputs: &[ManifestInputSpec],
    input_scope: &str,
) -> Result<()> {
    let declared = inputs
        .iter()
        .map(|input| (input.key.as_str(), input))
        .collect::<BTreeMap<_, _>>();
    for input in inputs {
        let Some(credential) = input.credential.as_ref() else {
            continue;
        };
        for method in &credential.methods {
            let Some(oauth) = method.oauth.as_ref() else {
                continue;
            };
            validate_oauth_endpoint_templates_for_method(
                &input.key,
                oauth,
                &declared,
                input_scope,
            )?;
        }
    }
    Ok(())
}

fn validate_oauth_endpoint_templates_for_method(
    input_key: &str,
    oauth: &ManifestOAuthCredentialSpec,
    declared: &BTreeMap<&str, &ManifestInputSpec>,
    input_scope: &str,
) -> Result<()> {
    for (field, template) in [
        ("authorization_url", oauth.authorization_url.as_deref()),
        (
            "device_authorization_url",
            oauth.device_authorization_url.as_deref(),
        ),
        ("token_url", Some(oauth.token_url.as_str())),
    ] {
        if let Some(template) = template {
            validate_oauth_endpoint_template(input_key, field, template, declared, input_scope)?;
        }
    }
    Ok(())
}

fn validate_oauth_endpoint_template(
    input_key: &str,
    field: &str,
    raw_template: &str,
    declared: &BTreeMap<&str, &ManifestInputSpec>,
    input_scope: &str,
) -> Result<()> {
    let (rendered, has_required_variable) = render_oauth_endpoint_template(
        raw_template,
        |token| {
            if !matches!(token.namespace(), TemplateNamespace::Input) {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' oauth.endpoints.{field} uses unsupported template token '{}'; OAuth endpoint templates only support source variable input tokens",
                    token.raw()
                )));
            }
            if token.default_value().is_some() {
                return Err(ManifestError::validation(format!(
                    "manifest input '{}' must declare defaults under {input_scope}",
                    token.key()
                )));
            }
            let Some(input) = declared.get(token.key()) else {
                return Err(ManifestError::validation(format!(
                    "manifest input '{}' is referenced but not declared under {input_scope}",
                    token.key()
                )));
            };
            if input.kind != ManifestInputKind::Variable {
                return Err(ManifestError::validation(format!(
                    "manifest input '{}' is referenced by oauth.endpoints.{field} but is not a variable",
                    token.key()
                )));
            }
            if input.required {
                Ok(OAuthEndpointTemplateValue::DeferUrlValidation)
            } else {
                Ok(OAuthEndpointTemplateValue::Append(
                    input.default_value.clone(),
                ))
            }
        },
    )?;

    if !has_required_variable {
        Url::parse(&rendered).map_err(|error| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.endpoints.{field} is invalid: {error}"
            ))
        })?;
    }

    Ok(())
}

fn parse_credential(input_key: &str, value: &Value) -> Result<ManifestCredentialSpec> {
    let credential = input_mapping(input_key, "credential", value)?;
    let methods = credential
        .get("methods")
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' credential is missing methods"
            ))
        })?
        .as_array()
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' credential.methods must be a list"
            ))
        })?;
    if methods.is_empty() {
        return Err(ManifestError::validation(format!(
            "manifest input '{input_key}' credential.methods must not be empty"
        )));
    }

    let methods = methods
        .iter()
        .enumerate()
        .map(|(index, method)| parse_credential_method(input_key, index, method))
        .collect::<Result<Vec<_>>>()?;
    Ok(ManifestCredentialSpec { methods })
}

fn parse_credential_method(
    input_key: &str,
    index: usize,
    value: &Value,
) -> Result<ManifestCredentialMethod> {
    let method = input_mapping(input_key, &format!("credential.methods[{index}]"), value)?;
    let label = method
        .get("label")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let description = method
        .get("description")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let hint = method
        .get("hint")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let (kind, oauth) = match method.get("type").and_then(Value::as_str) {
        Some("source_config") => {
            if method.contains_key("oauth") {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' source_config credential method must not contain oauth"
                )));
            }
            (ManifestCredentialMethodKind::SourceConfig, None)
        }
        Some("oauth") => {
            let oauth = method
                .get("oauth")
                .ok_or_else(|| {
                    ManifestError::validation(format!(
                        "manifest input '{input_key}' oauth credential method is missing oauth"
                    ))
                })
                .and_then(|oauth| parse_oauth(input_key, index, oauth))?;
            (ManifestCredentialMethodKind::OAuth, Some(oauth))
        }
        Some(other) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' credential method has unsupported type '{other}'"
            )));
        }
        None => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' credential method is missing type"
            )));
        }
    };
    Ok(ManifestCredentialMethod {
        kind,
        label,
        description,
        hint,
        oauth,
    })
}

fn parse_oauth(
    input_key: &str,
    method_index: usize,
    value: &Value,
) -> Result<ManifestOAuthCredentialSpec> {
    let oauth = input_mapping(
        input_key,
        &format!("credential.methods[{method_index}].oauth"),
        value,
    )?;
    let flow = oauth
        .get("flow")
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth credential method is missing flow"
            ))
        })
        .and_then(|flow| parse_oauth_flow(input_key, flow))?;
    let redirect_uri = optional_string(oauth, "redirect_uri", input_key, "oauth")?;
    let redirect_uri_port_mode = oauth
        .get("redirect_uri_port_mode")
        .map(|value| parse_redirect_uri_port_mode(input_key, value))
        .transpose()?
        .unwrap_or_else(|| {
            redirect_uri.as_deref().map_or(
                ManifestOAuthRedirectUriPortMode::Fixed,
                default_redirect_uri_port_mode,
            )
        });
    let endpoints = oauth
        .get("endpoints")
        .and_then(Value::as_object)
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth credential method is missing endpoints"
            ))
        })?;
    let authorization_url =
        optional_string(endpoints, "authorization_url", input_key, "oauth.endpoints")?;
    let device_authorization_url = optional_string(
        endpoints,
        "device_authorization_url",
        input_key,
        "oauth.endpoints",
    )?;
    let token_url = required_string(endpoints, "token_url", input_key, "oauth.endpoints")?;
    let client = oauth
        .get("client")
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth credential method is missing client"
            ))
        })
        .and_then(|client| parse_oauth_client(input_key, client))?;
    let scopes = oauth
        .get("scopes")
        .map(|scopes| parse_oauth_scopes(input_key, scopes))
        .transpose()?;
    validate_oauth_flow_fields(
        input_key,
        &flow,
        redirect_uri.as_deref(),
        oauth.contains_key("redirect_uri_port_mode"),
        authorization_url.as_deref(),
        device_authorization_url.as_deref(),
        client.secret.is_some(),
    )?;
    if let Some(redirect_uri) = redirect_uri.as_deref() {
        validate_loopback_redirect_uri(input_key, redirect_uri, redirect_uri_port_mode)?;
    }
    Ok(ManifestOAuthCredentialSpec {
        flow,
        redirect_uri,
        redirect_uri_port_mode,
        authorization_url,
        device_authorization_url,
        token_url,
        client,
        scopes,
    })
}

fn default_redirect_uri_port_mode(raw: &str) -> ManifestOAuthRedirectUriPortMode {
    if Url::parse(raw).ok().and_then(|url| url.port()) == Some(0) {
        ManifestOAuthRedirectUriPortMode::Random
    } else {
        ManifestOAuthRedirectUriPortMode::Fixed
    }
}

fn parse_redirect_uri_port_mode(
    input_key: &str,
    value: &Value,
) -> Result<ManifestOAuthRedirectUriPortMode> {
    match value.as_str() {
        Some("fixed") => Ok(ManifestOAuthRedirectUriPortMode::Fixed),
        Some("random") => Ok(ManifestOAuthRedirectUriPortMode::Random),
        Some(other) => Err(ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.redirect_uri_port_mode has unsupported value '{other}'"
        ))),
        None => Err(ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.redirect_uri_port_mode must be a string"
        ))),
    }
}

fn parse_oauth_flow(input_key: &str, value: &Value) -> Result<ManifestOAuthFlowSpec> {
    let flow = input_mapping(input_key, "oauth.flow", value)?;
    let kind = match flow.get("type").and_then(Value::as_str) {
        Some("authorization_code") => ManifestOAuthFlowKind::AuthorizationCode,
        Some("device_code") => ManifestOAuthFlowKind::DeviceCode,
        Some(other) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.flow.type has unsupported value '{other}'"
            )));
        }
        None => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.flow is missing type"
            )));
        }
    };
    let pkce = match (kind, flow.get("pkce").and_then(Value::as_str)) {
        (ManifestOAuthFlowKind::AuthorizationCode, Some("required")) => {
            ManifestOAuthPkceMode::Required
        }
        (_, Some("disabled")) | (ManifestOAuthFlowKind::DeviceCode, None) => {
            ManifestOAuthPkceMode::Disabled
        }
        (ManifestOAuthFlowKind::DeviceCode, Some("required")) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.flow.pkce must be disabled for device_code"
            )));
        }
        (_, Some(other)) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.flow.pkce has unsupported value '{other}'"
            )));
        }
        (ManifestOAuthFlowKind::AuthorizationCode, None) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.flow is missing pkce"
            )));
        }
    };
    Ok(ManifestOAuthFlowSpec { kind, pkce })
}

fn parse_oauth_client(input_key: &str, value: &Value) -> Result<ManifestOAuthClientSpec> {
    let client = input_mapping(input_key, "oauth.client", value)?;
    let id = client
        .get("id")
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.client is missing id"
            ))
        })
        .and_then(|id| parse_oauth_client_id(input_key, id))?;
    let secret = client
        .get("secret")
        .map(|secret| parse_oauth_client_secret(input_key, secret))
        .transpose()?;
    if secret.is_some() && id.input.is_none() {
        return Err(ManifestError::validation(format!(
            "manifest input '{input_key}' confidential oauth client must declare client.id.input"
        )));
    }
    Ok(ManifestOAuthClientSpec { id, secret })
}

fn parse_oauth_client_id(input_key: &str, value: &Value) -> Result<ManifestOAuthClientIdSpec> {
    let id = input_mapping(input_key, "oauth.client.id", value)?;
    let default = id
        .get("default")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let input = id
        .get("input")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    if default.is_none() && input.is_none() {
        return Err(ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.client.id must declare default or input"
        )));
    }
    if let Some(input) = input.as_deref() {
        validate_input_key("oauth client id input key", input)?;
    }
    Ok(ManifestOAuthClientIdSpec { default, input })
}

fn parse_oauth_client_secret(
    input_key: &str,
    value: &Value,
) -> Result<ManifestOAuthClientSecretSpec> {
    let secret = input_mapping(input_key, "oauth.client.secret", value)?;
    let input = required_string(secret, "input", input_key, "oauth.client.secret")?;
    validate_input_key("oauth client secret input key", &input)?;
    let transport = match secret.get("transport").and_then(Value::as_str) {
        Some(value) => ManifestOAuthClientSecretTransport::from_label(value).ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.client.secret.transport has unsupported value '{value}'"
            ))
        })?,
        None => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.client.secret is missing transport"
            )));
        }
    };
    Ok(ManifestOAuthClientSecretSpec { input, transport })
}

fn parse_oauth_scopes(input_key: &str, value: &Value) -> Result<ManifestOAuthScopesSpec> {
    let scopes = input_mapping(input_key, "oauth.scopes", value)?;
    let scope = scopes
        .get("scope")
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.scopes is missing scope"
            ))
        })
        .and_then(|scope| parse_oauth_scope(input_key, scope))?;
    Ok(ManifestOAuthScopesSpec { scope })
}

fn parse_oauth_scope(input_key: &str, value: &Value) -> Result<ManifestOAuthScopeSpec> {
    let scope = input_mapping(input_key, "oauth.scopes.scope", value)?;
    let delimiter = match scope.get("delimiter").and_then(Value::as_str) {
        Some("space") => ManifestOAuthScopeDelimiter::Space,
        Some("comma") => ManifestOAuthScopeDelimiter::Comma,
        Some(other) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.scopes.scope.delimiter has unsupported value '{other}'"
            )));
        }
        None => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.scopes.scope is missing delimiter"
            )));
        }
    };
    let values = scope
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.scopes.scope.values must be a list"
            ))
        })?
        .iter()
        .map(|value| {
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                ManifestError::validation(format!(
                    "manifest input '{input_key}' oauth.scopes.scope.values must contain strings"
                ))
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if values.is_empty() {
        return Err(ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.scopes.scope.values must not be empty"
        )));
    }
    Ok(ManifestOAuthScopeSpec { delimiter, values })
}

fn input_mapping<'a>(
    input_key: &str,
    context: &str,
    value: &'a Value,
) -> Result<&'a Map<String, Value>> {
    value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' {context} must be a mapping"
        ))
    })
}

fn required_string(
    object: &Map<String, Value>,
    key: &str,
    input_key: &str,
    context: &str,
) -> Result<String> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' {context} is missing {key}"
            ))
        })
}

fn optional_string(
    object: &Map<String, Value>,
    key: &str,
    input_key: &str,
    context: &str,
) -> Result<Option<String>> {
    let Some(value) = object.get(key) else {
        return Ok(None);
    };
    value
        .as_str()
        .map(|value| Some(value.to_string()))
        .ok_or_else(|| {
            ManifestError::validation(format!(
                "manifest input '{input_key}' {context}.{key} must be a string"
            ))
        })
}

fn validate_oauth_flow_fields(
    input_key: &str,
    flow: &ManifestOAuthFlowSpec,
    redirect_uri: Option<&str>,
    has_redirect_uri_port_mode: bool,
    authorization_url: Option<&str>,
    device_authorization_url: Option<&str>,
    has_client_secret: bool,
) -> Result<()> {
    match flow.kind {
        ManifestOAuthFlowKind::AuthorizationCode => validate_oauth_flow_rules(
            input_key,
            &[
                (
                    redirect_uri.is_none(),
                    "authorization_code oauth method is missing redirect_uri",
                ),
                (
                    authorization_url.is_none(),
                    "authorization_code oauth method is missing endpoints.authorization_url",
                ),
            ],
        ),
        ManifestOAuthFlowKind::DeviceCode => validate_oauth_flow_rules(
            input_key,
            &[
                (
                    redirect_uri.is_some(),
                    "device_code oauth method must not declare redirect_uri",
                ),
                (
                    has_redirect_uri_port_mode,
                    "device_code oauth method must not declare redirect_uri_port_mode",
                ),
                (
                    authorization_url.is_some(),
                    "device_code oauth method must not declare endpoints.authorization_url",
                ),
                (
                    device_authorization_url.is_none(),
                    "device_code oauth method is missing endpoints.device_authorization_url",
                ),
                (
                    has_client_secret,
                    "device_code oauth method must not declare client.secret",
                ),
            ],
        ),
    }
}

fn validate_oauth_flow_rules(input_key: &str, rules: &[(bool, &str)]) -> Result<()> {
    for (condition, message) in rules {
        if *condition {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' {message}"
            )));
        }
    }
    Ok(())
}

fn validate_loopback_redirect_uri(
    input_key: &str,
    raw: &str,
    port_mode: ManifestOAuthRedirectUriPortMode,
) -> Result<()> {
    let context = format!("manifest input '{input_key}' oauth.redirect_uri");
    redirect_bind_port(raw, port_mode, &context).map(|_| ())
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
            let port = url
                .port_or_known_default()
                .ok_or_else(|| fixed_redirect_port_error(context))?;
            if port == 0 {
                return Err(fixed_redirect_port_error(context));
            }
            Ok(ManifestOAuthRedirectBindPort::Fixed(port))
        }
        ManifestOAuthRedirectUriPortMode::Fixed => Err(fixed_redirect_port_error(context)),
        ManifestOAuthRedirectUriPortMode::Random if !has_explicit_port || url.port() == Some(0) => {
            Ok(ManifestOAuthRedirectBindPort::Random)
        }
        ManifestOAuthRedirectUriPortMode::Random => Err(ManifestError::validation(format!(
            "{context} must omit the port or use port 0 when redirect_uri_port_mode is random"
        ))),
    }
}

fn fixed_redirect_port_error(context: &str) -> ManifestError {
    ManifestError::validation(format!(
        "{context} must include an explicit non-zero port when redirect_uri_port_mode is fixed"
    ))
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

fn validate_input_key(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ManifestError::validation(format!("missing {label}")));
    }
    if trimmed != value {
        return Err(input_key_error(
            label,
            "must not contain leading or trailing whitespace",
        ));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(input_key_error(label, "must not contain '/' or '\\\\'"));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(input_key_error(
            label,
            "must not contain '=', '\\n', or '\\r'",
        ));
    }
    if trimmed.starts_with('#') {
        return Err(input_key_error(label, "must not start with '#'"));
    }
    if let Some(prefix) = RESERVED_INPUT_KEY_PREFIXES
        .iter()
        .find(|prefix| trimmed.starts_with(**prefix))
    {
        return Err(input_key_error(
            label,
            &format!("must not start with reserved prefix '{prefix}'"),
        ));
    }
    Ok(())
}

fn input_key_error(label: &str, reason: &str) -> ManifestError {
    ManifestError::validation(format!("{label} {reason}"))
}

pub(crate) fn validate_input_references(root: &Value, inputs: &[ManifestInputSpec]) -> Result<()> {
    let declared: BTreeMap<String, ManifestInputKind> = inputs
        .iter()
        .map(|input| (input.key.clone(), input.kind))
        .collect();
    validate_value(root, true, &declared, false)
}

fn validate_value(
    value: &Value,
    is_root: bool,
    declared: &BTreeMap<String, ManifestInputKind>,
    in_auth: bool,
) -> Result<()> {
    match value {
        Value::Object(map) => {
            validate_mapping(map, declared, in_auth)?;
            for (key, nested) in map {
                if is_root && key == "inputs" {
                    continue;
                }
                validate_value(
                    nested,
                    false,
                    declared,
                    in_auth || (is_root && key == "auth"),
                )?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_value(item, false, declared, in_auth)?;
            }
        }
        Value::String(raw) => validate_template(raw, declared)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn validate_mapping(
    map: &Map<String, Value>,
    declared: &BTreeMap<String, ManifestInputKind>,
    in_auth: bool,
) -> Result<()> {
    let Some(source_kind @ ("input" | "bearer")) = map.get("from").and_then(Value::as_str) else {
        return Ok(());
    };

    let key = map.get("key").and_then(Value::as_str).ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest '{source_kind}' value source is missing key"
        ))
    })?;
    let Some(kind) = declared.get(key) else {
        return Err(ManifestError::validation(format!(
            "manifest input '{key}' is referenced but not declared under top-level inputs"
        )));
    };
    if source_kind == "bearer" && *kind != ManifestInputKind::Secret {
        return Err(ManifestError::validation(format!(
            "manifest bearer value source '{key}' must reference a secret input"
        )));
    }
    if source_kind == "input" && in_auth && *kind != ManifestInputKind::Secret {
        return Err(ManifestError::validation(format!(
            "manifest auth input value source '{key}' must reference a secret input"
        )));
    }
    if map.contains_key("default") {
        return Err(ManifestError::validation(format!(
            "manifest input '{key}' must declare defaults under top-level inputs"
        )));
    }
    Ok(())
}

fn validate_template(template: &str, declared: &BTreeMap<String, ManifestInputKind>) -> Result<()> {
    let template = ParsedTemplate::parse(template)?;
    for token in template.tokens() {
        for key in token.input_keys() {
            if !declared.contains_key(key) {
                return Err(ManifestError::validation(format!(
                    "manifest input '{key}' is referenced but not declared under top-level inputs"
                )));
            }
        }
        if matches!(token.namespace(), TemplateNamespace::Input) && token.default_value().is_some()
        {
            return Err(ManifestError::validation(format!(
                "manifest input '{}' must declare defaults under top-level inputs",
                token.key()
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![expect(
        clippy::indexing_slicing,
        reason = "parsed input order assertions intentionally fail loudly in tests"
    )]

    use super::{
        ManifestCredentialMethod, ManifestCredentialMethodKind, ManifestInputKind,
        ManifestInputSpec, ManifestOAuthClientSecretTransport, ManifestOAuthCredentialSpec,
        ManifestOAuthFlowKind, ManifestOAuthPkceMode, ManifestOAuthRedirectBindPort,
        ManifestOAuthRedirectUriPortMode, ManifestOAuthScopeDelimiter, collect_source_inputs_value,
    };
    use crate::test_support::assert_error_contains;
    use crate::{ManifestError, Result};
    use std::collections::BTreeMap;
    use std::fmt::Write as _;

    fn collect(raw: &str) -> Result<Vec<ManifestInputSpec>> {
        let root: serde_json::Value =
            serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
        collect_source_inputs_value(&root)
    }

    #[test]
    fn declared_inputs_are_parsed_in_manifest_order() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_API_BASE:
    kind: variable
    default: https://api.github.com
    hint: For GitHub Enterprise, use https://<host>/api/v3
  GITHUB_TOKEN:
    kind: secret
    hint: Run `gh auth token` or create a PAT
base_url: "{{input.GITHUB_API_BASE}}"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: template
      template: Bearer {{input.GITHUB_TOKEN}}
tables: []
"#;

        let inputs = collect(manifest).expect("inputs");
        let [api_base, token] = inputs.as_slice() else {
            panic!("expected two inputs, got {inputs:?}");
        };
        assert_eq!(api_base.key, "GITHUB_API_BASE");
        assert_eq!(api_base.kind, ManifestInputKind::Variable);
        assert!(!api_base.required);
        assert_eq!(api_base.default_value, "https://api.github.com");
        assert_eq!(
            api_base.hint.as_deref(),
            Some("For GitHub Enterprise, use https://<host>/api/v3")
        );
        assert_eq!(token.key, "GITHUB_TOKEN");
        assert_eq!(token.kind, ManifestInputKind::Secret);
        assert!(token.required);
        assert_eq!(token.default_value, "");
        assert_eq!(
            token.hint.as_deref(),
            Some("Run `gh auth token` or create a PAT")
        );
        assert!(inputs[1].credential.is_none());
    }

    fn manifest_with_input(raw_input: &str) -> String {
        manifest_with_input_and_body(
            raw_input,
            r"
base_url: https://api.example.com
tables: []
",
        )
    }

    fn manifest_with_input_and_body(raw_input: &str, body: &str) -> String {
        format!(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
{raw_input}{body}"
        )
    }

    fn oauth_input(client: &str) -> String {
        manifest_with_input(&format!(
            r"
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect
          description: Use OAuth.
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://127.0.0.1:53682/oauth/callback
            endpoints:
              authorization_url: https://provider.example.com/oauth/authorize
              token_url: https://provider.example.com/oauth/token
            client:
{client}
            scopes:
              scope:
                delimiter: space
                values:
                  - repo
                  - read:org
"
        ))
    }

    const DEFAULT_OAUTH_CLIENT: &str = r"
              id:
                default: default-client
";
    const DEFAULT_REDIRECT_URI: &str = "http://127.0.0.1:53682/oauth/callback";
    const DEFAULT_REDIRECT_URI_LINE: &str =
        "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n";

    fn default_oauth_input() -> String {
        oauth_input(DEFAULT_OAUTH_CLIENT)
    }

    fn default_oauth_input_replacing(from: &str, to: &str) -> String {
        default_oauth_input().replace(from, to)
    }

    fn default_oauth_error(from: &str, to: &str, expectation: &str) -> ManifestError {
        collect(&default_oauth_input_replacing(from, to)).expect_err(expectation)
    }

    fn expect_collect_error(raw: &str, expectation: &str, expected: &str) {
        let error = collect(raw).expect_err(expectation);
        assert_error_contains(&error, expected);
    }

    fn auth_header_source(from: &str, key: &str) -> String {
        format!(
            r"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: {from}
      key: {key}
tables: []
"
        )
    }

    fn auth_header_one_of_sources(values: &[(&str, &str)]) -> String {
        let mut value_items = String::new();
        for (from, key) in values {
            writeln!(
                &mut value_items,
                "        - from: {from}\n          key: {key}"
            )
            .expect("write value source");
        }
        format!(
            r"
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: one_of
      values:
{value_items}tables: []
"
        )
    }

    fn request_header_source(name: &str, from: &str, key: &str) -> String {
        format!(
            r"
request_headers:
  - name: {name}
    from: {from}
    key: {key}
tables: []
"
        )
    }

    fn first_credential_method(inputs: &[ManifestInputSpec]) -> &ManifestCredentialMethod {
        let credential = inputs[0].credential.as_ref().expect("credential");
        assert_eq!(credential.methods.len(), 1);
        &credential.methods[0]
    }

    fn first_oauth(inputs: &[ManifestInputSpec]) -> &ManifestOAuthCredentialSpec {
        let method = first_credential_method(inputs);
        assert_eq!(method.kind, ManifestCredentialMethodKind::OAuth);
        method.oauth.as_ref().expect("oauth")
    }

    fn collect_oauth(raw: &str) -> Result<ManifestOAuthCredentialSpec> {
        collect(raw).map(|inputs| {
            inputs
                .iter()
                .find_map(|input| input.credential.as_ref()?.methods.first()?.oauth.as_ref())
                .expect("oauth")
                .clone()
        })
    }

    fn tenant_endpoint_oauth_input() -> String {
        default_oauth_input()
            .replace(
                "  API_TOKEN:\n",
                "  OUTLOOK_TENANT_ID:\n    kind: variable\n    default: organizations\n  API_TOKEN:\n",
            )
            .replace(
                "https://provider.example.com/oauth/authorize",
                "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/authorize",
            )
            .replace(
                "https://provider.example.com/oauth/token",
                "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/token",
            )
    }

    fn tenant_endpoint_oauth() -> ManifestOAuthCredentialSpec {
        collect_oauth(&tenant_endpoint_oauth_input().replace(
            "              token_url: https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/token",
            "              device_authorization_url: https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/devicecode\n              token_url: https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/token",
        ))
        .expect("tenant endpoint oauth")
    }

    fn outlook_tenant_inputs() -> BTreeMap<String, String> {
        BTreeMap::from([("OUTLOOK_TENANT_ID".to_string(), "organizations".to_string())])
    }

    #[test]
    fn reserved_input_key_prefix_is_rejected() {
        expect_collect_error(
            &manifest_with_input(
                r"
  __coral.API_TOKEN:
    kind: secret
",
            ),
            "reserved input key",
            "must not start with reserved prefix '__coral'",
        );
    }

    #[test]
    fn parses_source_config_credential_method() {
        let inputs = collect(&manifest_with_input(
            r"
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: source_config
          label: Paste token
          description: Paste a PAT.
",
        ))
        .expect("inputs");
        let method = first_credential_method(&inputs);
        assert_eq!(method.kind, ManifestCredentialMethodKind::SourceConfig);
        assert_eq!(method.label.as_deref(), Some("Paste token"));
        assert!(method.oauth.is_none());
    }

    #[test]
    fn parses_optional_secret_input() {
        let inputs = collect(&manifest_with_input(
            r"
  API_TOKEN:
    kind: secret
    required: false
",
        ))
        .expect("inputs");
        assert_eq!(inputs[0].key, "API_TOKEN");
        assert_eq!(inputs[0].kind, ManifestInputKind::Secret);
        assert!(!inputs[0].required);
    }

    #[test]
    fn parses_oauth_public_client_with_default_client_id() {
        let oauth = collect_oauth(&default_oauth_input()).expect("inputs");
        assert_eq!(oauth.flow.kind, ManifestOAuthFlowKind::AuthorizationCode);
        assert_eq!(oauth.flow.pkce, ManifestOAuthPkceMode::Required);
        assert_eq!(
            oauth.redirect_uri_port_mode,
            ManifestOAuthRedirectUriPortMode::Fixed
        );
        assert_eq!(
            oauth.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Fixed(53682)
        );
        assert_eq!(
            oauth.redirect_uri.as_deref(),
            Some("http://127.0.0.1:53682/oauth/callback")
        );
        assert_eq!(
            oauth.authorization_url.as_deref(),
            Some("https://provider.example.com/oauth/authorize")
        );
        assert_eq!(oauth.client.id.default.as_deref(), Some("default-client"));
        assert_eq!(
            oauth.scopes.as_ref().expect("scopes").scope.delimiter,
            ManifestOAuthScopeDelimiter::Space
        );
    }

    #[test]
    fn parses_random_redirect_uri_port_modes() {
        for (name, from, to) in [
            (
                "random mode without explicit port",
                DEFAULT_REDIRECT_URI_LINE,
                "            redirect_uri: http://127.0.0.1/oauth/callback\n            redirect_uri_port_mode: random\n",
            ),
            (
                "inferred random mode from explicit zero port",
                DEFAULT_REDIRECT_URI,
                "http://127.0.0.1:0/oauth/callback",
            ),
        ] {
            let oauth =
                collect_oauth(&default_oauth_input_replacing(from, to)).unwrap_or_else(|error| {
                    panic!("{name} should parse: {error}");
                });
            assert_eq!(
                oauth.redirect_uri_port_mode,
                ManifestOAuthRedirectUriPortMode::Random,
                "{name}"
            );
            assert_eq!(
                oauth.redirect_bind_port().expect("bind port"),
                ManifestOAuthRedirectBindPort::Random,
                "{name}"
            );
        }
    }

    #[test]
    fn parses_oauth_device_code_flow() {
        let inputs = collect(&manifest_with_input(
            r"
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect
          oauth:
            flow:
              type: device_code
            endpoints:
              device_authorization_url: https://provider.example.com/oauth/device/code
              token_url: https://provider.example.com/oauth/token
            client:
              id:
                input: OAUTH_CLIENT_ID
            scopes:
              scope:
                delimiter: space
                values:
                  - repo
                  - read:org
",
        ))
        .expect("inputs");
        let oauth = first_oauth(&inputs);
        assert_eq!(oauth.flow.kind, ManifestOAuthFlowKind::DeviceCode);
        assert_eq!(oauth.flow.pkce, ManifestOAuthPkceMode::Disabled);
        assert!(oauth.redirect_uri.is_none());
        assert!(oauth.authorization_url.is_none());
        assert_eq!(
            oauth.device_authorization_url.as_deref(),
            Some("https://provider.example.com/oauth/device/code")
        );
        assert_eq!(oauth.client.id.input.as_deref(), Some("OAUTH_CLIENT_ID"));
    }

    #[test]
    fn parses_oauth_public_client_id_variants() {
        for (client, expected_default, name) in [
            (
                r"
              id:
                input: OAUTH_CLIENT_ID
",
                None,
                "input-only client id",
            ),
            (
                r"
              id:
                default: default-client
                input: OAUTH_CLIENT_ID
",
                Some("default-client"),
                "default client id with input override",
            ),
        ] {
            let inputs = collect(&oauth_input(client)).unwrap_or_else(|error| {
                panic!("{name} should parse: {error}");
            });
            let oauth = first_oauth(&inputs);
            assert_eq!(
                oauth.client.id.default.as_deref(),
                expected_default,
                "{name}"
            );
            assert_eq!(
                oauth.client.id.input.as_deref(),
                Some("OAUTH_CLIENT_ID"),
                "{name}"
            );
        }
    }

    #[test]
    fn parses_confidential_oauth_client_secret_transports() {
        for (transport, expected) in [
            ("basic_auth", ManifestOAuthClientSecretTransport::BasicAuth),
            (
                "request_body",
                ManifestOAuthClientSecretTransport::RequestBody,
            ),
        ] {
            let inputs = collect(&oauth_input(&format!(
                r"
              id:
                input: OAUTH_CLIENT_ID
              secret:
                input: OAUTH_CLIENT_SECRET
                transport: {transport}
"
            )))
            .unwrap_or_else(|error| panic!("{transport} should parse: {error}"));
            let oauth = first_oauth(&inputs);
            assert_eq!(
                oauth.client.secret.as_ref().expect("secret").transport,
                expected,
                "{transport}"
            );
        }
    }

    #[test]
    fn oauth_client_secret_transport_labels_are_canonical() {
        for (transport, label) in [
            (ManifestOAuthClientSecretTransport::BasicAuth, "basic_auth"),
            (
                ManifestOAuthClientSecretTransport::RequestBody,
                "request_body",
            ),
        ] {
            assert_eq!(transport.label(), label);
            assert_eq!(
                ManifestOAuthClientSecretTransport::from_label(label),
                Some(transport)
            );
        }
        assert_eq!(
            ManifestOAuthClientSecretTransport::from_label("unsupported"),
            None
        );
    }

    #[test]
    fn rejects_invalid_oauth_credential_configurations() {
        for (case, manifest, expected) in [
            (
                "credential methods on variable input",
                manifest_with_input(
                    r"
  API_BASE:
    kind: variable
    credential:
      methods:
        - type: source_config
",
                ),
                "is not a secret",
            ),
            (
                "unknown credential method type",
                manifest_with_input(
                    r"
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: magic
",
                ),
                "unsupported type 'magic'",
            ),
            (
                "unsupported pkce mode",
                default_oauth_input_replacing("pkce: required", "pkce: optional"),
                "unsupported value 'optional'",
            ),
            (
                "malformed oauth endpoint URL",
                default_oauth_input_replacing(
                    "https://provider.example.com/oauth/authorize",
                    "not a url",
                ),
                "authorization_url is invalid",
            ),
            (
                "secret endpoint template reference",
                default_oauth_input_replacing(
                    "https://provider.example.com/oauth/token",
                    "https://provider.example.com/{{input.API_TOKEN}}/oauth/token",
                ),
                "is referenced by oauth.endpoints.token_url but is not a variable",
            ),
            (
                "runtime token endpoint template reference",
                default_oauth_input_replacing(
                    "https://provider.example.com/oauth/token",
                    "https://provider.example.com/{{filter.tenant}}/oauth/token",
                ),
                "only support source variable input tokens",
            ),
            (
                "client secret without transport",
                oauth_input(
                    r"
              id:
                input: OAUTH_CLIENT_ID
              secret:
                input: OAUTH_CLIENT_SECRET
",
                ),
                "missing transport",
            ),
        ] {
            expect_collect_error(&manifest, case, expected);
        }
    }

    #[test]
    fn parses_redirect_uri_with_explicit_default_http_port() {
        let oauth = collect_oauth(&default_oauth_input_replacing(
            DEFAULT_REDIRECT_URI,
            "http://127.0.0.1:80/oauth/callback",
        ))
        .expect("explicit default port should pass");
        assert_eq!(
            oauth.redirect_uri.as_deref(),
            Some("http://127.0.0.1:80/oauth/callback")
        );
        assert_eq!(
            oauth.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Fixed(80)
        );
    }

    #[test]
    fn rejects_invalid_redirect_uri_configurations() {
        for (name, from, to, expected) in [
            (
                "missing redirect uri",
                DEFAULT_REDIRECT_URI_LINE,
                "",
                "missing redirect_uri",
            ),
            (
                "missing port",
                DEFAULT_REDIRECT_URI,
                "http://127.0.0.1/oauth/callback",
                "explicit non-zero port",
            ),
            (
                "random mode with explicit nonzero port",
                DEFAULT_REDIRECT_URI_LINE,
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n            redirect_uri_port_mode: random\n",
                "must omit the port",
            ),
            (
                "random mode with explicit default port",
                DEFAULT_REDIRECT_URI_LINE,
                "            redirect_uri: http://127.0.0.1:80/oauth/callback\n            redirect_uri_port_mode: random\n",
                "must omit the port",
            ),
            (
                "fixed mode with explicit zero port",
                DEFAULT_REDIRECT_URI_LINE,
                "            redirect_uri: http://127.0.0.1:0/oauth/callback\n            redirect_uri_port_mode: fixed\n",
                "explicit non-zero port",
            ),
            (
                "non-loopback redirect",
                DEFAULT_REDIRECT_URI,
                "http://example.com:53682/oauth/callback",
                "loopback host",
            ),
        ] {
            let error = default_oauth_error(from, to, name);
            assert_error_contains(&error, expected);
        }
    }

    #[test]
    fn validates_oauth_endpoint_templates_with_declared_defaults() {
        expect_collect_error(
            &tenant_endpoint_oauth_input()
                .replace(
                    "OUTLOOK_TENANT_ID:\n    kind: variable\n    default: organizations",
                    "OUTLOOK_TENANT_ID:\n    kind: variable\n    default: foo bar.com",
                )
                .replace(
                    "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/token",
                    "https://{{input.OUTLOOK_TENANT_ID}}/oauth/token",
                ),
            "invalid default-rendered endpoint should fail",
            "oauth.endpoints.token_url is invalid",
        );
    }

    #[test]
    fn defers_oauth_endpoint_url_parsing_for_required_variables() {
        collect(
            &tenant_endpoint_oauth_input()
                .replace(
                    "OUTLOOK_TENANT_ID:\n    kind: variable\n    default: organizations",
                    "OUTLOOK_TENANT_ID:\n    kind: variable",
                )
                .replace(
                    "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/token",
                    "https://provider.example.com:{{input.OUTLOOK_TENANT_ID}}/oauth/token",
                ),
        )
        .expect("required variable endpoint parsing should be deferred");
    }

    #[test]
    fn endpoint_urls_reject_inline_template_defaults() {
        let mut oauth = tenant_endpoint_oauth();
        oauth.authorization_url = Some(
            "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID|organizations}}/oauth2/v2.0/authorize"
                .to_string(),
        );
        let error = oauth
            .endpoint_urls(&outlook_tenant_inputs())
            .expect_err("inline endpoint defaults should fail at the public render boundary");

        assert_error_contains(&error, "must declare defaults under top-level inputs");
    }

    #[test]
    fn endpoint_urls_render_source_input_templates() {
        let oauth = tenant_endpoint_oauth();
        assert_eq!(
            oauth.authorization_url.as_deref(),
            Some(
                "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/authorize"
            )
        );
        assert_eq!(
            oauth.device_authorization_url.as_deref(),
            Some(
                "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/devicecode"
            )
        );
        assert_eq!(
            oauth.token_url,
            "https://login.microsoftonline.com/{{input.OUTLOOK_TENANT_ID}}/oauth2/v2.0/token"
        );

        let endpoints = oauth
            .endpoint_urls(&outlook_tenant_inputs())
            .expect("endpoint urls");

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
    fn manifests_without_inputs_block_are_allowed() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://api.github.com
tables: []
";
        let inputs = collect(manifest).expect("no inputs is fine");
        assert!(inputs.is_empty());
    }

    #[test]
    fn references_without_inputs_block_are_rejected() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: "{{input.GITHUB_API_BASE}}"
tables: []
"#;
        expect_collect_error(
            manifest,
            "undeclared reference",
            "referenced but not declared under top-level inputs",
        );
    }

    #[test]
    fn undeclared_reference_is_rejected() {
        let manifest = manifest_with_input_and_body(
            r"
  GITHUB_TOKEN:
    kind: secret
",
            r#"
base_url: "{{input.GITHUB_API_BASE}}"
tables: []
"#,
        );
        expect_collect_error(
            &manifest,
            "undeclared input",
            "referenced but not declared under top-level inputs",
        );
    }

    #[test]
    fn value_source_references_resolve_against_declarations() {
        for (name, raw_input, body) in [
            (
                "auth input source",
                r"
  GITHUB_TOKEN:
    kind: secret
",
                auth_header_source("input", "GITHUB_TOKEN"),
            ),
            (
                "auth one_of source",
                r"
  API_KEY:
    kind: secret
    required: false
  OAUTH_TOKEN:
    kind: secret
    required: false
",
                auth_header_one_of_sources(&[("input", "API_KEY"), ("bearer", "OAUTH_TOKEN")]),
            ),
            (
                "auth bearer source",
                r"
  OAUTH_TOKEN:
    kind: secret
",
                auth_header_source("bearer", "OAUTH_TOKEN"),
            ),
            (
                "non-auth input source",
                r"
  API_VERSION:
    kind: variable
    default: 2026-01-01
",
                request_header_source("API-Version", "input", "API_VERSION"),
            ),
        ] {
            collect(&manifest_with_input_and_body(raw_input, &body))
                .unwrap_or_else(|error| panic!("{name} should resolve: {error}"));
        }
    }

    #[test]
    fn invalid_value_source_references_are_rejected() {
        for (name, raw_input, body, expected) in [
            (
                "undeclared one_of bearer",
                r"
  API_KEY:
    kind: secret
",
                auth_header_one_of_sources(&[("input", "API_KEY"), ("bearer", "OAUTH_TOKEN")]),
                "referenced but not declared under top-level inputs",
            ),
            (
                "bearer variable",
                r"
  HEADER_VALUE:
    kind: variable
    default: not-secret
",
                auth_header_source("bearer", "HEADER_VALUE"),
                "bearer value source 'HEADER_VALUE' must reference a secret input",
            ),
            (
                "auth input variable",
                r"
  HEADER_VALUE:
    kind: variable
    default: not-secret
",
                auth_header_one_of_sources(&[("input", "HEADER_VALUE")]),
                "auth input value source 'HEADER_VALUE' must reference a secret input",
            ),
        ] {
            expect_collect_error(
                &manifest_with_input_and_body(raw_input, &body),
                name,
                expected,
            );
        }
    }

    #[test]
    fn inline_template_defaults_are_rejected() {
        let manifest = manifest_with_input_and_body(
            r"
  GITHUB_API_BASE:
    kind: variable
    default: https://api.github.com
",
            r#"
base_url: "{{input.GITHUB_API_BASE|https://other.example.com}}"
tables: []
"#,
        );
        expect_collect_error(
            &manifest,
            "inline default",
            "must declare defaults under top-level inputs",
        );
    }

    #[test]
    fn secret_defaults_are_rejected() {
        let manifest = manifest_with_input(
            r"
  GITHUB_TOKEN:
    kind: secret
    default: abc123
",
        );
        expect_collect_error(&manifest, "secret default", "must not declare a default");
    }

    #[test]
    fn credential_like_variables_are_rejected() {
        for key in [
            "SERVICE_API_KEY",
            "STRIPE_SECRET_KEY",
            "WEAVIATE_API_KEY_STAGING",
        ] {
            let manifest = manifest_with_input(&format!(
                r"
  {key}:
    kind: variable
"
            ));
            expect_collect_error(&manifest, "credential variable", "looks credential-like");
        }
    }

    #[test]
    fn credential_like_check_respects_underscore_boundaries() {
        let manifest = manifest_with_input(
            r"
  SERVICE_SECRETARIAT_URL:
    kind: variable
",
        );
        let inputs = collect(&manifest).expect("non-credential variable");
        let [input] = inputs.as_slice() else {
            panic!("expected one input, got {inputs:?}");
        };
        assert_eq!(input.key, "SERVICE_SECRETARIAT_URL");
    }
}
