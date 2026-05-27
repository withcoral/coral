//! Extracts interactive source inputs from source-spec documents.
//!
//! Sources that need interactive configuration declare their inputs under a
//! top-level `inputs` map. Each entry fixes the input's kind (`variable` or
//! `secret`), an optional default, and an optional hint. References elsewhere
//! in the manifest use `{{input.KEY}}` templates or `from: input` value
//! sources; the declared kind determines whether the value is resolved from
//! the variable or secret store. Manifests that take no interactive inputs
//! may omit the block entirely.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{Map, Value};
use url::Url;

use crate::{ManifestError, ParsedTemplate, Result, TemplateNamespace};

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
    /// Provider authorization endpoint URL for authorization-code sessions.
    pub authorization_url: Option<String>,
    /// Provider device authorization endpoint URL for device-code sessions.
    pub device_authorization_url: Option<String>,
    /// Provider token endpoint URL.
    pub token_url: String,
    /// OAuth client configuration.
    pub client: ManifestOAuthClientSpec,
    /// Optional OAuth scope parameter configuration.
    pub scopes: Option<ManifestOAuthScopesSpec>,
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
    Ok(inputs)
}

fn collect_declared_inputs(root: &Value) -> Result<Vec<ManifestInputSpec>> {
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
        if kind != ManifestInputKind::Secret && credential.is_some() {
            return Err(ManifestError::validation(format!(
                "manifest input '{key}' declares credential methods but is not a secret"
            )));
        }
        ordered.push(ManifestInputSpec {
            key: key.clone(),
            kind,
            required: default_value.is_none(),
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

fn parse_credential(input_key: &str, value: &Value) -> Result<ManifestCredentialSpec> {
    let credential = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' credential must be a mapping"
        ))
    })?;
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
    let method = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' credential.methods[{index}] must be a mapping"
        ))
    })?;
    let label = method
        .get("label")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    let description = method
        .get("description")
        .and_then(Value::as_str)
        .map(ToString::to_string);
    match method.get("type").and_then(Value::as_str) {
        Some("source_config") => {
            if method.contains_key("oauth") {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' source_config credential method must not contain oauth"
                )));
            }
            Ok(ManifestCredentialMethod {
                kind: ManifestCredentialMethodKind::SourceConfig,
                label,
                description,
                oauth: None,
            })
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
            Ok(ManifestCredentialMethod {
                kind: ManifestCredentialMethodKind::OAuth,
                label,
                description,
                oauth: Some(oauth),
            })
        }
        Some(other) => Err(ManifestError::validation(format!(
            "manifest input '{input_key}' credential method has unsupported type '{other}'"
        ))),
        None => Err(ManifestError::validation(format!(
            "manifest input '{input_key}' credential method is missing type"
        ))),
    }
}

fn parse_oauth(
    input_key: &str,
    method_index: usize,
    value: &Value,
) -> Result<ManifestOAuthCredentialSpec> {
    let oauth = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' credential.methods[{method_index}].oauth must be a mapping"
        ))
    })?;
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
    if let Some(url) = authorization_url.as_deref() {
        validate_url(input_key, "authorization_url", url)?;
    }
    let device_authorization_url = optional_string(
        endpoints,
        "device_authorization_url",
        input_key,
        "oauth.endpoints",
    )?;
    if let Some(url) = device_authorization_url.as_deref() {
        validate_url(input_key, "device_authorization_url", url)?;
    }
    let token_url = required_string(endpoints, "token_url", input_key, "oauth.endpoints")?;
    validate_url(input_key, "token_url", &token_url)?;
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
    let flow = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.flow must be a mapping"
        ))
    })?;
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
    let client = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.client must be a mapping"
        ))
    })?;
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
    let id = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.client.id must be a mapping"
        ))
    })?;
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
    let secret = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.client.secret must be a mapping"
        ))
    })?;
    let input = required_string(secret, "input", input_key, "oauth.client.secret")?;
    validate_input_key("oauth client secret input key", &input)?;
    let transport = match secret.get("transport").and_then(Value::as_str) {
        Some("basic_auth") => ManifestOAuthClientSecretTransport::BasicAuth,
        Some("request_body") => ManifestOAuthClientSecretTransport::RequestBody,
        Some(other) => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.client.secret.transport has unsupported value '{other}'"
            )));
        }
        None => {
            return Err(ManifestError::validation(format!(
                "manifest input '{input_key}' oauth.client.secret is missing transport"
            )));
        }
    };
    Ok(ManifestOAuthClientSecretSpec { input, transport })
}

fn parse_oauth_scopes(input_key: &str, value: &Value) -> Result<ManifestOAuthScopesSpec> {
    let scopes = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.scopes must be a mapping"
        ))
    })?;
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
    let scope = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.scopes.scope must be a mapping"
        ))
    })?;
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
        ManifestOAuthFlowKind::AuthorizationCode => {
            if redirect_uri.is_none() {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' authorization_code oauth method is missing redirect_uri"
                )));
            }
            if authorization_url.is_none() {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' authorization_code oauth method is missing endpoints.authorization_url"
                )));
            }
        }
        ManifestOAuthFlowKind::DeviceCode => {
            if redirect_uri.is_some() {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' device_code oauth method must not declare redirect_uri"
                )));
            }
            if has_redirect_uri_port_mode {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' device_code oauth method must not declare redirect_uri_port_mode"
                )));
            }
            if authorization_url.is_some() {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' device_code oauth method must not declare endpoints.authorization_url"
                )));
            }
            if device_authorization_url.is_none() {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' device_code oauth method is missing endpoints.device_authorization_url"
                )));
            }
            if has_client_secret {
                return Err(ManifestError::validation(format!(
                    "manifest input '{input_key}' device_code oauth method must not declare client.secret"
                )));
            }
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

fn validate_url(input_key: &str, field: &str, raw: &str) -> Result<()> {
    Url::parse(raw).map_err(|error| {
        ManifestError::validation(format!(
            "manifest input '{input_key}' oauth.endpoints.{field} is invalid: {error}"
        ))
    })?;
    Ok(())
}

fn validate_input_key(label: &str, value: &str) -> Result<()> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err(ManifestError::validation(format!("missing {label}")));
    }
    if trimmed != value {
        return Err(ManifestError::validation(format!(
            "{label} must not contain leading or trailing whitespace"
        )));
    }
    if trimmed.contains('/') || trimmed.contains('\\') {
        return Err(ManifestError::validation(format!(
            "{label} must not contain '/' or '\\\\'"
        )));
    }
    if trimmed.contains('=') || trimmed.contains('\n') || trimmed.contains('\r') {
        return Err(ManifestError::validation(format!(
            "{label} must not contain '=', '\\n', or '\\r'"
        )));
    }
    if trimmed.starts_with('#') {
        return Err(ManifestError::validation(format!(
            "{label} must not start with '#'"
        )));
    }
    if let Some(prefix) = RESERVED_INPUT_KEY_PREFIXES
        .iter()
        .find(|prefix| trimmed.starts_with(**prefix))
    {
        return Err(ManifestError::validation(format!(
            "{label} must not start with reserved prefix '{prefix}'"
        )));
    }
    Ok(())
}

fn validate_input_references(root: &Value, inputs: &[ManifestInputSpec]) -> Result<()> {
    let declared: BTreeSet<String> = inputs.iter().map(|input| input.key.clone()).collect();
    validate_value(root, true, &declared)
}

fn validate_value(value: &Value, is_root: bool, declared: &BTreeSet<String>) -> Result<()> {
    match value {
        Value::Object(map) => {
            validate_mapping(map, declared)?;
            for (key, nested) in map {
                if is_root && key == "inputs" {
                    continue;
                }
                validate_value(nested, false, declared)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                validate_value(item, false, declared)?;
            }
        }
        Value::String(raw) => validate_template(raw, declared)?,
        Value::Null | Value::Bool(_) | Value::Number(_) => {}
    }
    Ok(())
}

fn validate_mapping(map: &Map<String, Value>, declared: &BTreeSet<String>) -> Result<()> {
    if map.get("from").and_then(Value::as_str) != Some("input") {
        return Ok(());
    }

    let key = map
        .get("key")
        .and_then(Value::as_str)
        .ok_or_else(|| ManifestError::validation("manifest 'input' value source is missing key"))?;
    if !declared.contains(key) {
        return Err(ManifestError::validation(format!(
            "manifest input '{key}' is referenced but not declared under top-level inputs"
        )));
    }
    if map.contains_key("default") {
        return Err(ManifestError::validation(format!(
            "manifest input '{key}' must declare defaults under top-level inputs"
        )));
    }
    Ok(())
}

fn validate_template(template: &str, declared: &BTreeSet<String>) -> Result<()> {
    let template = ParsedTemplate::parse(template)?;
    for token in template.tokens() {
        if !matches!(token.namespace(), TemplateNamespace::Input) {
            continue;
        }
        if !declared.contains(token.key()) {
            return Err(ManifestError::validation(format!(
                "manifest input '{}' is referenced but not declared under top-level inputs",
                token.key()
            )));
        }
        if token.default_value().is_some() {
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
        ManifestCredentialMethodKind, ManifestInputKind, ManifestInputSpec,
        ManifestOAuthClientSecretTransport, ManifestOAuthFlowKind, ManifestOAuthPkceMode,
        ManifestOAuthRedirectBindPort, ManifestOAuthRedirectUriPortMode,
        ManifestOAuthScopeDelimiter, collect_source_inputs_value,
    };
    use crate::{ManifestError, Result};

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
        format!(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
{raw_input}
base_url: https://api.example.com
tables: []
"
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

    #[test]
    fn reserved_input_key_prefix_is_rejected() {
        let error = collect(&manifest_with_input(
            r"
  __coral.API_TOKEN:
    kind: secret
",
        ))
        .expect_err("reserved input key");

        assert!(
            error
                .to_string()
                .contains("must not start with reserved prefix '__coral'")
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
        let credential = inputs[0].credential.as_ref().expect("credential");
        assert_eq!(credential.methods.len(), 1);
        assert_eq!(
            credential.methods[0].kind,
            ManifestCredentialMethodKind::SourceConfig
        );
        assert_eq!(credential.methods[0].label.as_deref(), Some("Paste token"));
        assert!(credential.methods[0].oauth.is_none());
    }

    #[test]
    fn parses_oauth_public_client_with_default_client_id() {
        let inputs = collect(&oauth_input(
            r"
              id:
                default: default-client
",
        ))
        .expect("inputs");
        let method = &inputs[0].credential.as_ref().expect("credential").methods[0];
        assert_eq!(method.kind, ManifestCredentialMethodKind::OAuth);
        let oauth = method.oauth.as_ref().expect("oauth");
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
    fn parses_random_redirect_uri_port_mode_without_explicit_port() {
        let inputs = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n",
                "            redirect_uri: http://127.0.0.1/oauth/callback\n            redirect_uri_port_mode: random\n",
            ),
        )
        .expect("inputs");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
        assert_eq!(
            oauth.redirect_uri_port_mode,
            ManifestOAuthRedirectUriPortMode::Random
        );
        assert_eq!(
            oauth.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Random
        );
    }

    #[test]
    fn infers_random_redirect_uri_port_mode_from_explicit_zero_port() {
        let inputs = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "http://127.0.0.1:53682/oauth/callback",
                "http://127.0.0.1:0/oauth/callback",
            ),
        )
        .expect("inputs");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
        assert_eq!(
            oauth.redirect_uri_port_mode,
            ManifestOAuthRedirectUriPortMode::Random
        );
        assert_eq!(
            oauth.redirect_bind_port().expect("bind port"),
            ManifestOAuthRedirectBindPort::Random
        );
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
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
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
    fn parses_oauth_public_client_with_input_client_id() {
        let inputs = collect(&oauth_input(
            r"
              id:
                input: OAUTH_CLIENT_ID
",
        ))
        .expect("inputs");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
        assert_eq!(oauth.client.id.input.as_deref(), Some("OAUTH_CLIENT_ID"));
        assert!(oauth.client.id.default.is_none());
    }

    #[test]
    fn parses_oauth_public_client_with_default_and_input_override() {
        let inputs = collect(&oauth_input(
            r"
              id:
                default: default-client
                input: OAUTH_CLIENT_ID
",
        ))
        .expect("inputs");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
        assert_eq!(oauth.client.id.default.as_deref(), Some("default-client"));
        assert_eq!(oauth.client.id.input.as_deref(), Some("OAUTH_CLIENT_ID"));
    }

    #[test]
    fn parses_confidential_oauth_client_with_basic_auth() {
        let inputs = collect(&oauth_input(
            r"
              id:
                input: OAUTH_CLIENT_ID
              secret:
                input: OAUTH_CLIENT_SECRET
                transport: basic_auth
",
        ))
        .expect("inputs");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
        assert_eq!(
            oauth.client.secret.as_ref().expect("secret").transport,
            ManifestOAuthClientSecretTransport::BasicAuth
        );
    }

    #[test]
    fn parses_confidential_oauth_client_with_request_body() {
        let inputs = collect(&oauth_input(
            r"
              id:
                input: OAUTH_CLIENT_ID
              secret:
                input: OAUTH_CLIENT_SECRET
                transport: request_body
",
        ))
        .expect("inputs");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
        assert_eq!(
            oauth.client.secret.as_ref().expect("secret").transport,
            ManifestOAuthClientSecretTransport::RequestBody
        );
    }

    #[test]
    fn rejects_credential_methods_on_variable_inputs() {
        let error = collect(&manifest_with_input(
            r"
  API_BASE:
    kind: variable
    credential:
      methods:
        - type: source_config
",
        ))
        .expect_err("variable credential should fail");
        assert!(error.to_string().contains("is not a secret"));
    }

    #[test]
    fn rejects_unknown_credential_method_type() {
        let error = collect(&manifest_with_input(
            r"
  API_TOKEN:
    kind: secret
    credential:
      methods:
        - type: magic
",
        ))
        .expect_err("unknown method should fail");
        assert!(error.to_string().contains("unsupported type 'magic'"));
    }

    #[test]
    fn rejects_unsupported_pkce_mode() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace("pkce: required", "pkce: optional"),
        )
        .expect_err("optional pkce should fail");
        assert!(error.to_string().contains("unsupported value 'optional'"));
    }

    #[test]
    fn rejects_missing_redirect_uri() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n",
                "",
            ),
        )
        .expect_err("missing redirect uri should fail");
        assert!(error.to_string().contains("missing redirect_uri"));
    }

    #[test]
    fn parses_redirect_uri_with_explicit_default_http_port() {
        let inputs = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "http://127.0.0.1:53682/oauth/callback",
                "http://127.0.0.1:80/oauth/callback",
            ),
        )
        .expect("explicit default port should pass");
        let oauth = inputs[0].credential.as_ref().expect("credential").methods[0]
            .oauth
            .as_ref()
            .expect("oauth");
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
    fn rejects_redirect_uri_without_explicit_port() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "http://127.0.0.1:53682/oauth/callback",
                "http://127.0.0.1/oauth/callback",
            ),
        )
        .expect_err("missing port should fail");
        assert!(error.to_string().contains("explicit non-zero port"));
    }

    #[test]
    fn rejects_random_redirect_uri_port_mode_with_explicit_nonzero_port() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n",
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n            redirect_uri_port_mode: random\n",
            ),
        )
        .expect_err("random port with explicit nonzero port should fail");
        assert!(error.to_string().contains("must omit the port"));
    }

    #[test]
    fn rejects_random_redirect_uri_port_mode_with_explicit_default_http_port() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n",
                "            redirect_uri: http://127.0.0.1:80/oauth/callback\n            redirect_uri_port_mode: random\n",
            ),
        )
        .expect_err("random port with explicit default port should fail");
        assert!(error.to_string().contains("must omit the port"));
    }

    #[test]
    fn rejects_fixed_redirect_uri_port_mode_with_explicit_zero_port() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "            redirect_uri: http://127.0.0.1:53682/oauth/callback\n",
                "            redirect_uri: http://127.0.0.1:0/oauth/callback\n            redirect_uri_port_mode: fixed\n",
            ),
        )
        .expect_err("fixed port with explicit zero port should fail");
        assert!(error.to_string().contains("explicit non-zero port"));
    }

    #[test]
    fn rejects_non_loopback_redirect_uri() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace(
                "http://127.0.0.1:53682/oauth/callback",
                "http://example.com:53682/oauth/callback",
            ),
        )
        .expect_err("non-loopback redirect should fail");
        assert!(error.to_string().contains("loopback host"));
    }

    #[test]
    fn rejects_malformed_oauth_endpoint_urls() {
        let error = collect(
            &oauth_input(
                r"
              id:
                default: default-client
",
            )
            .replace("https://provider.example.com/oauth/authorize", "not a url"),
        )
        .expect_err("bad endpoint should fail");
        assert!(error.to_string().contains("authorization_url is invalid"));
    }

    #[test]
    fn rejects_client_secret_without_transport() {
        let error = collect(&oauth_input(
            r"
              id:
                input: OAUTH_CLIENT_ID
              secret:
                input: OAUTH_CLIENT_SECRET
",
        ))
        .expect_err("missing transport should fail");
        assert!(error.to_string().contains("missing transport"));
    }

    #[test]
    fn from_input_value_source_resolves_against_declarations() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_TOKEN:
    kind: secret
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: input
      key: GITHUB_TOKEN
tables: []
";
        let inputs = collect(manifest).expect("inputs");
        let [input] = inputs.as_slice() else {
            panic!("expected one input, got {inputs:?}");
        };
        assert_eq!(input.kind, ManifestInputKind::Secret);
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
        let error = collect(manifest).expect_err("undeclared reference");
        assert!(
            error
                .to_string()
                .contains("referenced but not declared under top-level inputs"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn undeclared_reference_is_rejected() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_TOKEN:
    kind: secret
base_url: "{{input.GITHUB_API_BASE}}"
tables: []
"#;
        let error = collect(manifest).expect_err("undeclared input");
        assert!(
            error
                .to_string()
                .contains("referenced but not declared under top-level inputs")
        );
    }

    #[test]
    fn inline_template_defaults_are_rejected() {
        let manifest = r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_API_BASE:
    kind: variable
    default: https://api.github.com
base_url: "{{input.GITHUB_API_BASE|https://other.example.com}}"
tables: []
"#;
        let error = collect(manifest).expect_err("inline default");
        assert!(
            error
                .to_string()
                .contains("must declare defaults under top-level inputs")
        );
    }

    #[test]
    fn secret_defaults_are_rejected() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  GITHUB_TOKEN:
    kind: secret
    default: abc123
tables: []
";
        let error = collect(manifest).expect_err("secret default");
        assert!(error.to_string().contains("must not declare a default"));
    }

    #[test]
    fn credential_like_variables_are_rejected() {
        for key in [
            "SERVICE_API_KEY",
            "STRIPE_SECRET_KEY",
            "WEAVIATE_API_KEY_STAGING",
        ] {
            let manifest = format!(
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  {key}:
    kind: variable
tables: []
"
            );
            let error = collect(&manifest).expect_err("credential variable");
            assert!(error.to_string().contains("looks credential-like"));
        }
    }

    #[test]
    fn credential_like_check_respects_underscore_boundaries() {
        let manifest = r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
inputs:
  SERVICE_SECRETARIAT_URL:
    kind: variable
tables: []
";
        let inputs = collect(manifest).expect("non-credential variable");
        let [input] = inputs.as_slice() else {
            panic!("expected one input, got {inputs:?}");
        };
        assert_eq!(input.key, "SERVICE_SECRETARIAT_URL");
    }
}
