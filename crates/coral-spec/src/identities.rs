//! Declarative identity-spec parsing and validation.

mod schema;

pub use schema::generated_identity_manifest_schema;

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::inputs::{
    collect_declared_inputs, parse_identity_oauth_method,
    validate_oauth_endpoint_templates_for_method,
};
use crate::parser::parse_yaml_value;
use crate::{
    ManifestError, ManifestInputKind, ManifestInputSpec, ManifestOAuthCredentialSpec, Result,
    validate_identifier,
};

/// Current authored identity-spec format version.
pub const IDENTITY_SPEC_VERSION: u32 = 1;

/// Validate the stable name used by authored and persisted identity specs.
pub fn validate_identity_spec_name(name: &str) -> Result<()> {
    validate_identifier(name, "identity spec name")
}

/// Validated identity manifest.
#[derive(Debug, Clone, PartialEq)]
pub struct IdentityManifest {
    /// Authored identity-spec format version.
    pub spec_version: u32,
    /// Stable identity spec name.
    pub name: String,
    /// Version of this identity spec.
    pub version: String,
    /// Human-readable identity description.
    pub description: String,
    /// Provider or issuer name, such as `github` or `google`.
    pub issuer: String,
    /// Type of identity, which determines setup and request injection semantics.
    pub identity_type: IdentitySpecType,
    /// Required request host, optional port, and provider-specific audience constraints.
    pub audience: BTreeMap<String, Value>,
    /// Identity setup inputs owned by the installed identity spec.
    pub inputs: Vec<ManifestInputSpec>,
    /// Type-specific identity setup configuration.
    pub config: IdentitySpecConfig,
}

/// Supported identity spec types.
#[derive(Debug, Clone, Copy, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdentitySpecType {
    /// OAuth access token identity.
    #[serde(rename = "oauth")]
    OAuth,
    /// User-supplied fixed bearer token identity.
    FixedToken,
}

/// Type-specific identity setup configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IdentitySpecConfig {
    /// OAuth identity setup method.
    OAuth(Box<IdentityOAuthSpec>),
    /// Fixed token identities collect one token through the identity provider.
    FixedToken,
}

/// OAuth setup configuration for an identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityOAuthSpec {
    /// Authored OAuth setup method.
    pub method: IdentityOAuthMethodSpec,
}

/// One OAuth setup method for an identity spec.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IdentityOAuthMethodSpec {
    /// Optional display label.
    pub label: Option<String>,
    /// Optional display description.
    pub description: Option<String>,
    /// Optional setup hint.
    pub hint: Option<String>,
    /// OAuth flow and endpoint configuration.
    pub oauth: ManifestOAuthCredentialSpec,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIdentityManifest {
    #[serde(rename = "kind")]
    _kind: IdentityManifestKind,
    spec_version: u32,
    name: String,
    version: String,
    #[serde(default)]
    description: String,
    issuer: String,
    #[serde(rename = "type")]
    identity_type: IdentitySpecType,
    audience: RawIdentityAudience,
    #[serde(default, rename = "inputs")]
    _inputs: Option<Value>,
    #[serde(default)]
    oauth: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct RawIdentityAudience {
    host: String,
    #[serde(default)]
    port: Option<u16>,
}

impl RawIdentityAudience {
    fn validate_and_normalize(self, name: &str) -> Result<BTreeMap<String, Value>> {
        if self.host.trim().is_empty() {
            return Err(ManifestError::validation(format!(
                "identity '{name}' audience.host must be a non-empty string"
            )));
        }
        let host = url::Host::parse(&self.host).map_err(|_error| {
            ManifestError::validation(format!(
                "identity '{name}' audience.host must be a valid typed host"
            ))
        })?;
        if self.port == Some(0) {
            return Err(ManifestError::validation(format!(
                "identity '{name}' audience.port must be an integer from 1 through 65535"
            )));
        }
        let mut audience = BTreeMap::new();
        audience.insert("host".to_string(), Value::String(host.to_string()));
        if let Some(port) = self.port {
            audience.insert("port".to_string(), Value::from(port));
        }
        Ok(audience)
    }
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum IdentityManifestKind {
    Identity,
}

/// Parse and validate one identity manifest from YAML.
pub fn parse_identity_manifest_yaml(raw: &str) -> Result<IdentityManifest> {
    let manifest_value = parse_yaml_value(raw)?;
    parse_identity_manifest_value(manifest_value)
}

/// Parse and validate one identity manifest from a structured value.
pub fn parse_identity_manifest_value(value: Value) -> Result<IdentityManifest> {
    reject_legacy_identity_fields(&value)?;
    crate::schema::validate_identity_manifest_schema(&value)?;
    let inputs = collect_declared_inputs(&value)?;
    let raw: RawIdentityManifest =
        serde_json::from_value(value).map_err(ManifestError::deserialize)?;

    validate_identity_manifest(&raw)?;
    let audience = raw.audience.validate_and_normalize(&raw.name)?;
    let config = parse_identity_config(&raw.name, raw.identity_type, raw.oauth.as_ref())?;
    validate_identity_inputs(&raw.name, raw.identity_type, &inputs, &config)?;

    Ok(IdentityManifest {
        spec_version: raw.spec_version,
        name: raw.name,
        version: raw.version,
        description: raw.description,
        issuer: raw.issuer,
        identity_type: raw.identity_type,
        audience,
        inputs,
        config,
    })
}

fn reject_legacy_identity_fields(value: &Value) -> Result<()> {
    let Some(object) = value.as_object() else {
        return Ok(());
    };
    for field in ["auth", "injection_method"] {
        if object.contains_key(field) {
            return Err(ManifestError::validation(format!(
                "identity specs must not declare '{field}'; identity type determines setup and HTTP request injection"
            )));
        }
    }
    if object
        .get("oauth")
        .and_then(Value::as_object)
        .is_some_and(|oauth| oauth.contains_key("methods"))
    {
        let name = object
            .get("name")
            .and_then(Value::as_str)
            .unwrap_or("<unknown>");
        return Err(ManifestError::validation(format!(
            "identity '{name}' oauth.methods is not supported; use one oauth.method per identity spec"
        )));
    }
    Ok(())
}

fn validate_identity_manifest(raw: &RawIdentityManifest) -> Result<()> {
    if raw.spec_version != IDENTITY_SPEC_VERSION {
        return Err(ManifestError::validation(format!(
            "identity manifest spec_version must be {IDENTITY_SPEC_VERSION}"
        )));
    }
    validate_identity_spec_name(&raw.name)?;
    if raw.version.trim().is_empty() {
        return Err(ManifestError::validation(format!(
            "identity '{}' version must not be empty",
            raw.name
        )));
    }
    validate_identifier(&raw.issuer, &format!("identity '{}' issuer", raw.name))
}

fn parse_identity_config(
    name: &str,
    identity_type: IdentitySpecType,
    oauth: Option<&Value>,
) -> Result<IdentitySpecConfig> {
    match identity_type {
        IdentitySpecType::OAuth => {
            let oauth = oauth.ok_or_else(|| {
                ManifestError::validation(format!("identity '{name}' type oauth is missing oauth"))
            })?;
            parse_identity_oauth(name, oauth)
                .map(Box::new)
                .map(IdentitySpecConfig::OAuth)
        }
        IdentitySpecType::FixedToken => {
            if oauth.is_some() {
                return Err(ManifestError::validation(format!(
                    "identity '{name}' type fixed_token must not declare oauth"
                )));
            }
            Ok(IdentitySpecConfig::FixedToken)
        }
    }
}

fn parse_identity_oauth(name: &str, value: &Value) -> Result<IdentityOAuthSpec> {
    let object = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!("identity '{name}' oauth must be a mapping"))
    })?;
    if object.contains_key("methods") {
        return Err(ManifestError::validation(format!(
            "identity '{name}' oauth.methods is not supported; use one oauth.method per identity spec"
        )));
    }
    validate_known_fields(name, "oauth", object, &["method"])?;
    let method = object
        .get("method")
        .ok_or_else(|| {
            ManifestError::validation(format!("identity '{name}' oauth is missing method"))
        })
        .and_then(|method| parse_identity_oauth_method_config(name, method))?;
    Ok(IdentityOAuthSpec { method })
}

fn parse_identity_oauth_method_config(
    name: &str,
    value: &Value,
) -> Result<IdentityOAuthMethodSpec> {
    let method = value.as_object().ok_or_else(|| {
        ManifestError::validation(format!("identity '{name}' oauth.method must be a mapping"))
    })?;
    validate_identity_oauth_method_fields(name, method)?;
    Ok(IdentityOAuthMethodSpec {
        label: optional_string(method, "label", name)?,
        description: optional_string(method, "description", name)?,
        hint: optional_string(method, "hint", name)?,
        oauth: parse_identity_oauth_method(name, value)?,
    })
}

fn validate_identity_inputs(
    name: &str,
    identity_type: IdentitySpecType,
    inputs: &[ManifestInputSpec],
    config: &IdentitySpecConfig,
) -> Result<()> {
    if identity_type != IdentitySpecType::OAuth {
        if inputs.is_empty() {
            return Ok(());
        }
        return Err(ManifestError::validation(format!(
            "identity '{name}' type fixed_token must not declare inputs"
        )));
    }
    let IdentitySpecConfig::OAuth(oauth) = config else {
        return Ok(());
    };
    for input in inputs {
        if input.credential.is_some() {
            return Err(ManifestError::validation(format!(
                "identity '{name}' input '{}' must not declare credential methods",
                input.key
            )));
        }
    }
    let declared = inputs
        .iter()
        .map(|input| (input.key.as_str(), input))
        .collect::<BTreeMap<_, _>>();
    validate_oauth_endpoint_templates_for_method(
        name,
        &oauth.method.oauth,
        &declared,
        "identity inputs",
    )?;
    validate_identity_oauth_client_input(name, &declared, &oauth.method.oauth)
}

fn validate_identity_oauth_client_input(
    name: &str,
    declared: &BTreeMap<&str, &ManifestInputSpec>,
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<()> {
    if let Some(input_key) = oauth.client.id.input.as_deref() {
        match declared.get(input_key) {
            Some(input) if input.kind == ManifestInputKind::Variable => {}
            Some(_) => {
                return Err(ManifestError::validation(format!(
                    "identity '{name}' oauth.client.id.input '{input_key}' must reference a variable input"
                )));
            }
            None => {
                return Err(ManifestError::validation(format!(
                    "identity '{name}' oauth.client.id.input '{input_key}' must reference a declared variable input"
                )));
            }
        }
    }
    if let Some(secret) = oauth.client.secret.as_ref() {
        match declared.get(secret.input.as_str()) {
            Some(input) if input.kind == ManifestInputKind::Secret => {}
            Some(_) => {
                return Err(ManifestError::validation(format!(
                    "identity '{name}' oauth.client.secret.input '{}' must reference a secret input",
                    secret.input
                )));
            }
            None => {
                return Err(ManifestError::validation(format!(
                    "identity '{name}' oauth.client.secret.input '{}' must reference a declared secret input",
                    secret.input
                )));
            }
        }
    }
    Ok(())
}

fn validate_identity_oauth_method_fields(
    name: &str,
    method: &serde_json::Map<String, Value>,
) -> Result<()> {
    validate_known_fields(
        name,
        "oauth.method",
        method,
        &[
            "label",
            "description",
            "hint",
            "flow",
            "resource",
            "redirect_uri",
            "redirect_uri_port_mode",
            "endpoints",
            "client",
            "scopes",
        ],
    )?;
    validate_nested_known_fields(name, "oauth.method", method, "flow", &["type", "pkce"])?;
    validate_nested_known_fields(
        name,
        "oauth.method",
        method,
        "endpoints",
        &["authorization_url", "device_authorization_url", "token_url"],
    )?;
    let Some(client) = optional_object(name, "oauth.method", method, "client")? else {
        return Ok(());
    };
    validate_known_fields(
        name,
        "oauth.method.client",
        client,
        &["id", "secret", "dynamic_registration"],
    )?;
    validate_nested_known_fields(
        name,
        "oauth.method.client",
        client,
        "id",
        &["default", "input"],
    )?;
    validate_nested_known_fields(
        name,
        "oauth.method.client",
        client,
        "secret",
        &["input", "transport"],
    )?;
    validate_nested_known_fields(
        name,
        "oauth.method.client",
        client,
        "dynamic_registration",
        &[
            "registration_url",
            "client_name",
            "token_endpoint_auth_method",
            "request_refresh_token_grant",
        ],
    )?;
    let Some(scopes) = optional_object(name, "oauth.method", method, "scopes")? else {
        return Ok(());
    };
    validate_known_fields(name, "oauth.method.scopes", scopes, &["scope"])?;
    validate_nested_known_fields(
        name,
        "oauth.method.scopes",
        scopes,
        "scope",
        &["delimiter", "values"],
    )
}

fn validate_nested_known_fields(
    name: &str,
    parent_path: &str,
    parent: &serde_json::Map<String, Value>,
    field: &str,
    allowed: &[&str],
) -> Result<()> {
    let Some(object) = optional_object(name, parent_path, parent, field)? else {
        return Ok(());
    };
    validate_known_fields(name, &format!("{parent_path}.{field}"), object, allowed)
}

fn optional_object<'a>(
    name: &str,
    parent_path: &str,
    parent: &'a serde_json::Map<String, Value>,
    field: &str,
) -> Result<Option<&'a serde_json::Map<String, Value>>> {
    let Some(value) = parent.get(field) else {
        return Ok(None);
    };
    value.as_object().map(Some).ok_or_else(|| {
        ManifestError::validation(format!(
            "identity '{name}' {parent_path}.{field} must be a mapping"
        ))
    })
}

fn validate_known_fields(
    name: &str,
    path: &str,
    object: &serde_json::Map<String, Value>,
    allowed: &[&str],
) -> Result<()> {
    for key in object.keys() {
        if !allowed.contains(&key.as_str()) {
            return Err(ManifestError::validation(format!(
                "identity '{name}' {path} has unsupported field '{key}'"
            )));
        }
    }
    Ok(())
}

fn optional_string(
    object: &serde_json::Map<String, Value>,
    field: &str,
    name: &str,
) -> Result<Option<String>> {
    object
        .get(field)
        .map(|value| {
            value.as_str().map(ToString::to_string).ok_or_else(|| {
                ManifestError::validation(format!(
                    "identity '{name}' oauth.method.{field} must be a string"
                ))
            })
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use super::{IdentitySpecConfig, parse_identity_manifest_yaml, validate_identity_spec_name};

    const OAUTH_BODY: &str = "oauth:\n  method:\n    flow: {type: device_code}\n    endpoints: {device_authorization_url: 'https://provider.example.com/device', token_url: 'https://provider.example.com/token'}\n    client: {id: {default: demo-client}}";

    fn identity(identity_type: &str, body: &str) -> String {
        identity_with_audience(identity_type, "{host: provider.example.com}", body)
    }

    fn identity_with_audience(identity_type: &str, audience: &str, body: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: demo_{identity_type}\nversion: 0.1.0\nissuer: demo\ntype: {identity_type}\naudience: {audience}\n{body}"
        )
    }

    #[test]
    fn parses_minimal_identity_manifest_types() {
        let oauth = parse_identity_manifest_yaml(&identity("oauth", OAUTH_BODY)).expect("oauth");
        assert!(matches!(oauth.config, IdentitySpecConfig::OAuth(_)));
        let fixed =
            parse_identity_manifest_yaml(&identity("fixed_token", "")).expect("fixed token");
        assert!(matches!(fixed.config, IdentitySpecConfig::FixedToken));
    }

    #[test]
    fn parses_typed_audience() {
        let parsed = parse_identity_manifest_yaml(&identity_with_audience(
            "fixed_token",
            "{host: PROVIDER.EXAMPLE.COM, port: 8443}",
            "",
        ))
        .expect("typed audience");

        assert_eq!(
            parsed.audience.get("host").and_then(|value| value.as_str()),
            Some("provider.example.com")
        );
        assert_eq!(
            parsed
                .audience
                .get("port")
                .and_then(serde_json::Value::as_u64),
            Some(8443)
        );
    }

    #[test]
    fn identity_spec_names_use_the_manifest_identifier_grammar() {
        for valid in ["github", "github_oauth2", "_internal"] {
            validate_identity_spec_name(valid).expect(valid);
        }
        for invalid in [
            "",
            "9github",
            "github-oauth",
            "github oauth",
            "github/oauth",
        ] {
            validate_identity_spec_name(invalid).expect_err(invalid);
        }
    }

    #[test]
    fn parses_confidential_identity_oauth_with_default_client_id() {
        let raw = identity(
            "oauth",
            r"
inputs:
  CLIENT_SECRET: {kind: secret}
oauth:
  method:
    flow: {type: authorization_code, pkce: disabled}
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    endpoints:
      authorization_url: https://provider.example.com/authorize
      token_url: https://provider.example.com/token
    client:
      id: {default: demo-client}
      secret: {input: CLIENT_SECRET, transport: basic_auth}
",
        );

        let parsed = parse_identity_manifest_yaml(&raw).expect("confidential identity OAuth");
        let IdentitySpecConfig::OAuth(config) = parsed.config else {
            panic!("expected OAuth config");
        };
        assert_eq!(
            config.method.oauth.client.id.default.as_deref(),
            Some("demo-client")
        );
        assert_eq!(
            config
                .method
                .oauth
                .client
                .secret
                .as_ref()
                .map(|secret| secret.input.as_str()),
            Some("CLIENT_SECRET")
        );
    }

    #[test]
    fn rejects_duplicate_identity_oauth_mapping_keys() {
        let raw = identity(
            "oauth",
            r"
oauth:
  method:
    flow: {type: device_code}
    endpoints:
      device_authorization_url: https://provider.example.com/device
      token_url: https://provider.example.com/first-token
      token_url: https://provider.example.com/second-token
    client:
      id: {default: demo-client}
",
        );

        let error = parse_identity_manifest_yaml(&raw)
            .expect_err("a duplicate OAuth endpoint must not override an earlier value");

        assert!(
            error
                .to_string()
                .contains("duplicate entry with key \"token_url\""),
            "unexpected duplicate-key error: {error}"
        );
    }

    #[test]
    fn schema_validation_rejects_fixed_token_oauth_even_when_null() {
        let error = parse_identity_manifest_yaml(&identity("fixed_token", "oauth: null"))
            .expect_err("fixed-token identities must not declare oauth");
        assert!(
            error
                .to_string()
                .starts_with("identity manifest failed schema validation:"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_minimal_invalid_identity_manifests() {
        let dangling_input = "oauth:\n  method:\n    flow: {type: device_code}\n    endpoints: {device_authorization_url: 'https://provider.example.com/device', token_url: 'https://provider.example.com/token'}\n    client: {id: {input: OAUTH_CLIENT_ID}}";
        for raw in [
            identity("fixed_token", "unexpected: true"),
            "kind: identity\nspec_version: 2\nname: demo\nversion: 0.1.0\nissuer: demo\ntype: fixed_token\naudience: {host: provider.example.com}".to_string(),
            "kind: identity\nspec_version: 1\nname: demo\nversion: 0.1.0\nissuer: demo\ntype: fixed_token".to_string(),
            identity_with_audience("fixed_token", "{port: 443}", ""),
            identity_with_audience("fixed_token", "{host: provider.example.com, port: 0}", ""),
            identity_with_audience(
                "fixed_token",
                "{host: provider.example.com, tenant: demo}",
                "",
            ),
            identity_with_audience(
                "fixed_token",
                "{host: provider.example.com, port: 65536}",
                "",
            ),
            identity("oauth", &format!("{OAUTH_BODY}\n  unexpected: true")),
        ] {
            let error = parse_identity_manifest_yaml(&raw).expect_err(&raw);
            assert!(
                error
                    .to_string()
                    .starts_with("identity manifest failed schema validation:"),
                "unexpected error: {error}"
            );
        }

        for (raw, expected) in [
            (
                identity_with_audience("fixed_token", "{host: '   '}", ""),
                "audience.host must be a non-empty string",
            ),
            (
                identity_with_audience("fixed_token", "{host: 'https://provider.example.com'}", ""),
                "audience.host must be a valid typed host",
            ),
            (
                identity("oauth", dangling_input),
                "must reference a declared variable input",
            ),
        ] {
            let error = parse_identity_manifest_yaml(&raw).expect_err(&raw);
            assert!(
                error.to_string().contains(expected),
                "unexpected error: {error}"
            );
        }
    }
}
