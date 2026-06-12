//! Declarative identity-spec parsing and validation.
//!
//! Identity specs describe how an app-managed identity can be instantiated. The
//! identity type determines both setup and request injection semantics; source
//! specs only declare which identity shapes they can accept.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::inputs::{
    collect_declared_inputs, parse_identity_oauth_method,
    validate_oauth_endpoint_templates_for_method,
};
use crate::schema::validate_identity_manifest_schema;
use crate::{
    ManifestError, ManifestInputKind, ManifestInputSpec, ManifestOAuthCredentialSpec, Result,
    validate_identifier,
};

/// Current authored identity-spec format version.
pub const IDENTITY_SPEC_VERSION: u32 = 1;

/// Validated identity manifest.
#[derive(Debug, Clone, PartialEq, Serialize)]
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
    /// Provider-specific audience constraints for this identity.
    pub audience: BTreeMap<String, Value>,
    /// Identity setup inputs owned by the installed identity spec.
    pub inputs: Vec<ManifestInputSpec>,
    /// Type-specific identity setup configuration.
    pub config: IdentitySpecConfig,
}

/// Supported identity spec types.
#[derive(Debug, Clone, Copy, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IdentitySpecType {
    /// OAuth access token identity.
    #[serde(rename = "oauth")]
    OAuth,
    /// User-supplied fixed bearer token identity.
    FixedToken,
}

impl IdentitySpecType {
    /// Canonical manifest label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::OAuth => "oauth",
            Self::FixedToken => "fixed_token",
        }
    }
}

/// Type-specific identity setup configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum IdentitySpecConfig {
    /// OAuth identity setup method.
    OAuth(Box<IdentityOAuthSpec>),
    /// Fixed token identities collect one token through the identity provider.
    FixedToken,
}

/// OAuth setup configuration for an identity spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct IdentityOAuthSpec {
    /// Authored OAuth setup method.
    pub method: IdentityOAuthMethodSpec,
}

/// One OAuth setup method for an identity spec.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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
    // `kind` and `spec_version` are fully constrained by the JSON schema
    // (`const` values) before deserialization; they are declared here only so
    // `deny_unknown_fields` accepts them.
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
    #[serde(default)]
    audience: BTreeMap<String, Value>,
    #[serde(default, rename = "inputs")]
    _inputs: Option<Value>,
    #[serde(default)]
    oauth: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum IdentityManifestKind {
    Identity,
}

/// Parse and validate one identity manifest from YAML.
pub fn parse_identity_manifest_yaml(raw: &str) -> Result<IdentityManifest> {
    let manifest_value = serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    parse_identity_manifest_value(manifest_value)
}

/// Parse and validate one identity manifest from a structured value.
pub fn parse_identity_manifest_value(value: Value) -> Result<IdentityManifest> {
    reject_legacy_identity_fields(&value)?;
    validate_identity_manifest_schema(&value)?;
    let inputs = collect_declared_inputs(&value)?;
    let raw: RawIdentityManifest =
        serde_json::from_value(value).map_err(ManifestError::deserialize)?;

    validate_identity_manifest(&raw)?;
    let config = parse_identity_config(&raw.name, raw.identity_type, raw.oauth.as_ref())?;
    validate_identity_inputs(&raw.name, raw.identity_type, &inputs, &config)?;

    Ok(IdentityManifest {
        spec_version: raw.spec_version,
        name: raw.name,
        version: raw.version,
        description: raw.description,
        issuer: raw.issuer,
        identity_type: raw.identity_type,
        audience: raw.audience,
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
    validate_identifier(&raw.name, "identity spec name")?;
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
    let label = optional_string(method, "label", name)?;
    let description = optional_string(method, "description", name)?;
    let hint = optional_string(method, "hint", name)?;
    let oauth = parse_identity_oauth_method(name, 0, value)?;
    Ok(IdentityOAuthMethodSpec {
        label,
        description,
        hint,
        oauth,
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
            "identity '{name}' type '{}' must not declare inputs",
            identity_type.label()
        )));
    }
    let IdentitySpecConfig::OAuth(oauth) = config else {
        return Ok(());
    };
    if inputs.is_empty() {
        oauth.method.oauth.endpoint_urls(&BTreeMap::new())?;
        return Ok(());
    }
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
    validate_identity_oauth_client_input(name, inputs, &oauth.method.oauth)
}

fn validate_identity_oauth_client_input(
    name: &str,
    inputs: &[ManifestInputSpec],
    oauth: &ManifestOAuthCredentialSpec,
) -> Result<()> {
    let declared = inputs
        .iter()
        .map(|input| (input.key.as_str(), input))
        .collect::<BTreeMap<_, _>>();
    if let Some(input_key) = oauth.client.id.input.as_deref()
        && let Some(input) = declared.get(input_key)
        && input.kind != ManifestInputKind::Variable
    {
        return Err(ManifestError::validation(format!(
            "identity '{name}' oauth.client.id.input '{input_key}' must reference a variable input"
        )));
    }
    if let Some(secret) = oauth.client.secret.as_ref()
        && let Some(input) = declared.get(secret.input.as_str())
        && input.kind != ManifestInputKind::Secret
    {
        return Err(ManifestError::validation(format!(
            "identity '{name}' oauth.client.secret.input '{}' must reference a secret input",
            secret.input
        )));
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

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityManifestSchema {
    kind: IdentityManifestKind,
    spec_version: u32,
    name: String,
    version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    issuer: String,
    #[serde(rename = "type")]
    identity_type: IdentitySpecType,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    audience: BTreeMap<String, Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inputs: Option<BTreeMap<String, IdentityInputSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    oauth: Option<IdentityOAuthSchema>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityOAuthSchema {
    method: Value,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum IdentityInputSchema {
    #[serde(rename = "variable")]
    Variable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        required: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
    #[serde(rename = "secret")]
    Secret {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        required: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
}

/// Generate the JSON Schema for authored identity manifests.
///
/// # Panics
///
/// Panics only if the schema produced by `schemars` cannot be serialized to
/// JSON, which would indicate an invalid schema type definition in this crate.
pub fn generated_identity_manifest_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(IdentityManifestSchema))
        .expect("generated identity manifest schema must serialize");
    post_process_identity_schema(&mut schema);
    schema
}

fn post_process_identity_schema(schema: &mut Value) {
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    root.insert(
        "$id".to_string(),
        Value::String("https://coral.local/identity_manifest.schema.json".to_string()),
    );
    root.insert(
        "title".to_string(),
        Value::String("Coral Identity Manifest".to_string()),
    );
    root.entry("$schema".to_string()).or_insert_with(|| {
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string())
    });

    if let Some(properties) = root.get_mut("properties").and_then(Value::as_object_mut) {
        if let Some(kind) = properties.get_mut("kind").and_then(Value::as_object_mut) {
            kind.insert("const".to_string(), json!("identity"));
        }
        if let Some(spec_version) = properties
            .get_mut("spec_version")
            .and_then(Value::as_object_mut)
        {
            spec_version.insert("const".to_string(), json!(IDENTITY_SPEC_VERSION));
        }
        for field in ["name", "issuer"] {
            if let Some(value) = properties.get_mut(field).and_then(Value::as_object_mut) {
                value.insert("pattern".to_string(), json!("^[A-Za-z_][A-Za-z0-9_]*$"));
            }
        }
        if let Some(version) = properties.get_mut("version").and_then(Value::as_object_mut) {
            version.insert("minLength".to_string(), json!(1));
        }
        if let Some(description) = properties
            .get_mut("description")
            .and_then(Value::as_object_mut)
        {
            description.insert("type".to_string(), json!("string"));
        }
    }
}

#[cfg(test)]
mod tests {
    use jsonschema::JSONSchema;
    use serde_json::Value as JsonValue;

    use super::{
        generated_identity_manifest_schema, parse_identity_manifest_value,
        parse_identity_manifest_yaml,
    };
    use crate::{IdentitySpecConfig, IdentitySpecType, ManifestInputKind, ManifestOAuthFlowKind};

    /// Renders an identity manifest from the shared header fields plus a body.
    fn identity_yaml(name: &str, issuer: &str, identity_type: &str, body: &str) -> String {
        format!(
            "kind: identity\nspec_version: 1\nname: {name}\nversion: 0.1.0\nissuer: {issuer}\ntype: {identity_type}\n{body}"
        )
    }

    /// Demo-issuer `fixed_token` identity manifest plus a per-test body.
    fn demo_fixed_token(body: &str) -> String {
        identity_yaml("demo_identity", "demo", "fixed_token", body)
    }

    /// Demo-issuer `oauth` identity manifest plus a per-test body.
    fn demo_oauth(body: &str) -> String {
        identity_yaml("demo_oauth", "demo", "oauth", body)
    }

    fn valid_identity_manifest() -> String {
        identity_yaml(
            "github_oauth",
            "github",
            "oauth",
            r"description: GitHub OAuth access token.
audience: {host: github.com}
oauth:
  method:
    label: Connect with GitHub device code
    flow: {type: device_code}
    endpoints: {device_authorization_url: 'https://github.com/login/device/code', token_url: 'https://github.com/login/oauth/access_token'}
    client: {id: {input: GITHUB_OAUTH_CLIENT_ID}}",
        )
    }

    fn validator() -> JSONSchema {
        JSONSchema::compile(&generated_identity_manifest_schema()).expect("schema compiles")
    }

    fn manifest_json(raw: &str) -> JsonValue {
        serde_yaml::from_str(raw).expect("yaml parses as json value")
    }

    /// Asserts the manifest fails validation with an error containing `expected`.
    fn expect_validation_error(raw: &str, label: &str, expected: &str) {
        let error = parse_identity_manifest_yaml(raw).expect_err(label);
        assert!(
            error.to_string().contains(expected),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn parses_identity_manifest_with_oauth_type_config() {
        let manifest = parse_identity_manifest_yaml(&valid_identity_manifest()).expect("manifest");

        assert_eq!(manifest.name, "github_oauth");
        assert_eq!(manifest.issuer, "github");
        assert_eq!(manifest.identity_type, IdentitySpecType::OAuth);
        assert!(manifest.inputs.is_empty());
        let IdentitySpecConfig::OAuth(oauth) = &manifest.config else {
            panic!("expected oauth config");
        };
        let method = &oauth.method;
        assert_eq!(
            method.label.as_deref(),
            Some("Connect with GitHub device code")
        );
        assert_eq!(method.oauth.flow.kind, ManifestOAuthFlowKind::DeviceCode);
    }

    #[test]
    fn parses_fixed_token_identity_manifest_without_inputs_or_auth() {
        let manifest = parse_identity_manifest_yaml(&identity_yaml(
            "github_pat",
            "github",
            "fixed_token",
            "audience: {host: github.com}",
        ))
        .expect("fixed token identity");

        assert_eq!(manifest.identity_type, IdentitySpecType::FixedToken);
        assert!(matches!(manifest.config, IdentitySpecConfig::FixedToken));
    }

    #[test]
    fn parses_oauth_identity_manifest_with_inputs() {
        let manifest = parse_identity_manifest_yaml(&identity_yaml(
            "google_oauth",
            "google",
            "oauth",
            r"audience: {host: googleapis.com}
inputs:
  GOOGLE_TENANT: {kind: variable, default: oauth2}
  GOOGLE_OAUTH_CLIENT_SECRET: {kind: secret}
oauth:
  method:
    flow: {type: authorization_code, pkce: required}
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    endpoints: {authorization_url: 'https://accounts.google.com/o/oauth2/v2/auth', token_url: 'https://{{input.GOOGLE_TENANT}}.googleapis.com/token'}
    client: {id: {default: google-client}, secret: {input: GOOGLE_OAUTH_CLIENT_SECRET, transport: request_body}}",
        ))
        .expect("identity with inputs");

        let [tenant, client_secret] = manifest.inputs.as_slice() else {
            panic!("expected two inputs, got {:?}", manifest.inputs);
        };
        assert_eq!(tenant.key, "GOOGLE_TENANT");
        assert_eq!(tenant.kind, ManifestInputKind::Variable);
        assert_eq!(client_secret.key, "GOOGLE_OAUTH_CLIENT_SECRET");
        assert_eq!(client_secret.kind, ManifestInputKind::Secret);
    }

    #[test]
    fn rejects_fixed_token_identity_manifest_with_inputs() {
        expect_validation_error(
            &demo_fixed_token("inputs:\n  TOKEN: {kind: secret}"),
            "fixed token inputs should fail",
            "must not declare inputs",
        );
    }

    #[test]
    fn rejects_oauth_client_secret_input_that_is_not_secret() {
        let error = parse_identity_manifest_yaml(&demo_oauth(
            r"inputs:
  OAUTH_CLIENT_SECRET: {kind: variable}
oauth:
  method:
    flow: {type: authorization_code, pkce: required}
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    endpoints: {authorization_url: 'https://provider.example.com/oauth/authorize', token_url: 'https://provider.example.com/oauth/token'}
    client: {id: {default: demo-client}, secret: {input: OAUTH_CLIENT_SECRET, transport: request_body}}",
        ))
        .expect_err("client secret variable should fail");

        assert!(
            error.to_string().contains("looks credential-like")
                || error.to_string().contains("must reference a secret input"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn rejects_secret_input_in_oauth_endpoint_template() {
        expect_validation_error(
            &demo_oauth(
                r"inputs:
  TENANT_SECRET: {kind: secret}
oauth:
  method:
    flow: {type: device_code}
    endpoints: {device_authorization_url: 'https://{{input.TENANT_SECRET}}.example.com/device', token_url: 'https://provider.example.com/oauth/token'}
    client: {id: {default: demo-client}}",
            ),
            "secret endpoint template should fail",
            "is not a variable",
        );
    }

    #[test]
    fn rejects_identity_manifest_with_legacy_auth() {
        expect_validation_error(
            &demo_fixed_token(
                "auth:\n  type: HeaderAuth\n  headers:\n    - {name: Authorization, from: literal, value: Bearer token}",
            ),
            "legacy auth should fail",
            "must not declare 'auth'",
        );
    }

    #[test]
    fn rejects_identity_manifest_with_legacy_injection_method() {
        expect_validation_error(
            &demo_fixed_token("injection_method: bearer_authorization_header"),
            "legacy injection_method should fail",
            "must not declare 'injection_method'",
        );
    }

    #[test]
    fn rejects_oauth_identity_without_oauth_config() {
        expect_validation_error(
            &identity_yaml("demo_identity", "demo", "oauth", ""),
            "missing oauth config should fail",
            "type oauth is missing oauth",
        );
    }

    #[test]
    fn rejects_oauth_identity_with_multiple_methods_shape() {
        let value = manifest_json(&identity_yaml(
            "github_oauth",
            "github",
            "oauth",
            r"oauth:
  methods:
    - flow: {type: device_code}
      endpoints: {device_authorization_url: 'https://github.com/login/device/code', token_url: 'https://github.com/login/oauth/access_token'}
      client: {id: {input: GITHUB_OAUTH_CLIENT_ID}}",
        ));

        assert!(
            validator().validate(&value).is_err(),
            "schema should reject oauth.methods"
        );
        let error = parse_identity_manifest_value(value).expect_err("oauth.methods should fail");
        assert!(
            error.to_string().contains("oauth.methods is not supported"),
            "unexpected error: {error}"
        );
    }

    #[test]
    fn generated_schema_accepts_parser_valid_identity_manifest() {
        let raw = valid_identity_manifest();
        assert!(
            validator().validate(&manifest_json(&raw)).is_ok(),
            "schema should accept fixture"
        );
        parse_identity_manifest_yaml(&raw).expect("parser accepts fixture");
    }

    #[test]
    fn generated_schema_rejects_wrong_kind() {
        let mut value = manifest_json(&valid_identity_manifest());
        value
            .as_object_mut()
            .expect("manifest object")
            .insert("kind".to_string(), JsonValue::String("source".to_string()));

        assert!(
            validator().validate(&value).is_err(),
            "schema should reject wrong kind"
        );
        parse_identity_manifest_value(value).expect_err("parser should reject wrong kind");
    }
}
