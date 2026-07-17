//! Generated JSON Schema for authored identity manifests.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use super::IdentityManifestKind;
use crate::inputs::{RESERVED_INPUT_KEY_PREFIXES, credential_like_input_key_markers};
use crate::{
    ManifestOAuthClientSecretSpec, ManifestOAuthDynamicClientRegistrationSpec,
    ManifestOAuthPkceMode, ManifestOAuthRedirectUriPortMode, ManifestOAuthScopesSpec,
};

const RUST_TRIM_WHITESPACE_PATTERN: &str = "\u{0009}-\u{000d}\u{0020}\u{0085}\u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
#[schemars(title = "Coral Identity Manifest")]
enum IdentityManifestSchema {
    #[serde(rename = "oauth")]
    OAuth {
        #[schemars(extend("const" = "identity"))]
        kind: IdentityManifestKind,
        #[schemars(extend("const" = 1))]
        spec_version: u32,
        #[schemars(pattern(r"^[A-Za-z_][A-Za-z0-9_]*$"))]
        name: String,
        #[schemars(pattern(
            "[^\u{0009}-\u{000d}\u{0020}\u{0085}\u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}]"
        ))]
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(required)]
        description: Option<String>,
        #[schemars(pattern(r"^[A-Za-z_][A-Za-z0-9_]*$"))]
        issuer: String,
        audience: IdentityAudienceSchema,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(required)]
        inputs: Option<BTreeMap<String, IdentityInputSchema>>,
        oauth: IdentityOAuthSchema,
    },
    FixedToken {
        #[schemars(extend("const" = "identity"))]
        kind: IdentityManifestKind,
        #[schemars(extend("const" = 1))]
        spec_version: u32,
        #[schemars(pattern(r"^[A-Za-z_][A-Za-z0-9_]*$"))]
        name: String,
        #[schemars(pattern(
            "[^\u{0009}-\u{000d}\u{0020}\u{0085}\u{00a0}\u{1680}\u{2000}-\u{200a}\u{2028}\u{2029}\u{202f}\u{205f}\u{3000}]"
        ))]
        version: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(required)]
        description: Option<String>,
        #[schemars(pattern(r"^[A-Za-z_][A-Za-z0-9_]*$"))]
        issuer: String,
        audience: IdentityAudienceSchema,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(required)]
        inputs: Option<EmptyIdentityInputsSchema>,
    },
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityAudienceSchema {
    #[schemars(length(min = 1))]
    host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(range(min = 1, max = u16::MAX))]
    port: Option<u16>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct EmptyIdentityInputsSchema {}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum IdentityInputSchema {
    Variable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(required)]
        required: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
    Secret {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(required)]
        required: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityOAuthSchema {
    method: IdentityOAuthMethodSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum IdentityOAuthMethodSchema {
    AuthorizationCode(Box<IdentityAuthorizationCodeOAuthMethodSchema>),
    DeviceCode(Box<IdentityDeviceCodeOAuthMethodSchema>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityAuthorizationCodeOAuthMethodSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    hint: Option<String>,
    flow: IdentityAuthorizationCodeFlowSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, length(min = 1))]
    resource: Option<String>,
    #[serde(rename = "redirect_uri")]
    #[schemars(length(min = 1))]
    redirect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    redirect_uri_port_mode: Option<ManifestOAuthRedirectUriPortMode>,
    endpoints: IdentityAuthorizationCodeEndpointsSchema,
    client: IdentityAuthorizationCodeClientSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    scopes: Option<ManifestOAuthScopesSpec>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityDeviceCodeOAuthMethodSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    label: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    hint: Option<String>,
    flow: IdentityDeviceCodeFlowSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, length(min = 1))]
    resource: Option<String>,
    endpoints: IdentityDeviceCodeEndpointsSchema,
    client: IdentityDeviceCodeClientSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    scopes: Option<ManifestOAuthScopesSpec>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityAuthorizationCodeFlowSchema {
    #[serde(rename = "type")]
    flow_type: AuthorizationCodeFlowType,
    pkce: ManifestOAuthPkceMode,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum AuthorizationCodeFlowType {
    AuthorizationCode,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityDeviceCodeFlowSchema {
    #[serde(rename = "type")]
    flow_type: DeviceCodeFlowType,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pkce: Option<DisabledPkceMode>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DeviceCodeFlowType {
    DeviceCode,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum DisabledPkceMode {
    Disabled,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityAuthorizationCodeEndpointsSchema {
    #[serde(rename = "authorization_url")]
    #[schemars(length(min = 1))]
    authorization: String,
    #[serde(rename = "token_url")]
    #[schemars(length(min = 1))]
    token: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct IdentityDeviceCodeEndpointsSchema {
    #[serde(rename = "device_authorization_url")]
    #[schemars(length(min = 1))]
    device_authorization: String,
    #[serde(rename = "token_url")]
    #[schemars(length(min = 1))]
    token: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("anyOf" = [
    { "required": ["id"] },
    { "required": ["dynamic_registration"] }
]))]
struct IdentityAuthorizationCodeClientSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    id: Option<IdentityOAuthClientIdSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    secret: Option<ManifestOAuthClientSecretSpec>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    dynamic_registration: Option<ManifestOAuthDynamicClientRegistrationSpec>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("anyOf" = [
    { "required": ["id"] },
    { "required": ["dynamic_registration"] }
]))]
struct IdentityDeviceCodeClientSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    id: Option<IdentityOAuthClientIdSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    dynamic_registration: Option<ManifestOAuthDynamicClientRegistrationSpec>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("anyOf" = [
    { "required": ["default"] },
    { "required": ["input"] }
]))]
struct IdentityOAuthClientIdSchema {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, length(min = 1))]
    default: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, length(min = 1))]
    input: Option<String>,
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
    let root = schema
        .as_object_mut()
        .expect("generated identity manifest schema must be an object");
    root.insert(
        "$id".to_string(),
        Value::String("https://coral.local/identity_manifest.schema.json".to_string()),
    );
    root.entry("$schema".to_string()).or_insert_with(|| {
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string())
    });

    let Some(inputs) = root
        .get_mut("oneOf")
        .and_then(Value::as_array_mut)
        .and_then(|variants| {
            variants.iter_mut().find(|variant| {
                variant
                    .pointer("/properties/type/const")
                    .and_then(Value::as_str)
                    == Some("oauth")
            })
        })
        .and_then(|oauth| oauth.pointer_mut("/properties/inputs"))
        .and_then(Value::as_object_mut)
    else {
        return;
    };

    let mut forbidden_names = vec![json!({"pattern": r"[/\\=\r\n]"}), json!({"pattern": "^#"})];
    forbidden_names.extend(
        RESERVED_INPUT_KEY_PREFIXES
            .iter()
            .map(|prefix| json!({"pattern": format!("^{prefix}")})),
    );
    inputs.insert(
        "propertyNames".to_string(),
        json!({
            "minLength": 1,
            "pattern": format!("^[^{RUST_TRIM_WHITESPACE_PATTERN}](?:[\\s\\S]*[^{RUST_TRIM_WHITESPACE_PATTERN}])?$"),
            "not": {"anyOf": forbidden_names},
        }),
    );

    let marker_pattern = credential_like_input_key_markers()
        .iter()
        .map(|marker| ascii_case_insensitive_pattern(marker))
        .collect::<Vec<_>>()
        .join("|");
    inputs.insert(
        "patternProperties".to_string(),
        json!({
            format!("(^|_)({marker_pattern})(_|$)"): {
                "allOf": [
                    {"$ref": "#/$defs/IdentityInputSchema"},
                    {"properties": {"kind": {"const": "secret"}}, "required": ["kind"]},
                ],
            }
        }),
    );
}

fn ascii_case_insensitive_pattern(value: &str) -> String {
    value.chars().fold(String::new(), |mut pattern, character| {
        if character.is_ascii_alphabetic() {
            pattern.push('[');
            pattern.push(character.to_ascii_lowercase());
            pattern.push(character.to_ascii_uppercase());
            pattern.push(']');
        } else {
            pattern.push(character);
        }
        pattern
    })
}

#[cfg(test)]
mod tests {
    use jsonschema::Validator;
    use serde_json::{Value, json};

    use super::generated_identity_manifest_schema;
    use crate::parse_identity_manifest_value;

    const FIXED_TOKEN: &str = r"
kind: identity
spec_version: 1
name: demo_token
version: 0.1.0
issuer: demo
type: fixed_token
audience: {host: api.example.com}
inputs: {}
";

    const DEVICE_CODE: &str = r#"
kind: identity
spec_version: 1
name: demo_device
version: 0.1.0
issuer: demo
type: oauth
audience: {host: api.example.com}
inputs:
  UNUSED:
    kind: variable
    default: null
    hint: null
  "A\u2028B": {kind: variable}
oauth:
  method:
    flow: {type: device_code, pkce: null}
    endpoints:
      device_authorization_url: https://provider.example.com/device
      token_url: https://provider.example.com/token
    client: {id: {default: demo-client}}
"#;

    const AUTHORIZATION_CODE: &str = r"
kind: identity
spec_version: 1
name: demo_oauth
version: 0.1.0
description: Demo OAuth identity
issuer: demo
type: oauth
audience: {host: api.example.com}
inputs:
  CLIENT_SECRET: {kind: secret, required: true}
oauth:
  method:
    label: Connect Demo
    description: Authorize Coral
    hint: Sign in in the browser
    flow: {type: authorization_code, pkce: disabled}
    resource: https://api.example.com/
    redirect_uri: http://127.0.0.1:53682/oauth/callback
    redirect_uri_port_mode: fixed
    endpoints:
      authorization_url: https://provider.example.com/authorize
      token_url: https://provider.example.com/token
    client:
      id: {default: demo-client}
      secret: {input: CLIENT_SECRET, transport: basic_auth}
      dynamic_registration:
        registration_url: https://provider.example.com/register
        client_name: Coral
        token_endpoint_auth_method: client_secret_basic
        request_refresh_token_grant: true
    scopes:
      scope: {delimiter: space, values: [read, write]}
";

    fn manifest(raw: &str) -> Value {
        serde_yaml::from_str(raw).expect("identity fixture must parse")
    }

    fn validator() -> Validator {
        jsonschema::validator_for(&generated_identity_manifest_schema())
            .expect("identity schema compiles")
    }

    fn replace(mut value: Value, pointer: &str, replacement: Value) -> Value {
        *value.pointer_mut(pointer).expect("fixture pointer") = replacement;
        value
    }

    fn insert(mut value: Value, pointer: &str, key: &str) -> Value {
        value
            .pointer_mut(pointer)
            .and_then(Value::as_object_mut)
            .expect("fixture object")
            .insert(key.to_string(), json!(true));
        value
    }

    fn rename_input(mut value: Value, name: &str) -> Value {
        let inputs = value
            .get_mut("inputs")
            .and_then(Value::as_object_mut)
            .expect("fixture inputs");
        let input = inputs.remove("CLIENT_SECRET").expect("fixture input");
        inputs.insert(name.to_string(), input);
        value
    }

    fn assert_schema_and_parser_reject(label: &str, value: Value) {
        let errors = validator()
            .iter_errors(&value)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        assert!(!errors.is_empty(), "schema accepted {label}: {value}");
        parse_identity_manifest_value(value).expect_err(label);
    }

    #[test]
    fn generated_schema_accepts_identity_variants_and_parser_agrees() {
        let fixed_with_bom_version = replace(manifest(FIXED_TOKEN), "/version", json!("\u{feff}"));
        for value in [
            manifest(FIXED_TOKEN),
            manifest(DEVICE_CODE),
            manifest(AUTHORIZATION_CODE),
            fixed_with_bom_version,
        ] {
            let errors = validator()
                .iter_errors(&value)
                .map(|error| error.to_string())
                .collect::<Vec<_>>();
            assert!(errors.is_empty(), "schema rejected fixture: {errors:?}");
            parse_identity_manifest_value(value).expect("parser accepts fixture");
        }
    }

    #[test]
    fn generated_schema_rejects_invalid_envelope_shapes_and_parser_agrees() {
        let valid = manifest(AUTHORIZATION_CODE);
        for (label, pointer, replacement) in [
            ("wrong kind", "/kind", json!("source")),
            ("wrong spec version", "/spec_version", json!(2)),
            ("invalid identity name", "/name", json!("demo-oauth")),
            ("invalid issuer", "/issuer", json!("demo issuer")),
            ("whitespace version", "/version", json!("\u{0085}")),
            (
                "credential-like variable input",
                "/inputs/CLIENT_SECRET/kind",
                json!("variable"),
            ),
        ] {
            assert_schema_and_parser_reject(label, replace(valid.clone(), pointer, replacement));
        }

        for pointer in [
            "/description",
            "/inputs",
            "/inputs/CLIENT_SECRET/required",
            "/oauth",
            "/oauth/method",
            "/oauth/method/label",
            "/oauth/method/redirect_uri_port_mode",
            "/oauth/method/client/dynamic_registration/client_name",
            "/oauth/method/scopes",
        ] {
            assert_schema_and_parser_reject(
                &format!("null at {pointer}"),
                replace(valid.clone(), pointer, Value::Null),
            );
        }

        for name in [
            " CLIENT_SECRET",
            "CLIENT/SECRET",
            "#CLIENT_SECRET",
            "__coral_token",
            "\u{0085}CLIENT_SECRET",
        ] {
            assert_schema_and_parser_reject(name, rename_input(valid.clone(), name));
        }

        let mut missing_audience = manifest(FIXED_TOKEN);
        missing_audience
            .as_object_mut()
            .expect("fixed-token fixture")
            .remove("audience");
        assert_schema_and_parser_reject("missing audience", missing_audience);
    }

    #[test]
    fn generated_schema_rejects_unknown_fields_at_every_authored_nesting_level() {
        let valid = manifest(AUTHORIZATION_CODE);
        for pointer in [
            "",
            "/audience",
            "/inputs/CLIENT_SECRET",
            "/oauth",
            "/oauth/method",
            "/oauth/method/flow",
            "/oauth/method/endpoints",
            "/oauth/method/client",
            "/oauth/method/client/id",
            "/oauth/method/client/secret",
            "/oauth/method/client/dynamic_registration",
            "/oauth/method/scopes",
            "/oauth/method/scopes/scope",
        ] {
            assert_schema_and_parser_reject(
                &format!("unknown field at {pointer}"),
                insert(valid.clone(), pointer, "unexpected"),
            );
        }
    }

    #[test]
    fn generated_schema_enforces_identity_type_specific_fields() {
        let oauth = manifest(AUTHORIZATION_CODE);
        let mut missing_oauth = oauth.clone();
        missing_oauth
            .as_object_mut()
            .expect("manifest object")
            .remove("oauth");
        assert_schema_and_parser_reject("oauth identity without oauth", missing_oauth);

        let fixed = manifest(FIXED_TOKEN);
        let fixed_with_oauth = insert(fixed.clone(), "", "oauth");
        assert_schema_and_parser_reject("fixed token with oauth", fixed_with_oauth);

        let fixed_with_null_oauth = replace(insert(fixed, "", "oauth"), "/oauth", Value::Null);
        assert!(
            !validator().is_valid(&fixed_with_null_oauth),
            "fixed-token schema must forbid the oauth property even when null"
        );
    }
}
