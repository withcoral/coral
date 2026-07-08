//! Generated JSON Schema for authored DSL v4 source manifests.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::backends::http::RateLimitSpec;
use crate::backends::mcp::McpServerSpec;
use crate::{AuthSpec, HeaderSpec};

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(title = "Coral DSL v4 Source Manifest")]
struct V4SourceManifestSchema {
    #[schemars(extend("const" = 4))]
    dsl_version: u32,
    #[schemars(length(min = 1))]
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required)]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[schemars(inner(length(min = 1)))]
    test_queries: Vec<String>,
    #[schemars(length(min = 1))]
    surfaces: Vec<V4SurfaceSchema>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
enum V4SurfaceSchema {
    Openapi(V4OpenApiSurfaceSchema),
    Mcp(V4McpSurfaceSchema),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("oneOf" = [{ "required": ["url"] }, { "required": ["file"] }]))]
struct V4OpenApiSurfaceSchema {
    #[schemars(pattern(r"^[a-z][a-z0-9_]*$"))]
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        required,
        length(min = 1),
        pattern(r"^[a-z][a-z0-9_]*$"),
        description = "Source-relative relation namespace suffix. When present, Coral exposes the surface as <source_name>_<namespace_suffix>; when omitted, the surface uses <source_name>."
    )]
    namespace_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, pattern(r"^https://"))]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, length(min = 1))]
    file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, extend("propertyNames" = { "minLength": 1 }))]
    inputs: Option<BTreeMap<String, V4InputSpecSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, length(min = 1))]
    base_url: Option<String>,
    #[serde(default)]
    auth: AuthSpec,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    request_headers: Vec<HeaderSpec>,
    #[serde(default)]
    rate_limit: RateLimitSpec,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4McpSurfaceSchema {
    #[schemars(pattern(r"^[a-z][a-z0-9_]*$"))]
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(
        required,
        length(min = 1),
        pattern(r"^[a-z][a-z0-9_]*$"),
        description = "Source-relative relation namespace suffix. When present, Coral exposes the surface as <source_name>_<namespace_suffix>; when omitted, the surface uses <source_name>."
    )]
    namespace_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(required, extend("propertyNames" = { "minLength": 1 }))]
    inputs: Option<BTreeMap<String, V4InputSpecSchema>>,
    server: McpServerSpec,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum V4InputSpecSchema {
    Variable {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        required: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
    Secret {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        required: Option<bool>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        credential: Option<V4CredentialSpecSchema>,
    },
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4CredentialSpecSchema {
    #[schemars(length(min = 1))]
    methods: Vec<V4CredentialMethodSchema>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
enum V4CredentialMethodSchema {
    SourceConfig {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
    },
    Oauth {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        description: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hint: Option<String>,
        oauth: Box<V4OAuthCredentialMethodSchema>,
    },
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum V4OAuthCredentialMethodSchema {
    AuthorizationCode(Box<V4AuthorizationCodeOAuthCredentialMethodSchema>),
    DeviceCode(Box<V4DeviceCodeOAuthCredentialMethodSchema>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4AuthorizationCodeOAuthCredentialMethodSchema {
    flow: V4AuthorizationCodeOAuthFlowSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    resource: Option<String>,
    #[serde(rename = "redirect_uri")]
    #[schemars(length(min = 1))]
    redirect: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    redirect_uri_port_mode: Option<V4OAuthRedirectUriPortModeSchema>,
    endpoints: V4AuthorizationCodeOAuthEndpointsSchema,
    client: V4OAuthClientSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scopes: Option<V4OAuthScopesSchema>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4DeviceCodeOAuthCredentialMethodSchema {
    flow: V4DeviceCodeOAuthFlowSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    resource: Option<String>,
    endpoints: V4DeviceCodeOAuthEndpointsSchema,
    client: V4DeviceCodeOAuthClientSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    scopes: Option<V4OAuthScopesSchema>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4AuthorizationCodeOAuthFlowSchema {
    #[serde(rename = "type")]
    flow_type: V4AuthorizationCodeOAuthFlowTypeSchema,
    pkce: V4OAuthPkceModeSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4AuthorizationCodeOAuthFlowTypeSchema {
    AuthorizationCode,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4OAuthPkceModeSchema {
    Required,
    Disabled,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4OAuthRedirectUriPortModeSchema {
    Fixed,
    Random,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4DeviceCodeOAuthFlowSchema {
    #[serde(rename = "type")]
    flow_type: V4DeviceCodeOAuthFlowTypeSchema,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pkce: Option<V4OAuthDisabledPkceModeSchema>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4DeviceCodeOAuthFlowTypeSchema {
    DeviceCode,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4OAuthDisabledPkceModeSchema {
    Disabled,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4AuthorizationCodeOAuthEndpointsSchema {
    #[serde(rename = "authorization_url")]
    #[schemars(length(min = 1))]
    authorization: String,
    #[serde(rename = "token_url")]
    #[schemars(length(min = 1))]
    token: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4DeviceCodeOAuthEndpointsSchema {
    #[serde(rename = "token_url")]
    #[schemars(length(min = 1))]
    token: String,
    #[serde(rename = "device_authorization_url")]
    #[schemars(length(min = 1))]
    device_authorization: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum V4OAuthClientSchema {
    Public(Box<V4OAuthPublicClientSchema>),
    Confidential(Box<V4OAuthConfidentialClientSchema>),
    Dynamic(Box<V4OAuthDynamicClientSchema>),
    PublicDynamic(Box<V4OAuthPublicDynamicClientSchema>),
    ConfidentialDynamic(Box<V4OAuthConfidentialDynamicClientSchema>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum V4DeviceCodeOAuthClientSchema {
    Public(Box<V4OAuthPublicClientSchema>),
    Dynamic(Box<V4OAuthDynamicClientSchema>),
    PublicDynamic(Box<V4OAuthPublicDynamicClientSchema>),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthPublicClientSchema {
    id: V4OAuthClientIdSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthConfidentialClientSchema {
    id: V4OAuthClientIdWithInputSchema,
    secret: V4OAuthClientSecretSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthDynamicClientSchema {
    dynamic_registration: V4OAuthDynamicClientRegistrationSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthPublicDynamicClientSchema {
    id: V4OAuthClientIdSchema,
    dynamic_registration: V4OAuthDynamicClientRegistrationSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthConfidentialDynamicClientSchema {
    id: V4OAuthClientIdWithInputSchema,
    secret: V4OAuthClientSecretSchema,
    dynamic_registration: V4OAuthDynamicClientRegistrationSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum V4OAuthClientIdSchema {
    DefaultAndInput(V4OAuthClientIdDefaultAndInputSchema),
    Default(V4OAuthClientIdDefaultSchema),
    Input(V4OAuthClientIdInputSchema),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(untagged)]
enum V4OAuthClientIdWithInputSchema {
    DefaultAndInput(V4OAuthClientIdDefaultAndInputSchema),
    Input(V4OAuthClientIdInputSchema),
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthClientIdDefaultAndInputSchema {
    #[schemars(length(min = 1))]
    default: String,
    #[schemars(length(min = 1))]
    input: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthClientIdDefaultSchema {
    #[schemars(length(min = 1))]
    default: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthClientIdInputSchema {
    #[schemars(length(min = 1))]
    input: String,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthClientSecretSchema {
    #[schemars(length(min = 1))]
    input: String,
    transport: V4OAuthClientSecretTransportSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4OAuthClientSecretTransportSchema {
    BasicAuth,
    RequestBody,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthDynamicClientRegistrationSchema {
    #[schemars(length(min = 1))]
    registration_url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(length(min = 1))]
    client_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    token_endpoint_auth_method: Option<V4OAuthDynamicClientRegistrationAuthMethodSchema>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    request_refresh_token_grant: Option<bool>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4OAuthDynamicClientRegistrationAuthMethodSchema {
    None,
    ClientSecretBasic,
    ClientSecretPost,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthScopesSchema {
    scope: V4OAuthScopeSchema,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct V4OAuthScopeSchema {
    delimiter: V4OAuthScopeDelimiterSchema,
    #[schemars(length(min = 1), inner(length(min = 1)))]
    values: Vec<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum V4OAuthScopeDelimiterSchema {
    Space,
    Comma,
}

/// Generate the JSON Schema for authored DSL v4 source manifests.
///
/// # Panics
///
/// Panics only if the schema produced by `schemars` cannot be serialized to
/// JSON, which would indicate an invalid schema type definition in this crate.
pub fn generated_v4_source_manifest_schema() -> Value {
    let mut schema = serde_json::to_value(schemars::schema_for!(V4SourceManifestSchema))
        .expect("generated DSL v4 schema must serialize");
    post_process_schema(&mut schema);
    schema
}

fn post_process_schema(schema: &mut Value) {
    let Some(root) = schema.as_object_mut() else {
        return;
    };
    root.insert(
        "$id".to_string(),
        Value::String("https://coral.local/source_manifest_v4.schema.json".to_string()),
    );
    root.entry("$schema".to_string()).or_insert_with(|| {
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string())
    });

    let Some(defs) = root.get_mut("$defs").and_then(Value::as_object_mut) else {
        return;
    };
    post_process_flattened_value_source_defs(defs);
}

fn post_process_flattened_value_source_defs(defs: &mut serde_json::Map<String, Value>) {
    compose_flattened_value_source_def(
        defs,
        "McpEnvSpec",
        &json!({
            "type": "object",
            "properties": {
                "name": { "type": "string" }
            },
            "required": ["name"]
        }),
    );
    compose_flattened_value_source_def(
        defs,
        "McpHttpAuthSpec",
        &json!({
            "type": "object",
            "properties": {
                "type": { "type": "string", "const": "bearer" }
            },
            "required": ["type"]
        }),
    );
}

fn compose_flattened_value_source_def(
    defs: &mut serde_json::Map<String, Value>,
    name: &str,
    required_properties: &Value,
) {
    let Some(definition) = defs.get_mut(name) else {
        return;
    };
    let description = definition
        .get("description")
        .cloned()
        .unwrap_or(Value::Null);
    let mut replacement = json!({
        "allOf": [
            { "$ref": "#/$defs/ValueSourceSpec" },
            required_properties.clone()
        ]
    });
    if !description.is_null()
        && let Some(object) = replacement.as_object_mut()
    {
        object.insert("description".to_string(), description);
    }
    *definition = replacement;
}

#[cfg(test)]
mod tests {
    use jsonschema::Validator;
    use serde_json::Value as JsonValue;

    use super::generated_v4_source_manifest_schema;
    use crate::parse_source_manifest_yaml;

    fn validator() -> Validator {
        jsonschema::validator_for(&generated_v4_source_manifest_schema()).expect("schema compiles")
    }

    fn manifest_json(raw: &str) -> JsonValue {
        serde_yaml::from_str(raw).expect("yaml parses as json value")
    }

    fn validation_errors(validator: &Validator, manifest: &JsonValue) -> Vec<String> {
        validator
            .iter_errors(manifest)
            .map(|error| error.to_string())
            .collect()
    }

    #[test]
    fn generated_schema_accepts_core_v4_fixture_and_parser_agrees() {
        let raw = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../sources/core-v4/github_v4/manifest.yaml"),
        )
        .expect("core v4 fixture");
        let validator = validator();
        let manifest = manifest_json(&raw);
        let errors = validation_errors(&validator, &manifest);
        assert!(
            errors.is_empty(),
            "generated schema should accept core v4 fixture: {errors:?}"
        );
        parse_source_manifest_yaml(&raw).expect("parser accepts core v4 fixture");
    }

    #[test]
    fn generated_schema_accepts_mcp_surface() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    inputs:
      MCP_TOKEN:
        kind: secret
    server:
      transport: streamable_http
      url: https://mcp.example.com/mcp
      auth:
        type: bearer
        from: input
        key: MCP_TOKEN
";

        let validator = validator();
        let manifest = manifest_json(raw);
        let errors = validation_errors(&validator, &manifest);
        assert!(
            errors.is_empty(),
            "generated schema should accept MCP surface: {errors:?}"
        );
        parse_source_manifest_yaml(raw).expect("parser accepts MCP surface");
    }

    #[test]
    fn generated_schema_accepts_flattened_mcp_value_sources() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: stdio_mcp
    namespace_suffix: stdio
    type: mcp
    inputs:
      MCP_TOKEN:
        kind: secret
    server:
      transport: stdio
      command: demo-mcp-server
      env:
        - name: MCP_TOKEN
          from: input
          key: MCP_TOKEN
  - id: http_mcp
    namespace_suffix: http
    type: mcp
    inputs:
      HTTP_TOKEN:
        kind: secret
    server:
      transport: streamable_http
      url: https://mcp.example.com/mcp
      auth:
        type: bearer
        from: input
        key: HTTP_TOKEN
";

        let validator = validator();
        let manifest = manifest_json(raw);
        let errors = validation_errors(&validator, &manifest);
        assert!(
            errors.is_empty(),
            "generated schema should accept flattened MCP value sources: {errors:?}"
        );
        parse_source_manifest_yaml(raw).expect("parser accepts flattened MCP value sources");
    }

    #[test]
    fn generated_schema_accepts_oauth_surface_input_and_parser_agrees() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
    inputs:
      TENANT_ID:
        kind: variable
        default: organizations
      API_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              label: Connect
              description: Use OAuth.
              hint: Authorize in your browser.
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                redirect_uri: http://127.0.0.1:0/oauth/callback
                redirect_uri_port_mode: random
                endpoints:
                  authorization_url: https://login.example.com/{{input.TENANT_ID}}/oauth/authorize
                  token_url: https://login.example.com/{{input.TENANT_ID}}/oauth/token
                client:
                  dynamic_registration:
                    registration_url: https://login.example.com/{{input.TENANT_ID}}/oauth/register
                    client_name: Coral Demo
                    token_endpoint_auth_method: none
                    request_refresh_token_grant: true
                scopes:
                  scope:
                    delimiter: space
                    values:
                      - read
            - type: source_config
              label: Paste token
    base_url: https://api.example.com
    auth:
      type: HeaderAuth
      headers:
        - name: Authorization
          from: bearer
          key: API_TOKEN
";

        let validator = validator();
        let manifest = manifest_json(raw);
        let errors = validation_errors(&validator, &manifest);
        assert!(
            errors.is_empty(),
            "generated schema should accept v4 OAuth surface input: {errors:?}"
        );
        parse_source_manifest_yaml(raw).expect("parser accepts v4 OAuth surface input");
    }

    #[test]
    fn generated_schema_rejects_unknown_oauth_surface_input_field() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
    inputs:
      API_TOKEN:
        kind: secret
        credential:
          methods:
            - type: oauth
              oauth:
                flow:
                  type: authorization_code
                  pkce: required
                redirect_uri: http://127.0.0.1:0/oauth/callback
                endpoints:
                  authorization_url: https://login.example.com/oauth/authorize
                  token_url: https://login.example.com/oauth/token
                client:
                  id:
                    default: demo-client
                unsupported: true
";

        assert!(
            !validator().is_valid(&manifest_json(raw)),
            "generated schema should reject unknown OAuth fields"
        );
        parse_source_manifest_yaml(raw).expect_err("parser should reject unknown OAuth fields");
    }

    #[test]
    fn generated_schema_rejects_v3_only_fields_and_removed_snapshot_fields() {
        let invalid = [
            "version: 1.0.0\n",
            "backend: http\n",
            "tables: []\n",
            "auth: {type: HeaderAuth}\n",
            "functions: []\n",
        ];
        for field in invalid {
            let raw = format!(
                "name: demo\ndsl_version: 4\n{field}surfaces:\n  - id: rest\n    type: openapi\n    url: https://example.com/openapi.yaml\n"
            );
            assert!(
                !validator().is_valid(&manifest_json(&raw)),
                "field should be rejected: {field}"
            );
        }

        let raw = "name: demo\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n    url: https://example.com/openapi.yaml\n    sha256: 0000000000000000000000000000000000000000000000000000000000000000\n";
        assert!(
            !validator().is_valid(&manifest_json(raw)),
            "surface sha256 should be rejected"
        );
    }

    #[test]
    fn generated_schema_rejects_explicit_null_surface_fields() {
        let invalid_surfaces = [
            "    url: null\n",
            "    file: null\n",
            "    url: https://example.com/openapi.yaml\n    base_url: null\n",
            "    url: https://example.com/openapi.yaml\n    auth: null\n",
            "    url: https://example.com/openapi.yaml\n    rate_limit: null\n",
        ];
        for surface_fields in invalid_surfaces {
            let raw = format!(
                "name: demo\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n{surface_fields}"
            );
            assert!(
                !validator().is_valid(&manifest_json(&raw)),
                "explicit null should be rejected: {surface_fields}"
            );
        }
    }

    #[test]
    fn generated_schema_rejects_empty_surfaces_and_parser_agrees() {
        let raw = "name: demo\ndsl_version: 4\nsurfaces: []\n";

        assert!(
            !validator().is_valid(&manifest_json(raw)),
            "empty surfaces should be rejected by generated schema"
        );
        parse_source_manifest_yaml(raw).expect_err("parser should reject empty surfaces");
    }

    #[test]
    fn generated_schema_accepts_one_missing_multi_surface_namespace_and_parser_agrees() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
  - id: mcp
    namespace_suffix: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
";

        let validator = validator();
        let manifest = manifest_json(raw);
        let errors = validation_errors(&validator, &manifest);
        assert!(
            errors.is_empty(),
            "generated schema should accept one default relation namespace: {errors:?}"
        );
        parse_source_manifest_yaml(raw)
            .expect("parser should accept one missing multi-surface namespace");
    }

    #[test]
    fn parser_rejects_two_missing_multi_surface_namespaces_after_schema_accepts_shape() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
  - id: mcp
    type: mcp
    server:
      transport: stdio
      command: demo-mcp-server
";

        let validator = validator();
        let manifest = manifest_json(raw);
        let errors = validation_errors(&validator, &manifest);
        assert!(
            errors.is_empty(),
            "generated schema should accept this parser-owned invariant: {errors:?}"
        );
        parse_source_manifest_yaml(raw)
            .expect_err("parser should reject multiple missing multi-surface namespaces");
    }

    #[test]
    fn generated_schema_rejects_invalid_surface_namespace_and_parser_agrees() {
        let raw = r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    namespace_suffix: GitHubRest
    type: openapi
    url: https://example.com/openapi.yaml
";

        assert!(
            !validator().is_valid(&manifest_json(raw)),
            "mixed-case namespace_suffix should be rejected by generated schema"
        );
        parse_source_manifest_yaml(raw)
            .expect_err("parser should reject mixed-case namespace_suffix");
    }
}
