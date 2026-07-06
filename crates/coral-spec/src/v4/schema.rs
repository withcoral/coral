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
struct V4SourceManifestSchema {
    dsl_version: u32,
    name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    description: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    test_queries: Vec<String>,
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
struct V4OpenApiSurfaceSchema {
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    url: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    file: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    inputs: Option<BTreeMap<String, V4InputSpecSchema>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
    id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    namespace_suffix: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
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
        credential: Option<Value>,
    },
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
    root.insert(
        "title".to_string(),
        Value::String("Coral DSL v4 Source Manifest".to_string()),
    );
    root.entry("$schema".to_string()).or_insert_with(|| {
        Value::String("https://json-schema.org/draft/2020-12/schema".to_string())
    });

    if let Some(properties) = root.get_mut("properties").and_then(Value::as_object_mut) {
        post_process_root_properties(properties);
    }

    let Some(defs) = root.get_mut("$defs").and_then(Value::as_object_mut) else {
        return;
    };
    post_process_flattened_value_source_defs(defs);
    if let Some(surface_schema) = defs.get_mut("V4SurfaceSchema") {
        post_process_surface_variants(surface_schema);
        return;
    }
    let Some(openapi_surface) = defs
        .get_mut("V4OpenApiSurfaceSchema")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    openapi_surface.insert(
        "oneOf".to_string(),
        json!([{ "required": ["url"] }, { "required": ["file"] }]),
    );
    if let Some(properties) = openapi_surface
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    {
        post_process_surface_id(properties);
        post_process_surface_namespace_suffix(properties);
        if let Some(url) = properties.get_mut("url").and_then(Value::as_object_mut) {
            url.insert("type".to_string(), json!("string"));
            url.insert("pattern".to_string(), json!("^https://"));
        }
        if let Some(file) = properties.get_mut("file").and_then(Value::as_object_mut) {
            file.insert("type".to_string(), json!("string"));
            file.insert("minLength".to_string(), json!(1));
        }
        post_process_surface_inputs(properties);
        if let Some(base_url) = properties
            .get_mut("base_url")
            .and_then(Value::as_object_mut)
        {
            base_url.insert("type".to_string(), json!("string"));
            base_url.insert("minLength".to_string(), json!(1));
        }
    }

    let Some(mcp_surface) = defs
        .get_mut("V4McpSurfaceSchema")
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    if let Some(properties) = mcp_surface
        .get_mut("properties")
        .and_then(Value::as_object_mut)
    {
        post_process_surface_id(properties);
        post_process_surface_namespace_suffix(properties);
        post_process_surface_inputs(properties);
    }
}

fn post_process_root_properties(properties: &mut serde_json::Map<String, Value>) {
    if let Some(dsl_version) = properties
        .get_mut("dsl_version")
        .and_then(Value::as_object_mut)
    {
        dsl_version.insert("const".to_string(), json!(4));
    }
    if let Some(name) = properties.get_mut("name").and_then(Value::as_object_mut) {
        name.insert("minLength".to_string(), json!(1));
    }
    if let Some(description) = properties
        .get_mut("description")
        .and_then(Value::as_object_mut)
    {
        description.insert("type".to_string(), json!("string"));
    }
    if let Some(test_queries) = properties
        .get_mut("test_queries")
        .and_then(Value::as_object_mut)
        && let Some(items) = test_queries.get_mut("items").and_then(Value::as_object_mut)
    {
        items.insert("minLength".to_string(), json!(1));
    }
    if let Some(surfaces) = properties
        .get_mut("surfaces")
        .and_then(Value::as_object_mut)
    {
        surfaces.insert("minItems".to_string(), json!(1));
    }
}

fn post_process_surface_variants(surface_schema: &mut Value) {
    let Some(variants) = surface_schema
        .get_mut("oneOf")
        .and_then(Value::as_array_mut)
    else {
        return;
    };
    for variant in variants {
        let Some(variant) = variant.as_object_mut() else {
            continue;
        };
        let surface_type = variant
            .get("properties")
            .and_then(|value| value.get("type"))
            .and_then(|value| value.get("const"))
            .and_then(Value::as_str)
            .map(ToString::to_string);
        if matches!(surface_type.as_deref(), Some("openapi")) {
            variant.insert(
                "oneOf".to_string(),
                json!([{ "required": ["url"] }, { "required": ["file"] }]),
            );
        }
        let Some(properties) = variant.get_mut("properties").and_then(Value::as_object_mut) else {
            continue;
        };
        post_process_surface_id(properties);
        post_process_surface_namespace_suffix(properties);
        post_process_surface_inputs(properties);
        if let Some("openapi") = surface_type.as_deref() {
            if let Some(url) = properties.get_mut("url").and_then(Value::as_object_mut) {
                url.insert("type".to_string(), json!("string"));
                url.insert("pattern".to_string(), json!("^https://"));
            }
            if let Some(file) = properties.get_mut("file").and_then(Value::as_object_mut) {
                file.insert("type".to_string(), json!("string"));
                file.insert("minLength".to_string(), json!(1));
            }
            if let Some(base_url) = properties
                .get_mut("base_url")
                .and_then(Value::as_object_mut)
            {
                base_url.insert("type".to_string(), json!("string"));
                base_url.insert("minLength".to_string(), json!(1));
            }
        }
    }
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

fn post_process_surface_id(properties: &mut serde_json::Map<String, Value>) {
    if let Some(id) = properties.get_mut("id").and_then(Value::as_object_mut) {
        id.insert("pattern".to_string(), json!("^[a-z][a-z0-9_]*$"));
    }
}

fn post_process_surface_namespace_suffix(properties: &mut serde_json::Map<String, Value>) {
    if let Some(namespace_suffix) = properties
        .get_mut("namespace_suffix")
        .and_then(Value::as_object_mut)
    {
        namespace_suffix.insert("type".to_string(), json!("string"));
        namespace_suffix.insert(
            "description".to_string(),
            json!(
                "Source-relative relation namespace suffix. When present, Coral exposes the surface as <source_name>_<namespace_suffix>; when omitted, the surface uses <source_name>."
            ),
        );
        namespace_suffix.insert("minLength".to_string(), json!(1));
        namespace_suffix.insert("pattern".to_string(), json!("^[a-z][a-z0-9_]*$"));
    }
}

fn post_process_surface_inputs(properties: &mut serde_json::Map<String, Value>) {
    if let Some(inputs) = properties.get_mut("inputs").and_then(Value::as_object_mut) {
        inputs.insert("type".to_string(), json!("object"));
        inputs.insert("propertyNames".to_string(), json!({ "minLength": 1 }));
    }
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
