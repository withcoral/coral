//! Generated JSON Schema for authored DSL v4 source manifests.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{AuthSpec, HeaderSpec, backends::http::RateLimitSpec};

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
#[serde(deny_unknown_fields)]
struct V4SurfaceSchema {
    id: String,
    #[serde(rename = "type")]
    surface_type: V4SurfaceTypeSchema,
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
#[serde(rename_all = "snake_case")]
enum V4SurfaceTypeSchema {
    Openapi,
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

    if let Some(dsl_version) = root
        .get_mut("properties")
        .and_then(Value::as_object_mut)
        .and_then(|properties| properties.get_mut("dsl_version"))
        .and_then(Value::as_object_mut)
    {
        dsl_version.insert("const".to_string(), json!(4));
    }
    if let Some(properties) = root.get_mut("properties").and_then(Value::as_object_mut) {
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

    let Some(surface) = root
        .get_mut("$defs")
        .and_then(Value::as_object_mut)
        .and_then(|defs| defs.get_mut("V4SurfaceSchema"))
        .and_then(Value::as_object_mut)
    else {
        return;
    };
    surface.insert(
        "oneOf".to_string(),
        json!([{ "required": ["url"] }, { "required": ["file"] }]),
    );
    if let Some(properties) = surface.get_mut("properties").and_then(Value::as_object_mut) {
        if let Some(id) = properties.get_mut("id").and_then(Value::as_object_mut) {
            id.insert("pattern".to_string(), json!("^[a-z][a-z0-9_]*$"));
        }
        if let Some(url) = properties.get_mut("url").and_then(Value::as_object_mut) {
            url.insert("type".to_string(), json!("string"));
            url.insert("pattern".to_string(), json!("^https://"));
        }
        if let Some(file) = properties.get_mut("file").and_then(Value::as_object_mut) {
            file.insert("type".to_string(), json!("string"));
            file.insert("minLength".to_string(), json!(1));
        }
        if let Some(inputs) = properties.get_mut("inputs").and_then(Value::as_object_mut) {
            inputs.insert("type".to_string(), json!("object"));
            inputs.insert("propertyNames".to_string(), json!({ "minLength": 1 }));
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

#[cfg(test)]
mod tests {
    use jsonschema::JSONSchema;
    use serde_json::Value as JsonValue;

    use super::generated_v4_source_manifest_schema;
    use crate::parse_source_manifest_yaml;

    fn validator() -> JSONSchema {
        JSONSchema::compile(&generated_v4_source_manifest_schema()).expect("schema compiles")
    }

    fn manifest_json(raw: &str) -> JsonValue {
        serde_yaml::from_str(raw).expect("yaml parses as json value")
    }

    #[test]
    fn generated_schema_accepts_core_v4_fixture_and_parser_agrees() {
        let raw = std::fs::read_to_string(
            std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("../../sources/core-v4/github_v4/manifest.yaml"),
        )
        .expect("core v4 fixture");
        if let Err(errors) = validator().validate(&manifest_json(&raw)) {
            let errors = errors.map(|error| error.to_string()).collect::<Vec<_>>();
            panic!("generated schema should accept core v4 fixture: {errors:?}");
        }
        parse_source_manifest_yaml(&raw).expect("parser accepts core v4 fixture");
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
                validator().validate(&manifest_json(&raw)).is_err(),
                "field should be rejected: {field}"
            );
        }

        let raw = "name: demo\ndsl_version: 4\nsurfaces:\n  - id: rest\n    type: openapi\n    url: https://example.com/openapi.yaml\n    sha256: 0000000000000000000000000000000000000000000000000000000000000000\n";
        assert!(
            validator().validate(&manifest_json(raw)).is_err(),
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
                validator().validate(&manifest_json(&raw)).is_err(),
                "explicit null should be rejected: {surface_fields}"
            );
        }
    }

    #[test]
    fn generated_schema_rejects_empty_surfaces_and_parser_agrees() {
        let raw = "name: demo\ndsl_version: 4\nsurfaces: []\n";

        assert!(
            validator().validate(&manifest_json(raw)).is_err(),
            "empty surfaces should be rejected by generated schema"
        );
        parse_source_manifest_yaml(raw).expect_err("parser should reject empty surfaces");
    }
}
