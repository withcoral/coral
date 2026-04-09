//! JSON Schema validation for source manifests.

use std::sync::OnceLock;

use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::{ManifestError, Result};

static SOURCE_SCHEMA: OnceLock<JSONSchema> = OnceLock::new();

pub(crate) fn validate_manifest_schema(manifest_json: &JsonValue) -> Result<()> {
    let validator = SOURCE_SCHEMA.get_or_init(|| {
        let schema_json: JsonValue =
            serde_json::from_str(include_str!("schema/source_manifest.schema.json"))
                .expect("embedded source schema must be valid JSON");
        JSONSchema::compile(&schema_json).expect("embedded source schema must compile")
    });
    if let Err(errors) = validator.validate(manifest_json) {
        let problems: Vec<String> = errors
            .take(8)
            .map(|error| {
                let path = error.instance_path.to_string();
                let location = if path.is_empty() { "/" } else { &path };
                format!("  {location}: {error}")
            })
            .collect();
        return Err(ManifestError::validation(format!(
            "source manifest failed schema validation:\n{}",
            problems.join("\n")
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::Value as JsonValue;

    use super::validate_manifest_schema;
    use crate::parse_source_manifest_yaml;

    fn valid_http_manifest() -> &'static str {
        r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
"
    }

    fn manifest_json(raw: &str) -> JsonValue {
        serde_yaml::from_str(raw).expect("test manifest should parse as yaml")
    }

    #[test]
    fn validate_manifest_schema_accepts_valid_http_manifest() {
        let manifest = manifest_json(valid_http_manifest());
        validate_manifest_schema(&manifest).expect("valid manifest should pass schema validation");
    }

    #[test]
    fn validate_manifest_schema_rejects_unknown_top_level_field() {
        let manifest = manifest_json(&format!("schema: legacy\n{}", valid_http_manifest()));
        let error = validate_manifest_schema(&manifest).expect_err("schema validation should fail");
        assert_eq!(
            error.to_string(),
            "source manifest failed schema validation:\n  /: Additional properties are not allowed ('schema' was unexpected)"
        );
    }

    #[test]
    fn validate_manifest_schema_rejects_missing_backend() {
        let manifest = manifest_json(
            r"
name: demo
version: 1.0.0
dsl_version: 3
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
",
        );
        let error = validate_manifest_schema(&manifest).expect_err("schema validation should fail");
        assert_eq!(
            error.to_string(),
            "source manifest failed schema validation:\n  /: \"backend\" is a required property"
        );
    }

    #[test]
    fn parse_source_manifest_yaml_surfaces_request_path_schema_errors() {
        let error = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: ""
"#,
        )
        .expect_err("schema validation should fail");
        assert_eq!(
            error.to_string(),
            "source manifest failed schema validation:\n  /tables/0/request/path: \"\" is shorter than 1 character"
        );
    }
}
