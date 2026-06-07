//! JSON Schema validation for replacement `SourceSpec` manifests.

use std::sync::OnceLock;

use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::{ManifestError, Result};

static SOURCE_SPEC_SCHEMA: OnceLock<JSONSchema> = OnceLock::new();

pub(crate) fn validate_source_spec_schema(manifest_json: &JsonValue) -> Result<()> {
    validate_with_schema(manifest_json, source_spec_schema())
}

fn source_spec_schema() -> &'static JSONSchema {
    SOURCE_SPEC_SCHEMA.get_or_init(|| {
        let schema_json: JsonValue =
            serde_json::from_str(include_str!("schema/source_spec.schema.json"))
                .expect("embedded SourceSpec schema must be valid JSON");
        JSONSchema::compile(&schema_json).expect("embedded SourceSpec schema must compile")
    })
}

fn validate_with_schema(manifest_json: &JsonValue, validator: &JSONSchema) -> Result<()> {
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

    use super::validate_source_spec_schema;
    use crate::parser::parse_source_manifest_yaml;

    fn valid_source_spec() -> &'static str {
        r"
name: demo
spec_version: 1
kind: source
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
"
    }

    fn manifest_json(raw: &str) -> JsonValue {
        serde_yaml::from_str(raw).expect("test manifest should parse as yaml")
    }

    #[test]
    fn validate_source_spec_schema_accepts_valid_source_spec() {
        let manifest = manifest_json(valid_source_spec());
        validate_source_spec_schema(&manifest)
            .expect("valid SourceSpec should pass schema validation");
    }

    #[test]
    fn validate_source_spec_schema_accepts_all_interface_variants() {
        let manifest = manifest_json(
            r"
name: demo
spec_version: 1
kind: source
inputs:
  - key: token
    kind: secret
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.json
    auth:
      kind: bearer_input
      key: token
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.example.com/mcp
  - id: graph
    type: graphql
    endpoint: https://api.example.com/graphql
    schema:
      kind: introspection_query
  - id: files
    type: file
    files: [./events.parquet]
    format:
      kind: parquet
",
        );
        validate_source_spec_schema(&manifest)
            .expect("all SourceSpec interface variants should pass schema validation");
    }

    #[test]
    fn validate_source_spec_schema_accepts_oauth_credential_methods() {
        let manifest = manifest_json(
            r"
name: slack
spec_version: 1
kind: source
inputs:
  - key: SLACK_USER_TOKEN
    kind: secret
    credential:
      methods:
        - type: oauth
          label: Connect with Slack
          oauth:
            flow:
              type: authorization_code
              pkce: required
            redirect_uri: http://localhost:53682/oauth/callback
            redirect_uri_port_mode: fixed
            endpoints:
              authorization_url: https://slack.com/oauth/v2_user/authorize
              token_url: https://slack.com/api/oauth.v2.user.access
            client:
              id:
                default: '6057250636981.7381814187793'
            scopes:
              scope:
                delimiter: comma
                values:
                  - search:read.public
        - type: source_config
interfaces:
  - id: mcp
    type: mcp
    server:
      transport:
        type: streamable_http
        url: https://mcp.slack.com/mcp
      auth:
        kind: bearer_input
        key: SLACK_USER_TOKEN
",
        );

        validate_source_spec_schema(&manifest)
            .expect("OAuth credential SourceSpec should pass schema validation");
    }

    #[test]
    fn validate_source_spec_schema_rejects_missing_contract_markers() {
        let manifest = manifest_json(
            r"
name: demo
interfaces: []
",
        );

        let error =
            validate_source_spec_schema(&manifest).expect_err("schema validation should fail");
        let message = error.to_string();
        assert!(
            message.starts_with("source manifest failed schema validation:"),
            "{message}"
        );
        assert!(
            message.contains("spec_version") || message.contains("kind"),
            "{message}"
        );
    }

    #[test]
    fn validate_source_spec_schema_rejects_unknown_top_level_fields() {
        let manifest = manifest_json(
            r"
name: demo
spec_version: 1
kind: source
interfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
extra: true
",
        );
        validate_source_spec_schema(&manifest).expect_err("schema should reject unknown fields");
    }

    #[test]
    fn validate_source_spec_schema_rejects_unknown_interface_type() {
        let manifest = manifest_json(
            r"
name: demo
spec_version: 1
kind: source
interfaces:
  - id: rest
    type: swagger
    url: https://example.com/openapi.json
",
        );
        let error =
            validate_source_spec_schema(&manifest).expect_err("schema validation should fail");
        let message = error.to_string();
        assert!(message.contains("/interfaces/0"), "{message}");
        assert!(message.contains("swagger"), "{message}");
    }

    #[test]
    fn parse_source_manifest_yaml_accepts_source_spec_schema_shape() {
        let manifest = manifest_json(valid_source_spec());
        validate_source_spec_schema(&manifest).expect("schema validation");
        parse_source_manifest_yaml(valid_source_spec()).expect("parser should accept schema shape");
    }
}
