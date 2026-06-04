//! JSON Schema validation for source manifests.

use std::sync::OnceLock;

use jsonschema::JSONSchema;
use serde_json::Value as JsonValue;

use crate::{ManifestError, Result};

static SOURCE_SCHEMA: OnceLock<JSONSchema> = OnceLock::new();
static SOURCE_V4_SCHEMA: OnceLock<JSONSchema> = OnceLock::new();

pub(crate) fn validate_manifest_schema(manifest_json: &JsonValue) -> Result<()> {
    validate_with_schema(manifest_json, source_schema())
}

pub(crate) fn validate_manifest_schema_for_dsl_version(
    manifest_json: &JsonValue,
    dsl_version: u32,
) -> Result<()> {
    if dsl_version == 4 {
        return validate_with_schema(manifest_json, source_v4_schema());
    }
    validate_manifest_schema(manifest_json)
}

fn source_schema() -> &'static JSONSchema {
    SOURCE_SCHEMA.get_or_init(|| {
        let schema_json: JsonValue =
            serde_json::from_str(include_str!("schema/source_manifest.schema.json"))
                .expect("embedded source schema must be valid JSON");
        JSONSchema::compile(&schema_json).expect("embedded source schema must compile")
    })
}

fn source_v4_schema() -> &'static JSONSchema {
    SOURCE_V4_SCHEMA.get_or_init(|| {
        let schema_json: JsonValue =
            serde_json::from_str(include_str!("schema/source_manifest_v4.schema.json"))
                .expect("embedded DSL v4 source schema must be valid JSON");
        JSONSchema::compile(&schema_json).expect("embedded DSL v4 source schema must compile")
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

    use super::validate_manifest_schema;
    use crate::parser::parse_source_manifest_yaml;

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

    fn schema_error(raw: &str) -> String {
        let manifest = manifest_json(raw);
        let message = validate_manifest_schema(&manifest)
            .expect_err("schema validation should fail")
            .to_string();
        assert!(
            message.starts_with("source manifest failed schema validation:"),
            "{message}"
        );
        message
    }

    fn assert_schema_ok(raw: &str, expectation: &str) {
        validate_manifest_schema(&manifest_json(raw)).expect(expectation);
    }

    fn assert_schema_error_contains(case: &str, raw: &str, expected: &[&str]) {
        let message = schema_error(raw);
        for expected in expected {
            assert!(message.contains(expected), "{case}: {message}");
        }
    }

    #[test]
    fn validate_manifest_schema_accepts_valid_http_manifest() {
        assert_schema_ok(
            valid_http_manifest(),
            "valid manifest should pass schema validation",
        );
    }

    #[test]
    fn validate_manifest_schema_accepts_quoted_sql_table_names() {
        let manifest = manifest_json(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: player.stats
    description: Demo messages
    request:
      method: GET
      path: /messages
  - name: message-events
    description: Event messages
    request:
      method: GET
      path: /events
",
        );
        validate_manifest_schema(&manifest)
            .expect("table names that require SQL quoting should pass schema validation");
    }

    #[test]
    fn validate_manifest_schema_rejects_invalid_table_function_identifier() {
        let manifest = manifest_json(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
functions:
  - name: search-messages
    request:
      method: GET
      path: /messages/search
",
        );

        let error = validate_manifest_schema(&manifest).expect_err("schema validation should fail");
        let message = error.to_string();
        assert!(
            message.starts_with("source manifest failed schema validation:"),
            "{message}"
        );
        assert!(message.contains("/functions/0/name"), "{message}");
        assert!(message.contains("^[A-Za-z_][A-Za-z0-9_]*$"), "{message}");
    }

    #[test]
    fn validate_manifest_schema_directly_rejects_v4_manifest() {
        let manifest = manifest_json(
            r"
name: demo
dsl_version: 4
surfaces:
  - id: rest
    type: openapi
    url: https://example.com/openapi.yaml
",
        );
        validate_manifest_schema(&manifest).expect_err("v3 schema should reject v4 manifests");
    }

    #[test]
    fn validate_manifest_schema_accepts_one_of_bearer_auth_headers() {
        assert_schema_ok(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
auth:
  type: HeaderAuth
  headers:
    - name: Authorization
      from: one_of
      values:
        - from: input
          key: API_KEY
        - from: bearer
          key: OAUTH_TOKEN
tables:
  - name: messages
    description: Demo messages
    request:
      method: GET
      path: /messages
",
            "one_of bearer auth header should pass schema validation",
        );
    }

    #[test]
    fn validate_manifest_schema_accepts_legacy_search_filter_mode() {
        assert_schema_ok(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    filters:
      - name: query
        mode: search
    request:
      method: GET
      path: /messages
",
            "legacy search filter mode should pass schema validation",
        );
    }

    #[test]
    fn parse_source_manifest_yaml_accepts_http_table_search_metadata() {
        parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    filters:
      - name: query
      - name: id
    search_limits:
      default_top_k: 5
      max_top_k: 20
      max_calls_per_query: 2
    detail_hints:
      - table: messages
        search_result_column: id
        detail_filter: id
        purpose: Fetch the full message record.
    request:
      method: GET
      path: /messages
    columns:
      - name: id
        type: Utf8
",
        )
        .expect("HTTP table search metadata should pass full manifest parsing");
    }

    #[test]
    fn validate_manifest_schema_rejects_invalid_search_structures() {
        for (case, raw, expected) in [
            (
                "search function without search limits",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
functions:
  - name: search_messages
    kind: search
    request:
      method: GET
      path: /messages/search
",
                &["/functions/0", "search_limits"][..],
            ),
            (
                "unknown filter type",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    filters:
      - name: query
        type: Banana
    request:
      method: GET
      path: /messages
",
                &["/tables/0/filters/0/type"][..],
            ),
            (
                "search limits above cap",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    search_limits:
      default_top_k: 5
      max_top_k: 1001
      max_calls_per_query: 1
    request:
      method: GET
      path: /messages
",
                &["/tables/0/search_limits/max_top_k"][..],
            ),
        ] {
            assert_schema_error_contains(case, raw, expected);
        }
    }

    #[test]
    fn validate_manifest_schema_rejects_backend_specific_table_fields() {
        for (case, raw, expected) in [
            (
                "file table search metadata",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Demo messages
    format: parquet
    source:
      location: file:///tmp/messages.parquet
    search_limits:
      default_top_k: 5
      max_top_k: 20
      max_calls_per_query: 2
    detail_hints: []
",
                &["/tables/0", "search_limits", "detail_hints"][..],
            ),
            (
                "http table source",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    source:
      location: file:///tmp/messages.jsonl
    request:
      method: GET
      path: /messages
",
                &["/tables/0", "source"][..],
            ),
        ] {
            assert_schema_error_contains(case, raw, expected);
        }
    }

    #[test]
    fn parse_source_manifest_yaml_accepts_jsonl_file_metadata() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Demo events
    format: jsonl
    source:
      location: file:///tmp/events/
      metadata:
        - name: session_path
          kind: relative_path
        - name: session_file
          kind: file_stem
        - name: event_index
          kind: line_number
    columns:
      - name: type
        type: Utf8
",
        )
        .expect("JSONL file metadata should pass full manifest parsing");
        assert!(manifest.as_file().is_some());
    }

    #[test]
    fn validate_manifest_schema_accepts_non_jsonl_file_metadata_cases() {
        for (case, raw) in [
            (
                "empty metadata",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Demo events
    format: parquet
    source:
      location: file:///tmp/events/
      metadata: []
",
            ),
            (
                "file-scoped metadata",
                r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Demo events
    format: parquet
    source:
      location: file:///tmp/events/
      metadata:
        - name: file_path
          kind: relative_path
",
            ),
        ] {
            assert_schema_ok(raw, case);
        }
    }

    #[test]
    fn validate_manifest_schema_rejects_file_metadata_cases() {
        assert_schema_error_contains(
            "line_number metadata on non-JSONL table",
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Demo events
    format: parquet
    source:
      location: file:///tmp/events/
      metadata:
        - name: event_index
          kind: line_number
",
            &["/tables/0/source", "metadata", "line_number"],
        );
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

    fn mcp_streamable_http_manifest(auth_yaml: &str) -> String {
        format!(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: mcp
inputs:
  MCP_TOKEN:
    kind: secret
server:
  transport: streamable_http
  url: https://mcp.example.com/mcp
  auth:
{auth_yaml}
tables:
  - name: hello
    tool: hello
    columns:
      - name: id
        type: Utf8
"
        )
    }

    #[test]
    fn validate_manifest_schema_accepts_mcp_streamable_http_bearer_auth_from_input() {
        assert_schema_ok(
            &mcp_streamable_http_manifest(
                "    type: bearer\n    from: input\n    key: MCP_TOKEN\n",
            ),
            "MCP bearer auth from a declared input must pass schema validation",
        );
    }

    #[test]
    fn validate_manifest_schema_rejects_mcp_streamable_http_bearer_auth_from_literal() {
        let manifest = manifest_json(&mcp_streamable_http_manifest(
            "    type: bearer\n    from: literal\n    value: Bearer hardcoded\n",
        ));
        let error = validate_manifest_schema(&manifest)
            .expect_err("MCP bearer auth from a literal must fail schema validation");
        let message = error.to_string();
        assert!(
            message.contains("/server"),
            "expected error location to point at the server subtree, got: {message}"
        );
    }
}
