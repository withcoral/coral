//! Generic source-spec parsing and backend dispatch.
//!
//! This module keeps the public source-spec parsing surface backend-agnostic.
//! Callers parse once into [`ValidatedSourceManifest`] and then inspect it
//! through narrow accessors such as [`ValidatedSourceManifest::as_http`].

use std::collections::BTreeSet;

use serde_json::Value;

use crate::backends::file::FileSourceManifest;
use crate::backends::http::HttpSourceManifest;
use crate::backends::mcp::McpSourceManifest;
use crate::schema::validate_manifest_schema_for_dsl_version;
use crate::v4::V4SourceManifest;
use crate::{ManifestError, ManifestInputSpec, Result, SourceBackend};

/// Validated top-level source spec for one registered source.
///
/// This is the main parsed output of `coral-spec`. It preserves the common
/// source identity fields and provides typed access to the backend-specific
/// validated source-spec model without exposing parser internals.
#[derive(Debug, Clone)]
pub struct ValidatedSourceManifest {
    inner: ValidatedManifestKind,
}

#[derive(Debug, Clone)]
enum ValidatedManifestKind {
    Http(Box<HttpSourceManifest>),
    File(FileSourceManifest),
    Mcp(McpSourceManifest),
    V4(Box<V4SourceManifest>),
}

impl ValidatedSourceManifest {
    /// Returns the stable backend kind declared by the source spec.
    ///
    /// This accessor is currently test-only because production callers
    /// typically branch through `as_http` or `as_file`.
    #[cfg(test)]
    #[must_use]
    pub fn backend(&self) -> SourceBackend {
        match &self.inner {
            ValidatedManifestKind::Http(_) => SourceBackend::Http,
            ValidatedManifestKind::File(_) => SourceBackend::File,
            ValidatedManifestKind::Mcp(_) => SourceBackend::Mcp,
            ValidatedManifestKind::V4(manifest) => match manifest.surface.surface_type {
                crate::v4::SurfaceType::OpenApi => SourceBackend::Http,
                crate::v4::SurfaceType::Mcp => SourceBackend::Mcp,
                crate::v4::SurfaceType::Database => SourceBackend::Database,
            },
        }
    }

    /// Returns the declared source-spec DSL version.
    #[must_use]
    pub fn dsl_version(&self) -> u32 {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => manifest.common.dsl_version,
            ValidatedManifestKind::File(manifest) => manifest.common.dsl_version,
            ValidatedManifestKind::Mcp(manifest) => manifest.common.dsl_version,
            ValidatedManifestKind::V4(manifest) => manifest.common.dsl_version,
        }
    }

    #[must_use]
    /// Returns the source-spec `name`.
    pub fn schema_name(&self) -> &str {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.name,
            ValidatedManifestKind::File(manifest) => &manifest.common.name,
            ValidatedManifestKind::Mcp(manifest) => &manifest.common.name,
            ValidatedManifestKind::V4(manifest) => &manifest.common.name,
        }
    }

    #[must_use]
    /// Returns the authored source-spec version string when the DSL declares one.
    pub fn source_version(&self) -> Option<&str> {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => Some(&manifest.common.version),
            ValidatedManifestKind::File(manifest) => Some(&manifest.common.version),
            ValidatedManifestKind::Mcp(manifest) => Some(&manifest.common.version),
            ValidatedManifestKind::V4(_) => None,
        }
    }

    #[must_use]
    /// Returns the source-spec description string.
    pub fn description(&self) -> &str {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.description,
            ValidatedManifestKind::File(manifest) => &manifest.common.description,
            ValidatedManifestKind::Mcp(manifest) => &manifest.common.description,
            ValidatedManifestKind::V4(manifest) => &manifest.common.description,
        }
    }

    #[must_use]
    /// Returns the optional top-level validation queries declared by the source spec.
    pub fn test_queries(&self) -> &[String] {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.test_queries,
            ValidatedManifestKind::File(manifest) => &manifest.common.test_queries,
            ValidatedManifestKind::Mcp(manifest) => &manifest.common.test_queries,
            ValidatedManifestKind::V4(manifest) => &manifest.common.test_queries,
        }
    }

    /// Returns the set of source secrets required to compile or authenticate
    /// the source spec.
    #[must_use]
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => manifest.required_secret_names(),
            ValidatedManifestKind::File(manifest) => manifest.required_secret_names(),
            ValidatedManifestKind::Mcp(manifest) => manifest.required_secret_names(),
            ValidatedManifestKind::V4(manifest) => manifest
                .declared_inputs
                .iter()
                .filter(|input| input.kind == crate::ManifestInputKind::Secret && input.required)
                .map(|input| input.key.clone())
                .collect(),
        }
    }

    /// Returns the set of declared source secrets that may be passed to runtime.
    #[must_use]
    pub fn declared_secret_names(&self) -> BTreeSet<String> {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => manifest.declared_secret_names(),
            ValidatedManifestKind::File(manifest) => manifest.declared_secret_names(),
            ValidatedManifestKind::Mcp(manifest) => manifest.declared_secret_names(),
            ValidatedManifestKind::V4(manifest) => manifest
                .declared_inputs
                .iter()
                .filter(|input| input.kind == crate::ManifestInputKind::Secret)
                .map(|input| input.key.clone())
                .collect(),
        }
    }

    /// Returns the declared top-level inputs for this manifest in authored order.
    #[must_use]
    pub fn declared_inputs(&self) -> &[ManifestInputSpec] {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.declared_inputs,
            ValidatedManifestKind::File(manifest) => &manifest.declared_inputs,
            ValidatedManifestKind::Mcp(manifest) => &manifest.declared_inputs,
            ValidatedManifestKind::V4(manifest) => &manifest.declared_inputs,
        }
    }

    /// Returns the validated HTTP source spec when `backend: http`.
    #[must_use]
    pub fn as_http(&self) -> Option<&HttpSourceManifest> {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => Some(manifest),
            ValidatedManifestKind::File(_)
            | ValidatedManifestKind::Mcp(_)
            | ValidatedManifestKind::V4(_) => None,
        }
    }

    /// Returns the validated file source spec when `backend: file`.
    #[must_use]
    pub fn as_file(&self) -> Option<&FileSourceManifest> {
        match &self.inner {
            ValidatedManifestKind::File(manifest) => Some(manifest),
            ValidatedManifestKind::Http(_)
            | ValidatedManifestKind::Mcp(_)
            | ValidatedManifestKind::V4(_) => None,
        }
    }

    /// Returns the validated MCP source spec when `backend: mcp`.
    #[must_use]
    pub fn as_mcp(&self) -> Option<&McpSourceManifest> {
        match &self.inner {
            ValidatedManifestKind::Mcp(manifest) => Some(manifest),
            ValidatedManifestKind::Http(_)
            | ValidatedManifestKind::File(_)
            | ValidatedManifestKind::V4(_) => None,
        }
    }

    /// Returns the validated DSL v4 source spec when `dsl_version: 4`.
    #[must_use]
    pub fn as_v4(&self) -> Option<&V4SourceManifest> {
        match &self.inner {
            ValidatedManifestKind::V4(manifest) => Some(manifest),
            ValidatedManifestKind::Http(_)
            | ValidatedManifestKind::File(_)
            | ValidatedManifestKind::Mcp(_) => None,
        }
    }
}

/// Parse and validate a source-spec manifest from `YAML` text.
///
/// Runs the same validation the server uses at install time. Callers that
/// need the declared interactive inputs can read them via
/// [`ValidatedSourceManifest::declared_inputs`].
///
/// # Errors
///
/// Returns a [`ManifestError`] if the `YAML` cannot be parsed or the source
/// spec violates any validation rules.
pub fn parse_source_manifest_yaml(raw: &str) -> Result<ValidatedSourceManifest> {
    let manifest_value: Value = serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    parse_source_manifest_value(manifest_value)
}

/// Parse and validate a source spec from structured source-spec data.
///
/// # Errors
///
/// Returns a [`ManifestError`] if the source spec violates any validation
/// rules.
pub fn parse_source_manifest_value(value: Value) -> Result<ValidatedSourceManifest> {
    let dsl_version = parse_dsl_version(&value)?;
    validate_manifest_schema_for_dsl_version(&value, dsl_version)?;
    if dsl_version == 4 {
        return Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::V4(Box::new(V4SourceManifest::parse_manifest_value(
                value,
            )?)),
        });
    }
    let backend_kind = parse_source_backend(&value)?;
    match backend_kind {
        SourceBackend::Http => Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::Http(Box::new(HttpSourceManifest::parse_manifest_value(
                value,
            )?)),
        }),
        SourceBackend::File => Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::File(FileSourceManifest::parse_manifest_value(value)?),
        }),
        SourceBackend::Mcp => Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::Mcp(McpSourceManifest::parse_manifest_value(value)?),
        }),
        SourceBackend::Database => Err(ManifestError::validation(
            "database sources require dsl_version 4",
        )),
    }
}

fn parse_dsl_version(value: &Value) -> Result<u32> {
    let Some(raw) = value.get("dsl_version").and_then(Value::as_u64) else {
        return Err(ManifestError::validation(
            "failed to deserialize manifest: missing dsl_version",
        ));
    };
    u32::try_from(raw)
        .map_err(|_err| ManifestError::validation("manifest dsl_version exceeds supported range"))
}

fn parse_source_backend(value: &Value) -> Result<SourceBackend> {
    let backend = value.get("backend").cloned().ok_or_else(|| {
        ManifestError::validation("failed to deserialize manifest: missing backend")
    })?;
    let backend: SourceBackend =
        serde_json::from_value(backend).map_err(ManifestError::deserialize)?;
    Ok(backend)
}

#[cfg(test)]
mod tests {
    use super::parse_source_manifest_yaml;

    #[test]
    fn v3_runtime_sql_identity_adapts_authored_backend_names() {
        let http = parse_source_manifest_yaml(
            r"
name: github
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
tables:
  - name: issues
    description: Issues
    request:
      path: /issues
    columns:
      - name: id
        type: Int64
functions:
  - name: search_issues
    request:
      path: /search/issues
    columns:
      - name: id
        type: Int64
",
        )
        .expect("HTTP manifest");
        let http = http.as_http().expect("HTTP backend");
        let table = http.tables.first().expect("HTTP table");
        assert_eq!(
            (
                table.common.catalog_name.as_str(),
                table.common.schema_name.as_str(),
                table.table_name()
            ),
            ("datafusion", "github", "issues")
        );
        let function = http.functions.first().expect("HTTP function");
        assert_eq!(
            (
                function.catalog_name.as_str(),
                function.schema_name.as_str(),
                function.function_name.as_str()
            ),
            ("datafusion", "github", "search_issues")
        );
        let serialized_table = serde_json::to_value(table).expect("serialize HTTP table");
        assert_eq!(serialized_table["common"]["name"], "issues");
        assert!(serialized_table["common"].get("catalog_name").is_none());
        assert!(serialized_table["common"].get("schema_name").is_none());
        let serialized_function = serde_json::to_value(function).expect("serialize HTTP function");
        assert_eq!(serialized_function["name"], "search_issues");
        assert!(serialized_function.get("catalog_name").is_none());
        assert!(serialized_function.get("schema_name").is_none());

        let file = parse_source_manifest_yaml(
            r"
name: logs
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: events
    description: Events
    format: jsonl
    source:
      location: file:///tmp/events/
    columns:
      - name: id
        type: Int64
",
        )
        .expect("file manifest");
        let table = file
            .as_file()
            .expect("file backend")
            .tables
            .first()
            .expect("file table");
        assert_eq!(
            (
                table.common.catalog_name.as_str(),
                table.common.schema_name.as_str(),
                table.table_name()
            ),
            ("datafusion", "logs", "events")
        );

        let mcp = parse_source_manifest_yaml(
            r"
name: github_mcp
version: 1.0.0
dsl_version: 3
backend: mcp
server:
  transport: stdio
  command: github-mcp-server
tables:
  - name: issues
    tool: list_issues
    columns:
      - name: id
        type: Int64
functions:
  - name: search_issues
    tool: search_issues
    columns:
      - name: id
        type: Int64
",
        )
        .expect("MCP manifest");
        let mcp = mcp.as_mcp().expect("MCP backend");
        let table = mcp.tables.first().expect("MCP table");
        assert_eq!(
            (
                table.common.catalog_name.as_str(),
                table.common.schema_name.as_str(),
                table.table_name()
            ),
            ("datafusion", "github_mcp", "issues")
        );
        let function = mcp.functions.first().expect("MCP function");
        assert_eq!(
            (
                function.common.catalog_name.as_str(),
                function.common.schema_name.as_str(),
                function.function_name()
            ),
            ("datafusion", "github_mcp", "search_issues")
        );
    }

    #[test]
    fn parse_source_manifest_preserves_test_query_order() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
test_queries:
  - SELECT 1
  - SELECT 2
tables:
  - name: messages
    description: Demo messages
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: kind
        type: Utf8
",
        )
        .expect("manifest should parse");

        assert_eq!(manifest.test_queries(), &["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn parse_source_manifest_preserves_do_not_index_policy() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Demo messages
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: title
        type: Utf8
      - name: internal_note
        type: Utf8
        do_not_index: true
",
        )
        .expect("manifest should parse");
        let columns = manifest
            .as_file()
            .expect("file manifest")
            .tables
            .first()
            .expect("messages table")
            .columns();

        assert!(!columns.first().expect("title column").do_not_index);
        assert!(columns.get(1).expect("internal note column").do_not_index);
    }

    #[test]
    fn reserved_source_name_is_rejected() {
        for name in ["coral", "coral_admin", "datafusion", "public"] {
            let raw = format!(
                r"
name: {name}
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Demo messages
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: kind
        type: Utf8
"
            );
            let error =
                parse_source_manifest_yaml(&raw).expect_err("reserved source name should fail");

            assert!(
                error
                    .to_string()
                    .contains(&format!("source name '{name}' is reserved")),
                "unexpected error: {error}"
            );
        }
    }

    #[test]
    fn lookup_key_on_file_jsonl_rejects_at_spec_layer() {
        let error = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Demo messages
    format: jsonl
    filters:
      - name: id
        lookup_key: true
    source:
      location: file:///tmp/demo/
    columns:
      - name: id
        type: Utf8
",
        )
        .expect_err("spec layer should reject lookup_key filters on file sources");

        assert!(error.to_string().contains(
            "demo.messages filter 'id': backend=file does not support lookup_key filters"
        ));
    }

    #[test]
    fn lookup_key_on_file_parquet_rejects_at_spec_layer() {
        let error = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Demo messages
    format: parquet
    filters:
      - name: id
        lookup_key: true
    source:
      location: file:///tmp/demo/
    columns:
      - name: id
        type: Utf8
",
        )
        .expect_err("spec layer should reject lookup_key filters on file sources");

        assert!(error.to_string().contains(
            "demo.messages filter 'id': backend=file does not support lookup_key filters"
        ));
    }

    #[test]
    fn http_rate_limit_max_concurrency_is_not_manifest_metadata() {
        let error = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
rate_limit:
  max_concurrency: 8
tables:
  - name: messages
    description: Demo messages
    request:
      path: /messages
    columns:
      - name: id
        type: Utf8
",
        )
        .expect_err("manifest-owned concurrency should fail schema validation");

        assert!(error.to_string().contains("max_concurrency"));
    }

    #[test]
    fn parse_source_manifest_rejects_duplicate_table_names() {
        let error = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Demo messages
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: kind
        type: Utf8
  - name: messages
    description: Duplicate messages
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: id
        type: Int64
",
        )
        .expect_err("duplicate table names should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' table 'messages' is declared more than once"
        );
    }

    #[test]
    fn parse_source_manifest_accepts_http_functions_without_tables() {
        let manifest = parse_source_manifest_yaml(
            r"
name: searchy
version: 1.0.0
dsl_version: 3
backend: http
base_url: https://example.com
functions:
  - name: search_issues
    args:
      - name: q
        required: true
        bind:
          arg: q
    request:
      method: GET
      path: /search/issues
      query:
        - name: q
          from: arg
          key: q
    columns:
      - name: title
        type: Utf8
",
        )
        .expect("function-only HTTP manifest should parse");

        let http = manifest.as_http().expect("HTTP manifest");
        assert!(http.tables.is_empty());
        assert_eq!(http.functions.len(), 1);
        let function = http.functions.first().expect("HTTP function");
        assert_eq!(function.function_name, "search_issues");
    }

    #[test]
    fn parse_source_manifest_rejects_whitespace_only_test_query() {
        let error = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: file
test_queries:
  - "   "
tables:
  - name: messages
    description: Demo messages
    format: jsonl
    source:
      location: file:///tmp/demo/
    columns:
      - name: kind
        type: Utf8
"#,
        )
        .expect_err("whitespace-only query should fail");

        assert_eq!(
            error.to_string(),
            "source 'demo' test_queries[0] must not be empty"
        );
    }
}
