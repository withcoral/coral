//! Generic source-spec parsing and backend dispatch.
//!
//! This module keeps the public source-spec parsing surface backend-agnostic.
//! Callers parse once into [`ValidatedSourceManifest`] and then inspect it
//! through narrow accessors such as [`ValidatedSourceManifest::as_http`].

use std::collections::BTreeSet;

use serde_json::Value;

use crate::backends::file::{JsonlSourceManifest, ParquetSourceManifest};
use crate::backends::http::HttpSourceManifest;
use crate::schema::validate_manifest_schema;
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
    Parquet(ParquetSourceManifest),
    Jsonl(JsonlSourceManifest),
}

impl ValidatedSourceManifest {
    /// Returns the stable backend kind declared by the source spec.
    ///
    /// This accessor is currently test-only because production callers
    /// typically branch through `as_http`, `as_parquet`, or `as_jsonl`.
    #[cfg(test)]
    #[must_use]
    pub fn backend(&self) -> SourceBackend {
        match &self.inner {
            ValidatedManifestKind::Http(_) => SourceBackend::Http,
            ValidatedManifestKind::Parquet(_) => SourceBackend::Parquet,
            ValidatedManifestKind::Jsonl(_) => SourceBackend::Jsonl,
        }
    }

    #[must_use]
    /// Returns the source-spec `name`, which is also the stable SQL schema name.
    pub fn schema_name(&self) -> &str {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.name,
            ValidatedManifestKind::Parquet(manifest) => &manifest.common.name,
            ValidatedManifestKind::Jsonl(manifest) => &manifest.common.name,
        }
    }

    #[must_use]
    /// Returns the source-spec version string for the source.
    pub fn source_version(&self) -> &str {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.version,
            ValidatedManifestKind::Parquet(manifest) => &manifest.common.version,
            ValidatedManifestKind::Jsonl(manifest) => &manifest.common.version,
        }
    }

    #[must_use]
    /// Returns the source-spec description string.
    pub fn description(&self) -> &str {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.description,
            ValidatedManifestKind::Parquet(manifest) => &manifest.common.description,
            ValidatedManifestKind::Jsonl(manifest) => &manifest.common.description,
        }
    }

    #[must_use]
    /// Returns the optional top-level validation queries declared by the source spec.
    pub fn test_queries(&self) -> &[String] {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.common.test_queries,
            ValidatedManifestKind::Parquet(manifest) => &manifest.common.test_queries,
            ValidatedManifestKind::Jsonl(manifest) => &manifest.common.test_queries,
        }
    }

    /// Returns the set of source secrets required to compile or authenticate
    /// the source spec.
    #[must_use]
    pub fn required_secret_names(&self) -> BTreeSet<String> {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => manifest.required_secret_names(),
            ValidatedManifestKind::Parquet(manifest) => manifest.required_secret_names(),
            ValidatedManifestKind::Jsonl(manifest) => manifest.required_secret_names(),
        }
    }

    /// Returns the declared top-level inputs for this manifest in authored order.
    #[must_use]
    pub fn declared_inputs(&self) -> &[ManifestInputSpec] {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => &manifest.declared_inputs,
            ValidatedManifestKind::Parquet(manifest) => &manifest.declared_inputs,
            ValidatedManifestKind::Jsonl(manifest) => &manifest.declared_inputs,
        }
    }

    /// Returns the validated HTTP source spec when `backend: http`.
    #[must_use]
    pub fn as_http(&self) -> Option<&HttpSourceManifest> {
        match &self.inner {
            ValidatedManifestKind::Http(manifest) => Some(manifest),
            _ => None,
        }
    }

    /// Returns the validated Parquet source spec when `backend: parquet`.
    #[must_use]
    pub fn as_parquet(&self) -> Option<&ParquetSourceManifest> {
        match &self.inner {
            ValidatedManifestKind::Parquet(manifest) => Some(manifest),
            _ => None,
        }
    }

    /// Returns the validated JSONL source spec when `backend: jsonl`.
    #[must_use]
    pub fn as_jsonl(&self) -> Option<&JsonlSourceManifest> {
        match &self.inner {
            ValidatedManifestKind::Jsonl(manifest) => Some(manifest),
            _ => None,
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
    validate_manifest_schema(&value)?;
    let backend_kind = parse_source_backend(&value)?;
    match backend_kind {
        SourceBackend::Http => Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::Http(Box::new(HttpSourceManifest::parse_manifest_value(
                value,
            )?)),
        }),
        SourceBackend::Parquet => Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::Parquet(ParquetSourceManifest::parse_manifest_value(
                value,
            )?),
        }),
        SourceBackend::Jsonl => Ok(ValidatedSourceManifest {
            inner: ValidatedManifestKind::Jsonl(JsonlSourceManifest::parse_manifest_value(value)?),
        }),
    }
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
    fn parse_source_manifest_preserves_test_query_order() {
        let manifest = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: jsonl
test_queries:
  - SELECT 1
  - SELECT 2
tables:
  - name: messages
    description: Demo messages
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
    fn parse_source_manifest_rejects_duplicate_table_names() {
        let error = parse_source_manifest_yaml(
            r"
name: demo
version: 1.0.0
dsl_version: 3
backend: jsonl
tables:
  - name: messages
    description: Demo messages
    source:
      location: file:///tmp/demo/
    columns:
      - name: kind
        type: Utf8
  - name: messages
    description: Duplicate messages
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
            "source 'demo' has duplicate table 'messages'"
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
        assert_eq!(function.name, "search_issues");
    }

    #[test]
    fn parse_source_manifest_rejects_whitespace_only_test_query() {
        let error = parse_source_manifest_yaml(
            r#"
name: demo
version: 1.0.0
dsl_version: 3
backend: jsonl
test_queries:
  - "   "
tables:
  - name: messages
    description: Demo messages
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
