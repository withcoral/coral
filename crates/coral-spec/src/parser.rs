//! `SourceSpec` parsing.
//!
//! The public parser accepts only the `spec_version: 1`, `kind: source`
//! contract.

use serde_json::Value;

use crate::schema::validate_source_spec_schema;
use crate::source::SourceSpec;
use crate::{ManifestError, Result};

/// Parse and validate a `SourceSpec` from YAML text.
///
/// # Errors
///
/// Returns a [`ManifestError`] if the YAML cannot be parsed or the `SourceSpec`
/// violates the `spec_version: 1`, `kind: source` contract.
pub fn parse_source_manifest_yaml(raw: &str) -> Result<SourceSpec> {
    let value: Value = serde_yaml::from_str(raw).map_err(ManifestError::parse_yaml)?;
    parse_source_manifest_value(value)
}

/// Parse and validate a `SourceSpec` from structured source data.
///
/// # Errors
///
/// Returns a [`ManifestError`] if the value violates the `SourceSpec` contract.
pub fn parse_source_manifest_value(value: Value) -> Result<SourceSpec> {
    if value.get("spec_version").is_none() || value.get("kind").is_none() {
        return Err(ManifestError::validation(
            "source spec must declare spec_version: 1 and kind: source",
        ));
    }
    validate_source_spec_schema(&value)?;
    SourceSpec::parse_value(value)
}

#[cfg(test)]
mod tests {
    use super::parse_source_manifest_yaml;

    #[test]
    fn parse_source_manifest_preserves_test_query_order() {
        let manifest = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: demo
test_queries:
  - SELECT 1
  - SELECT 2
interfaces:
  - id: files
    type: file
    files: [./messages.jsonl]
    format:
      kind: jsonl
",
        )
        .expect("manifest should parse");

        assert_eq!(manifest.test_queries, ["SELECT 1", "SELECT 2"]);
    }

    #[test]
    fn parse_source_manifest_rejects_unknown_contract_keys() {
        let raw = "spec_version: 1\nkind: source\nname: demo\nunused_key: 3\ninterfaces: []\n";
        let error = parse_source_manifest_yaml(raw).expect_err("unknown key must be rejected");

        assert!(error.to_string().contains("unused_key"));
    }

    #[test]
    fn parse_source_manifest_rejects_missing_new_markers() {
        let error = parse_source_manifest_yaml(
            r"
name: demo
interfaces: []
",
        )
        .expect_err("manifest must declare source spec markers");

        assert!(
            error
                .to_string()
                .contains("source spec must declare spec_version: 1 and kind: source")
        );
    }

    #[test]
    fn parse_source_manifest_rejects_whitespace_only_test_query() {
        let error = parse_source_manifest_yaml(
            r"
spec_version: 1
kind: source
name: demo
test_queries:
  - '  '
interfaces:
  - id: files
    type: file
    files: [./messages.jsonl]
    format:
      kind: jsonl
",
        )
        .expect_err("blank test query should fail validation");

        assert!(
            error
                .to_string()
                .contains("test_queries[0] must not be empty")
        );
    }
}
