//! Loads persisted source manifests from the managed sources directory.

use std::fs;
use std::path::Path;

use crate::parser::parse_source_manifest_yaml;
use crate::{ManifestError, Result, ValidatedSourceManifest};

/// Read and parse a source manifest from a file path.
///
/// # Errors
///
/// Returns a [`ManifestError`] if the file cannot be read or the manifest
/// violates any validation rules.
pub fn load_manifest_path(path: &Path) -> Result<ValidatedSourceManifest> {
    let raw = fs::read_to_string(path).map_err(|e| {
        ManifestError::validation(format!("failed to read {}: {e}", path.display()))
    })?;
    let manifest = parse_source_manifest_yaml(raw.as_str())?;
    Ok(manifest)
}

#[cfg(test)]
mod tests {
    use super::load_manifest_path;
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    fn write_manifest(raw: &str) -> (TempDir, PathBuf) {
        let root = TempDir::new().expect("temp root");
        let path = root.path().join("source.yml");
        fs::write(&path, raw).expect("write manifest");
        (root, path)
    }

    #[test]
    fn load_manifest_path_accepts_parquet_file_manifest() {
        let (_temp, path) = write_manifest(
            r#"
name: otel_metrics
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: metrics
    description: Metrics exported as parquet
    format: parquet
    source:
      location: file:///tmp/coral-otel-metrics/
      glob: "**/*.parquet"
      partitions:
        - name: date
          type: Utf8
    columns: []
"#,
        );

        let manifest = load_manifest_path(&path).expect("parquet manifest should load");
        assert_eq!(manifest.schema_name(), "otel_metrics");
    }

    #[test]
    fn load_manifest_path_accepts_jsonl_file_manifest() {
        let (_temp, path) = write_manifest(
            r#"
name: claude
version: 0.1.0
dsl_version: 3
backend: file
tables:
  - name: messages
    description: Claude Code conversation messages
    format: jsonl
    source:
      location: file:///tmp/claude-jsonl/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: sessionId
        type: Utf8
"#,
        );

        let manifest = load_manifest_path(&path).expect("jsonl manifest should load");
        assert_eq!(manifest.schema_name(), "claude");
        assert!(manifest.as_file().is_some());
    }

    #[test]
    fn load_manifest_path_rejects_malformed_manifest() {
        let (_temp, path) = write_manifest(
            r"
name: bad_plugin
version: 0.1.0
backend: http
tables:
  - name: stuff
    columns: []
",
        );

        let error =
            load_manifest_path(&path).expect_err("manifest missing dsl_version should fail");
        assert!(error.to_string().contains("dsl_version"));
    }
}
