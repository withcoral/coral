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
    use crate::{ManifestError, Result, ValidatedSourceManifest};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Base name for source manifest files (without extension).
    const SOURCE_MANIFEST_NAME: &str = "source";

    /// Accepted `YAML` extensions in preferred order.
    const YAML_EXTENSIONS: &[&str] = &["yml", "yaml"];

    /// Load and validate source manifests from a sources directory.
    ///
    /// Invalid source manifests are skipped after logging an error; the returned vector
    /// contains only successfully loaded manifests.
    fn load_manifests<P: AsRef<Path>>(root: P) -> Result<Vec<ValidatedSourceManifest>> {
        let root = root.as_ref();
        if !root.exists() {
            return Ok(Vec::new());
        }

        let mut files = Vec::new();

        // Support both <root>/<name>/source.{yml,yaml} and <root>/source.{yml,yaml}
        if let Some(direct) = find_manifest_in(root) {
            files.push(direct);
        }

        let entries = fs::read_dir(root).map_err(|e| {
            ManifestError::validation(format!("failed to read {}: {e}", root.display()))
        })?;
        for entry in entries {
            let entry = entry.map_err(|e| {
                ManifestError::validation(format!("failed to read {}: {e}", root.display()))
            })?;
            let path = entry.path();
            if path.is_dir()
                && let Some(file) = find_manifest_in(&path)
            {
                files.push(file);
            }
        }

        files.sort();

        if files.is_empty() {
            return Ok(Vec::new());
        }

        let mut manifests = Vec::new();
        for file in files {
            match load_manifest_path(&file) {
                Ok(manifest) => manifests.push(manifest),
                Err(error) => {
                    tracing::warn!(path = %file.display(), error = %error, "skipping malformed source");
                }
            }
        }

        Ok(manifests)
    }

    /// Return the first existing manifest file in `dir`, preferring `.yml` over `.yaml`.
    fn find_manifest_in(dir: &Path) -> Option<PathBuf> {
        YAML_EXTENSIONS
            .iter()
            .map(|ext| dir.join(format!("{SOURCE_MANIFEST_NAME}.{ext}")))
            .find(|p| p.is_file())
    }

    fn unique_temp_dir(prefix: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("time should be after unix epoch")
            .as_nanos();
        std::env::temp_dir().join(format!("{prefix}-{unique}"))
    }

    #[test]
    fn load_manifests_returns_empty_for_missing_directory() {
        let root = unique_temp_dir("coral-loader-missing");
        let manifests = load_manifests(&root).expect("missing source dir should not error");
        assert!(manifests.is_empty());
    }

    #[test]
    fn load_manifests_returns_empty_for_existing_directory_without_manifests() {
        let root = unique_temp_dir("coral-loader-empty");
        fs::create_dir_all(&root).expect("create temp root");

        let manifests =
            load_manifests(&root).expect("empty source dir should not fail manifest loading");
        assert!(manifests.is_empty());

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn load_manifests_accepts_parquet_backend_manifest() {
        let root = unique_temp_dir("coral-loader-parquet");
        let source_dir = root.join("otel_metrics");
        fs::create_dir_all(&source_dir).expect("create source dir");
        fs::write(
            source_dir.join("source.yml"),
            r#"
name: otel_metrics
version: 0.1.0
dsl_version: 3
backend: parquet
tables:
  - name: metrics
    description: Metrics exported as parquet
    source:
      location: file:///tmp/coral-otel-metrics/
      glob: "**/*.parquet"
      partitions:
        - name: date
          type: Utf8
    columns: []
"#,
        )
        .expect("write manifest");

        let manifests = load_manifests(&root).expect("parquet manifest should load");
        assert_eq!(manifests.len(), 1);
        let manifest = manifests.first().expect("parquet manifest");
        assert_eq!(manifest.schema_name(), "otel_metrics");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn load_manifests_accepts_jsonl_backend_manifest() {
        let root = unique_temp_dir("coral-loader-jsonl");
        let source_dir = root.join("claude");
        fs::create_dir_all(&source_dir).expect("create source dir");
        fs::write(
            source_dir.join(format!("{SOURCE_MANIFEST_NAME}.yml")),
            r#"
name: claude
version: 0.1.0
dsl_version: 3
backend: jsonl
tables:
  - name: messages
    description: Claude Code conversation messages
    source:
      location: file:///tmp/claude-jsonl/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: sessionId
        type: Utf8
"#,
        )
        .expect("write manifest");

        let manifests = load_manifests(&root).expect("jsonl manifest should load");
        assert_eq!(manifests.len(), 1);
        let manifest = manifests.first().expect("jsonl manifest");
        assert_eq!(manifest.schema_name(), "claude");
        assert_eq!(manifest.backend(), crate::SourceBackend::Jsonl);

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn load_manifests_skips_malformed_source_and_loads_valid_ones() {
        let root = unique_temp_dir("coral-loader-malformed");

        // Create a valid source
        let good_dir = root.join("good");
        fs::create_dir_all(&good_dir).expect("create good dir");
        fs::write(
            good_dir.join("source.yml"),
            r#"
name: good_plugin
version: 0.1.0
dsl_version: 3
backend: parquet
tables:
  - name: data
    description: Some data
    source:
      location: file:///tmp/good/
      glob: "**/*.parquet"
    columns: []
"#,
        )
        .expect("write good manifest");

        // Create a malformed source (missing dsl_version)
        let bad_dir = root.join("bad");
        fs::create_dir_all(&bad_dir).expect("create bad dir");
        fs::write(
            bad_dir.join("source.yml"),
            r"
name: bad_plugin
version: 0.1.0
backend: http
tables:
  - name: stuff
    columns: []
",
        )
        .expect("write bad manifest");

        let manifests = load_manifests(&root).expect("should not error on malformed source");
        assert_eq!(manifests.len(), 1, "only the valid source should be loaded");
        let manifest = manifests.first().expect("valid manifest");
        assert_eq!(manifest.schema_name(), "good_plugin");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn load_manifests_accepts_yaml_extension() {
        let root = unique_temp_dir("coral-loader-yaml-ext");
        let source_dir = root.join("my_plugin");
        fs::create_dir_all(&source_dir).expect("create source dir");
        fs::write(
            source_dir.join("source.yaml"),
            r#"
name: my_plugin
version: 0.1.0
dsl_version: 3
backend: parquet
tables:
  - name: data
    description: Some data
    source:
      location: file:///tmp/my/
      glob: "**/*.parquet"
    columns: []
"#,
        )
        .expect("write manifest with .yaml extension");

        let manifests = load_manifests(&root).expect(".yaml manifest should load");
        assert_eq!(manifests.len(), 1);
        let manifest = manifests.first().expect("yaml manifest");
        assert_eq!(manifest.schema_name(), "my_plugin");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn load_manifests_prefers_yml_over_yaml() {
        let root = unique_temp_dir("coral-loader-yml-priority");
        let source_dir = root.join("dual");
        fs::create_dir_all(&source_dir).expect("create source dir");

        // Write both extensions — .yml should win
        for ext in &["yml", "yaml"] {
            fs::write(
                source_dir.join(format!("source.{ext}")),
                r#"
name: dual
version: 0.1.0
dsl_version: 3
backend: parquet
tables:
  - name: data
    description: Some data
    source:
      location: file:///tmp/dual/
      glob: "**/*.parquet"
    columns: []
"#,
            )
            .expect("write manifest");
        }

        let manifests = load_manifests(&root).expect("should load exactly one manifest");
        assert_eq!(manifests.len(), 1, "should not load both .yml and .yaml");

        drop(fs::remove_dir_all(&root));
    }

    #[test]
    fn load_manifests_returns_empty_when_all_sources_are_malformed() {
        let root = unique_temp_dir("coral-loader-all-bad");

        let bad_dir = root.join("broken");
        fs::create_dir_all(&bad_dir).expect("create dir");
        fs::write(bad_dir.join("source.yml"), "not: valid: yaml: content: [")
            .expect("write bad yaml");

        let manifests = load_manifests(&root).expect("should not error when all sources are bad");
        assert!(manifests.is_empty());

        drop(fs::remove_dir_all(&root));
    }
}
