#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use tempfile::TempDir;

fn temp_manifest(content: &str) -> (TempDir, PathBuf) {
    let dir = tempfile::tempdir().expect("temp dir");
    let path = dir.path().join("manifest.yaml");
    std::fs::write(&path, content).expect("write temp manifest");
    (dir, path)
}

fn coral_lint(file: &Path) -> std::process::Output {
    let config_dir = tempfile::tempdir().expect("config dir");
    Command::new(env!("CARGO_BIN_EXE_coral"))
        .args([
            "source",
            "lint",
            file.to_str().expect("temp manifest path is valid UTF-8"),
        ])
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run coral source lint")
}

#[test]
fn lint_accepts_valid_manifest() {
    let (_dir, path) = temp_manifest(
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
      path: /messages
",
    );
    let output = coral_lint(&path);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "expected success, stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        stdout.contains("Manifest is valid"),
        "expected 'Manifest is valid' in stdout, got: {stdout}"
    );
}

#[test]
fn lint_rejects_schema_violation() {
    let (_dir, path) = temp_manifest(
        r"
name: demo
version: 1.0.0
dsl_version: 3
base_url: https://example.com
tables:
  - name: messages
    description: Demo messages
    request:
      path: /messages
",
    );
    let output = coral_lint(&path);

    assert!(!output.status.success(), "expected non-zero exit status");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("\"backend\" is a required property"),
        "expected missing-backend schema error, got: {stderr}"
    );
}

#[test]
fn lint_rejects_semantic_violation() {
    let (_dir, path) = temp_manifest(
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
      path: /messages
    columns:
      - name: id
        type: Utf8
      - name: id
        type: Int64
",
    );
    let output = coral_lint(&path);

    assert!(!output.status.success(), "expected non-zero exit status");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("duplicate column 'id'"),
        "expected duplicate-column error, got: {stderr}"
    );
}
