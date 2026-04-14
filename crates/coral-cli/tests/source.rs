#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

use tempfile::tempdir;

use std::process::Command;

#[test]
fn source_test_errors_when_required_secret_is_missing() {
    let config_dir = tempdir().expect("failed to create temp dir");

    let manifest = r#"
    name: fake
    version: 1.0.0
    dsl_version: 3
    backend: http
    base_url: https://example.com
    inputs:
      TEST_API_KEY:
        kind: secret
    auth:
      headers:
        - name: Authorization
          from: template
          template: "{{input.TEST_API_KEY}}"
    tables:
      - name: dummy
        description: dummy table
        request:
          method: GET
          path: /dummy
          query: []
        columns:
          - name: id
            type: Utf8
            description: dummy id"#;
    let manifest_dir = config_dir
        .path()
        .join("workspaces")
        .join("default")
        .join("sources")
        .join("fake");
    let manifest_file_path = manifest_dir.join("manifest.yaml");
    let secrets_env_path = manifest_dir.join("secrets.env");
    std::fs::create_dir_all(manifest_dir).expect("failed to create manifest directory");
    std::fs::write(manifest_file_path, manifest).expect("Failed to write manifest");
    std::fs::write(secrets_env_path, "").expect("failed to write secrets.env");

    // Write a basic config that references the fake source, but don't set the required secret.
    let config = r#"
        [workspaces.default.sources.fake]
        version = "1.0.0"
        variables = {}
        secrets = []
        origin = "imported"
    "#;
    std::fs::write(config_dir.path().join("config.toml"), config).expect("failed to write config");

    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("source")
        .arg("test")
        .arg("fake")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .output()
        .expect("failed to run coral source test");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert!(
        stderr.contains("source 'fake' is missing secret 'TEST_API_KEY'"),
        "expected missing secret error in stderr, got: {stderr}"
    );
}
