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

#[test]
fn source_test_exits_non_zero_when_query_tests_fail() {
    let config_dir = tempdir().expect("failed to create temp dir");
    let source_dir = config_dir
        .path()
        .join("workspaces")
        .join("default")
        .join("sources")
        .join("local_messages");
    std::fs::create_dir_all(source_dir.join("fixture-data")).expect("create source dir");
    std::fs::write(
        source_dir.join("fixture-data/messages.jsonl"),
        r#"{"type":"user","text":"hello"}
"#,
    )
    .expect("write fixture data");
    std::fs::write(
        source_dir.join("manifest.yaml"),
        r#"
name: local_messages
version: 0.1.0
dsl_version: 3
backend: jsonl
test_queries:
  - SELECT * FROM local_messages.missing
tables:
  - name: messages
    description: fixture messages
    source:
      location: file://FIXTURE_ROOT/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: text
        type: Utf8
"#
        .replace(
            "file://FIXTURE_ROOT/",
            &format!("file://{}/", source_dir.join("fixture-data").display()),
        ),
    )
    .expect("write manifest");
    std::fs::write(
        config_dir.path().join("config.toml"),
        r#"
        [workspaces.default.sources.local_messages]
        version = "0.1.0"
        variables = {}
        secrets = []
        origin = "imported"
    "#,
    )
    .expect("failed to write config");

    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("source")
        .arg("test")
        .arg("local_messages")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .output()
        .expect("failed to run coral source test");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert!(
        stdout.contains("Query tests"),
        "expected query-test summary in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("SELECT * FROM local_messages.missing"),
        "expected failing query text in stdout, got: {stdout}"
    );
    assert!(
        stderr.contains("1 of 1 validation query failed"),
        "expected strict failure in stderr, got: {stderr}"
    );
}

#[test]
fn source_test_succeeds_when_query_tests_pass() {
    let config_dir = tempdir().expect("failed to create temp dir");
    let source_dir = config_dir
        .path()
        .join("workspaces")
        .join("default")
        .join("sources")
        .join("local_messages");
    std::fs::create_dir_all(source_dir.join("fixture-data")).expect("create source dir");
    std::fs::write(
        source_dir.join("fixture-data/messages.jsonl"),
        r#"{"type":"user","text":"hello"}
"#,
    )
    .expect("write fixture data");
    std::fs::write(
        source_dir.join("manifest.yaml"),
        r#"
name: local_messages
version: 0.1.0
dsl_version: 3
backend: jsonl
test_queries:
  - SELECT COUNT(*) AS n FROM local_messages.messages
tables:
  - name: messages
    description: fixture messages
    source:
      location: file://FIXTURE_ROOT/
      glob: "**/*.jsonl"
    columns:
      - name: type
        type: Utf8
      - name: text
        type: Utf8
"#
        .replace(
            "file://FIXTURE_ROOT/",
            &format!("file://{}/", source_dir.join("fixture-data").display()),
        ),
    )
    .expect("write manifest");
    std::fs::write(
        config_dir.path().join("config.toml"),
        r#"
        [workspaces.default.sources.local_messages]
        version = "0.1.0"
        variables = {}
        secrets = []
        origin = "imported"
    "#,
    )
    .expect("failed to write config");

    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("source")
        .arg("test")
        .arg("local_messages")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .output()
        .expect("failed to run coral source test");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success(),
        "expected zero exit status: {stderr}"
    );
    assert!(
        stdout.contains("Query tests"),
        "expected query-test summary in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("SELECT COUNT(*) AS n FROM local_messages.messages"),
        "expected passing query text in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("1 declared · 1 passed · 0 failed"),
        "expected passing query-test counts in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("1 row"),
        "expected passing query row count in stdout, got: {stdout}"
    );
    assert!(
        stderr.trim().is_empty(),
        "expected no stderr output, got: {stderr}"
    );
}

