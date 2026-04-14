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

    // Write a basic config that references the linear source, but don't set the required secret.
    let config = r#"
        [workspaces.default.sources.linear]
        version = "2.2.0"
        variables = {}
        secrets = []
        origin = "bundled"
    "#;
    std::fs::write(config_dir.path().join("config.toml"), config).expect("failed to write config");

    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("source")
        .arg("test")
        .arg("linear")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .output()
        .expect("failed to run coral source test");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert!(
        stderr.contains("source 'linear' is missing secret 'LINEAR_API_KEY'"),
        "expected missing secret error in stderr, got: {stderr}"
    );
}
