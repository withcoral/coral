#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration test crate only uses a subset of dev dependencies."
)]

use std::fs;
use std::path::Path;

use assert_cmd::Command;
use tempfile::TempDir;

fn coral_cmd(config_dir: &Path) -> Command {
    let mut cmd = Command::cargo_bin("coral").expect("cargo bin");
    cmd.env("CORAL_CONFIG_DIR", config_dir);
    cmd.env_remove("CORAL_ENDPOINT");
    cmd
}

fn write_config(config_dir: &Path, raw: &str) {
    fs::create_dir_all(config_dir).expect("config dir");
    fs::write(config_dir.join("config.toml"), raw).expect("config file");
}

fn read_config(config_dir: &Path) -> String {
    fs::read_to_string(config_dir.join("config.toml")).expect("config file")
}

#[test]
fn features_help_lists_enable_disable_without_reset() {
    let assert = Command::cargo_bin("coral")
        .expect("cargo bin")
        .args(["features", "--help"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(stdout.contains("list"), "help should list list: {stdout}");
    assert!(
        stdout.contains("enable"),
        "help should list enable: {stdout}"
    );
    assert!(
        stdout.contains("disable"),
        "help should list disable: {stdout}"
    );
    assert!(
        !stdout.contains("reset"),
        "help should not list removed reset command: {stdout}"
    );
}

#[test]
fn features_without_subcommand_requires_explicit_action() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("missing-config");

    coral_cmd(&config_dir).arg("features").assert().failure();

    assert!(
        !config_dir.exists(),
        "argument validation should not create state"
    );
}

#[test]
fn features_list_shows_feedback_status_without_state_creation() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("missing-config");

    let assert = coral_cmd(&config_dir)
        .args(["features", "list"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(stdout.contains("Feature"), "missing table header: {stdout}");
    assert!(
        stdout.contains("Configured"),
        "missing table header: {stdout}"
    );
    assert!(stdout.contains("Enabled"), "missing table header: {stdout}");
    assert!(
        stdout.contains("feedback"),
        "missing feedback row: {stdout}"
    );
    assert!(
        stdout.contains("default"),
        "missing default status: {stdout}"
    );
    assert!(
        stdout.contains("false"),
        "missing disabled effective state: {stdout}"
    );
    assert!(
        stdout.contains("Exposes the MCP feedback tool when enabled. Feedback reports are stored locally and anonymous copies may be uploaded to Coral."),
        "feature list should match documented feedback text: {stdout}"
    );
    assert!(
        !config_dir.exists(),
        "read-only feature listing should not create state"
    );
}

#[test]
fn features_list_applies_global_process_override_without_state_creation() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("missing-config");

    let assert = coral_cmd(&config_dir)
        .args(["--enable-feedback", "features", "list"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("feedback"),
        "missing feedback row: {stdout}"
    );
    assert!(
        stdout.contains("default"),
        "config status should remain default: {stdout}"
    );
    assert!(
        stdout.contains("true"),
        "process override should enable feature in effective state: {stdout}"
    );
    assert!(
        !config_dir.exists(),
        "read-only feature listing should not create state"
    );
}

#[test]
fn features_enable_creates_config_with_feedback_enabled() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");

    let assert = coral_cmd(&config_dir)
        .args(["features", "enable", "feedback"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout, "Enabled feature `feedback` in config.toml.\n");

    let raw = read_config(&config_dir);
    assert!(raw.contains("version = 1"), "missing config version: {raw}");
    assert!(raw.contains("[features]"), "missing features table: {raw}");
    assert!(
        raw.contains("feedback = true"),
        "missing feedback opt-in: {raw}"
    );
}

#[test]
fn features_disable_after_enable_persists_false_override() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");

    coral_cmd(&config_dir)
        .args(["features", "enable", "feedback"])
        .assert()
        .success();
    let assert = coral_cmd(&config_dir)
        .args(["features", "disable", "feedback"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert_eq!(stdout, "Disabled feature `feedback` in config.toml.\n");

    let raw = read_config(&config_dir);
    assert!(
        raw.contains("feedback = false"),
        "disable should persist explicit feedback opt-out: {raw}"
    );

    let assert = coral_cmd(&config_dir)
        .args(["features", "list"])
        .assert()
        .success();
    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("feedback"),
        "missing feedback row: {stdout}"
    );
    assert!(
        stdout.contains("disabled"),
        "missing disabled configured status: {stdout}"
    );
    assert!(stdout.contains("false"), "missing disabled state: {stdout}");
}

#[test]
fn features_disable_missing_config_creates_false_override() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("missing-config");

    coral_cmd(&config_dir)
        .args(["features", "disable", "feedback"])
        .assert()
        .success();

    let raw = read_config(&config_dir);
    assert!(raw.contains("version = 1"), "missing config version: {raw}");
    assert!(
        raw.contains("feedback = false"),
        "disable should persist explicit feedback opt-out: {raw}"
    );
}

#[test]
fn features_unknown_key_fails_without_writing_config() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");

    let assert = coral_cmd(&config_dir)
        .args(["features", "enable", "unknown"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("unknown feature 'unknown'"),
        "missing unknown feature error: {stderr}"
    );
    assert!(
        stderr.contains("feedback"),
        "error should mention valid feature keys: {stderr}"
    );
    assert!(
        !config_dir.exists(),
        "unknown feature must not create state"
    );
}

#[test]
fn feature_mutations_preserve_unknown_keys_and_invalid_values() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    write_config(
        &config_dir,
        r#"
[features]
future_flag = "yes"
feedback = true
"#,
    );

    coral_cmd(&config_dir)
        .args(["features", "disable", "feedback"])
        .assert()
        .success();

    let raw = read_config(&config_dir);
    assert!(
        raw.contains("future_flag = \"yes\""),
        "lost unknown key: {raw}"
    );
    assert!(
        raw.contains("feedback = false"),
        "feedback override should be persisted as disabled: {raw}"
    );
}

#[test]
fn features_list_reports_invalid_known_value_as_default_effective_state() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    write_config(
        &config_dir,
        r#"
[features]
feedback = "yes"
"#,
    );

    let assert = coral_cmd(&config_dir)
        .args(["features", "list"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("invalid-value"),
        "missing invalid value status: {stdout}"
    );
    assert!(
        stdout.contains("false"),
        "invalid value should fall back to default disabled state: {stdout}"
    );
}

#[test]
fn features_list_reports_unsupported_container_as_default_effective_state() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    write_config(&config_dir, "features = { feedback = true }\n");

    let assert = coral_cmd(&config_dir)
        .args(["features", "list"])
        .assert()
        .success();

    let stdout = String::from_utf8_lossy(&assert.get_output().stdout);
    assert!(
        stdout.contains("invalid-container"),
        "missing invalid container status: {stdout}"
    );
    assert!(
        stdout.contains("false"),
        "invalid container should fall back to default disabled state: {stdout}"
    );
}

#[test]
fn feature_mutations_reject_unsupported_container_without_rewriting_config() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let original = "features = { feedback = true }\n";
    write_config(&config_dir, original);

    let assert = coral_cmd(&config_dir)
        .args(["features", "enable", "feedback"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("unsupported [features] config"),
        "missing unsupported container error: {stderr}"
    );
    assert_eq!(read_config(&config_dir), original);
}

#[test]
fn feature_mutations_reject_invalid_toml_without_rewriting_config() {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join("coral-config");
    let original = "[features\nfeedback = true\n";
    write_config(&config_dir, original);

    let assert = coral_cmd(&config_dir)
        .args(["features", "enable", "feedback"])
        .assert()
        .failure();

    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr.contains("TOML parse error"),
        "missing invalid TOML error: {stderr}"
    );
    assert_eq!(read_config(&config_dir), original);
}
