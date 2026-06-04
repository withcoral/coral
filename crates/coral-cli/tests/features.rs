#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration test crate only uses a subset of dev dependencies."
)]

mod common;

use std::fs;
use std::path::{Path, PathBuf};

use assert_cmd::Command;
use tempfile::TempDir;

use common::{assert_contains, assert_contains_all, stderr, stdout};

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

fn temp_config_dir(name: &str) -> (TempDir, PathBuf) {
    let temp = TempDir::new().expect("temp dir");
    let config_dir = temp.path().join(name);
    (temp, config_dir)
}

fn success_stdout<const N: usize>(config_dir: &Path, args: [&str; N]) -> String {
    stdout(&coral_cmd(config_dir).args(args).assert().success())
}

fn failure_stderr<const N: usize>(config_dir: &Path, args: [&str; N]) -> String {
    stderr(&coral_cmd(config_dir).args(args).assert().failure())
}

#[test]
fn features_help_lists_enable_disable_without_reset() {
    let assert = Command::cargo_bin("coral")
        .expect("cargo bin")
        .args(["features", "--help"])
        .assert()
        .success();

    let stdout = stdout(&assert);
    assert_contains_all(&stdout, &["list", "enable", "disable"]);
    assert!(
        !stdout.contains("reset"),
        "help should not list removed reset command: {stdout}"
    );
}

#[test]
fn features_without_subcommand_requires_explicit_action() {
    let (_temp, config_dir) = temp_config_dir("missing-config");

    coral_cmd(&config_dir).arg("features").assert().failure();

    assert!(
        !config_dir.exists(),
        "argument validation should not create state"
    );
}

#[test]
fn features_list_shows_feedback_status_without_state_creation() {
    let (_temp, config_dir) = temp_config_dir("missing-config");

    let stdout = success_stdout(&config_dir, ["features", "list"]);
    assert_contains_all(
        &stdout,
        &[
            "Feature",
            "Configured",
            "Enabled",
            "feedback",
            "default",
            "false",
            "Exposes the MCP feedback tool when enabled. Feedback reports are stored locally and anonymous copies may be uploaded to Coral.",
        ],
    );
    assert!(
        !config_dir.exists(),
        "read-only feature listing should not create state"
    );
}

#[test]
fn features_list_applies_global_process_override_without_state_creation() {
    let (_temp, config_dir) = temp_config_dir("missing-config");

    let stdout = success_stdout(&config_dir, ["--enable-feedback", "features", "list"]);
    assert_contains_all(&stdout, &["feedback", "default", "true"]);
    assert!(
        !config_dir.exists(),
        "read-only feature listing should not create state"
    );
}

#[test]
fn features_enable_creates_config_with_feedback_enabled() {
    let (_temp, config_dir) = temp_config_dir("coral-config");

    let stdout = success_stdout(&config_dir, ["features", "enable", "feedback"]);
    assert_eq!(stdout, "Enabled feature `feedback` in config.toml.\n");

    let raw = read_config(&config_dir);
    assert_contains_all(&raw, &["version = 1", "[features]", "feedback = true"]);
}

#[test]
fn features_disable_after_enable_persists_false_override() {
    let (_temp, config_dir) = temp_config_dir("coral-config");

    coral_cmd(&config_dir)
        .args(["features", "enable", "feedback"])
        .assert()
        .success();
    let stdout = success_stdout(&config_dir, ["features", "disable", "feedback"]);
    assert_eq!(stdout, "Disabled feature `feedback` in config.toml.\n");

    let raw = read_config(&config_dir);
    assert_contains(&raw, "feedback = false");

    let stdout = success_stdout(&config_dir, ["features", "list"]);
    assert_contains_all(&stdout, &["feedback", "disabled", "false"]);
}

#[test]
fn features_disable_missing_config_creates_false_override() {
    let (_temp, config_dir) = temp_config_dir("missing-config");

    coral_cmd(&config_dir)
        .args(["features", "disable", "feedback"])
        .assert()
        .success();

    let raw = read_config(&config_dir);
    assert_contains_all(&raw, &["version = 1", "feedback = false"]);
}

#[test]
fn features_unknown_key_fails_without_writing_config() {
    let (_temp, config_dir) = temp_config_dir("coral-config");

    let stderr = failure_stderr(&config_dir, ["features", "enable", "unknown"]);
    assert_contains_all(&stderr, &["unknown feature 'unknown'", "feedback"]);
    assert!(
        !config_dir.exists(),
        "unknown feature must not create state"
    );
}

#[test]
fn feature_mutations_preserve_unknown_keys_and_invalid_values() {
    let (_temp, config_dir) = temp_config_dir("coral-config");
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
    assert_contains_all(&raw, &["future_flag = \"yes\"", "feedback = false"]);
}

#[test]
fn features_list_reports_invalid_config_as_default_effective_state() {
    for (raw_config, expected_label) in [
        (
            r#"
[features]
feedback = "yes"
"#,
            "invalid-value",
        ),
        ("features = { feedback = true }\n", "invalid-container"),
    ] {
        let (_temp, config_dir) = temp_config_dir("coral-config");
        write_config(&config_dir, raw_config);

        let stdout = success_stdout(&config_dir, ["features", "list"]);
        assert_contains_all(&stdout, &[expected_label, "false"]);
    }
}

#[test]
fn feature_mutations_reject_invalid_config_without_rewriting() {
    for (original, expected_error) in [
        (
            "features = { feedback = true }\n",
            "unsupported [features] config",
        ),
        ("[features\nfeedback = true\n", "TOML parse error"),
    ] {
        let (_temp, config_dir) = temp_config_dir("coral-config");
        write_config(&config_dir, original);

        let stderr = failure_stderr(&config_dir, ["features", "enable", "feedback"]);
        assert_contains(&stderr, expected_error);
        assert_eq!(read_config(&config_dir), original);
    }
}
