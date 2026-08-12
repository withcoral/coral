#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

use std::process::{Command, Stdio};

#[test]
fn onboard_rejects_non_interactive_terminals() {
    let temp = tempfile::tempdir().expect("temp dir");
    let config_dir = temp.path().join("state-must-not-exist");
    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("onboard")
        .env("CORAL_CONFIG_DIR", &config_dir)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run coral onboard");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert!(
        stderr.contains("interactive source install requires a TTY"),
        "expected TTY error in stderr, got: {stderr}"
    );
    assert!(
        !config_dir.exists(),
        "TTY validation must run before bootstrap creates local state"
    );
}
