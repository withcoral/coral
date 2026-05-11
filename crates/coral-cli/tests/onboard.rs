#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

use std::process::{Command, Stdio};

#[test]
fn onboard_rejects_non_interactive_terminals() {
    let config_dir = tempfile::tempdir().expect("config dir");
    let output = Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("onboard")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .expect("failed to run coral onboard");

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "expected non-zero exit status");
    assert!(
        stderr.contains("Error: Interactive input is not available"),
        "expected TTY error in stderr, got: {stderr}"
    );
    assert!(
        stderr.contains("Detail: `--interactive` needs a terminal"),
        "expected TTY detail in stderr, got: {stderr}"
    );
    assert!(
        stderr.contains("Hint: Re-run the command in a terminal"),
        "expected TTY hint in stderr, got: {stderr}"
    );
}
