#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test crates only use a small subset of the package dependencies."
)]

use std::path::PathBuf;
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

fn temp_config_dir() -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!("coral-cli-test-{}-{nanos}", std::process::id()))
}

#[test]
fn onboard_rejects_non_interactive_terminals() {
    let config_dir = temp_config_dir();
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

    let _ = std::fs::remove_dir_all(config_dir);
}
