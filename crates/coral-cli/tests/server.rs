#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "exercises one real server process"
)]
use assert_cmd::Command as AssertCommand;
use std::io::{BufRead as _, BufReader};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};
use tempfile::TempDir;
const PROCESS_TIMEOUT: Duration = Duration::from_secs(20);
struct ChildGuard(Child);
impl ChildGuard {
    fn stop(&mut self) -> ExitStatus {
        #[cfg(unix)]
        Command::new("kill")
            .args(["-TERM", &self.0.id().to_string()])
            .status()
            .expect("send SIGTERM to server");
        #[cfg(not(unix))]
        self.0.kill().expect("stop server");
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        while Instant::now() < deadline {
            if let Some(status) = self.0.try_wait().expect("read server status") {
                return status;
            }
            std::thread::sleep(Duration::from_millis(25));
        }
        self.0.kill().expect("kill unresponsive server");
        self.0.wait().expect("wait for killed server")
    }
}
impl Drop for ChildGuard {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            drop(self.0.kill());
            drop(self.0.wait());
        }
    }
}
fn write_config(config_dir: &TempDir, workspaces: &str) {
    let config = format!(
        "[trace_history]\nenabled = false\n\n[server]\nbind_addr = '127.0.0.1:0'\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n\n{workspaces}"
    );
    std::fs::write(config_dir.path().join("config.toml"), config).expect("write server config");
}
#[test]
fn server_passes_the_requested_workspace_to_local_mcp() {
    let config_dir = TempDir::new().expect("config dir");
    write_config(&config_dir, "[workspaces.alpha]\n[workspaces.beta]\n");
    let mut server = ChildGuard(
        Command::new(env!("CARGO_BIN_EXE_coral"))
            .args(["--workspace", "alpha", "server"])
            .env("CORAL_CONFIG_DIR", config_dir.path())
            .env_remove("CORAL_WORKSPACE")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start Coral server process"),
    );
    let stdout = server.0.stdout.take().expect("server stdout");
    let (sender, ready) = mpsc::sync_channel(1);
    std::thread::spawn(move || {
        let found = BufReader::new(stdout)
            .lines()
            .map_while(Result::ok)
            .any(|line| line.contains("Coral MCP HTTP server listening"));
        let _send_result = sender.send(found);
    });
    assert_eq!(ready.recv_timeout(PROCESS_TIMEOUT), Ok(true));
    let status = server.stop();
    #[cfg(unix)]
    assert!(status.success(), "server shutdown failed with {status}");
    #[cfg(not(unix))]
    drop(status);
}
#[test]
fn server_without_a_workspace_does_not_fall_back_to_default() {
    let config_dir = TempDir::new().expect("config dir");
    write_config(&config_dir, "");
    let mut command = AssertCommand::cargo_bin("coral").expect("locate Coral binary");
    let assert = command
        .arg("server")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .env_remove("CORAL_WORKSPACE")
        .timeout(PROCESS_TIMEOUT)
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    let guidance = "no workspace is available; create one with `coral workspace create <name>`";
    assert!(stderr.contains(guidance), "{stderr}");
}
