#![allow(
    missing_docs,
    unused_crate_dependencies,
    reason = "Integration tests need only process-control and temporary-state test dependencies."
)]

use std::io::{BufRead as _, BufReader};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use assert_cmd::Command as AssertCommand;
use tempfile::TempDir;

const PROCESS_TIMEOUT: Duration = Duration::from_secs(20);

struct ServerProcess {
    child: Child,
    stdout_lines: Receiver<String>,
    stdout_reader: Option<JoinHandle<()>>,
    stopped: bool,
}

impl ServerProcess {
    fn spawn(config_dir: &TempDir, workspace: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_coral"))
            .args(["--workspace", workspace, "server"])
            .env("CORAL_CONFIG_DIR", config_dir.path())
            .env_remove("CORAL_WORKSPACE")
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start Coral server process");
        let stdout = child.stdout.take().expect("server stdout");
        let (sender, stdout_lines) = mpsc::channel();
        let stdout_reader = std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines().map_while(Result::ok) {
                if sender.send(line).is_err() {
                    break;
                }
            }
        });
        Self {
            child,
            stdout_lines,
            stdout_reader: Some(stdout_reader),
            stopped: false,
        }
    }

    fn wait_for_stdout(&mut self, expected: &str) {
        let deadline = Instant::now() + PROCESS_TIMEOUT;
        loop {
            match self.stdout_lines.recv_timeout(Duration::from_millis(100)) {
                Ok(line) if line.contains(expected) => return,
                Ok(_) | Err(mpsc::RecvTimeoutError::Timeout) => {}
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    panic!("server stdout closed before '{expected}'")
                }
            }
            if let Some(status) = self.child.try_wait().expect("read server status") {
                panic!("server exited with {status} before '{expected}'");
            }
            assert!(
                Instant::now() < deadline,
                "server did not print '{expected}'"
            );
        }
    }

    fn stop(&mut self) {
        if self.stopped {
            return;
        }
        #[cfg(unix)]
        Command::new("kill")
            .args(["-TERM", &self.child.id().to_string()])
            .status()
            .expect("send SIGTERM to server");
        #[cfg(not(unix))]
        self.child.kill().expect("stop server");

        let deadline = Instant::now() + PROCESS_TIMEOUT;
        let status = loop {
            if let Some(status) = self.child.try_wait().expect("read server status") {
                break status;
            }
            if Instant::now() >= deadline {
                self.child.kill().expect("kill unresponsive server");
                break self.child.wait().expect("wait for killed server");
            }
            std::thread::sleep(Duration::from_millis(25));
        };
        self.stopped = true;
        if let Some(reader) = self.stdout_reader.take() {
            reader.join().expect("join stdout reader");
        }
        #[cfg(unix)]
        assert!(status.success(), "server shutdown failed with {status}");
        #[cfg(not(unix))]
        drop(status);
    }
}

impl Drop for ServerProcess {
    fn drop(&mut self) {
        if !self.stopped {
            drop(self.child.kill());
            drop(self.child.wait());
        }
    }
}

fn write_config(config_dir: &TempDir, workspaces: &str) {
    std::fs::write(
        config_dir.path().join("config.toml"),
        format!(
            "[trace_history]\nenabled = false\n\n[server]\nbind_addr = '127.0.0.1:0'\n\n[server.mcp_http]\nenabled = true\nbind = '127.0.0.1:0'\n\n{workspaces}"
        ),
    )
    .expect("write server config");
}

#[test]
fn server_passes_the_requested_workspace_to_local_mcp() {
    let config_dir = TempDir::new().expect("config dir");
    write_config(&config_dir, "[workspaces.alpha]\n[workspaces.beta]\n");

    let mut server = ServerProcess::spawn(&config_dir, "alpha");
    server.wait_for_stdout("Coral MCP HTTP server listening");
    server.stop();
}

#[test]
fn server_without_a_workspace_does_not_fall_back_to_default() {
    let config_dir = TempDir::new().expect("config dir");
    write_config(&config_dir, "");

    let assert = AssertCommand::cargo_bin("coral")
        .expect("locate Coral binary")
        .arg("server")
        .env("CORAL_CONFIG_DIR", config_dir.path())
        .env_remove("CORAL_WORKSPACE")
        .timeout(PROCESS_TIMEOUT)
        .assert()
        .failure();
    let stderr = String::from_utf8_lossy(&assert.get_output().stderr);
    assert!(
        stderr
            .contains("no workspace is available; create one with `coral workspace create <name>`"),
        "expected actionable workspace guidance, got: {stderr}"
    );
}
