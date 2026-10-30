#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test: only the runtime process and its filesystem effects are under test."
)]

use std::io::{BufRead as _, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Child, Stdio};
use std::sync::mpsc;
use std::time::{Duration, Instant};

use assert_cmd::Command;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);
/// `EX_TEMPFAIL`: the exit status launchers retry. Desktop matches this value.
const STATE_LOCKED_EXIT_CODE: i32 = 75;

/// A runtime process started on loopback listeners, killed if a test fails.
struct Runtime {
    child: Child,
    mcp_origin: String,
}

impl Runtime {
    /// Starts a runtime on `state_root` with stdin piped and waits until its
    /// MCP listener reports ready.
    fn start(state_root: &Path, stop_on_stdin_close: bool) -> Self {
        Self::start_with(state_root, stop_on_stdin_close, |_command| {})
    }

    /// [`Self::start`] with `configure` applied to the command first.
    fn start_with(
        state_root: &Path,
        stop_on_stdin_close: bool,
        configure: impl FnOnce(&mut std::process::Command),
    ) -> Self {
        let mut command = runtime_command(state_root);
        if stop_on_stdin_close {
            command.env("CORAL_STOP_ON_STDIN_CLOSE", "1");
        }
        configure(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn runtime");
        let stdout = child.stdout.take().expect("runtime stdout");
        let (lines_tx, lines_rx) = mpsc::channel();
        // Keep draining stdout so the runtime never blocks on a full pipe.
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                drop(lines_tx.send(line));
            }
        });
        let mut runtime = Self {
            child,
            mcp_origin: String::new(),
        };
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        runtime.mcp_origin = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = lines_rx
                .recv_timeout(remaining)
                .expect("runtime reports its MCP listener");
            if let Some(rest) =
                line.strip_prefix("Coral MCP HTTP server serving each Workspace at ")
                && let Some((origin, _route)) = rest.split_once("/workspaces")
            {
                break origin.to_owned();
            }
        };
        // The listener serves before the readiness probe first passes.
        while !runtime.ready() {
            assert!(
                Instant::now() < deadline,
                "runtime MCP listener is not ready"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        runtime
    }

    fn ready(&self) -> bool {
        reqwest::blocking::get(format!("{}/readyz", self.mcp_origin))
            .is_ok_and(|response| response.status() == reqwest::StatusCode::OK)
    }

    fn close_stdin(&mut self) {
        drop(self.child.stdin.take().expect("runtime stdin"));
    }

    /// Returns the exit status once the runtime exits, or `None` if it is
    /// still running after `timeout`.
    fn exit_within(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll runtime") {
                return Some(status);
            }
            if Instant::now() >= deadline {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Runtime {
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
    }
}

/// A runtime command over `state_root` with launch settings cleared, so the
/// test's own environment cannot change the launch.
fn runtime_command(state_root: &Path) -> std::process::Command {
    let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_coral"));
    command
        .env("CORAL_CONFIG_DIR", state_root)
        .env("CORAL_LOCAL_WEB_URL", "http://127.0.0.1:5173")
        .env_remove("CORAL_DESKTOP")
        .env_remove("CORAL_STOP_ON_STDIN_CLOSE");
    command
}

/// Writes a local-mode state root whose listeners all take ephemeral
/// loopback ports, so runtimes never contend for a fixed port.
fn local_state_root(temp: &tempfile::TempDir) -> PathBuf {
    let state_root = temp.path().join("coral-state");
    std::fs::create_dir(&state_root).expect("create state root");
    std::fs::write(
        state_root.join("config.toml"),
        "[listeners.grpc]
bind = '127.0.0.1:0'
         [listeners.mcp]
bind = '127.0.0.1:0'
         [listeners.auth]
bind = '127.0.0.1:0'
",
    )
    .expect("write config");
    state_root
}

#[test]
fn process_reports_its_version_without_starting_the_runtime() {
    Command::new(env!("CARGO_BIN_EXE_coral"))
        .arg("--version")
        .assert()
        .success()
        .stdout(format!(
            "coral {}+{}\n",
            env!("CARGO_PKG_VERSION"),
            env!("CORAL_GIT_SHA")
        ));
}

#[test]
fn runtime_process_rejects_pre_v1_state_without_mutation() {
    let temp = tempfile::tempdir().expect("temp dir");
    let state_root = temp.path().join("coral-state");
    std::fs::create_dir_all(&state_root).expect("create retired state root");
    let legacy_database = state_root.join("coral.db");
    let legacy_bytes = b"retired Coral database bytes\n";
    std::fs::write(&legacy_database, legacy_bytes).expect("write retired database");
    std::fs::write(
        state_root.join("config.toml"),
        "version = 1\n\n[workspaces.legacy\n",
    )
    .expect("write retired config");

    let assert = Command::new(env!("CARGO_BIN_EXE_coral"))
        .env("CORAL_CONFIG_DIR", &state_root)
        .timeout(Duration::from_secs(10))
        .assert()
        // Only a state root held by another runtime is temporary.
        .code(1);
    let output = assert.get_output();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("pre-v1"), "unexpected stderr: {stderr}");
    assert!(
        stderr.contains("move or remove"),
        "operator recovery is missing: {stderr}"
    );
    assert_eq!(
        std::fs::read(&legacy_database).expect("read rejected database"),
        legacy_bytes
    );
    let mut entries = std::fs::read_dir(&state_root)
        .expect("read rejected state root")
        .map(|entry| entry.expect("read state entry").file_name())
        .collect::<Vec<_>>();
    entries.sort();
    assert_eq!(
        entries,
        [
            std::ffi::OsString::from("config.toml"),
            std::ffi::OsString::from("coral.db"),
        ]
    );
}

/// A cloud deployment hands the credential key to the process as a secret.
/// The runtime serves with it and writes no key file, and a start without the
/// secret names the variable it needs.
#[test]
fn runtime_takes_its_credential_key_from_the_configured_variable() {
    let temp = tempfile::tempdir().expect("temp dir");
    let state_root = local_state_root(&temp);
    let config = state_root.join("config.toml");
    let mut raw = std::fs::read_to_string(&config).expect("read config");
    raw.push_str("[credentials]\nkey_env = 'CORAL_CREDENTIALS_KEY'\n");
    std::fs::write(&config, raw).expect("write config");
    // The standard base64 encoding of 32 bytes, as `openssl rand -base64 32` prints it.
    let key = "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8=";

    let mut runtime = Runtime::start_with(&state_root, true, |command| {
        command.env("CORAL_CREDENTIALS_KEY", key);
    });
    assert!(
        !state_root.join("credentials.key").exists(),
        "a secret-held key never reaches the state root"
    );
    runtime.close_stdin();
    let status = runtime
        .exit_within(EXIT_TIMEOUT)
        .expect("runtime stops after stdin closes");
    assert_eq!(status.code(), Some(0), "{status}");

    let mut command = runtime_command(&state_root);
    command.env_remove("CORAL_CREDENTIALS_KEY");
    let output = Command::from_std(command)
        .timeout(EXIT_TIMEOUT)
        .output()
        .expect("run runtime without its secret");
    assert_eq!(output.status.code(), Some(1), "{:?}", output.status);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("CORAL_CREDENTIALS_KEY"),
        "the missing secret is named: {stderr}"
    );
}

/// Closing stdin is a launcher's graceful stop on every platform, but only a
/// launcher that opts in gets it: a container without an attached stdin
/// starts with stdin already closed.
#[test]
fn runtime_stops_gracefully_when_stdin_closes_only_when_its_launcher_opts_in() {
    for stop_on_stdin_close in [true, false] {
        let temp = tempfile::tempdir().expect("temp dir");
        let mut runtime = Runtime::start(&local_state_root(&temp), stop_on_stdin_close);

        runtime.close_stdin();

        if stop_on_stdin_close {
            let status = runtime
                .exit_within(EXIT_TIMEOUT)
                .expect("runtime stops after stdin closes");
            assert_eq!(status.code(), Some(0), "{status}");
        } else {
            assert_eq!(runtime.exit_within(Duration::from_secs(2)), None);
            assert!(runtime.ready(), "runtime keeps serving after stdin closes");
        }
    }
}

/// Telemetry is best effort. With a collector that refuses connections, and
/// with one that accepts them and never answers, a runtime comes ready, stays
/// ready while traced requests fail to export, and stops cleanly having lost
/// what it could not flush.
#[test]
fn an_unreachable_collector_changes_neither_readiness_nor_a_clean_stop() {
    // Connections queue in the backlog of a listener that never accepts them.
    let stalled = std::net::TcpListener::bind("127.0.0.1:0").expect("bind stalled collector");
    let refused = std::net::TcpListener::bind("127.0.0.1:0")
        .expect("bind a free port")
        .local_addr()
        .expect("free port");
    for collector in [
        refused,
        stalled.local_addr().expect("stalled collector address"),
    ] {
        let temp = tempfile::tempdir().expect("temp dir");
        let state_root = local_state_root(&temp);
        let config = state_root.join("config.toml");
        let raw = std::fs::read_to_string(&config).expect("read config");
        std::fs::write(
            &config,
            format!("{raw}[otel]\nendpoint = 'http://{collector}'\n"),
        )
        .expect("write config");
        let mut runtime = Runtime::start(&state_root, true);

        let client = reqwest::blocking::Client::new();
        let endpoint = format!("{}/workspaces/absent/mcp", runtime.mcp_origin);
        for _ in 0..32 {
            let traced = client.post(&endpoint).send().expect("traced request");
            assert!(traced.status().is_client_error(), "{collector}");
        }
        assert!(
            runtime.ready(),
            "{collector}: export never decides readiness"
        );

        runtime.close_stdin();
        let status = runtime
            .exit_within(EXIT_TIMEOUT)
            .expect("the runtime stops without its collector");
        assert_eq!(status.code(), Some(0), "{collector}: {status}");
    }
}

/// A relaunch while an earlier runtime still runs must not open the same
/// stores. It exits with `EX_TEMPFAIL` so a launcher can retry it.
#[test]
fn second_runtime_on_a_held_state_root_exits_temporarily() {
    let temp = tempfile::tempdir().expect("temp dir");
    let state_root = local_state_root(&temp);
    let mut first = Runtime::start(&state_root, true);

    let output = Command::from_std(runtime_command(&state_root))
        .timeout(EXIT_TIMEOUT)
        .output()
        .expect("run second runtime");

    // coral-app owns the refusal and its message; the process owns the code.
    assert_eq!(
        output.status.code(),
        Some(STATE_LOCKED_EXIT_CODE),
        "{:?}",
        output.status
    );
    assert!(
        !output.stderr.is_empty(),
        "the process reports why it exited"
    );
    assert!(first.ready(), "the first runtime keeps serving");

    first.close_stdin();
    let status = first
        .exit_within(EXIT_TIMEOUT)
        .expect("first runtime stops after stdin closes");
    assert_eq!(status.code(), Some(0), "{status}");
}
