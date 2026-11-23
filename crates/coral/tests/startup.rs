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

mod composed;

const STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);
/// `EX_TEMPFAIL`: the exit status launchers retry. Desktop matches this value.
const STATE_LOCKED_EXIT_CODE: i32 = 75;

/// A runtime process started on loopback listeners, killed if a test fails.
struct Runtime {
    child: Child,
    grpc_endpoint: String,
    mcp_origin: String,
    /// The Run Executor listener's address, which a Run Executor dials.
    #[cfg_attr(
        not(unix),
        expect(dead_code, reason = "only the unix dialed tests read it")
    )]
    run_executor_address: String,
}

impl Runtime {
    /// Starts a runtime on `state_root` with stdin piped and waits until its
    /// MCP listener reports ready.
    fn start(state_root: &Path, stop_on_stdin_close: bool) -> Self {
        Self::start_with(state_root, stop_on_stdin_close, |_command| {})
    }

    /// [`Self::start`] with `configure` applied to the command first. Stderr
    /// goes nowhere unless `configure` sends it somewhere.
    fn start_with(
        state_root: &Path,
        stop_on_stdin_close: bool,
        configure: impl FnOnce(&mut std::process::Command),
    ) -> Self {
        let mut command = runtime_command(state_root);
        if stop_on_stdin_close {
            command.env("CORAL_STOP_ON_STDIN_CLOSE", "1");
        }
        command.stderr(Stdio::null());
        configure(&mut command);
        let mut child = command
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
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
            grpc_endpoint: String::new(),
            mcp_origin: String::new(),
            run_executor_address: String::new(),
        };
        let deadline = Instant::now() + STARTUP_TIMEOUT;
        runtime.mcp_origin = loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = lines_rx
                .recv_timeout(remaining)
                .expect("runtime reports its MCP listener");
            if let Some(endpoint) = line.strip_prefix("Coral gRPC server listening on ") {
                endpoint.clone_into(&mut runtime.grpc_endpoint);
            }
            if let Some(address) = line.strip_prefix("Coral Run Executor listener on ") {
                runtime.run_executor_address = format!("http://{address}");
            }
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
/// loopback ports, so runtimes never contend for a fixed port, and whose
/// semantic search is off.
fn local_state_root(temp: &tempfile::TempDir) -> PathBuf {
    local_state_root_with(temp, "")
}

/// [`local_state_root`] with the TOML `extra_toml` appended to its config.
fn local_state_root_with(temp: &tempfile::TempDir, extra_toml: &str) -> PathBuf {
    let state_root = temp.path().join("coral-state");
    std::fs::create_dir(&state_root).expect("create state root");
    coral_test_support::write_server_config(
        &state_root,
        &format!(
            "[listeners.grpc]
bind = '127.0.0.1:0'
         [listeners.mcp]
bind = '127.0.0.1:0'
         [listeners.auth]
bind = '127.0.0.1:0'
{extra_toml}"
        ),
    );
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

/// Catches: a stop that lets the Tokio runtime shut down under a search
/// index write. A new Workspace starts index writes, and a write that still
/// runs then panics at its next timer. The panic leaves the exit status at
/// 0, so the test reads stderr. Not every stop lands in a write, so the test
/// stops three times.
#[test]
fn a_runtime_stopped_while_it_indexes_a_new_workspace_does_not_panic() {
    for _ in 0..3 {
        let temp = tempfile::tempdir().expect("temp dir");
        // Outside the state root, which must hold only the config.
        let stderr_path = temp.path().join("stderr.log");
        let stderr = std::fs::File::create(&stderr_path).expect("create the stderr log");
        let mut runtime = Runtime::start_with(&local_state_root(&temp), true, |command| {
            command.stderr(stderr);
        });

        workspace_mcp_url(&runtime);
        runtime.close_stdin();

        let status = runtime
            .exit_within(EXIT_TIMEOUT)
            .expect("runtime stops after stdin closes");
        assert_eq!(status.code(), Some(0), "{status}");
        let stderr = std::fs::read_to_string(&stderr_path).expect("read the stderr log");
        assert!(!stderr.contains("panicked"), "{stderr}");
    }
}

/// A collector that reports each connection the runtime opens to it, and
/// either closes it at once, which fails the export, or never answers it.
fn collector(answers_nothing: bool) -> (std::net::SocketAddr, mpsc::Receiver<()>) {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind collector");
    let address = listener.local_addr().expect("collector address");
    let (connected, connections) = mpsc::channel();
    std::thread::spawn(move || {
        let mut unanswered = Vec::new();
        for stream in listener.incoming().flatten() {
            let _sent = connected.send(());
            if answers_nothing {
                unanswered.push(stream);
            }
        }
    });
    (address, connections)
}

/// Telemetry is best effort. With a collector that fails each export, and with
/// one that never answers, a runtime comes ready, stays ready once it has
/// tried to export, and stops cleanly having lost what it could not flush.
#[test]
fn a_failing_collector_changes_neither_readiness_nor_a_clean_stop() {
    for answers_nothing in [false, true] {
        let (address, connections) = collector(answers_nothing);
        let temp = tempfile::tempdir().expect("temp dir");
        let state_root = local_state_root(&temp);
        let config = state_root.join("config.toml");
        let raw = std::fs::read_to_string(&config).expect("read config");
        std::fs::write(
            &config,
            format!("{raw}[otel]\nendpoint = 'http://{address}'\n"),
        )
        .expect("write config");
        let mut runtime = Runtime::start(&state_root, true);

        // Requests give the exporters spans and metrics to send.
        let client = reqwest::blocking::Client::new();
        let endpoint = format!("{}/workspaces/absent/mcp", runtime.mcp_origin);
        for _ in 0..8 {
            let traced = client.post(&endpoint).send().expect("traced request");
            assert!(traced.status().is_client_error(), "{answers_nothing}");
        }
        connections
            .recv_timeout(EXIT_TIMEOUT)
            .expect("the runtime tries to export");
        assert!(runtime.ready(), "{answers_nothing}: export decides nothing");

        runtime.close_stdin();
        let status = runtime
            .exit_within(EXIT_TIMEOUT)
            .expect("the runtime stops without its collector");
        assert_eq!(status.code(), Some(0), "{answers_nothing}: {status}");
    }
}

/// A relaunch while an earlier runtime still runs must not open the same
/// stores. It exits with `EX_TEMPFAIL` so a launcher can retry it.
#[test]
fn second_runtime_on_a_held_state_root_exits_temporarily() {
    let temp = tempfile::tempdir().expect("temp dir");
    let state_root = local_state_root(&temp);
    let mut first = Runtime::start(&state_root, true);

    // A launcher that stops the runtime by closing its stdin reads this code
    // too: the second runtime's stdin is closed from the start, and it must
    // still report the held state root, not stop as if asked to.
    for stop_on_stdin_close in [false, true] {
        let mut second = runtime_command(&state_root);
        if stop_on_stdin_close {
            second.env("CORAL_STOP_ON_STDIN_CLOSE", "1");
        }
        let output = Command::from_std(second)
            .timeout(EXIT_TIMEOUT)
            .output()
            .expect("run second runtime");

        // coral-app owns the refusal and its message; the process owns the code.
        assert_eq!(
            output.status.code(),
            Some(STATE_LOCKED_EXIT_CODE),
            "stop_on_stdin_close={stop_on_stdin_close}: {:?}",
            output.status
        );
        assert!(
            !output.stderr.is_empty(),
            "stop_on_stdin_close={stop_on_stdin_close}: the process reports why it exited"
        );
    }
    assert!(first.ready(), "the first runtime keeps serving");

    first.close_stdin();
    let status = first
        .exit_within(EXIT_TIMEOUT)
        .expect("first runtime stops after stdin closes");
    assert_eq!(status.code(), Some(0), "{status}");
}

/// Runs `future` to its end on a Tokio runtime of its own.
fn block_on<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a Tokio runtime")
        .block_on(future)
}

/// Calls the MCP `exec` tool at `url` with `arguments`, a JSON object, and
/// returns the answer's body.
#[cfg(unix)]
fn exec_over_mcp(url: String, arguments: &str) -> String {
    call_over_mcp(url, "exec", arguments)
}

/// Calls the MCP tool `tool` at `url` with `arguments`, a JSON object, and
/// returns the answer's body.
#[cfg(unix)]
fn call_over_mcp(url: String, tool: &str, arguments: &str) -> String {
    let call = format!(
        r#"{{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{{"name":"{tool}","arguments":{arguments}}}}}"#
    );
    let response = reqwest::blocking::Client::builder()
        .timeout(STARTUP_TIMEOUT)
        .build()
        .expect("an HTTP client")
        .post(url)
        .header("Accept", "application/json, text/event-stream")
        .header("Content-Type", "application/json")
        .body(call)
        .send()
        .expect("call exec");
    assert_eq!(response.status(), reqwest::StatusCode::OK);
    response.text().expect("the exec answer")
}

/// A Workspace created over the runtime's gRPC listener: its MCP URL.
fn workspace_mcp_url(runtime: &Runtime) -> String {
    use coral_grpc::coral::management::v1::CreateWorkspaceRequest;
    use coral_grpc::coral::management::v1::workspace_service_client::WorkspaceServiceClient;

    let workspace = block_on(async {
        let mut client = WorkspaceServiceClient::connect(runtime.grpc_endpoint.clone())
            .await
            .expect("connect gRPC");
        let mut request = tonic::Request::new(CreateWorkspaceRequest {
            display_name: "Children".into(),
            request_id: uuid::Uuid::new_v4().to_string(),
        });
        request
            .metadata_mut()
            .insert("coral-audience", "reef".parse().expect("metadata"));
        client
            .create_workspace(request)
            .await
            .expect("create a Workspace")
            .into_inner()
    });
    let id = workspace
        .name
        .strip_prefix("workspaces/")
        .expect("a Workspace name");
    format!("{}/workspaces/{id}/mcp", runtime.mcp_origin)
}

/// The ids of `parent`'s child processes that run `coral run-executor`.
#[cfg(unix)]
fn run_executor_children(parent: u32) -> Vec<String> {
    let found = std::process::Command::new("pgrep")
        .args(["-P", &parent.to_string(), "-f", "run-executor"])
        .output()
        .expect("run pgrep");
    String::from_utf8_lossy(&found.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Kills `runtime`, which runs one local child, with SIGKILL, and waits for
/// that child to exit.
#[cfg(unix)]
fn kill_and_outlive_no_child(runtime: &mut Runtime) {
    let children = run_executor_children(runtime.child.id());
    let [child] = children.as_slice() else {
        panic!("one local child: {children:?}");
    };
    runtime.child.kill().expect("kill the runtime");
    runtime.child.wait().expect("reap the runtime");
    let deadline = Instant::now() + EXIT_TIMEOUT;
    while runs(child) {
        assert!(Instant::now() < deadline, "the child outlives the runtime");
        std::thread::sleep(Duration::from_millis(50));
    }
}

/// Whether process `pid` still runs; a zombie has exited.
#[cfg(unix)]
fn runs(pid: &str) -> bool {
    let status = std::process::Command::new("ps")
        .args(["-o", "stat=", "-p", pid])
        .output()
        .expect("run ps");
    let state = String::from_utf8_lossy(&status.stdout);
    status.status.success() && !state.trim().is_empty() && !state.trim().starts_with('Z')
}

/// A Program run over MCP compiles on a local Run Executor that the runtime
/// starts as its child with no Run Executor configuration, and a runtime
/// killed with SIGKILL leaves no child behind.
#[cfg(unix)]
#[test]
fn a_program_compiles_on_a_local_child_that_exits_when_the_runtime_is_killed() {
    let temp = tempfile::tempdir().expect("temp dir");
    let mut runtime = Runtime::start(&local_state_root(&temp), false);
    let url = workspace_mcp_url(&runtime);
    let body = exec_over_mcp(
        url,
        r#"{"goal":"Compile on a child","intent":"Compile on a child","program":"return 40 + 2;"}"#,
    );
    assert!(
        body.contains(r#""final_value":{"kind":"value","value":42}"#),
        "{body}"
    );

    kill_and_outlive_no_child(&mut runtime);
}

/// Catches: a boot that leaves running the Runs a killed runtime's local
/// children held, or a child that outlives its runtime while it holds a Run.
#[cfg(unix)]
#[test]
fn a_killed_runtimes_run_ends_interrupted_when_the_runtime_starts_again() {
    let temp = tempfile::tempdir().expect("temp dir");
    let state_root = local_state_root(&temp);
    let mut runtime = Runtime::start(&state_root, false);
    let url = workspace_mcp_url(&runtime);
    let body = exec_over_mcp(
        url,
        r#"{"goal":"Spin until stopped","intent":"Spin until stopped","program":"while (true) {}","wait_ms":200}"#,
    );
    let run_id = run_id_in(&body);
    block_on(composed::started_on_a_run_executor(&state_root, &run_id));
    kill_and_outlive_no_child(&mut runtime);
    let state = || block_on(composed::recorded_run_state(&state_root, &run_id));
    assert_eq!(state(), "running");
    // The boot pass runs before the listeners serve.
    let _again = Runtime::start(&state_root, false);
    assert_eq!(state(), "interrupted");
}

/// The id of the Run that an MCP `exec` answer `body` names.
#[cfg(unix)]
fn run_id_in(body: &str) -> String {
    body.split_once(r#""run_id":""#)
        .and_then(|(_, rest)| rest.split_once('"'))
        .map(|(id, _)| id.to_owned())
        .expect("the Run's id")
}

/// The variable that holds the operator token in the dialed tests.
#[cfg(unix)]
const OPERATOR_TOKEN_ENV: &str = "CORAL_TEST_RUN_EXECUTOR_TOKEN";
#[cfg(unix)]
const OPERATOR_TOKEN: &str = "operator-token-for-the-dialed-run-executor-tests";

/// A runtime with its local Run Executors off, admitting the operator token,
/// on a state root in `temp`.
#[cfg(unix)]
fn runtime_with_children_off(temp: &tempfile::TempDir) -> (Runtime, PathBuf) {
    let state_root = local_state_root_with(
        temp,
        &format!(
            "[listeners.run_executor]
bind = '127.0.0.1:0'
token_env = '{OPERATOR_TOKEN_ENV}'
[runtime]
local_run_executors = false
"
        ),
    );
    let runtime = Runtime::start_with(&state_root, false, |command| {
        command.env(OPERATOR_TOKEN_ENV, OPERATOR_TOKEN);
    });
    (runtime, state_root)
}

/// A `coral run-executor` that `runtime` did not start, dialing it with the
/// operator token and holding at most `max_runs_held` Runs.
#[cfg(unix)]
struct Dialed(Child);

#[cfg(unix)]
impl Dialed {
    fn dial(runtime: &Runtime, max_runs_held: u32) -> Self {
        let mut command = std::process::Command::new(env!("CARGO_BIN_EXE_coral"));
        command
            .env_clear()
            .env("CORAL_RUN_EXECUTOR_TOKEN", OPERATOR_TOKEN)
            .args(["run-executor", "--coral-address"])
            .arg(&runtime.run_executor_address)
            .args(["--max-runs-held", &max_runs_held.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null());
        Self(command.spawn().expect("spawn the Run Executor"))
    }
}

#[cfg(unix)]
impl Drop for Dialed {
    fn drop(&mut self) {
        drop(self.0.kill());
        drop(self.0.wait());
    }
}

/// Asserts that `runtime` has started no local Run Executor.
#[cfg(unix)]
fn assert_no_child(runtime: &Runtime) {
    let children = run_executor_children(runtime.child.id());
    assert!(children.is_empty(), "local children started: {children:?}");
}

/// Catches: a runtime with its children off that cannot run a Program on a
/// Run Executor it did not start, or that starts a child anyway.
#[cfg(unix)]
#[test]
fn a_dialed_run_executor_compiles_and_runs_with_local_children_off() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (runtime, state_root) = runtime_with_children_off(&temp);
    let _dialed = Dialed::dial(&runtime, 128);
    let body = exec_over_mcp(
        workspace_mcp_url(&runtime),
        r#"{"goal":"Run on a dialed one","intent":"Run on a dialed one","program":"return 40 + 2;"}"#,
    );
    assert!(
        body.contains(r#""final_value":{"kind":"value","value":42}"#),
        "{body}"
    );
    let kind = block_on(composed::recorded_run_executor_kind(
        &state_root,
        &run_id_in(&body),
    ));
    assert_eq!(kind.as_deref(), Some("dialed"));
    assert_no_child(&runtime);
}

/// Catches: a full fleet that starts a local child with children off, or
/// work that a full Run Executor takes, or that never runs once room frees.
/// A compile holds a unit of room too, so the new Program waits for room
/// before it is a Run.
#[cfg(unix)]
#[test]
fn a_full_fleet_leaves_work_waiting_and_starts_no_child() {
    let temp = tempfile::tempdir().expect("temp dir");
    let (runtime, state_root) = runtime_with_children_off(&temp);
    let _dialed = Dialed::dial(&runtime, 1);
    let url = workspace_mcp_url(&runtime);
    let spinning = run_id_in(&exec_over_mcp(
        url.clone(),
        r#"{"goal":"Hold the room","intent":"Hold the room","program":"while (true) {}","wait_ms":200}"#,
    ));
    block_on(composed::started_on_a_run_executor(&state_root, &spinning));
    let waiting = std::thread::spawn({
        let url = url.clone();
        move || {
            exec_over_mcp(
                url,
                r#"{"goal":"Wait for room","intent":"Wait for room","program":"return 1;"}"#,
            )
        }
    });
    // Several take periods and the children's request backstop pass.
    std::thread::sleep(Duration::from_secs(3));
    assert!(!waiting.is_finished(), "work ran on a full fleet");
    assert_no_child(&runtime);

    let cancelled = call_over_mcp(url, "cancel", &format!(r#"{{"run_id":"{spinning}"}}"#));
    assert!(!cancelled.contains(r#""isError":true"#), "{cancelled}");
    let body = waiting.join().expect("the waiting exec");
    assert!(
        body.contains(r#""final_value":{"kind":"value","value":1}"#),
        "{body}"
    );
    assert_no_child(&runtime);
}
