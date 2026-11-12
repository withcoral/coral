//! The `coral` process: starts the runtime, runs a Run Executor, or reports
//! its version. A hidden `embed-check` times the embedding model.
//!
//! Application composition belongs to `coral-app`. This process parses its
//! arguments, reports where the runtime listens, and maps signals and errors
//! to a graceful stop and an exit status.
//!
//! Each binary that ships Coral calls [`run`] with what it adds to Coral, so
//! none copies this process code (ADR 0033).

#![allow(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "the process renders operator output"
)]
#![cfg_attr(
    test,
    allow(
        unused_crate_dependencies,
        reason = "dev-dependencies serve the startup integration test"
    )
)]

use std::future::Future;
use std::net::SocketAddr;
use std::process::ExitCode;

use anyhow::{Context as _, bail};
use clap::Parser;
use coral_app::bootstrap::is_loopback_ip;
use tokio::sync::oneshot;

mod embed_check;

/// This binary's version and the commit it was built from.
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("CORAL_GIT_SHA"));

#[derive(Debug, Parser)]
#[command(name = "coral", version = VERSION)]
/// Starts the Coral runtime process.
struct ProcessArgs {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Debug, clap::Subcommand)]
enum Command {
    /// Runs a Run Executor, which dials Coral and holds its pull stream.
    RunExecutor(RunExecutorArgs),
    /// Loads the embedding model and times query embedding.
    #[command(hide = true)]
    EmbedCheck(embed_check::EmbedCheckArgs),
}

/// A Run Executor's flags. It keeps no config file and no state.
#[derive(Debug, clap::Args)]
struct RunExecutorArgs {
    /// Coral's one address, such as `http://coral:50052`.
    #[arg(long, env = "CORAL_RUN_EXECUTOR_CORAL_ADDRESS")]
    coral_address: coral_run_executor::CoralAddress,
    /// Read the token from stdin up to its first newline, and retire when
    /// stdin closes. Without it, set `CORAL_RUN_EXECUTOR_TOKEN`.
    #[arg(long, env = "CORAL_RUN_EXECUTOR_TOKEN_STDIN")]
    token_stdin: bool,
    /// The most Runs it holds at once.
    #[arg(
        long,
        env = "CORAL_RUN_EXECUTOR_MAX_RUNS_HELD",
        default_value_t = 128,
        value_parser = clap::value_parser!(u32).range(1..)
    )]
    max_runs_held: u32,
    /// The Program Compiler's heap limit, in bytes.
    #[arg(long, env = "CORAL_RUN_EXECUTOR_COMPILER_HEAP_BYTES", default_value_t = 512 << 20)]
    compiler_heap_bytes: usize,
    /// Serve the gRPC health service on this `host:port`.
    #[arg(long, env = "CORAL_RUN_EXECUTOR_HEALTH_LISTEN")]
    health_listen: Option<String>,
}

/// The environment variable that holds a dialing Run Executor's token. No
/// flag carries the token, so it never shows in a process listing.
const RUN_EXECUTOR_TOKEN_ENV: &str = "CORAL_RUN_EXECUTOR_TOKEN";

/// Error surfaced by process startup.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
struct ProcessError(#[from] anyhow::Error);

/// Exit status for another runtime holding the state root (`EX_TEMPFAIL`).
/// A launcher may retry it; the condition clears when that runtime exits.
const STATE_LOCKED_EXIT_CODE: u8 = 75;

impl ProcessError {
    /// Only a state root held by another runtime exits with
    /// [`STATE_LOCKED_EXIT_CODE`]; every other failure exits with 1.
    fn exit_status(&self) -> u8 {
        if self.state_locked() {
            STATE_LOCKED_EXIT_CODE
        } else {
            1
        }
    }

    fn state_locked(&self) -> bool {
        self.0
            .downcast_ref::<coral_app::RuntimeError>()
            .is_some_and(coral_app::RuntimeError::state_locked)
    }
}

impl coral_app::RunErrorTelemetry for ProcessError {
    fn telemetry_error_code(&self) -> &'static str {
        if self.state_locked() {
            "state_locked"
        } else {
            "internal"
        }
    }

    fn telemetry_exit_code(&self) -> u8 {
        self.exit_status()
    }
}

/// Runs the `coral` process with what the launching binary adds to Coral, and
/// returns its exit status.
///
/// # Panics
///
/// Panics if the async runtime cannot start.
#[must_use]
#[tokio::main]
pub async fn run(extensions: coral_app::Extensions) -> ExitCode {
    let ProcessArgs { command } = ProcessArgs::parse();
    let result = match command {
        Some(Command::RunExecutor(args)) => Some(run_executor(args).await),
        Some(Command::EmbedCheck(args)) => Some(embed_check::run(&args)),
        None => None,
    };
    if let Some(result) = result {
        return match result {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("Error: {error:?}");
                ExitCode::FAILURE
            }
        };
    }
    let context = coral_app::RunContext::from_env();
    let result = coral_app::run_with_context(&context, Box::pin(run_runtime(extensions))).await;
    let _shutdown_result = tokio::task::spawn_blocking(coral_app::shutdown_tracing).await;
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            let code = ExitCode::from(error.exit_status());
            eprintln!("Error: {:?}", anyhow::Error::from(error));
            code
        }
    }
}

async fn run_runtime(extensions: coral_app::Extensions) -> Result<(), ProcessError> {
    let launch = coral_app::Launch {
        service_version: Some(VERSION),
        extensions,
        migration_report: Some(print_migrations()),
        ..coral_app::Launch::from_env()
            .map_err(|error| anyhow::anyhow!("invalid launch environment: {error}"))?
    };
    let stop_on_stdin_close = launch.stop_on_stdin_close;
    let server = coral_app::start_runtime(launch)
        .await
        .map_err(anyhow::Error::from)?;
    let endpoint = server.endpoint_uri().to_string();

    if let Some(warning) =
        grpc_exposure_warning(server.grpc_addr(), server.grpc_authentication_enabled())
    {
        eprintln!("{warning}");
    }
    if let Some(callback) = server.provider_oauth_callback_url() {
        println!("Coral Provider OAuth callback listening on {callback}");
    }
    println!("Coral gRPC server listening on {endpoint}");
    let address = server.mcp_http_addr();
    let url_path = coral_mcp::WORKSPACE_MCP_ROUTE;
    println!("Coral MCP HTTP server serving each Workspace at http://{address}{url_path}");
    println!("Press Ctrl-C to stop the server.");
    if let Some(search_model) = server.search_model() {
        tokio::spawn(async move {
            match search_model.ready().await {
                Some(dir) => println!("Coral search model ready in {}", dir.display()),
                None => println!("Coral search model absent: search matches by words only"),
            }
        });
    }

    let wait = wait_for_shutdown_signal_or_server_exit(
        server.wait_for_exit(),
        wait_for_shutdown_request(stop_on_stdin_close),
    )
    .await;
    let shutdown = server.shutdown().await;
    wait.map_err(anyhow::Error::from)?;
    shutdown.map_err(anyhow::Error::from)?;
    Ok(())
}

/// Prints each database migration step while startup applies it.
///
/// Startup applies them before it listens and before telemetry starts, so
/// these lines are the only sign that the process is on a long step.
fn print_migrations() -> coral_app::MigrationReport {
    let lines = MigrationLines::default();
    std::sync::Arc::new(move |progress| {
        if let Some(line) = lines.line(progress) {
            println!("{line}");
        }
    })
}

/// Chooses the line for each piece of one startup's migration progress.
#[derive(Default)]
struct MigrationLines {
    /// The database had steps before this startup, so this is an upgrade.
    upgrading: std::sync::atomic::AtomicBool,
}

impl MigrationLines {
    /// The line for `progress`. A new database has none: its steps are many
    /// and each takes milliseconds.
    fn line(&self, progress: coral_app::MigrationProgress<'_>) -> Option<String> {
        use std::sync::atomic::Ordering::Relaxed;
        if let coral_app::MigrationProgress::Pending { applied, .. } = progress {
            self.upgrading.store(applied > 0, Relaxed);
        }
        self.upgrading
            .load(Relaxed)
            .then(|| migration_line(progress))
    }
}

/// The line for one piece of an upgrade's migration progress.
fn migration_line(progress: coral_app::MigrationProgress<'_>) -> String {
    use coral_app::MigrationProgress::{Finished, Pending, Started};
    match progress {
        Pending { pending, .. } => {
            let steps = if pending == 1 { "step" } else { "steps" };
            format!(
                "Coral is migrating the database: {pending} {steps} to apply. A step over a \
                 large table can take minutes, and Coral listens when the last one finishes."
            )
        }
        Started {
            version,
            description,
        } => format!("Database migration {version:04} ({description}) started."),
        Finished {
            version,
            description,
            elapsed,
        } => format!(
            "Database migration {version:04} ({description}) finished in {:.1} s.",
            elapsed.as_secs_f64()
        ),
    }
}

/// Runs a Run Executor until it retires: on a termination signal or, when its
/// token came down stdin, when stdin closes.
async fn run_executor(args: RunExecutorArgs) -> anyhow::Result<()> {
    let RunExecutorArgs {
        coral_address,
        token_stdin,
        max_runs_held,
        compiler_heap_bytes,
        health_listen,
    } = args;
    // The Run Executor's own warnings, such as a refused protocol level.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_max_level(tracing_subscriber::filter::LevelFilter::WARN)
        .init();
    let token = match (token_stdin, run_executor_token_env()?) {
        (true, None) => read_stdin_token()?
            .await
            .context("stdin closed before the token's newline")?,
        (false, Some(token)) => token,
        (true, Some(_)) => {
            bail!("pass the token on stdin or in {RUN_EXECUTOR_TOKEN_ENV}, not both")
        }
        (false, None) => bail!("pass --token-stdin or set {RUN_EXECUTOR_TOKEN_ENV}"),
    };
    let health = match health_listen {
        Some(address) => Some(
            tokio::net::TcpListener::bind(&address)
                .await
                .with_context(|| format!("bind the health listener on {address}"))?,
        ),
        None => None,
    };
    let config = coral_run_executor::RunExecutorConfig {
        coral_address,
        token: coral_run_executor::Token::new(token)?,
        max_runs_held,
        compiler_heap_bytes,
    };
    let mut retire_failure = None;
    let retire = async {
        if let Err(error) = wait_for_shutdown_request(token_stdin).await {
            retire_failure = Some(error);
        }
    };
    coral_run_executor::run(config, health, retire).await?;
    match retire_failure {
        Some(error) => Err(error).context("wait for the order to retire"),
        None => Ok(()),
    }
}

#[expect(
    clippy::disallowed_methods,
    reason = "process bootstrap reads the Run Executor's token once"
)]
fn run_executor_token_env() -> anyhow::Result<Option<String>> {
    match std::env::var(RUN_EXECUTOR_TOKEN_ENV) {
        Ok(token) => Ok(Some(token)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("{RUN_EXECUTOR_TOKEN_ENV} must be valid UTF-8")
        }
    }
}

/// Reads stdin's first line, without its newline, on a detached thread. The
/// bytes after it stay buffered in stdin, which stays open.
fn read_stdin_token() -> std::io::Result<oneshot::Receiver<String>> {
    use std::io::BufRead as _;

    let (token_tx, token_rx) = oneshot::channel();
    std::thread::Builder::new()
        .name("coral-stdin-token".to_owned())
        .spawn(move || {
            let mut stdin = std::io::stdin().lock();
            let mut line = String::new();
            if stdin.read_line(&mut line).is_ok()
                && let Some(token) = line.strip_suffix('\n')
            {
                let token = token.strip_suffix('\r').unwrap_or(token);
                drop(token_tx.send(token.to_owned()));
            }
        })?;
    Ok(token_rx)
}

async fn wait_for_shutdown_signal_or_server_exit(
    server_exit: impl Future<Output = ()>,
    shutdown_signal: impl Future<Output = Result<(), std::io::Error>>,
) -> Result<(), std::io::Error> {
    tokio::select! {
        result = shutdown_signal => result,
        () = server_exit => Ok(()),
    }
}

/// Warns when the private gRPC listener is reachable from another machine.
fn grpc_exposure_warning(address: SocketAddr, authentication_enabled: bool) -> Option<String> {
    if is_loopback_ip(address.ip()) {
        return None;
    }
    let exposure = if authentication_enabled {
        "it serves cleartext h2c, so bearer tokens can be read off the wire"
    } else {
        "it does not authenticate clients, so anything that can reach it can access Coral"
    };
    Some(format!(
        "Warning: the private gRPC server at {address} is not bound to loopback and {exposure}. \
         Terminate TLS in front of Coral and keep untrusted clients off the listener."
    ))
}

/// Waits for a termination signal or, when the launcher opted in, for stdin to
/// close. Both request the same graceful shutdown.
async fn wait_for_shutdown_request(stop_on_stdin_close: bool) -> Result<(), std::io::Error> {
    if stop_on_stdin_close {
        tokio::select! {
            result = wait_for_server_shutdown_signal() => result,
            result = wait_for_stdin_close() => result,
        }
    } else {
        wait_for_server_shutdown_signal().await
    }
}

/// Completes when stdin reaches EOF or fails. Bytes read are discarded.
///
/// The read runs on a detached thread rather than the runtime's blocking pool:
/// a read that never returns must not hold up process exit after a signal.
async fn wait_for_stdin_close() -> Result<(), std::io::Error> {
    use std::io::Read as _;

    let (closed_tx, closed_rx) = oneshot::channel::<()>();
    std::thread::Builder::new()
        .name("coral-stdin-close".to_owned())
        .spawn(move || {
            let mut stdin = std::io::stdin().lock();
            let mut buffer = [0_u8; 512];
            loop {
                match stdin.read(&mut buffer) {
                    Ok(0) => break,
                    Ok(_) => {}
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => {}
                    Err(_) => break,
                }
            }
            drop(closed_tx);
        })?;
    // The thread drops the sender only once stdin has closed.
    drop(closed_rx.await);
    Ok(())
}

#[cfg(unix)]
async fn wait_for_server_shutdown_signal() -> Result<(), std::io::Error> {
    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal,
        _ = sigterm.recv() => Ok(()),
    }
}

#[cfg(windows)]
async fn wait_for_server_shutdown_signal() -> Result<(), std::io::Error> {
    let mut ctrl_break = tokio::signal::windows::ctrl_break()?;
    let mut ctrl_close = tokio::signal::windows::ctrl_close()?;
    let mut ctrl_shutdown = tokio::signal::windows::ctrl_shutdown()?;
    tokio::select! {
        signal = tokio::signal::ctrl_c() => signal,
        _ = ctrl_break.recv() => Ok(()),
        _ = ctrl_close.recv() => Ok(()),
        _ = ctrl_shutdown.recv() => Ok(()),
    }
}

#[cfg(all(not(unix), not(windows)))]
async fn wait_for_server_shutdown_signal() -> Result<(), std::io::Error> {
    tokio::signal::ctrl_c().await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn telemetry_reports_the_status_the_process_exits_with() {
        use coral_app::RunErrorTelemetry as _;

        for (error, exit_code, error_code) in [
            (
                coral_app::AppError::StateLocked {
                    path: "state".into(),
                },
                STATE_LOCKED_EXIT_CODE,
                "state_locked",
            ),
            (coral_app::AppError::MissingConfigDir, 1, "internal"),
        ] {
            let error =
                ProcessError::from(anyhow::Error::from(coral_app::RuntimeError::from(error)));
            assert_eq!(error.exit_status(), exit_code);
            assert_eq!(error.telemetry_exit_code(), exit_code);
            assert_eq!(error.telemetry_error_code(), error_code);
        }
    }

    /// A new database's steps are many and quick, so printing them would
    /// bury the listener lines. An upgrade's steps are the ones to show.
    #[test]
    fn an_upgrade_prints_each_migration_step_and_a_new_database_prints_none() {
        use coral_app::MigrationProgress::{Finished, Pending, Started};

        const WAIT: &str = "A step over a large table can take minutes, and Coral listens when \
                            the last one finishes.";
        let started = Started {
            version: 63,
            description: "every callable is a tool",
        };
        let finished = Finished {
            version: 63,
            description: "every callable is a tool",
            elapsed: std::time::Duration::from_millis(262_440),
        };
        let pending = |applied, pending| Pending { applied, pending };
        for (case, progress, lines) in [
            (
                "an upgrade of one step",
                vec![pending(62, 1), started, finished],
                vec![
                    format!("Coral is migrating the database: 1 step to apply. {WAIT}"),
                    "Database migration 0063 (every callable is a tool) started.".to_owned(),
                    "Database migration 0063 (every callable is a tool) finished in 262.4 s."
                        .to_owned(),
                ],
            ),
            (
                "an upgrade of several steps",
                vec![pending(62, 4)],
                vec![format!(
                    "Coral is migrating the database: 4 steps to apply. {WAIT}"
                )],
            ),
            (
                "a new database",
                vec![pending(0, 66), started, finished],
                vec![],
            ),
        ] {
            let chooser = MigrationLines::default();
            let printed: Vec<String> = progress
                .into_iter()
                .filter_map(|progress| chooser.line(progress))
                .collect();
            assert_eq!(printed, lines, "{case}");
        }
    }

    #[test]
    fn exposed_grpc_listener_warns() {
        const UNAUTHENTICATED: &str = "it does not authenticate clients, so anything that can \
                                       reach it can access Coral";
        const CLEARTEXT: &str =
            "it serves cleartext h2c, so bearer tokens can be read off the wire";
        for (address, authenticated, exposure) in [
            ("0.0.0.0:50051", false, Some(UNAUTHENTICATED)),
            ("0.0.0.0:50051", true, Some(CLEARTEXT)),
            ("[::]:50051", false, Some(UNAUTHENTICATED)),
            ("127.0.0.1:50051", false, None),
            ("[::1]:50051", true, None),
        ] {
            let expected = exposure.map(|exposure| {
                format!(
                    "Warning: the private gRPC server at {address} is not bound to loopback \
                     and {exposure}. Terminate TLS in front of Coral and keep untrusted clients \
                     off the listener."
                )
            });
            assert_eq!(
                grpc_exposure_warning(address.parse().unwrap(), authenticated),
                expected,
                "{address}"
            );
        }
    }
}
