//! The `coral` executable: starts the runtime or reports its version.
//!
//! Application composition belongs to `coral-app`. This process parses its
//! arguments, reports where the runtime listens, and maps signals and errors
//! to a graceful stop and an exit status.

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

use clap::Parser;
use coral_app::bootstrap::is_loopback_ip;

/// This binary's version and the commit it was built from.
const VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "+", env!("CORAL_GIT_SHA"));

#[derive(Debug, Parser)]
#[command(name = "coral", version = VERSION)]
/// Starts the Coral runtime process.
struct ProcessArgs {}

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

#[tokio::main]
async fn main() -> ExitCode {
    let ProcessArgs {} = ProcessArgs::parse();
    let context = coral_app::RunContext::from_env();
    let result = coral_app::run_with_context(&context, Box::pin(run_runtime())).await;
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

async fn run_runtime() -> Result<(), ProcessError> {
    let launch = coral_app::Launch {
        service_version: Some(VERSION),
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

    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel::<()>();
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
