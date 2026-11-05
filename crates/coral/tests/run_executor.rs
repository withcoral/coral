#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test: only the Run Executor process and its pull stream are under test."
)]

use std::process::Stdio;
use std::time::Duration;

use coral_run_executor::PROTOCOL_LEVEL;
use coral_run_executor::testing::{Answer, ScriptedCoral};
use tokio::io::AsyncWriteExt as _;
use tokio::process::{Child, Command};

/// Starts the real binary against `coral`. The environment stays: Windows
/// needs `SystemRoot` to open a socket.
fn spawn(coral: &ScriptedCoral, configure: impl FnOnce(&mut Command)) -> Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_coral"));
    command
        .args(["run-executor", "--coral-address", coral.address()])
        .env_remove("CORAL_RUN_EXECUTOR_TOKEN")
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true);
    configure(&mut command);
    command.spawn().expect("spawn the Run Executor")
}

#[tokio::test]
async fn a_local_child_says_hello_with_its_stdin_token_and_retires_when_stdin_closes() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let mut child = spawn(&coral, |command| {
        command.arg("--token-stdin");
    });
    let mut stdin = child.stdin.take().expect("the Run Executor's stdin");
    stdin
        .write_all(b"local-child-token\n")
        .await
        .expect("write the token");

    let call = coral.next_call().await;
    assert_eq!(
        call.authorization.as_deref(),
        Some("Bearer local-child-token")
    );
    let hello = call.hello.expect("the pull stream opens with Hello");
    assert_eq!((hello.protocol_level, hello.free), (PROTOCOL_LEVEL, 128));
    assert!(
        child.try_wait().expect("poll the Run Executor").is_none(),
        "the Run Executor runs while stdin is open"
    );

    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("the Run Executor retires when stdin closes")
        .expect("wait for the Run Executor");
    assert!(status.success(), "{status}");
}

#[tokio::test]
async fn a_dialing_run_executor_takes_its_token_from_the_environment() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let _child = spawn(&coral, |command| {
        command.env("CORAL_RUN_EXECUTOR_TOKEN", "dialing-token");
    });

    let call = coral.next_call().await;
    assert_eq!(call.authorization.as_deref(), Some("Bearer dialing-token"));
}
