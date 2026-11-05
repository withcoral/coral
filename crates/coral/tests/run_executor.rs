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

#[tokio::test]
async fn a_local_child_says_hello_with_its_stdin_token_and_retires_when_stdin_closes() {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    // A local child runs with an empty environment.
    let mut child = tokio::process::Command::new(env!("CARGO_BIN_EXE_coral"))
        .args(["run-executor", "--coral-address", coral.address()])
        .arg("--token-stdin")
        .env_clear()
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn the Run Executor");
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

    drop(stdin);
    let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
        .await
        .expect("the Run Executor retires when stdin closes")
        .expect("wait for the Run Executor");
    assert!(status.success(), "{status}");
}
