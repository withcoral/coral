//! A local Run Executor that dies ends its Run at once (ADR 0027). Its own
//! test binary: it kills every local child of its process.
#![cfg(unix)]
#![allow(
    unused_crate_dependencies,
    missing_docs,
    reason = "Integration test: only the composed runtime and its children are under test."
)]

mod composed;

use std::time::Duration;

use composed::{WEB, compose, recorded_run_state, spin_a_run};

/// The ids of the local Run Executors this process runs now.
fn local_children() -> Vec<String> {
    let found = std::process::Command::new("pgrep")
        .args(["-P", &std::process::id().to_string(), "-f", "run-executor"])
        .output()
        .expect("run pgrep");
    String::from_utf8_lossy(&found.stdout)
        .split_whitespace()
        .map(str::to_owned)
        .collect()
}

/// Catches: a local child's exit that leaves its Run running until the
/// child's lease lapses, instead of ending it at once.
#[tokio::test(flavor = "multi_thread")]
async fn a_local_childs_exit_ends_its_run_interrupted_at_once() {
    let composed = compose(WEB).await;
    let run_id = spin_a_run(&composed).await;
    let children = local_children();
    assert!(!children.is_empty(), "a local child runs the Run");
    for pid in children {
        std::process::Command::new("kill")
            .args(["-KILL", &pid])
            .status()
            .expect("kill the local child");
    }
    tokio::time::timeout(Duration::from_secs(5), async {
        while recorded_run_state(composed.state_root.path(), &run_id).await != "interrupted" {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("the child's exit ends its Run at once");

    composed.client.cancel().await.expect("close MCP client");
    composed
        .runtime
        .shutdown()
        .await
        .expect("stop the composed runtime");
}
