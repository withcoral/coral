//! Concurrent-Runs cell: `CORAL_LOAD_RUNS` one-call, no-approval Runs start at
//! once in one Workspace, more than the default pool holds connections, and the
//! probes report what sign-in, readiness and a neighbouring Workspace see.
//!
//! Phases: baseline, burst (first start until every Run is terminal), after. Per-Run
//! start and total latency come from the callers; admission, dispatch and total
//! per call from the Invocation rows.

use super::{
    Harness, Phase, counts, distribution, enable_http, millis, nanos_to_ms, setting_u64, settle,
    submission,
};
use coral_run::{RunId, RunState};
use serde_json::json;
use std::time::{Duration, Instant};

/// One Run as its caller saw it.
struct Outcome {
    run: Option<RunId>,
    start_ms: f64,
    total_ms: f64,
    state: Result<RunState, String>,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load harness: run through scripts/load.mjs"]
#[expect(clippy::too_many_lines, reason = "one scripted measurement run")]
async fn run_concurrent_runs_load() {
    let concurrency = setting_u64("CORAL_LOAD_RUNS", 20);

    let mut h = Harness::arrange("concurrent").await;
    enable_http(&h.f, &h.f.workspace, "evidence", &h.upstream.uri()).await;
    let mut probe = h.open_probe().await;
    eprintln!("load: runs={concurrency}");

    let probes = h.start_probes();
    let mut phases = Vec::new();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let burst_started = h.elapsed();
    phases.push(Phase {
        name: "baseline",
        from: Duration::ZERO,
        to: burst_started,
    });

    let tasks: Vec<_> = (0..concurrency)
        .map(|_| {
            let runs = h.f.runs.clone();
            let actor = h.actor.clone();
            let workspace = h.f.workspace.id().clone();
            tokio::spawn(async move {
                let t = Instant::now();
                let started = runs
                    .start(
                        &actor,
                        &workspace,
                        submission(
                            "return await coral.providers.evidence.read({body: {}});",
                            600_000,
                        ),
                    )
                    .await;
                let start_ms = millis(t.elapsed());
                match started {
                    Ok(update) => {
                        let run = update.run_id().clone();
                        let state = settle(&runs, &actor, &workspace, &run, 600).await;
                        Outcome {
                            run: Some(run),
                            start_ms,
                            total_ms: millis(t.elapsed()),
                            state,
                        }
                    }
                    Err(error) => Outcome {
                        run: None,
                        start_ms,
                        total_ms: millis(t.elapsed()),
                        state: Err(error.to_string()),
                    },
                }
            })
        })
        .collect();
    let mut outcomes = Vec::new();
    for task in tasks {
        outcomes.push(task.await.unwrap());
    }
    let all_ended = h.elapsed();
    eprintln!(
        "load: {} Runs ended after {:?}",
        outcomes.len(),
        all_ended.saturating_sub(burst_started)
    );
    phases.push(Phase {
        name: "burst",
        from: burst_started,
        to: all_ended,
    });

    tokio::time::sleep(Duration::from_secs(5)).await;
    let end = h.elapsed();
    phases.push(Phase {
        name: "after",
        from: all_ended,
        to: end,
    });
    let probes = probes.finish().await;

    let mut admit = Vec::new();
    let mut dispatch = Vec::new();
    let mut total = Vec::new();
    let mut invocation_states = Vec::new();
    for run in outcomes.iter().filter_map(|o| o.run.as_ref()) {
        let accepted_at = probe.run_row(run).await.0;
        for row in probe.invocations(run).await {
            admit.push(nanos_to_ms(row.0 - accepted_at));
            if let (Some(started), Some(ended)) = (row.1, row.2) {
                dispatch.push(nanos_to_ms(ended - started));
                total.push(nanos_to_ms(ended - row.0));
            }
            invocation_states.push(row.3);
        }
    }
    let states: Vec<String> = outcomes
        .iter()
        .map(|o| match &o.state {
            Ok(state) => format!("{state:?}"),
            Err(_) => "Error".to_owned(),
        })
        .collect();
    let succeeded = outcomes
        .iter()
        .filter(|o| o.state == Ok(RunState::Succeeded))
        .count();
    let first_error = outcomes
        .iter()
        .find_map(|o| o.state.as_ref().err().cloned());
    h.report(
        "concurrent",
        json!({
            "runs": {
                "concurrency": concurrency,
                "count": outcomes.len(),
                "by_state": counts(states.iter().map(String::as_str)),
                "failed": outcomes.len() - succeeded,
                "first_error": first_error,
                "all_ended_after_ms": millis(all_ended.saturating_sub(burst_started)),
                "start_ms": distribution(outcomes.iter().map(|o| o.start_ms).collect()),
                "total_ms": distribution(outcomes.iter().map(|o| o.total_ms).collect()),
            },
            "invocations": {
                "count": invocation_states.len(),
                "by_state": counts(invocation_states.iter().map(String::as_str)),
                "admit_ms": distribution(admit),
                "dispatch_ms": distribution(dispatch),
                "total_ms": distribution(total),
            },
        }),
        &probes,
        &phases,
    );
}
