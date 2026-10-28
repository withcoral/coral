//! Fan-out cell: one Run makes many parallel approval-gated calls (#2391).
//!
//! Phases: baseline, burst (until every Approval Request is visible or the Run
//! ends), steady, decide or cancel, settle, after. Per-call admission, dispatch and
//! total latency come from the Invocation rows.

use super::{
    Harness, InvocationRow, Phase, counts, distribution, millis, nanos_to_ms, publish_http,
    setting, setting_u64, settle, submission,
};
use coral_run::RunObservation;
use coral_run::approval::{ApprovalDecision, ApprovalState};
use serde_json::json;
use std::time::{Duration, Instant};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load harness: run through scripts/load.mjs"]
#[expect(clippy::too_many_lines, reason = "one scripted measurement run")]
async fn run_fan_out_load() {
    let calls = setting_u64("CORAL_LOAD_CALLS", 30);
    let mode = setting("CORAL_LOAD_MODE").unwrap_or_else(|| "approve".to_owned());
    assert!(
        mode == "approve" || mode == "hold",
        "CORAL_LOAD_MODE is approve or hold"
    );
    let hold = Duration::from_secs(setting_u64("CORAL_LOAD_HOLD_SECS", 10));
    let admit_wait = Duration::from_secs(setting_u64("CORAL_LOAD_ADMIT_WAIT_SECS", 120));

    let mut h = Harness::arrange("fanout").await;
    // HTTP Tools require approval by default; the busy Run keeps that policy.
    publish_http(&h.f, &h.f.workspace, "evidence", &h.upstream.uri()).await;
    let mut probe = h.open_probe().await;
    let approvals = h.f.runs.approvals();
    eprintln!("load: calls={calls} mode={mode} hold={hold:?}");

    let probes = h.start_probes();
    let mut phases = Vec::new();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let run_started = h.elapsed();
    phases.push(Phase {
        name: "baseline",
        from: Duration::ZERO,
        to: run_started,
    });

    let run = h
        .f
        .runs
        .start(
            &h.actor,
            h.f.workspace.id(),
            submission(
                &format!(
                    "await Promise.all(Array.from({{length: {calls}}}, () => coral.providers.evidence.read({{body: {{}}}}))); return {calls};"
                ),
                900_000,
            ),
        )
        .await
        .unwrap()
        .run_id()
        .clone();
    let visible = loop {
        let open = probe.count_approvals(&run).await;
        if open >= i64::try_from(calls).unwrap() {
            break true;
        }
        let ended = probe.run_row(&run).await.1.is_some();
        if ended || h.elapsed().saturating_sub(run_started) > admit_wait {
            eprintln!(
                "load: only {open}/{calls} approval requests visible after {:?} (Run ended: {ended})",
                h.elapsed().saturating_sub(run_started)
            );
            break false;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    };
    let all_visible = h.elapsed();
    eprintln!(
        "load: {calls} calls, burst ended after {:?} (every approval request visible: {visible})",
        all_visible.saturating_sub(run_started)
    );
    phases.push(Phase {
        name: "burst",
        from: run_started,
        to: all_visible,
    });

    tokio::time::sleep(hold).await;
    let steady_end = h.elapsed();
    phases.push(Phase {
        name: "steady",
        from: all_visible,
        to: steady_end,
    });

    let mut decisions = Vec::new();
    let mut cancel_ms = None;
    if mode == "approve" {
        let pending: Vec<_> = approvals
            .list_for_run(&h.actor, h.f.workspace.id(), &run)
            .await
            .unwrap()
            .into_iter()
            .filter(|request| request.state == ApprovalState::Pending)
            .collect();
        for request in &pending {
            let t = Instant::now();
            h.f.runs
                .decide_approval(
                    &h.actor,
                    h.f.workspace.id(),
                    &request.id,
                    ApprovalDecision::Approve,
                    None,
                    &coral_workspace::TraceId::current_or_new(),
                )
                .await
                .unwrap();
            decisions.push(millis(t.elapsed()));
        }
        eprintln!("load: approved {} requests", pending.len());
    } else {
        let t = Instant::now();
        h.f.runs
            .cancel(
                &h.actor,
                h.f.workspace.id(),
                &run,
                &RunObservation::initial(Duration::from_secs(30)).unwrap(),
            )
            .await
            .unwrap();
        cancel_ms = Some(millis(t.elapsed()));
    }
    let decided = h.elapsed();
    phases.push(Phase {
        name: if mode == "approve" {
            "decide"
        } else {
            "cancel"
        },
        from: steady_end,
        to: decided,
    });

    let final_state = settle(&h.f.runs, &h.actor, h.f.workspace.id(), &run, 600).await;
    let terminal = h.elapsed();
    eprintln!(
        "load: Run ended {final_state:?} after {:?}",
        terminal.saturating_sub(run_started)
    );
    phases.push(Phase {
        name: "settle",
        from: decided,
        to: terminal,
    });

    tokio::time::sleep(Duration::from_secs(5)).await;
    let end = h.elapsed();
    phases.push(Phase {
        name: "after",
        from: terminal,
        to: end,
    });
    let probes = probes.finish().await;

    let (accepted_at, terminal_at, run_state, termination_kind, head) = probe.run_row(&run).await;
    let rows = probe.invocations(&run).await;
    let admit: Vec<f64> = rows
        .iter()
        .map(|r| nanos_to_ms(r.0 - accepted_at))
        .collect();
    let dispatch: Vec<f64> = rows
        .iter()
        .filter_map(|r| Some(nanos_to_ms(r.2? - r.1?)))
        .collect();
    let total: Vec<f64> = rows
        .iter()
        .filter_map(|r| Some(nanos_to_ms(r.2? - r.0)))
        .collect();
    let max_dispatching_at_once = max_dispatching_at_once(&rows);
    h.report(
        "fanout",
        json!({
            "calls": calls,
            "mode": mode,
            "hold_secs": hold.as_secs(),
            "run": {
                "state": run_state,
                "termination_kind": termination_kind,
                "observed_state": format!("{final_state:?}"),
                "lifetime_ms": terminal_at.map(|t| nanos_to_ms(t - accepted_at)),
                "head": head,
                "all_visible": visible,
                "all_visible_after_ms": millis(all_visible.saturating_sub(run_started)),
                "cancel_ms": cancel_ms,
            },
            "invocations": {
                "count": rows.len(),
                "by_state": counts(rows.iter().map(|r| r.3.as_str())),
                "by_result": counts(rows.iter().filter_map(|r| r.4.as_deref())),
                "admit_ms": distribution(admit),
                "dispatch_ms": distribution(dispatch),
                "total_ms": distribution(total),
                "max_dispatching_at_once": max_dispatching_at_once,
            },
            "decisions_ms": distribution(decisions),
        }),
        &probes,
        &phases,
    );
}

/// The most Invocation rows that were `dispatching` at one instant, from
/// their recorded dispatch starts and ends: the Fan-out Limit as persistence
/// holds it. A row that ends in the commit that starts another is counted
/// as ended first; a row without an end stays dispatching to the horizon.
fn max_dispatching_at_once(rows: &[InvocationRow]) -> usize {
    let mut edges: Vec<(i64, i8)> = Vec::new();
    for row in rows {
        if let Some(started) = row.1 {
            edges.push((started, 1));
            edges.push((row.2.unwrap_or(i64::MAX), -1));
        }
    }
    edges.sort_unstable();
    let mut live: i64 = 0;
    let mut most: i64 = 0;
    for (_, delta) in edges {
        live += i64::from(delta);
        most = most.max(live);
    }
    usize::try_from(most).unwrap_or(0)
}
