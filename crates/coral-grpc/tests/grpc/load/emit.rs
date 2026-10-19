//! Emit cell: one Program emits `CORAL_LOAD_EMITS` values, each padded with
//! `CORAL_LOAD_EMIT_BYTES`, at most one per `CORAL_LOAD_EMIT_INTERVAL_MS`
//! (0, the default, emits as fast as each `emit` answers), and every Emission
//! is timed from the Program's own clock (`Date.now()` inside the emitted
//! value) to four points:
//!
//! - `emit_ack_ms`: `await coral.emit(...)` resolved in the Program: Applied,
//!   once the Run Owner has the Emission in memory (before the owner, the
//!   Emission's own commit).
//! - `held_ms`: the Emission's `occurred_at`, the owner's apply time (before the
//!   owner, the time the write transaction got the Run row: pool wait plus the
//!   row hold).
//! - `recorded_ms`: a poller on the measurement connection first saw the Emission
//!   row committed, at `RECORDED_POLL` resolution.
//! - `visible_ms`: an observer paging through `RunModule::observe` from the Run's
//!   initial cursor first returned the Emission.
//!
//! Phases: baseline, emitting (Run start until terminal), after.

use super::{
    Harness, Phase, Probe, distribution, millis, nanos_to_ms, setting_u64, submission, unix_ms,
};
use coral_run::{
    FinalValue, RUN_EVENT_PAGE_MAX, RunCursor, RunEventKind, RunId, RunModule, RunObservation,
    RunState, RunTerminalOutcome,
};
use coral_workspace::{AuthenticatedPrincipal, WorkspaceId};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The poller's cadence, and so the resolution of `recorded_ms`.
const RECORDED_POLL: Duration = Duration::from_millis(2);

/// One Emission as the observer returned it.
struct Seen {
    emitted_at_ms: f64,
    visible_at_ms: f64,
}

/// What the observer learned: every Emission by sequence, the terminal state, and
/// the Program's own per-emit acknowledgement times from its Final Value.
struct Observed {
    seen: BTreeMap<i64, Seen>,
    state: Result<RunState, String>,
    acks_ms: Vec<f64>,
    pages: u64,
}

/// Pages output through `RunModule::observe` until the Run is terminal and its
/// final page is delivered, recording when each Emission first came back.
async fn observe_emissions(
    runs: &RunModule,
    actor: &AuthenticatedPrincipal,
    workspace: &WorkspaceId,
    run: &RunId,
    mut cursor: RunCursor,
    budget: Duration,
) -> Observed {
    let deadline = Instant::now() + budget;
    let mut seen = BTreeMap::new();
    let mut acks_ms = Vec::new();
    let mut pages = 0;
    let state = loop {
        let observation = RunObservation::immediate(Some(cursor.clone()), RUN_EVENT_PAGE_MAX)
            .and_then(|o| o.with_wait(Duration::from_secs(30)))
            .unwrap();
        let update = match runs.observe(actor, workspace, run, &observation).await {
            Ok(update) => update,
            Err(error) => break Err(error.to_string()),
        };
        let visible_at_ms = unix_ms();
        pages += 1;
        for event in update.events() {
            if let RunEventKind::ProgramEmitted { value } = event.kind() {
                let emitted_at_ms = value.as_json()["t"].as_f64().unwrap_or_default();
                seen.entry(i64::try_from(event.sequence()).unwrap())
                    .or_insert(Seen {
                        emitted_at_ms,
                        visible_at_ms,
                    });
            }
        }
        cursor = update.next_cursor().clone();
        if let Some(outcome) = update.terminal_outcome() {
            if let RunTerminalOutcome::Succeeded {
                final_value: FinalValue::Present(value),
            } = outcome
            {
                acks_ms = value.as_json()["acks"]
                    .as_array()
                    .map(|acks| acks.iter().filter_map(Value::as_f64).collect())
                    .unwrap_or_default();
            }
            break Ok(update.state());
        }
        if Instant::now() > deadline {
            break Err(format!("Run still {:?} after {budget:?}", update.state()));
        }
    };
    Observed {
        seen,
        state,
        acks_ms,
        pages,
    }
}

/// Polls the measurement connection for committed Emissions until told to stop,
/// then sweeps once more; records when each sequence first appeared.
async fn poll_recorded(
    mut probe: Probe,
    run: RunId,
    stop: Arc<AtomicBool>,
) -> BTreeMap<i64, (f64, i64, i64)> {
    let mut recorded = BTreeMap::new();
    let mut after = 0;
    let mut stopped = false;
    while !stopped {
        stopped = stop.load(Ordering::SeqCst);
        let rows = probe.emissions_after(&run, after).await;
        let first_seen_ms = unix_ms();
        for (sequence, occurred_at, bytes) in rows {
            recorded.insert(sequence, (first_seen_ms, occurred_at, bytes));
            after = after.max(sequence);
        }
        tokio::time::sleep(RECORDED_POLL).await;
    }
    recorded
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "load harness: run through scripts/load.mjs"]
#[expect(clippy::too_many_lines, reason = "one scripted measurement run")]
async fn run_emit_load() {
    let emits = setting_u64("CORAL_LOAD_EMITS", 1000);
    let pad = setting_u64("CORAL_LOAD_EMIT_BYTES", 0);
    let interval_ms = setting_u64("CORAL_LOAD_EMIT_INTERVAL_MS", 0);

    let mut h = Harness::arrange("emit").await;
    let mut probe = h.open_probe().await;
    let recorder_probe = h.open_probe().await;
    eprintln!("load: emits={emits} pad_bytes={pad} interval_ms={interval_ms}");

    let probes = h.start_probes();
    let mut phases = Vec::new();

    tokio::time::sleep(Duration::from_secs(5)).await;
    let run_started = h.elapsed();
    phases.push(Phase {
        name: "baseline",
        from: Duration::ZERO,
        to: run_started,
    });

    // A paced Program spins on the clock between emits: the sandbox has no
    // timers, and the point is the gap, not the CPU.
    let program = format!(
        "const pad = 'x'.repeat({pad}); const acks = []; let next = Date.now(); \
         for (let i = 0; i < {emits}; i += 1) {{ \
           while (Date.now() < next) {{}} next += {interval_ms}; \
           const t = Date.now(); await coral.emit({{i, t, pad}}); acks.push(Date.now() - t); \
         }} \
         return {{acks}};"
    );
    let started =
        h.f.runs
            .start(&h.actor, h.f.workspace.id(), submission(&program, 900_000))
            .await
            .unwrap();
    let run = started.run_id().clone();
    let stop = Arc::new(AtomicBool::new(false));
    let recorder_task = tokio::spawn(poll_recorded(recorder_probe, run.clone(), stop.clone()));
    let observed = observe_emissions(
        &h.f.runs,
        &h.actor,
        h.f.workspace.id(),
        &run,
        started.next_cursor().clone(),
        Duration::from_mins(15),
    )
    .await;
    let terminal = h.elapsed();
    stop.store(true, Ordering::SeqCst);
    let recorded = recorder_task.await.unwrap();
    eprintln!(
        "load: Run ended {:?} after {:?}, {} emissions observed in {} pages",
        observed.state,
        terminal.saturating_sub(run_started),
        observed.seen.len(),
        observed.pages
    );
    phases.push(Phase {
        name: "emitting",
        from: run_started,
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
    let mut visible_ms = Vec::new();
    let mut recorded_ms = Vec::new();
    let mut held_ms = Vec::new();
    let mut unrecorded = 0;
    let mut value_bytes = 0;
    for (sequence, seen) in &observed.seen {
        visible_ms.push(seen.visible_at_ms - seen.emitted_at_ms);
        match recorded.get(sequence) {
            Some((first_seen_ms, occurred_at, bytes)) => {
                recorded_ms.push(first_seen_ms - seen.emitted_at_ms);
                held_ms.push(nanos_to_ms(*occurred_at) - seen.emitted_at_ms);
                value_bytes = value_bytes.max(*bytes);
            }
            None => unrecorded += 1,
        }
    }
    let lifetime_ms = terminal_at.map(|t| nanos_to_ms(t - accepted_at));
    #[expect(
        clippy::cast_precision_loss,
        reason = "emissions per second over a small count"
    )]
    let emits_per_s = lifetime_ms
        .filter(|ms| *ms > 0.0)
        .map(|ms| observed.seen.len() as f64 / (ms / 1000.0));
    h.report(
        "emit",
        json!({
            "emits": emits,
            "pad_bytes": pad,
            "interval_ms": interval_ms,
            "value_bytes": value_bytes,
            "recorded_poll_ms": millis(RECORDED_POLL),
            "run": {
                "state": run_state,
                "termination_kind": termination_kind,
                "observed_state": format!("{:?}", observed.state),
                "lifetime_ms": lifetime_ms,
                "head": head,
                "emitting_ms": millis(terminal.saturating_sub(run_started)),
            },
            "emissions": {
                "observed": observed.seen.len(),
                "recorded": recorded.len(),
                "unrecorded_when_observed": unrecorded,
                "observe_pages": observed.pages,
                "per_s": emits_per_s,
                "emit_ack_ms": distribution(observed.acks_ms),
                "held_ms": distribution(held_ms),
                "recorded_ms": distribution(recorded_ms),
                "visible_ms": distribution(visible_ms),
            },
        }),
        &probes,
        &phases,
    );
}
