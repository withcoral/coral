//! The take (ADR 0027): a Run accepted for Run streams waits with no Run
//! Owner until the take loop claims it for the pull stream with the most
//! room and sends it there in one `Taken`.
use super::*;
use coral_run_executor::PullStreams;
use coral_run_executor::wire::v1::{PullResponse, TakenRun, pull_response};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use crate::take::{Pace, TAKE_MAX, Timing};

const LONG: Duration = Duration::from_secs(3600);

/// Takes at once, then never again unless room appears or a test waits a
/// short backstop.
const fn timing(backstop: Duration) -> Timing {
    Timing {
        spacing: LONG,
        backstop,
    }
}

/// Accepts `count` Runs for Run streams, oldest first.
async fn waiting_runs(fixture: &TestModules, runs: &RunModule, count: usize) -> Vec<String> {
    let (workspace_id, principal) = fixture.user_scope("taker", "Take queue").await;
    let mut ids = Vec::new();
    for _ in 0..count {
        let update = runs
            .start(&principal, &workspace_id, submission("return 1;"))
            .await
            .expect("accept a Run");
        ids.push(update.run_id().to_string());
    }
    ids
}

fn take_loop(runs: &RunModule, streams: &PullStreams, timing: Timing) {
    let (runs, streams) = (runs.clone(), streams.clone());
    tokio::spawn(async move { runs.take_loop(streams, timing).await });
}

/// The Run ids of the next `Taken` on a scripted stream.
async fn next_taken<E: std::fmt::Debug>(
    sent: &mut UnboundedReceiver<Result<PullResponse, E>>,
) -> Vec<TakenRun> {
    let sent = tokio::time::timeout(EVENTUALLY, sent.recv())
        .await
        .expect("a Taken in time")
        .expect("the stream is open")
        .expect("a message, not a failure");
    match sent.message {
        Some(pull_response::Message::Taken(taken)) => taken.runs,
        other => panic!("expected Taken, got {other:?}"),
    }
}

fn ids(taken: &[TakenRun]) -> Vec<String> {
    taken.iter().map(|run| run.run_id.clone()).collect()
}

/// The take columns of Run `id`: its claim, take time, and Run Executor.
async fn take_row(
    database: &coral_db::Database,
    id: &str,
) -> (Option<String>, Option<i64>, Option<String>, Option<String>) {
    let row = Query::select()
        .columns(
            [
                "execution_claim_id",
                "taken_at_unix_nanos",
                "run_executor_id",
                "run_executor_kind",
            ]
            .map(Alias::new),
        )
        .from(Alias::new("program_run_records"))
        .and_where(Expr::col(Alias::new("id")).eq(id))
        .to_owned();
    database
        .fetch_optional(row)
        .await
        .expect("read the take")
        .expect("the Run row")
}

#[tokio::test]
async fn a_run_accepted_for_run_streams_waits_running_with_no_owner() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
    let (workspace_id, principal) = fixture.user_scope("waiter", "Waiting").await;
    let update = runs
        .start(&principal, &workspace_id, submission("return 1;"))
        .await
        .expect("accept a Run");

    let run = fixture.run_record(&workspace_id, update.run_id()).await;
    assert_eq!(run.state, RunState::Running);
    assert_eq!(
        (
            run.worker_claim_id,
            run.worker_claim_expires_at_unix_nanos,
            run.holder_instance_id,
            run.execution_claim_id,
        ),
        (None, None, None, None)
    );
    assert!(runs.owners.get(update.run_id()).is_none());
}

#[tokio::test]
async fn a_take_claims_the_oldest_runs_for_the_stream_with_the_most_room() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
    // Older than every waiting Run, and its Run Owner's: never taken.
    let (workspace_id, principal) = fixture.user_scope("owned", "Owned").await;
    fixture
        .staged_run(&principal, &workspace_id, "return 1;")
        .await;
    let accepted = waiting_runs(&fixture, &runs, 3).await;
    let streams = PullStreams::default();
    let (_small, mut small_sent) = streams.scripted("small", 2);
    let (large, mut large_sent) = streams.scripted("large", 5);

    take_loop(&runs, &streams, timing(LONG));
    let taken = next_taken(&mut large_sent).await;

    assert_eq!(
        ids(&taken),
        accepted,
        "the waiting Runs, oldest first, in one Taken"
    );
    for run in &taken {
        let (claim, taken_at, executor, kind) = take_row(&fixture.database, &run.run_id).await;
        assert_eq!(claim.as_deref(), Some(run.claim_id.as_str()));
        assert!(taken_at.is_some());
        assert_eq!(
            (executor.as_deref(), kind.as_deref()),
            (Some("large"), Some("dialed"))
        );
    }
    let claims: std::collections::HashSet<_> = taken.iter().map(|run| &run.claim_id).collect();
    assert_eq!(claims.len(), 3, "each Run has its own claim");
    let room = streams.held().into_iter().find(|held| held.stream == large);
    assert_eq!(
        room.map(|held| held.free),
        Some(5 - u32::try_from(taken.len()).expect("a count")),
        "each Run spends a unit"
    );
    assert!(small_sent.try_recv().is_err(), "no Run waits for the other");
}

#[tokio::test]
async fn one_take_claims_at_most_sixteen_and_the_pass_repeats_for_the_rest() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
    let accepted = waiting_runs(&fixture, &runs, 20).await;
    let streams = PullStreams::default();
    let (_stream, mut sent) = streams.scripted("roomy", 32);

    take_loop(&runs, &streams, timing(LONG));
    let first = next_taken(&mut sent).await;
    let second = next_taken(&mut sent).await;

    let cap = usize::try_from(TAKE_MAX).expect("the cap");
    let (oldest, rest) = accepted.split_at(cap);
    assert_eq!(
        (ids(&first).as_slice(), ids(&second).as_slice()),
        (oldest, rest)
    );
}

#[tokio::test]
async fn no_room_takes_nothing_and_new_room_takes_at_once() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
    let accepted = waiting_runs(&fixture, &runs, 1).await;
    let streams = PullStreams::default();
    let (_full, mut full_sent) = streams.scripted("full", 0);

    take_loop(&runs, &streams, timing(LONG));
    let (_opened, mut opened_sent) = streams.scripted("opened", 1);

    assert_eq!(ids(&next_taken(&mut opened_sent).await), accepted);
    assert!(
        full_sent.try_recv().is_err(),
        "a stream with no room gets none"
    );
}

#[tokio::test]
async fn the_backstop_takes_a_run_that_nothing_rang_for() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
    let accepted = waiting_runs(&fixture, &runs, 1).await;
    let run_id = accepted.first().expect("one Run");
    let streams = PullStreams::default();
    let (_stream, mut sent) = streams.scripted("roomy", 2);
    take_loop(&runs, &streams, timing(Duration::from_millis(50)));
    let first = next_taken(&mut sent).await;

    // Back to waiting by fixture SQL, which raises no change signal.
    fixture
        .inject_fault(&format!(
            "UPDATE program_run_records SET execution_claim_id = NULL, \
             taken_at_unix_nanos = NULL WHERE id = '{run_id}'"
        ))
        .await;
    let again = next_taken(&mut sent).await;

    assert_eq!(ids(&again), accepted);
    let claim = |taken: &[TakenRun]| taken.first().map(|run| run.claim_id.clone());
    assert_ne!(claim(&again), claim(&first), "a new take, a new claim");
}

#[tokio::test(flavor = "multi_thread")]
async fn two_coral_instances_take_disjoint_runs_from_one_queue() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.runs.clone().with_run_streams();
    let accepted = waiting_runs(&fixture, &runs, 40).await;
    let peer = Arc::new(
        fixture
            .test_database
            .connect_peer()
            .await
            .expect("a second Coral Instance's database handle"),
    );
    let other = RunModule::with_adapters(
        peer,
        fixture.workspaces.clone(),
        fixture.providers.clone(),
        Arc::new(ImmediateEngine::default()),
        fixture.upstream.clone(),
        fixture.clock.clone(),
        crate::TestObservers::new(
            Arc::new(NoopInvocationObserver),
            Arc::new(NoopExecutionObserver),
        ),
    )
    .with_run_streams();
    let (one, two) = (PullStreams::default(), PullStreams::default());
    let (_first, mut one_sent) = one.scripted("one", TAKE_MAX);
    let (_second, mut two_sent) = two.scripted("two", TAKE_MAX);

    take_loop(&runs, &one, timing(LONG));
    take_loop(&other, &two, timing(LONG));
    let cap = usize::try_from(TAKE_MAX).expect("the cap");
    let mut taken = Vec::new();
    for sent in [&mut one_sent, &mut two_sent] {
        let mut mine = Vec::new();
        while mine.len() < cap {
            mine.extend(ids(&next_taken(sent).await));
        }
        taken.extend(mine);
    }

    let distinct: std::collections::HashSet<_> = taken.iter().collect();
    assert_eq!(distinct.len(), 2 * cap, "no Run is taken twice");
    assert!(taken.iter().all(|id| accepted.contains(id)));
}

#[test]
fn the_pace_coalesces_rings_and_keeps_a_backstop() {
    let timing = Timing {
        spacing: Duration::from_millis(50),
        backstop: Duration::from_secs(1),
    };
    let start = Instant::now();
    let after = |millis| start + Duration::from_millis(millis);
    let passed = || {
        let mut pace = Pace::new(timing);
        pace.passed(start);
        pace
    };
    let rang = |rings| {
        let mut pace = passed();
        (0..rings).for_each(|_ring| pace.ring());
        pace
    };
    let slow = Timing {
        spacing: LONG,
        ..timing
    };
    let mut slow_rang = Pace::new(slow);
    slow_rang.passed(start);
    slow_rang.ring();
    let cases = [
        (
            "a new loop takes at once",
            Pace::new(timing),
            true,
            Some(start),
        ),
        ("no room, no take", rang(1), false, None),
        ("one ring waits the spacing", rang(1), true, Some(after(50))),
        ("many rings make one take", rang(10), true, Some(after(50))),
        (
            "nothing rang: the backstop",
            passed(),
            true,
            Some(after(1000)),
        ),
        (
            "the backstop bounds the spacing",
            slow_rang,
            true,
            Some(after(1000)),
        ),
    ];
    for (case, pace, room, due) in cases {
        assert_eq!(pace.due(room, start), due, "{case}");
    }
}
