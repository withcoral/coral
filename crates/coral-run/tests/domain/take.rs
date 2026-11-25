//! The take (ADR 0027): a Run accepted for Run streams waits with no Run
//! Owner until the take loop claims it for the pull stream with the most
//! room and sends it there in one `Taken`.
use super::*;
use coral_run_executor::wire::v1::{PullResponse, TakenRun, pull_response};
use coral_run_executor::{
    Credentials, LocalChild, LocalRunExecutors, ProcessStarter, PullStreams, Token,
};
use tokio::sync::mpsc::UnboundedReceiver;
use tokio::time::Instant;

use crate::take::{Pace, TAKE_MAX};

/// A backstop that does not come during a test: the loop takes at once and
/// when a Run starts to wait, then never again unless room appears.
const LONG: Duration = Duration::from_secs(3600);

/// Accepts `count` Runs for Run streams, oldest first.
async fn waiting_runs(fixture: &TestModules, runs: &RunModule, count: usize) -> Vec<String> {
    let (workspace_id, principal) = fixture.user_scope("taker", "Take queue").await;
    waiting_runs_in(runs, &principal, &workspace_id, count).await
}

/// [`waiting_runs`] for `principal` in `workspace_id`.
async fn waiting_runs_in(
    runs: &RunModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    count: usize,
) -> Vec<String> {
    let mut ids = Vec::new();
    for _ in 0..count {
        let update = runs
            .start(principal, workspace_id, submission("return 1;"))
            .await
            .expect("accept a Run");
        ids.push(update.run_id().to_string());
    }
    ids
}

fn take_loop(runs: &RunModule, streams: &PullStreams, backstop: Duration) {
    let (runs, streams) = (runs.clone(), streams.clone());
    tokio::spawn(async move { runs.take_loop(streams, None, backstop).await });
}

/// A process starter that counts its starts; each child it starts runs
/// until the supervisor stops and never opens a pull stream.
#[derive(Clone, Default)]
struct CountingStarter(Arc<std::sync::atomic::AtomicUsize>);

impl CountingStarter {
    fn starts(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl ProcessStarter for CountingStarter {
    fn start(&mut self, _token: &Token) -> std::io::Result<Box<dyn LocalChild>> {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(Box::new(SilentChild))
    }
}

struct SilentChild;

impl LocalChild for SilentChild {
    fn close_stdin(&mut self) {}
    fn kill(&mut self) {}
    fn exited(&mut self) -> Option<Option<i32>> {
        None
    }
}

/// Catches: a Run that waits while no pull stream has room and starts no
/// local child.
#[tokio::test]
async fn a_run_that_waits_with_no_room_asks_for_a_local_child() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let streams = PullStreams::default();
    let starter = CountingStarter::default();
    let children = LocalRunExecutors::start(
        starter.clone(),
        1,
        Credentials::default(),
        streams.clone(),
        None,
    );
    let (taker, held) = (runs.clone(), streams.clone());
    let loop_children = children.clone();
    tokio::spawn(async move {
        taker
            .take_loop(held, Some(loop_children), Duration::from_millis(10))
            .await
    });
    waiting_runs(&fixture, &runs, 1).await;
    tokio::time::timeout(EVENTUALLY, async {
        while starter.starts() == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("a local child starts for the waiting Run");
    children.stop();
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
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
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
            run.holder_instance_id,
            run.execution_claim_id,
        ),
        (None, None, None)
    );
    assert!(runs.owners.get(update.run_id()).is_none());
}

/// Catches: a decode that rejects a taken Run, or accepts a take's claim
/// with no start mark on a Run that has ended; only a running Run waits
/// taken.
#[tokio::test]
async fn a_take_claim_with_no_start_mark_decodes_only_while_the_run_is_running() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("ender", "Taken and ended").await;
    let mut ended = Vec::new();
    for (keeps_claim, claim) in [(true, "execution_claim_id"), (false, "NULL")] {
        let update = runs
            .start(&principal, &workspace_id, submission("return 1;"))
            .await
            .expect("accept a Run");
        ended.push((update.run_id().clone(), keeps_claim, claim));
    }
    let streams = PullStreams::default();
    let (_stream, mut sent) = streams.scripted("ender", 2);
    take_loop(&runs, &streams, LONG);
    let taken = next_taken(&mut sent).await;
    for (run_id, ..) in &ended {
        let claim = taken
            .iter()
            .find(|run| run.run_id == run_id.to_string())
            .map(|run| run.claim_id.clone());
        let run = fixture.run_record(&workspace_id, run_id).await;
        assert_eq!(
            (run.state, run.execution_claim_id),
            (RunState::Running, claim),
            "a taken Run reads running with its claim"
        );
    }

    let succeeded = RunState::Succeeded.as_storage_str();
    for (run_id, keeps_claim, claim) in &ended {
        fixture
            .inject_fault(&format!(
                "UPDATE program_run_records SET state = '{succeeded}', \
                 terminal_at_unix_nanos = accepted_at_unix_nanos + 1, \
                 execution_claim_id = {claim} WHERE id = '{run_id}'"
            ))
            .await;
        let decoded = fixture
            .try_run_record(&workspace_id, run_id)
            .await
            .map(|run| run.state);
        match (keeps_claim, decoded) {
            (true, Err(Error::CorruptData(_))) | (false, Ok(RunState::Succeeded)) => {}
            other => panic!("{other:?}"),
        }
    }
}

#[tokio::test]
async fn a_take_claims_the_oldest_runs_for_the_stream_with_the_most_room() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    // Older than every waiting Run, and an older release's Run Owner's:
    // never taken.
    let (workspace_id, principal) = fixture.user_scope("owned", "Owned").await;
    let owned = fixture
        .staged_run(&principal, &workspace_id, "return 1;")
        .await;
    fixture
        .inject_fault(&format!(
            "UPDATE program_run_records SET worker_claim_id = 'owner-claim', \
             worker_claim_expires_at_unix_nanos = accepted_at_unix_nanos WHERE id = '{owned}'"
        ))
        .await;
    let accepted = waiting_runs(&fixture, &runs, 3).await;
    let streams = PullStreams::default();
    let (_small, mut small_sent) = streams.scripted("small", 2);
    let (large, mut large_sent) = streams.scripted("large", 5);

    take_loop(&runs, &streams, LONG);
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
async fn one_take_claims_at_most_the_cap_and_the_pass_repeats_for_the_rest() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let cap = usize::try_from(TAKE_MAX).expect("the cap");
    let accepted = waiting_runs(&fixture, &runs, cap + 1).await;
    let streams = PullStreams::default();
    let (_stream, mut sent) = streams.scripted("roomy", TAKE_MAX + 1);

    take_loop(&runs, &streams, LONG);
    let first = next_taken(&mut sent).await;
    let second = next_taken(&mut sent).await;

    let (oldest, rest) = accepted.split_at(cap);
    assert_eq!(
        (ids(&first).as_slice(), ids(&second).as_slice()),
        (oldest, rest)
    );
}

#[tokio::test]
async fn no_room_takes_nothing_and_new_room_takes_at_once() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let accepted = waiting_runs(&fixture, &runs, 1).await;
    let streams = PullStreams::default();
    let (_full, mut full_sent) = streams.scripted("full", 0);

    take_loop(&runs, &streams, LONG);
    // On this one thread the spawned loop runs to its wait for room.
    tokio::task::yield_now().await;
    let run_id = accepted.first().expect("one Run");
    assert_eq!(
        take_row(&fixture.database, run_id).await.0,
        None,
        "no room, no take"
    );
    let (_opened, mut opened_sent) = streams.scripted("opened", 1);

    assert_eq!(ids(&next_taken(&mut opened_sent).await), accepted);
    assert!(
        full_sent.try_recv().is_err(),
        "a stream with no room gets none"
    );
}

/// Catches: a loop that waits for its backstop after a Run's acceptance
/// rang the queue signal, or a take that rings it and wakes the loop for
/// nothing.
#[tokio::test]
async fn a_run_accepted_while_the_loop_waits_is_taken_when_the_queue_signal_rings() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let (workspace_id, principal) = fixture.user_scope("ringer", "Rung").await;
    let mut queue = fixture
        .database
        .subscribe_signal(crate::run_repository::QUEUE)
        .expect("subscribe to the queue signal");
    let first = waiting_runs_in(&runs, &principal, &workspace_id, 1).await;
    // Postgres delivers the acceptance's ring after its commit: hear it
    // here, so a later ring is the take's.
    tokio::time::timeout(EVENTUALLY, queue.changed())
        .await
        .expect("the acceptance rings the queue signal")
        .expect("the signal is available");
    let streams = PullStreams::default();
    let (_stream, mut sent) = streams.scripted("roomy", 2);
    // A backstop that does not come during the test.
    take_loop(&runs, &streams, LONG);
    assert_eq!(ids(&next_taken(&mut sent).await), first);
    tokio::time::timeout(Duration::from_millis(50), queue.changed())
        .await
        .expect_err("a take rings no queue signal");

    // The pass ended with nothing left waiting, so only the queue signal
    // takes this one.
    let later = waiting_runs_in(&runs, &principal, &workspace_id, 1).await;
    assert_eq!(ids(&next_taken(&mut sent).await), later);
}

#[tokio::test]
async fn the_backstop_takes_a_run_that_nothing_rang_for() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let accepted = waiting_runs(&fixture, &runs, 1).await;
    let run_id = accepted.first().expect("one Run");
    let streams = PullStreams::default();
    let (_stream, mut sent) = streams.scripted("roomy", 2);
    take_loop(&runs, &streams, Duration::from_millis(50));
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
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.runs.clone();
    let cap = usize::try_from(TAKE_MAX).expect("the cap");
    // One more than both streams hold.
    let accepted = waiting_runs(&fixture, &runs, 2 * cap + 1).await;
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
        Arc::new(ImmediateEngine),
        fixture.upstream.clone(),
        fixture.clock.clone(),
        crate::TestObservers::new(
            Arc::new(NoopInvocationObserver),
            Arc::new(NoopExecutionObserver),
        ),
    );
    let (one, two) = (PullStreams::default(), PullStreams::default());
    let (_first, mut one_sent) = one.scripted("one", TAKE_MAX);
    let (_second, mut two_sent) = two.scripted("two", TAKE_MAX);

    take_loop(&runs, &one, LONG);
    take_loop(&other, &two, LONG);
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
fn the_pace_takes_at_once_on_a_ring_and_keeps_a_backstop() {
    let backstop = Duration::from_secs(1);
    let start = Instant::now();
    let after = |millis| start + Duration::from_millis(millis);
    let now = after(10);
    let passed = || {
        let mut pace = Pace::new(backstop);
        pace.passed(start);
        pace
    };
    let rang = |rings| {
        let mut pace = passed();
        (0..rings).for_each(|_ring| pace.ring());
        pace
    };
    let mut rang_then_passed = rang(10);
    rang_then_passed.passed(after(5));
    let cases = [
        (
            "a new loop takes at once",
            Pace::new(backstop),
            true,
            Some(now),
        ),
        ("no room, no take", rang(1), false, None),
        ("a ring takes at once", rang(1), true, Some(now)),
        (
            "many rings make one take",
            rang_then_passed,
            true,
            Some(after(1005)),
        ),
        (
            "nothing rang: the backstop",
            passed(),
            true,
            Some(after(1000)),
        ),
    ];
    for (case, pace, room, due) in cases {
        assert_eq!(pace.due(room, now), due, "{case}");
    }
}
