//! The Run queue pass: the Runs waiting, the oldest wait and the Runs held,
//! read from the database for the gauges every Coral Instance reports alike.
use super::*;
use coral_db::UnixNanos;
use coral_telemetry::testing::MetricCapture;
use coral_test_support::ManualClock;

use super::metrics::measured;
use super::run_streams::{Scripted, take};

const SECOND: i64 = 1_000_000_000;
const START: i64 = 1_000 * SECOND;

/// The gauges' one point each: the Runs waiting, the oldest wait in
/// seconds, and the Runs held.
fn gauges(capture: &MetricCapture) -> [f64; 3] {
    [
        "coral.run.waiting",
        "coral.run.waiting.age",
        "coral.run.held",
    ]
    .map(|name| match capture.named(name).as_slice() {
        [point] => point.value,
        points => panic!("{name} has one point: {points:?}"),
    })
}

/// Catches: a gauge that counts a taken, started or ended Run in the wrong
/// place, or an age that does not follow the clock the pass is given.
#[tokio::test]
async fn the_queue_gauges_move_by_the_runs_that_wait_and_the_runs_held() {
    let (capture, _measured) = measured();
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = &fixture.runs;
    let (workspace_id, principal) = fixture.user_scope("user:queue", "Queue").await;
    let clock = ManualClock::starting_at(UnixNanos::new(START));
    runs.read_queue(&clock).await.expect("read the queue");
    let [waiting, _, held] = gauges(&capture);

    fixture.clock.next.store(START, Ordering::SeqCst);
    let mut accepted = Vec::new();
    for _ in 0..7 {
        let run = runs
            .start(&principal, &workspace_id, submission("return 1;"))
            .await
            .expect("accept a Run");
        accepted.push(run.run_id().clone());
        fixture.clock.next.fetch_add(SECOND, Ordering::SeqCst);
    }
    let [_, started, oldest_waiting, _, _, older_release, newest] =
        <[_; 7]>::try_from(accepted).unwrap();
    // The two oldest are taken, and the second of them starts.
    take(runs).await;
    let claim = take(runs).await;
    let mut stream = Scripted::open(runs, &started.to_string(), &claim);
    stream.start().await;
    drop(stream);
    // An older release started this one and released its worker claim
    // when it recorded the Run's interruption: no Run Executor holds it.
    assert_eq!(
        fixture
            .inject_fault(&format!(
                "UPDATE program_run_records SET execution_claim_id = 'an-older-release', \
                 execution_started_at_unix_nanos = accepted_at_unix_nanos \
                 WHERE id = '{older_release}'"
            ))
            .await,
        1
    );
    // The newest ends before any take.
    runs.cancel(&principal, &workspace_id, &newest, &immediate(None, 1))
        .await
        .expect("cancel a waiting Run");
    let oldest_accepted = fixture
        .run_record(&workspace_id, &oldest_waiting)
        .await
        .accepted_at_unix_nanos;

    clock.set(UnixNanos::new(START + 10 * SECOND));
    runs.read_queue(&clock).await.expect("read the queue");
    let age = |now: i64| Duration::from_nanos(u64::try_from(now - oldest_accepted).unwrap());
    let [now_waiting, now_age, now_held] = gauges(&capture);
    assert_eq!(
        (now_waiting - waiting, now_held - held),
        (3.0, 2.0),
        "an ended Run and an older release's Run count nowhere"
    );
    assert_eq!(now_age, age(START + 10 * SECOND).as_secs_f64());

    clock.advance(Duration::from_secs(5));
    assert_eq!(
        gauges(&capture),
        [now_waiting, now_age, now_held],
        "the gauge reports the last read"
    );
    runs.read_queue(&clock).await.expect("read the queue");
    let [_, later_age, _] = gauges(&capture);
    assert_eq!(later_age, age(START + 15 * SECOND).as_secs_f64());
}

/// Catches: a queue read that sends a statement for each Run.
#[tokio::test]
async fn the_queue_read_sends_the_statements_of_few_runs_for_many() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    // A peer handle counts only its own statements.
    let reads = fixture.test_database.connect_peer().await.unwrap();
    let (workspace_id, principal) = fixture.user_scope("user:queue", "Queue").await;
    let clock = ManualClock::starting_at(UnixNanos::new(START));
    let mut sent = Vec::new();
    for count in [1, 40] {
        for _ in 0..count {
            fixture
                .runs
                .start(&principal, &workspace_id, submission("return 1;"))
                .await
                .expect("accept a Run");
            fixture.clock.next.fetch_add(SECOND, Ordering::SeqCst);
        }
        take(&fixture.runs).await;
        let before = reads.statements_sent();
        let queue = crate::queue::read(&reads, &clock)
            .await
            .expect("read the queue");
        sent.push((reads.statements_sent() - before, queue.held));
    }
    let [(few, few_held), (many, many_held)] = <[_; 2]>::try_from(sent).unwrap();
    assert_eq!(few, many);
    assert_eq!(many_held - few_held, 1, "each pass read the take before it");
}
