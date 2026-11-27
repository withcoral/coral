//! The Run sweeper: what it ends, expires, puts back and releases when no
//! Run stream decided in time, and that it touches nothing else.
use super::*;
use crate::approval::ApprovalState;
use crate::run_repository::Taker;
use crate::sweep::Pass;
use crate::{RunId, Swept};
use coral_db::{DatabaseSystem, UnixNanos};
use coral_run_executor::Kind;
use coral_run_executor::leases;
use coral_run_executor::wire::v1::RunEnd as WireEnd;

use super::metrics::{counts, measured, owned};
use super::run_streams::{Scripted, ended, invoke_forecast, take_for};

const SECOND: i64 = 1_000_000_000;
/// Where the clock starts, so a time ten seconds earlier is still positive.
const START: i64 = 1_000 * SECOND;
const LIVE: &str = "6f0c2a59-55a7-4e0b-9a2e-7c1d4f3b8e21";
const LAPSED: &str = "0b9d6c1e-2f3a-4b5c-8d7e-9f0a1b2c3d4e";
const LOCAL: &str = "5a4b3c2d-1e0f-4a9b-8c7d-6e5f4a3b2c1d";

/// One fixture whose Runs wait for a take, and a Tool that asks for approval.
struct Sweep {
    fixture: TestModules,
    runs: RunModule,
    tool: ToolFixture,
}

impl Sweep {
    async fn new() -> Self {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        fixture.clock.next.store(START, Ordering::SeqCst);
        let runs = fixture.runs.clone();
        let tool = fixture.user_tool("user:sweep", "Sweep").await;
        fixture.require_approval(&tool).await;
        Self {
            fixture,
            runs,
            tool,
        }
    }

    fn now(&self) -> i64 {
        self.fixture.clock.next.load(Ordering::SeqCst)
    }

    fn advance(&self, by: i64) {
        self.fixture.clock.next.fetch_add(by, Ordering::SeqCst);
    }

    /// Applies fixture SQL that writes one row.
    async fn sql(&self, sql: &str) {
        assert_eq!(self.fixture.inject_fault(sql).await, 1, "{sql}");
    }

    async fn lease(&self, id: &str, kind: Kind, lease_until: i64) {
        self.sql(&format!(
            "INSERT INTO run_executors (id, kind, lease_until) \
             VALUES ('{id}', '{}', {lease_until})",
            kind.as_storage_str()
        ))
        .await;
    }

    async fn set(&self, run_id: &RunId, assignment: &str) {
        self.sql(&format!(
            "UPDATE program_run_records SET {assignment} WHERE id = '{run_id}'"
        ))
        .await;
    }

    /// A Run taken for `run_executor_id` at `taken_at`, never started.
    async fn taken(&self, run_executor_id: &str, kind: Kind, taken_at: i64) -> (RunId, String) {
        let accepted = self
            .runs
            .start(
                &self.tool.principal,
                &self.tool.workspace_id,
                submission("return 1;"),
            )
            .await
            .expect("accept a Run");
        let taker = Taker {
            run_executor_id,
            run_executor_kind: kind,
        };
        let claim = take_for(&self.runs, &taker, taken_at).await;
        (accepted.run_id().clone(), claim)
    }

    /// A Run taken for `run_executor_id` and started on a stream, which ran
    /// `requests` and then closed: no loop holds the Run.
    async fn detached(
        &self,
        run_executor_id: &str,
        kind: Kind,
        requests: impl AsyncFnOnce(&mut Scripted, &RunId),
    ) -> RunId {
        let (run_id, claim) = self.taken(run_executor_id, kind, self.now()).await;
        let mut stream = Scripted::open(&self.runs, &run_id.to_string(), &claim);
        stream.start().await;
        requests(&mut stream, &run_id).await;
        drop(stream);
        wait_until(|| self.runs.loops.get(&run_id).is_none()).await;
        run_id
    }

    async fn started(&self, run_executor_id: &str, kind: Kind) -> RunId {
        self.detached(run_executor_id, kind, async |_, _| {}).await
    }

    async fn run(&self, run_id: &RunId) -> crate::lifecycle::RunRecord {
        self.fixture
            .run_record(&self.tool.workspace_id, run_id)
            .await
    }

    async fn events(&self, run_id: &RunId) -> Vec<RunEvent> {
        self.fixture
            .events_after(&self.tool.workspace_id, run_id, 0)
            .await
    }

    /// A started Run whose one Invocation waits for an Approval Request,
    /// already past its deadline, with no loop holding the Run.
    async fn waiting_for_approval(&self) -> (RunId, crate::approval::ApprovalRequest) {
        let approvals = self.fixture.approvals();
        let mut approval = None;
        let run_id = self
            .detached(LIVE, Kind::Dialed, async |stream, run_id| {
                stream.request(1, invoke_forecast());
                approval = Some(
                    wait_for_approval(
                        &approvals,
                        &self.tool.principal,
                        &self.tool.workspace_id,
                        run_id,
                    )
                    .await,
                );
            })
            .await;
        let approval = approval.expect("an Approval Request");
        self.sql(&format!(
            "UPDATE invocation_approvals SET deadline_at_unix_nanos = created_at_unix_nanos \
             WHERE id = '{}'",
            approval.id
        ))
        .await;
        (run_id, approval)
    }

    /// A Run an older release's Run Owner holds by its worker claim, past
    /// its deadline.
    async fn owned_past_deadline(&self) -> RunId {
        let owned = self
            .fixture
            .staged_run(&self.tool.principal, &self.tool.workspace_id, "return 1;")
            .await;
        self.set(
            &owned,
            &format!(
                "worker_claim_id = 'owner-claim', \
                 worker_claim_expires_at_unix_nanos = {}, \
                 execution_claim_id = 'owner-claim', \
                 execution_started_at_unix_nanos = accepted_at_unix_nanos, \
                 deadline_at_unix_nanos = accepted_at_unix_nanos",
                START + 60 * SECOND
            ),
        )
        .await;
        owned
    }

    /// Each Run's row and Events, to compare after a pass.
    async fn rows(&self, run_ids: &[&RunId]) -> Vec<(crate::lifecycle::RunRecord, Vec<RunEvent>)> {
        let mut rows = Vec::new();
        for run_id in run_ids {
            rows.push((self.run(run_id).await, self.events(run_id).await));
        }
        rows
    }

    /// The ids of every Run Executor row, in order.
    async fn lease_rows(&self) -> Vec<String> {
        let rows: Vec<(String,)> = self
            .fixture
            .database
            .fetch_all(
                Query::select()
                    .column(Alias::new("id"))
                    .from(Alias::new("run_executors"))
                    .order_by(Alias::new("id"), sea_query::Order::Asc)
                    .to_owned(),
            )
            .await
            .expect("read the rows");
        rows.into_iter().map(|(id,)| id).collect()
    }
}

/// Catches: an Approval Request past its deadline left pending on a Run with
/// no stream, or an expiry that is not one chunk under the Run's head.
#[tokio::test]
async fn a_pass_expires_an_approval_request_past_its_deadline_in_one_chunk() {
    let sweep = Sweep::new().await;
    let (run_id, approval) = sweep.waiting_for_approval().await;
    let head = sweep.run(&run_id).await.chunk_sequence;

    let swept = sweep.runs.sweep_runs().await.expect("sweep");

    assert_eq!(swept.map(|swept| swept.expired), Some(1));
    let run = sweep.run(&run_id).await;
    assert_eq!(
        (run.state, run.chunk_sequence),
        (RunState::Running, head + 1)
    );
    let expired = sweep
        .fixture
        .approvals()
        .get(
            &sweep.tool.principal,
            &sweep.tool.workspace_id,
            &approval.id,
        )
        .await
        .expect("read the request");
    assert_eq!(expired.state, ApprovalState::Expired);
    assert!(matches!(
        sweep.fixture.invocation_rows(&sweep.tool.workspace_id, &run_id).await.as_slice(),
        [row] if row.failure_code.as_deref() == Some("approval_expired")
    ));
}

/// Catches: a pass that misses a Run past its deadline, a stale take or a
/// lapsed Run Executor, or writes a Run it must leave: one before its
/// deadline, a recent take, a live Run Executor's, or one an older release's Run Owner holds.
#[tokio::test]
async fn one_pass_ends_puts_back_and_releases_only_what_no_stream_decided() {
    let sweep = Sweep::new().await;
    let lease = 60 * SECOND;
    sweep.lease(LIVE, Kind::Dialed, START + lease).await;
    sweep.lease(LAPSED, Kind::Dialed, START + 1).await;
    sweep.lease(LOCAL, Kind::Local, START + 1).await;

    let past_deadline = sweep.started(LIVE, Kind::Dialed).await;
    sweep
        .set(
            &past_deadline,
            "deadline_at_unix_nanos = accepted_at_unix_nanos",
        )
        .await;
    let before_deadline = sweep.started(LIVE, Kind::Dialed).await;
    let (stale_take, stale_claim) = sweep.taken(LIVE, Kind::Dialed, START - 11 * SECOND).await;
    let (recent_take, _) = sweep.taken(LIVE, Kind::Dialed, START - 9 * SECOND).await;
    let lapsed_started = sweep.started(LAPSED, Kind::Dialed).await;
    let (lapsed_unstarted, _) = sweep.taken(LAPSED, Kind::Dialed, sweep.now()).await;
    let owned = sweep.owned_past_deadline().await;
    let untouched = [&before_deadline, &recent_take, &owned];
    let kept = sweep.rows(&untouched).await;

    let swept = sweep
        .runs
        .sweep_runs()
        .await
        .expect("sweep")
        .expect("elected");

    assert_eq!(
        swept,
        Swept {
            ended: 2,
            expired: 0,
            put_back: 2,
            run_executors_removed: 2,
        }
    );
    assert_eq!(sweep.run(&past_deadline).await.state, RunState::TimedOut);
    assert_eq!(
        sweep.run(&lapsed_started).await.state,
        RunState::Interrupted
    );
    // Both unstarted takes wait again, and the first take's claim no longer
    // opens the Run.
    for run_id in [&stale_take, &lapsed_unstarted] {
        let run = sweep.run(run_id).await;
        assert_eq!(
            (run.state, run.execution_claim_id, run.run_executor_kind),
            (RunState::Running, None, None)
        );
    }
    let mut late = Scripted::open(&sweep.runs, &stale_take.to_string(), &stale_claim);
    assert_eq!(late.next().await, ended(WireEnd::NotHeld));
    assert_eq!(sweep.rows(&untouched).await, kept);
    // The live Run Executor keeps its row; the lapsed ones, now holding no
    // Run, lose theirs.
    assert_eq!(sweep.lease_rows().await, [LIVE]);
}

/// Catches: a put-back that is not counted, or is counted under the wrong
/// kind: a stale take and a lapsed local child's take each count once, and
/// a recent take counts nothing.
#[tokio::test]
async fn each_put_back_counts_once_by_the_kind_it_was_taken_for() {
    let (metrics, _guard) = measured();
    let sweep = Sweep::new().await;
    sweep.lease(LIVE, Kind::Dialed, START + 60 * SECOND).await;
    sweep.lease(LOCAL, Kind::Local, START + 1).await;
    sweep.taken(LIVE, Kind::Dialed, START - 11 * SECOND).await;
    sweep.taken(LIVE, Kind::Dialed, START - 9 * SECOND).await;
    sweep.taken(LOCAL, Kind::Local, sweep.now()).await;

    let swept = sweep
        .runs
        .sweep_runs()
        .await
        .expect("sweep")
        .expect("elected");

    assert_eq!(swept.put_back, 2);
    assert_eq!(
        counts(&metrics, "coral.run.put_backs"),
        owned(&[
            ("run_executor.kind=dialed", 1),
            ("run_executor.kind=local", 1)
        ])
    );
}

/// Catches: a release that ends a Run Executor's Runs after it renewed its
/// lease, between the pass's read and its write, or counts a put-back it
/// did not write.
#[tokio::test]
async fn a_run_executor_that_renews_between_the_read_and_the_write_loses_no_run() {
    let (metrics, _guard) = measured();
    let sweep = Sweep::new().await;
    sweep.lease(LAPSED, Kind::Dialed, START + 1).await;
    let started = sweep.started(LAPSED, Kind::Dialed).await;
    let (unstarted, _) = sweep.taken(LAPSED, Kind::Dialed, sweep.now()).await;
    let before = [sweep.run(&started).await, sweep.run(&unstarted).await];
    let read = leases::lapsed(&*sweep.fixture.database, UnixNanos::new(sweep.now()), 100)
        .await
        .expect("read the lapsed rows");
    let [lapsed] = read.as_slice() else {
        panic!("one lapsed row: {read:?}");
    };
    let renewed = sweep.now() + 60 * SECOND;
    sweep
        .sql(&format!(
            "UPDATE run_executors SET lease_until = {renewed} WHERE id = '{LAPSED}'"
        ))
        .await;

    let released = sweep
        .runs
        .release(lapsed, sweep.now(), 100)
        .await
        .expect("release");

    assert_eq!(released, Swept::default());
    assert_eq!(
        [sweep.run(&started).await, sweep.run(&unstarted).await],
        before
    );
    assert_eq!(counts(&metrics, "coral.run.put_backs"), Vec::new());
    let row = leases::find(&*sweep.fixture.database, LAPSED)
        .await
        .expect("read the row")
        .expect("the row stays");
    assert_eq!(row.lease_until(), UnixNanos::new(renewed));
}

/// Catches: two Coral Instances that both sweep, or one that never takes
/// over a holder that stopped.
#[tokio::test]
async fn two_instances_elect_one_sweeper_and_the_other_takes_over_once_its_lease_lapses() {
    let sweep = Sweep::new().await;
    let first = &sweep.runs;
    let second = sweep.fixture.next_process();
    let elected = async |runs: &RunModule| runs.sweep_runs().await.expect("sweep").is_some();

    assert!(elected(first).await);
    assert!(!elected(&second).await);
    sweep.advance(44 * SECOND);
    assert!(!elected(&second).await, "the first's lease holds");
    assert!(elected(first).await, "the holder renews");
    sweep.advance(46 * SECOND);
    assert!(elected(&second).await, "the first stopped renewing");
    assert!(!elected(first).await);
}

/// Catches: a boot pass on SQLite that waits for an earlier process's
/// election, leaves a local child's Run running or ends a dialed Run
/// Executor's; or a boot pass on Postgres that releases a live local child.
#[tokio::test]
async fn the_sqlite_boot_pass_ends_a_local_childs_run_and_leaves_a_dialed_ones() {
    let sweep = Sweep::new().await;
    let lease = START + 60 * SECOND;
    sweep.lease(LOCAL, Kind::Local, lease).await;
    sweep.lease(LIVE, Kind::Dialed, lease).await;
    let local = sweep.started(LOCAL, Kind::Local).await;
    let dialed = sweep.started(LIVE, Kind::Dialed).await;
    let sqlite = sweep.fixture.database.system() == DatabaseSystem::Sqlite;
    if sqlite {
        // An earlier process's election still holds.
        let earlier = sweep.fixture.next_process();
        assert!(earlier.sweep_runs().await.expect("sweep").is_some());
    }

    let swept = sweep
        .runs
        .sweep_runs_at_boot()
        .await
        .expect("the boot pass");

    assert!(swept.is_some(), "the boot pass sweeps");
    assert_eq!(
        sweep.run(&local).await.state,
        if sqlite {
            RunState::Interrupted
        } else {
            RunState::Running
        }
    );
    assert_eq!(sweep.run(&dialed).await.state, RunState::Running);
    let rows = sweep.lease_rows().await;
    assert_eq!(rows.contains(&LOCAL.to_owned()), !sqlite);
    assert!(rows.contains(&LIVE.to_owned()));
}

/// Catches: a local child's exit that leaves its Runs to its lease, or a
/// put-back that leaves its Run to the take loop's backstop.
#[tokio::test]
async fn a_local_childs_exit_releases_its_runs_at_once() {
    let sweep = Sweep::new().await;
    sweep.lease(LOCAL, Kind::Local, START + 60 * SECOND).await;
    let started = sweep.started(LOCAL, Kind::Local).await;
    let (unstarted, _) = sweep.taken(LOCAL, Kind::Local, sweep.now()).await;
    // The acceptances rang the queue signal. SQLite rings before a commit
    // returns. Postgres delivers a ring later, but only to a connection
    // that listened before the commit, so a new one hears the put-back alone.
    let listener = match sweep.fixture.database.system() {
        DatabaseSystem::Sqlite => sweep.fixture.database.clone(),
        DatabaseSystem::Postgres => Arc::new(
            sweep
                .fixture
                .test_database
                .connect_peer()
                .await
                .expect("a handle that listens after the acceptances"),
        ),
    };
    let mut queue = listener
        .subscribe_signal(crate::run_repository::QUEUE)
        .expect("subscribe to the queue signal");

    let released = sweep
        .runs
        .release_run_executor(LOCAL)
        .await
        .expect("release");
    tokio::time::timeout(EVENTUALLY, queue.changed())
        .await
        .expect("the put-back rings the queue signal")
        .expect("the signal is available");

    assert_eq!(
        released,
        Swept {
            ended: 1,
            put_back: 1,
            run_executors_removed: 1,
            ..Swept::default()
        }
    );
    assert_eq!(sweep.run(&started).await.state, RunState::Interrupted);
    assert_eq!(sweep.run(&unstarted).await.execution_claim_id, None);
    assert_eq!(sweep.lease_rows().await, Vec::<String>::new());
    assert_eq!(
        sweep
            .runs
            .release_run_executor(LOCAL)
            .await
            .expect("release again"),
        Swept::default()
    );
}

/// Catches: a pass that writes past its limit, or not oldest first.
#[tokio::test]
async fn a_pass_ends_the_oldest_deadlines_first_up_to_its_limit() {
    let sweep = Sweep::new().await;
    let mut runs = Vec::new();
    for _ in 0..3 {
        runs.push(sweep.started(LIVE, Kind::Dialed).await);
    }
    let base = sweep.now();
    for (run_id, after) in runs.iter().zip([3, 1, 2]) {
        sweep
            .set(
                run_id,
                &format!("deadline_at_unix_nanos = {}", base + after),
            )
            .await;
    }
    sweep.advance(SECOND);
    let states = async || {
        let mut states = Vec::new();
        for run_id in &runs {
            states.push(sweep.run(run_id).await.state);
        }
        states
    };

    let first = sweep.runs.sweep(Pass::Periodic, 2).await.expect("sweep");
    assert_eq!(first.map(|swept| swept.ended), Some(2));
    assert_eq!(
        states().await,
        [RunState::Running, RunState::TimedOut, RunState::TimedOut]
    );
    let next = sweep.runs.sweep(Pass::Periodic, 2).await.expect("sweep");
    assert_eq!(next.map(|swept| swept.ended), Some(1));
    assert_eq!(states().await, [RunState::TimedOut; 3]);
}

/// Catches: a pass that stops at a Run row it cannot decode, so the Runs
/// after it stay past their deadline, or that writes the row it could not
/// read.
#[tokio::test]
async fn a_pass_skips_a_run_it_cannot_decode_and_still_ends_the_rest() {
    let sweep = Sweep::new().await;
    // The pass reads by deadline: the undecodable Run, accepted first, has
    // the strictly earlier one.
    let undecodable = sweep.started(LIVE, Kind::Dialed).await;
    let decodable = sweep.started(LIVE, Kind::Dialed).await;
    sweep
        .set(
            &undecodable,
            "deadline_at_unix_nanos = accepted_at_unix_nanos",
        )
        .await;
    sweep
        .set(
            &decodable,
            "deadline_at_unix_nanos = accepted_at_unix_nanos + 1",
        )
        .await;
    let before = sweep.run(&undecodable).await;
    // A build that knows one more limit wrote it.
    let limits = "value_limits_json = replace(value_limits_json, '}', ',\"mcp_result_bytes\":1}')";
    sweep.set(&undecodable, limits).await;

    let swept = sweep.runs.sweep_runs().await.expect("sweep");

    assert_eq!(swept.map(|swept| swept.ended), Some(1));
    assert_eq!(sweep.run(&decodable).await.state, RunState::TimedOut);
    sweep
        .set(
            &undecodable,
            "value_limits_json = replace(value_limits_json, ',\"mcp_result_bytes\":1', '')",
        )
        .await;
    assert_eq!(
        sweep.run(&undecodable).await,
        before,
        "the pass wrote nothing"
    );
}
