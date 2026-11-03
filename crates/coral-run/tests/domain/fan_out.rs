//! The Fan-out Limit (ADR 0018): at most the limit's number of one Run's
//! Invocations are `dispatching` at once. Admission stays eager; a ready
//! Invocation above the limit stays `pending`, with no Event and no required
//! action, and starts its dispatch in readiness order when an earlier one
//! ends; a wait for an approval or a refresh holds no slot, so every Approval
//! Request is visible at once; a Run terminated while a call waits for a slot
//! ends it as `run_terminated`. The owner's in-memory rule has its own rows in
//! the `apply` table (`owner.rs`).
use super::*;

use crate::approval::{ApprovalDecision, ApprovalState};
use crate::{FanOutLimit, InvocationResultKind};

use axum::http::StatusCode;

/// How long a call that must not move is watched before it is called still.
const STILL: Duration = Duration::from_millis(300);
/// How long a call may take to reach the enforcement point once its owner wakes
/// its parked wait, on a loaded test machine.
const UNTIL_ENFORCED: Duration = Duration::from_secs(10);

const WEATHER: &str = "coral.providers.weather.getForecast";
const ALERTS: &str = "coral.providers.alerts.getItems";

/// How a [`FanOutRun`] is arranged: the Fan-out Limit, the enforcement
/// observer, whether the `weather` Tool waits for an approval, and whether its
/// Provider is an OAuth Provider.
struct Setup {
    user: &'static str,
    limit: usize,
    latch: Arc<EnforcementLatch>,
    approval_required: bool,
    oauth: Option<OAuth>,
}

/// The `weather` Provider as an OAuth Provider with the User's Account
/// connected through `token_url`, served from `server`.
struct OAuth {
    server: MockServer,
    token_url: String,
}

impl Setup {
    fn new(user: &'static str) -> Self {
        Self {
            user,
            limit: 2,
            latch: EnforcementLatch::counting(),
            approval_required: false,
            oauth: None,
        }
    }
}

/// A Run under a Fan-out Limit whose calls the test starts one at a time,
/// so readiness order is the test's own, and whose upstream sends it ends
/// one at a time. `weather` is the fixture's `getForecast`; `alerts` is a
/// second credential-free Tool of the same Workspace.
struct FanOutRun {
    fixture: TestModules,
    runs: RunModule,
    upstream: Arc<SequencedUpstream>,
    latch: Arc<EnforcementLatch>,
    calls: ScriptedCalls,
    tool: ToolFixture,
    run_id: crate::RunId,
    /// The OAuth Account's server and credential store, kept for the Run's life.
    _oauth: Option<(MockServer, tempfile::TempDir)>,
}

impl FanOutRun {
    async fn start(setup: Setup) -> Self {
        // The arrangement's future is large (the OAuth connection inlines
        // every step); boxed, each test's own future stays small.
        Box::pin(Self::arrange(setup)).await
    }

    async fn arrange(setup: Setup) -> Self {
        let upstream = SequencedUpstream::new(HttpOutcome::new(200, serde_json::json!({})));
        let (engine, calls) = ScriptedCallsEngine::new();
        let fixture = TestModules::with_invocation_adapters(
            Arc::new(engine),
            upstream.clone(),
            setup.latch.clone(),
        )
        .await;
        let tool = match &setup.oauth {
            Some(oauth) => {
                fixture
                    .user_tool_at(setup.user, "Fan out", &oauth.server.uri())
                    .await
            }
            None => fixture.user_tool(setup.user, "Fan out").await,
        };
        assert_eq!(tool.call_path, WEATHER);
        fixture
            .publish_tool_under(&tool, "alerts", "getItems")
            .await;
        if setup.approval_required {
            fixture.require_approval(&tool).await;
        }
        let mut runs = fixture
            .runs
            .clone()
            .with_fan_out_limit(FanOutLimit::new(setup.limit).expect("a limit"));
        let mut kept = None;
        if let Some(oauth) = setup.oauth {
            let (directory, accounts) =
                oauth_account(&fixture, &tool, &oauth.server, &oauth.token_url).await;
            runs = runs.with_accounts(accounts);
            kept = Some((oauth.server, directory));
        }
        let run_id = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission("scripted calls"),
            )
            .await
            .expect("accept the Program")
            .run_id()
            .clone();
        Self {
            fixture,
            runs,
            upstream,
            latch: setup.latch,
            calls,
            tool,
            run_id,
            _oauth: kept,
        }
    }

    async fn rows(&self) -> Vec<InvocationRow> {
        self.fixture
            .invocation_rows(&self.tool.workspace_id, &self.run_id)
            .await
    }

    async fn events(&self) -> Vec<RunEvent> {
        self.fixture
            .events_after(&self.tool.workspace_id, &self.run_id, 0)
            .await
    }

    async fn observe(&self) -> crate::RunUpdate {
        self.runs
            .observe(
                &self.tool.principal,
                &self.tool.workspace_id,
                &self.run_id,
                &immediate(None, 100),
            )
            .await
            .expect("observe the Run")
    }

    /// Starts one call and waits until it is admitted: its row exists.
    async fn admit(&self, call_path: &str, admitted_so_far: usize) {
        self.calls.invoke(call_path);
        wait_until_async(async || self.rows().await.len() > admitted_so_far).await;
    }

    /// Starts one call and waits until its send reached the upstream, which
    /// a dispatch does only after its enforcement check committed.
    async fn dispatch(&self, call_path: &str, sent_so_far: usize) {
        self.calls.invoke(call_path);
        self.upstream.until_sent(sent_so_far + 1).await;
    }

    /// A call that waits for a slot sits still: `pending` with no dispatch
    /// start, no new Event over [`STILL`], no enforcement check past
    /// `attempts_so_far`, and no required action.
    async fn assert_slot_wait_is_silent(&self, waiting: &[&str], attempts_so_far: usize) {
        let events = self.events().await.len();
        tokio::time::sleep(STILL).await;
        let rows = self.rows().await;
        for call_path in waiting {
            assert_waiting(&rows, call_path);
        }
        assert_eq!(
            self.events().await.len(),
            events,
            "a slot wait writes no Event"
        );
        assert_eq!(
            self.latch.enforcement_checks.len(),
            attempts_so_far,
            "no enforcement check ran for a waiting call"
        );
        assert!(
            self.observe().await.required_actions().is_empty(),
            "a slot wait is never a required action"
        );
    }

    /// Ends the Program and returns the Run's terminal update.
    async fn finish(&self) -> crate::RunUpdate {
        self.calls.finish();
        wait_for_terminal(
            &self.runs,
            &self.tool.principal,
            &self.tool.workspace_id,
            &self.run_id,
        )
        .await
    }
}

/// The rows in `state`.
fn in_state<'a>(rows: &'a [InvocationRow], state: &str) -> Vec<&'a InvocationRow> {
    rows.iter().filter(|row| row.state == state).collect()
}

/// The Call Paths of `rows`, sorted.
fn paths<'a>(rows: &[&'a InvocationRow]) -> Vec<&'a str> {
    let mut paths: Vec<&'a str> = rows.iter().map(|row| row.call_path.as_str()).collect();
    paths.sort_unstable();
    paths
}

/// The one `call_path` row that waits: `pending`, with no dispatch start.
fn assert_waiting(rows: &[InvocationRow], call_path: &str) {
    let waiting: Vec<_> = rows
        .iter()
        .filter(|row| row.call_path == call_path && row.state == "pending")
        .collect();
    let [waiting] = waiting.as_slice() else {
        panic!("one {call_path} row waits: {rows:?}");
    };
    assert_eq!(waiting.dispatch_started_at_unix_nanos, None, "{rows:?}");
}

fn assert_succeeded(terminal: &crate::RunUpdate, outcomes: &[&str]) {
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(outcomes)),
        "{terminal:?}"
    );
}

/// Two calls dispatch at once; a third and a fourth ready call stay `pending`
/// with no Event and no required action, and dispatch in the order they
/// became ready, each as an earlier dispatch ends. Every row ends with its
/// upstream outcome.
#[tokio::test]
async fn ready_calls_above_the_limit_wait_in_memory_and_dispatch_in_readiness_order() {
    let run = FanOutRun::start(Setup::new("user:fan-out")).await;
    run.dispatch(WEATHER, 0).await;
    run.dispatch(WEATHER, 1).await;
    // The third (alerts) and the fourth (weather) become ready in this order.
    run.admit(ALERTS, 2).await;
    run.latch.until_arrived_within(3, UNTIL_ENFORCED).await;
    run.admit(WEATHER, 3).await;
    run.latch.until_arrived_within(4, UNTIL_ENFORCED).await;
    run.assert_slot_wait_is_silent(&[ALERTS, WEATHER], 2).await;
    let rows = run.rows().await;
    assert_eq!(paths(&in_state(&rows, "dispatching")), [WEATHER, WEATHER]);
    assert_eq!(paths(&in_state(&rows, "pending")), [ALERTS, WEATHER]);

    // The first dispatch ends: the call that became ready first (alerts)
    // takes its slot; the fourth keeps waiting.
    run.upstream.release_one();
    run.upstream.until_sent(3).await;
    let rows = run.rows().await;
    let ended = in_state(&rows, "ended");
    let [ended] = ended.as_slice() else {
        panic!("one call ended: {rows:?}");
    };
    assert_eq!(ended.call_path, WEATHER);
    assert_eq!(
        ended.result_kind,
        Some(InvocationResultKind::UpstreamOutcome)
    );
    assert_eq!(paths(&in_state(&rows, "dispatching")), [ALERTS, WEATHER]);
    assert_waiting(&rows, WEATHER);
    let third = rows
        .iter()
        .find(|row| row.call_path == ALERTS)
        .expect("the third call");
    assert!(
        third.dispatch_started_at_unix_nanos >= ended.ended_at_unix_nanos,
        "the third dispatch started no earlier than the first ended: {rows:?}"
    );

    // The second dispatch ends: the fourth call takes its slot.
    run.upstream.release_one();
    run.upstream.until_sent(4).await;
    let rows = run.rows().await;
    assert_eq!(in_state(&rows, "ended").len(), 2, "{rows:?}");
    assert_eq!(paths(&in_state(&rows, "dispatching")), [ALERTS, WEATHER]);
    assert!(in_state(&rows, "pending").is_empty(), "{rows:?}");
    assert_eq!(run.latch.enforcement_checks.len(), 4);

    run.upstream.release_all();
    assert_succeeded(&run.finish().await, &["ok", "ok", "ok", "ok"]);
    let rows = run.rows().await;
    assert_eq!(in_state(&rows, "ended").len(), 4, "{rows:?}");
    assert!(
        rows.iter().all(|row| {
            row.result_kind == Some(InvocationResultKind::UpstreamOutcome)
                && row.dispatch_started_at_unix_nanos.is_some()
        }),
        "{rows:?}"
    );
}

/// A call parked on its approval holds no slot: two later ready calls both
/// dispatch while it waits, and its Approval Request is visible meanwhile.
/// Approved, it is ready and waits for a slot, still `pending` with no Event
/// and no required action, then dispatches when an earlier call ends.
#[tokio::test]
async fn a_call_parked_on_approval_holds_no_slot_and_its_request_stays_visible() {
    let run = FanOutRun::start(Setup {
        approval_required: true,
        ..Setup::new("user:fan-out-approval")
    })
    .await;
    let approvals = run.fixture.approvals();
    run.admit(WEATHER, 0).await;
    let request = wait_for_approval(
        &approvals,
        &run.tool.principal,
        &run.tool.workspace_id,
        &run.run_id,
    )
    .await;
    assert_eq!(request.state, ApprovalState::Pending);
    run.dispatch(ALERTS, 0).await;
    run.dispatch(ALERTS, 1).await;

    let rows = run.rows().await;
    assert_eq!(paths(&in_state(&rows, "dispatching")), [ALERTS, ALERTS]);
    assert_waiting(&rows, WEATHER);
    let pending: Vec<_> = approvals
        .list_for_run(&run.tool.principal, &run.tool.workspace_id, &run.run_id)
        .await
        .expect("list the Run's Approval Requests")
        .into_iter()
        .filter(|listed| listed.state == ApprovalState::Pending)
        .map(|listed| listed.id)
        .collect();
    assert_eq!(
        pending.as_slice(),
        std::slice::from_ref(&request.id),
        "the Approval Request is visible while both slots are taken"
    );
    assert_eq!(run.observe().await.required_actions().len(), 1);
    assert_eq!(
        run.latch.enforcement_checks.len(),
        2,
        "the parked call never reached the enforcement point"
    );

    // Approved, the call is ready: it waits for a slot, with nothing to see.
    approvals
        .decide(
            &run.tool.principal,
            &run.tool.workspace_id,
            &request.id,
            ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"f".repeat(32)).unwrap(),
        )
        .await
        .expect("approve the call");
    run.latch.until_arrived_within(3, UNTIL_ENFORCED).await;
    run.assert_slot_wait_is_silent(&[WEATHER], 2).await;
    assert_eq!(
        paths(&in_state(&run.rows().await, "dispatching")),
        [ALERTS, ALERTS]
    );

    run.upstream.release_one();
    run.upstream.until_sent(3).await;
    let rows = run.rows().await;
    assert_eq!(paths(&in_state(&rows, "dispatching")), [ALERTS, WEATHER]);
    assert_eq!(paths(&in_state(&rows, "ended")), [ALERTS]);

    run.upstream.release_all();
    assert_succeeded(&run.finish().await, &["ok", "ok", "ok"]);
}

/// A Run canceled while a call waits for a slot ends that call as
/// `run_terminated`, with no dispatch start; the two dispatching calls end
/// indeterminate, and nothing more is sent.
#[tokio::test]
async fn a_run_terminated_while_a_call_waits_for_a_slot_ends_it_as_run_terminated() {
    let run = FanOutRun::start(Setup::new("user:fan-out-cancel")).await;
    run.dispatch(WEATHER, 0).await;
    run.dispatch(WEATHER, 1).await;
    run.admit(ALERTS, 2).await;
    run.latch.until_arrived_within(3, UNTIL_ENFORCED).await;
    run.assert_slot_wait_is_silent(&[ALERTS], 2).await;

    let canceled = run
        .runs
        .cancel(
            &run.tool.principal,
            &run.tool.workspace_id,
            &run.run_id,
            &immediate(None, 100),
        )
        .await
        .expect("cancel the Run");
    assert_eq!(canceled.state(), RunState::Canceled);
    let terminal = wait_for_terminal(
        &run.runs,
        &run.tool.principal,
        &run.tool.workspace_id,
        &run.run_id,
    )
    .await;
    assert_eq!(terminal.state(), RunState::Canceled);

    let rows = run.rows().await;
    let waiting = rows
        .iter()
        .find(|row| row.call_path == ALERTS)
        .expect("the waiting call");
    assert_eq!(waiting.state, "ended", "{rows:?}");
    assert_eq!(
        waiting.result_kind,
        Some(InvocationResultKind::RunTerminated)
    );
    assert_eq!(waiting.dispatch_started_at_unix_nanos, None);
    assert_eq!(waiting.failure_code, None);
    assert!(
        run.events().await.iter().any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                invocation_id,
                result_kind: InvocationResultKind::RunTerminated,
            } if invocation_id == &waiting.id
        )),
        "the slot wait ends with `invocation.ended` as run_terminated"
    );
    let dispatching: Vec<_> = rows.iter().filter(|row| row.call_path == WEATHER).collect();
    assert_eq!(dispatching.len(), 2);
    assert!(
        dispatching.iter().all(|row| {
            row.state == "ended"
                && row.result_kind == Some(InvocationResultKind::Indeterminate)
                && row.dispatch_started_at_unix_nanos.is_some()
        }),
        "{rows:?}"
    );
    assert_eq!(
        run.upstream.sends.load(Ordering::SeqCst),
        2,
        "nothing more was sent"
    );
}

/// A refresh wait parked by the enforcement point holds no slot: under a limit
/// of one, a call whose token the enforcement point found expired frees its
/// slot while it refreshes outside every lock, a later ready call dispatches
/// meanwhile, and the refreshed call then waits for that slot like any ready
/// call.
#[tokio::test]
async fn a_refresh_wait_parked_by_the_enforcement_point_holds_no_slot() {
    /// The token expires this long after the connection; the Run deadline is
    /// a minute away.
    const TOKEN_LIFETIME_SECS: u64 = 1;
    let endpoint = TokenEndpoint::with_initial_expiry(StatusCode::OK, TOKEN_LIFETIME_SECS).await;
    // Arrivals are held before their enforcement point once: the token expires
    // while the first call stands there with its credentials prepared, so the
    // enforcement point, not the selection before it, finds the token expired.
    let run = FanOutRun::start(Setup {
        limit: 1,
        latch: EnforcementLatch::holding_arrivals(),
        oauth: Some(OAuth {
            server: MockServer::start().await,
            token_url: endpoint.url.clone(),
        }),
        ..Setup::new("user:fan-out-refresh")
    })
    .await;

    // The enforcement point parks the OAuth call on its refresh, which the
    // endpoint holds.
    run.calls.invoke(WEATHER);
    run.latch.until_arrived(1).await;
    run.fixture.clock.next.store(
        i64::try_from(TOKEN_LIFETIME_SECS).unwrap() * 1_000_000_000 + 1_000,
        Ordering::SeqCst,
    );
    run.latch.release_arrivals();
    tokio::time::timeout(Duration::from_secs(5), endpoint.entered.notified())
        .await
        .expect("the parked call refreshes its token outside the enforcement point");
    // The one slot is free: a ready call dispatches while the refresh is held.
    run.dispatch(ALERTS, 0).await;
    let rows = run.rows().await;
    assert_eq!(paths(&in_state(&rows, "dispatching")), [ALERTS], "{rows:?}");
    assert_waiting(&rows, WEATHER);
    assert_eq!(endpoint.exchanges.load(Ordering::SeqCst), 1);

    // Refreshed, the call is ready again and waits for the slot the other
    // holds; it dispatches when that one ends.
    endpoint.release.notify_one();
    run.assert_slot_wait_is_silent(&[WEATHER], 2).await;
    run.upstream.release_one();
    run.upstream.until_sent(2).await;
    let rows = run.rows().await;
    assert_eq!(
        paths(&in_state(&rows, "dispatching")),
        [WEATHER],
        "{rows:?}"
    );
    assert_eq!(paths(&in_state(&rows, "ended")), [ALERTS], "{rows:?}");

    run.upstream.release_all();
    assert_succeeded(&run.finish().await, &["ok", "ok"]);
    assert_eq!(
        run.latch.enforcement_checks.pairs(),
        [(false, false), (true, true), (true, true)],
        "the refresh enforcement check, the ready call's, the refreshed call's"
    );
}
