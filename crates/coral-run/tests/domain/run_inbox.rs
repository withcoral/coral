//! Outside Run Requests reach the Run Owner through the Run Inbox (ADR 0025):
//! two Coral Instances share one database, and a cancel sent to either one is
//! recorded by the Run's owner.
use crate::tests::*;

/// A started Run that the fixture's Coral Instance owns, with its Program
/// running until it is stopped.
async fn running_run(
    fixture: &TestModules,
    engine: &StopAwareEngine,
    user_id: &str,
) -> (
    coral_workspace::WorkspaceId,
    AuthenticatedPrincipal,
    crate::RunId,
) {
    let (workspace_id, principal) = fixture.user_scope(user_id, "Run Inbox").await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("run-inbox"))
        .await
        .expect("accept Program");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    wait_until_started(fixture, &workspace_id, accepted.run_id()).await;
    (workspace_id, principal, accepted.run_id().clone())
}

/// A cancel sent to the Coral Instance that does not own the Run waits in
/// the Run Inbox until the owner records it: the Run ends canceled, once,
/// and the row is gone.
#[tokio::test]
async fn a_cancel_sent_to_another_coral_instance_is_recorded_and_ends_the_run() {
    let engine = Arc::new(StopAwareEngine::default());
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal, run_id) =
        running_run(&fixture, &engine, "user:inbox-elsewhere").await;
    let elsewhere = fixture.next_process();
    assert_ne!(elsewhere.instance(), fixture.runs.instance());

    let canceled = elsewhere
        .cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
        .await
        .expect("the other Coral Instance cancels");

    assert_eq!(canceled.state(), RunState::Canceled);
    assert!(engine.stopped.load(Ordering::SeqCst));
    assert_eq!(engine.executions.load(Ordering::SeqCst), 1);
    assert_eq!(
        cancellations(&fixture, &principal, &workspace_id, &run_id).await,
        1
    );
    assert_eq!(fixture.inbox_rows(&workspace_id, &run_id).await, 0);
}

/// While no chunk that records a cancel has landed, its row stays: the
/// reader delivers it again and the owner applies it once, and a repeat
/// sent to the other Coral Instance meanwhile waits. Once the chunk lands,
/// both receive the recorded answer and no row is left.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_whose_chunk_has_not_landed_is_delivered_again_and_its_repeat_waits() {
    let engine = Arc::new(StopAwareEngine::default());
    let (probe, release) = InboxProbe::holding_the_first_commit();
    let mut fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
    // Each cancel waits a shutdown grace past its arrival: long enough that
    // only the held chunk decides when it is answered.
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_shutdown_grace(Duration::from_secs(30));
    let (workspace_id, principal, run_id) =
        running_run(&fixture, &engine, "user:inbox-redelivery").await;
    let owner = fixture.runs.owners.get(&run_id).expect("live owner");
    let cancel = |runs: RunModule| {
        let (principal, workspace_id, run_id) =
            (principal.clone(), workspace_id.clone(), run_id.clone());
        tokio::spawn(async move {
            runs.cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
                .await
        })
    };

    let first = cancel(fixture.next_process());
    wait_until(|| owner.is_stopping() && probe.entered()).await;
    let delivered = probe.delivered();
    // Another Run's acceptance rings the change signal, and the reader reads
    // the waiting row again.
    fixture
        .runs
        .start(&principal, &workspace_id, submission("ring"))
        .await
        .expect("accept another Program");
    wait_until(|| probe.delivered() > delivered).await;
    let repeat = cancel(fixture.next_process());
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!first.is_finished() && !repeat.is_finished());
    assert_eq!(fixture.inbox_rows(&workspace_id, &run_id).await, 1);

    release.send(()).expect("land the held chunk");
    for cancel in [first, repeat] {
        let canceled = cancel.await.expect("join").expect("cancel");
        assert_eq!(canceled.state(), RunState::Canceled);
    }
    assert_eq!(
        cancellations(&fixture, &principal, &workspace_id, &run_id).await,
        1
    );
    assert_eq!(fixture.inbox_rows(&workspace_id, &run_id).await, 0);
}

/// A decision sent to the Coral Instance that does not own the Run waits in
/// the Run Inbox until the owner records it: the request is decided as its
/// decider asked, once, with the decider's trace, and the row is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_decision_sent_to_another_coral_instance_is_recorded() {
    use crate::approval::{ApprovalDecision, ApprovalState};
    let held = HeldApproval::new("user:inbox-decision").await;
    let (fixture, workspace_id, run_id) = (&held.fixture, &held.tool.workspace_id, &held.run);
    let crate::RequiredActionRequest::InvocationApproval(id) = &held.action().request else {
        panic!("an approval")
    };
    let trace = TraceId::parse(&"c".repeat(32)).unwrap();

    let approved = fixture
        .next_process()
        .approvals()
        .decide(
            &held.tool.principal,
            workspace_id,
            id,
            ApprovalDecision::Approve,
            Some("elsewhere"),
            &trace,
        )
        .await
        .expect("the other Coral Instance decides");

    assert_eq!(approved.state, ApprovalState::Approved);
    assert_eq!(
        approved.approver.as_ref(),
        Some(held.tool.principal.principal())
    );
    assert_eq!(approved.comment.as_deref(), Some("elsewhere"));
    let events = held
        .approvals
        .list_lifecycle_events(&held.tool.principal, workspace_id, id)
        .await
        .unwrap();
    assert!(
        matches!(events.as_slice(), [event] if event.state == ApprovalState::Approved && event.trace_id == trace),
        "{events:?}"
    );
    assert_eq!(fixture.inbox_rows(workspace_id, run_id).await, 0);
}

/// The `cancellation.requested` Events of the Run.
async fn cancellations(
    fixture: &TestModules,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
) -> usize {
    retained_events(&fixture.runs, principal, workspace_id, run_id)
        .await
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
        .count()
}

/// A cancel whose record does not land within its requester's wait leaves a
/// log on each side: the requester logs its wait running out, and the reader
/// logs a delivery that reaches the owner late. Together they say whether
/// delivery or the owner was slow.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slow_outside_request_is_logged_by_its_requester_and_a_late_delivery() {
    let telemetry = super::trace::telemetry();
    let engine = Arc::new(StopAwareEngine::default());
    let (probe, release) = InboxProbe::holding_the_first_commit();
    let fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
    let (workspace_id, principal, run_id) = running_run(&fixture, &engine, "user:inbox-slow").await;
    let owner = fixture.runs.owners.get(&run_id).expect("live owner");

    // The owner applies the cancel, but its chunk is held: the requester's
    // wait (the shutdown grace and the settle margin) runs out.
    let waited = fixture
        .next_process()
        .cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
        .await;
    assert!(
        matches!(waited, Err(Error::RunRequestNotYetRecorded)),
        "{waited:?}"
    );
    assert!(owner.is_stopping() && probe.entered());
    let requester = telemetry.logs_holding(&workspace_id.to_string());
    assert!(
        matches!(requester.as_slice(), [log]
            if log.contains("within its requester's wait")
                && log.contains("cancel")
                && log.contains(run_id.as_str())),
        "one requester warning names the Run and the request's kind: {requester:?}"
    );

    // Two seconds later on the Run Module's clock, the reader delivers the
    // waiting row again, twice: late, and logged once.
    fixture
        .clock
        .next
        .fetch_add(2_000_000_000, Ordering::SeqCst);
    for ring in ["ring", "ring again"] {
        let delivered = probe.delivered();
        fixture
            .runs
            .start(&principal, &workspace_id, submission(ring))
            .await
            .expect("accept another Program");
        wait_until(|| probe.delivered() > delivered).await;
    }
    let late: Vec<_> = telemetry
        .logs_holding(run_id.as_str())
        .into_iter()
        .filter(|log| log.contains("delivered a request to its Run Owner late"))
        .collect();
    assert!(
        matches!(late.as_slice(), [log] if log.contains("cancel") && log.contains("delay_ms")),
        "the reader warns of a late delivery once: {late:?}"
    );

    release.send(()).expect("land the held chunk");
    let ended = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(ended.state(), RunState::Canceled);
    assert_eq!(
        cancellations(&fixture, &principal, &workspace_id, &run_id).await,
        1
    );
}

/// A request that waited in the Run Inbox while no owner held its Run is
/// logged as late when the expired-lease sweep hands it to the recreated
/// owner, as the reader logs one it delivers late.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_a_recreated_owner_finds_late_is_logged() {
    let telemetry = super::trace::telemetry();
    let held = HeldApproval::new("user:recovered-late").await;
    let (fixture, workspace_id, run_id) = (&held.fixture, &held.tool.workspace_id, &held.run);
    fixture
        .hand_to(workspace_id, run_id, "dead-holder", None)
        .await;
    // No owner here holds the Run, so the cancel waits in the Run Inbox.
    let cancelling = {
        let (next, actor, workspace_id, run_id) = (
            fixture.next_process(),
            held.actor.clone(),
            workspace_id.clone(),
            run_id.clone(),
        );
        tokio::spawn(async move {
            next.cancel(&actor, &workspace_id, &run_id, &immediate(None, 100))
                .await
        })
    };
    wait_until_async(async || fixture.inbox_rows(workspace_id, run_id).await == 1).await;
    fixture
        .clock
        .next
        .fetch_add(2_000_000_000, Ordering::SeqCst);

    assert_eq!(
        fixture.next_process().sweep_expired_leases().await.unwrap(),
        1
    );
    let late: Vec<_> = telemetry
        .logs_holding(run_id.as_str())
        .into_iter()
        .filter(|log| log.contains("delivered a request to its Run Owner late"))
        .collect();
    assert!(
        matches!(late.as_slice(), [log] if log.contains("cancel")),
        "the sweep warns of the late request once: {late:?}"
    );
    let _answered = cancelling.await.expect("join");
    // The recreated owner records the request: the Run ends canceled, once,
    // and the row is gone.
    let ended = wait_for_terminal(&fixture.runs, &held.actor, workspace_id, run_id).await;
    assert_eq!(ended.state(), RunState::Canceled);
    assert_eq!(
        cancellations(fixture, &held.actor, workspace_id, run_id).await,
        1
    );
    assert_eq!(fixture.inbox_rows(workspace_id, run_id).await, 0);
}
