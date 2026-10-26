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
    let fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        probe.clone(),
    )
    .await;
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
