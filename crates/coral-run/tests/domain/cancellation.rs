//! Cancellation, deadlines and recovery of running work.
use super::*;

#[tokio::test]
async fn run_deadline_is_measured_from_durable_acceptance() {
    let (engine, release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine.clone())).await;
    let (workspace_id, principal) = fixture.user_scope("user:deadline", "Deadline").await;
    let accepted = fixture
        .runs
        .start(
            &principal,
            &workspace_id,
            RunSubmission::new(
                ProgramSource::parse("blocked-deadline").expect("Program source"),
                RunTimeout::from_millis(5_000).expect("Run timeout"),
            ),
        )
        .await
        .expect("accept deadline Program");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    let accepted_at = retained_events(&fixture.runs, &principal, &workspace_id, accepted.run_id())
        .await
        .first()
        .expect("run.started")
        .occurred_at_unix_nanos();
    fixture
        .clock
        .next
        .store(accepted_at + 5_000_000_001, Ordering::SeqCst);
    release
        .send(ExecutionCompletion::Succeeded(FinalValue::Absent))
        .expect("release execution after deadline");

    let terminal =
        wait_for_terminal(&fixture.runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::TimedOut)
    ));
    assert_eq!(engine.executions.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn user_and_agent_cancellation_wait_for_started_program_execution_to_stop() {
    for caller in CancellationCaller::ALL {
        let (engine, release) = BlockingEngine::new();
        let fixture = TestModules::new(Arc::new(engine.clone())).await;
        let user_id = format!("user:bounded-cancel-{}", caller.label());
        let (workspace_id, user) = fixture.user_scope(&user_id, "Bounded cancellation").await;
        let principal = fixture
            .cancellation_caller(caller, &workspace_id, &user)
            .await;
        let accepted = fixture
            .runs
            .start(
                &principal,
                &workspace_id,
                submission("blocked-cancellation"),
            )
            .await
            .expect("accept cancellable Program");
        wait_until(|| engine.started.load(Ordering::SeqCst)).await;

        let runs = fixture.runs.clone();
        let cancel_principal = principal.clone();
        let cancel_workspace = workspace_id.clone();
        let cancel_run_id = accepted.run_id().clone();
        let cancel = tokio::spawn(async move {
            runs.cancel(
                &cancel_principal,
                &cancel_workspace,
                &cancel_run_id,
                &immediate(None, 100),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(
            !cancel.is_finished(),
            "{caller:?} cancellation made a started Run terminal before execution stopped"
        );

        release
            .send(ExecutionCompletion::Succeeded(FinalValue::Absent))
            .expect("stop Program execution");
        let canceled = cancel
            .await
            .expect("join cancellation")
            .expect("cancel Program");
        assert_eq!(canceled.state(), RunState::Canceled, "{caller:?}");
        assert!(
            matches!(
                canceled.terminal_outcome(),
                Some(RunTerminalOutcome::Canceled)
            ),
            "{caller:?}"
        );
    }
}

/// A nonterminal Run with no owner in this process answers `OwnedElsewhere`
/// and writes nothing: no intent, no Event, no head move, the Program
/// running on. The owning process then cancels it.
#[tokio::test]
async fn a_cancel_for_a_run_owned_elsewhere_is_refused_without_a_write() {
    let engine = Arc::new(StopAwareEngine::default());
    let mut fixture = TestModules::new(engine.clone()).await;
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_claim_lease(Duration::from_secs(60));
    let (workspace_id, principal) = fixture
        .user_scope("user:remote-cancel", "Cancel from another process")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("remote-cancel"))
        .await
        .expect("accept Program");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    wait_until_started(&fixture, &workspace_id, accepted.run_id()).await;
    let before = fixture.run_record(&workspace_id, accepted.run_id()).await;
    // Another process: the same database, none of this process's owners.
    let elsewhere = RunModule::with_adapters(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        fixture.providers.clone(),
        engine.clone(),
        fixture.upstream.clone(),
        fixture.clock.clone(),
        crate::TestObservers::new(
            Arc::new(NoopInvocationObserver),
            Arc::new(NoopExecutionObserver),
        ),
    );

    let refused = elsewhere
        .cancel(
            &principal,
            &workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await;
    assert!(matches!(refused, Err(Error::OwnedElsewhere)), "{refused:?}");
    assert_eq!(
        fixture.run_record(&workspace_id, accepted.run_id()).await,
        before,
        "a refused request writes nothing"
    );
    assert!(!engine.stopped.load(Ordering::SeqCst));
    assert!(
        retained_events(&fixture.runs, &principal, &workspace_id, accepted.run_id())
            .await
            .iter()
            .all(|event| !matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
    );

    let canceled = fixture
        .runs
        .cancel(
            &principal,
            &workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("the owning process cancels");
    assert_eq!(canceled.state(), RunState::Canceled);
    assert!(engine.stopped.load(Ordering::SeqCst));
    assert_eq!(engine.executions.load(Ordering::SeqCst), 1);
}

/// A cancel that reaches the owner while a Dispatch Fence is in flight is
/// applied at once: the Program is terminated before the fence returns, and
/// nothing is recorded until it has. Then `cancellation.requested` is
/// recorded, every Invocation the fence permitted ends `run_terminated`
/// instead of sending, and `run.ended` follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one journey: the cancel applied under a held fence, then the recorded order"
)]
async fn a_cancel_during_an_in_flight_fence_is_applied_at_once_and_run_ended_follows_the_fence() {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let latch = FenceLatch::holding_both();
    let fixture = TestModules::with_invocation_adapters(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        upstream.clone(),
        latch.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:cancel-mid-fence", "Cancel during a fence")
        .await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission(
                "await Promise.all([coral.providers.weather.getForecast(), coral.providers.weather.getForecast()]); return 'sent';",
            ),
        )
        .await
        .unwrap();
    latch.until_arrived(2).await;
    latch.release_arrivals();
    // The batch of both has read its facts and holds the commit slot.
    latch.until_loaded(1).await;
    let before = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await;
    let runs = fixture.runs.clone();
    let actor = operation.principal.clone();
    let workspace = operation.workspace_id.clone();
    let run_id = run.run_id().clone();
    let cancel = tokio::spawn(async move {
        runs.cancel(&actor, &workspace, &run_id, &immediate(None, 100))
            .await
    });
    let owner = fixture
        .runs
        .owners
        .find(
            run.run_id(),
            before.execution_claim_id.as_deref().expect("started"),
        )
        .expect("the owner is live");
    wait_until(|| owner.is_stopping()).await;
    assert_eq!(
        fixture
            .run_record(&operation.workspace_id, run.run_id())
            .await,
        before,
        "the intent waits behind the fence in flight"
    );
    latch.release_loaded();
    let canceled = cancel.await.expect("join").expect("cancel");
    assert_eq!(canceled.state(), RunState::Canceled);
    let events = fixture
        .events_after(
            &operation.workspace_id,
            run.run_id(),
            last_sequence(&before),
        )
        .await;
    let kinds: Vec<&str> = events
        .iter()
        .map(|event| match event.kind() {
            RunEventKind::CancellationRequested { .. } => "cancellation.requested",
            RunEventKind::InvocationEnded {
                result_kind: crate::InvocationResultKind::RunTerminated,
                ..
            } => "invocation.ended(run_terminated)",
            RunEventKind::Ended {
                state: RunState::Canceled,
                ..
            } => "run.ended(canceled)",
            other => panic!("{other:?}"),
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "cancellation.requested",
            "invocation.ended(run_terminated)",
            "invocation.ended(run_terminated)",
            "run.ended(canceled)",
        ]
    );
    let rows = fixture
        .invocation_rows(&operation.workspace_id, run.run_id())
        .await;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter()
            .all(|row| row.result_kind == Some(crate::InvocationResultKind::RunTerminated)),
        "{rows:?}"
    );
    // The batch in flight permitted its Invocations and the pre-send check
    // ended them; a request that reached the owner after that batch went in
    // flight was answered terminated with no dispatch start.
    assert!(
        rows.iter()
            .any(|row| row.dispatch_started_at_unix_nanos.is_some()),
        "{rows:?}"
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
}

/// A second cancel on an ending Run joins no chunk: the first intent is
/// recorded once, the head does not move for the repeat, and both receive
/// the recorded answer once the Run has ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_cancel_on_an_ending_run_joins_no_chunk() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    )));
    let mut fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    fixture.runs.execution_policy =
        crate::execution::ExecutionPolicy::testing_with_shutdown_grace(Duration::from_secs(30));
    let operation = fixture
        .user_operation("user:second-cancel", "Second cancel")
        .await;
    let run = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("invoke"),
        )
        .await
        .unwrap();
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    let cancel = |label: &'static str| {
        let runs = fixture.runs.clone();
        let actor = operation.principal.clone();
        let workspace = operation.workspace_id.clone();
        let run_id = run.run_id().clone();
        tokio::spawn(async move {
            runs.cancel(&actor, &workspace, &run_id, &immediate(None, 100))
                .await
                .unwrap_or_else(|error| panic!("{label}: {error}"))
        })
    };
    let first = cancel("first");
    wait_until_async(async || {
        fixture
            .run_record(&operation.workspace_id, run.run_id())
            .await
            .termination
            .is_some()
    })
    .await;
    let recorded = fixture
        .run_record(&operation.workspace_id, run.run_id())
        .await;
    let second = cancel("second");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(!second.is_finished(), "the Run is still ending");
    assert_eq!(
        fixture
            .run_record(&operation.workspace_id, run.run_id())
            .await,
        recorded,
        "the repeat moves nothing"
    );
    upstream.release.store(true, Ordering::SeqCst);
    let first = first.await.expect("join");
    let second = second.await.expect("join");
    assert_eq!(first.state(), RunState::Canceled);
    assert_eq!(second.state(), RunState::Canceled);
    let requested: Vec<_> = retained_events(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run.run_id(),
    )
    .await
    .into_iter()
    .filter(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
    .collect();
    assert_eq!(requested.len(), 1, "{requested:?}");
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn queued_user_and_agent_cancellation_record_intent_without_starting_javascript() {
    for caller in CancellationCaller::ALL {
        let engine = Arc::new(ImmediateEngine::default());
        let claim_gate = Arc::new(ClaimGate::default());
        let fixture = TestModules::with_execution_adapters(
            engine.clone(),
            Arc::new(RejectingUpstream),
            Arc::new(NoopInvocationObserver),
            claim_gate.clone(),
        )
        .await;
        let user_id = format!("user:queued-cancel-{}", caller.label());
        let (workspace_id, user) = fixture.user_scope(&user_id, "Cancel queued Run").await;
        let principal = fixture
            .cancellation_caller(caller, &workspace_id, &user)
            .await;
        let accepted = fixture
            .runs
            .start(&principal, &workspace_id, submission("queued-cancel"))
            .await
            .expect("accept queued Program");
        wait_until(|| claim_gate.entered.load(Ordering::SeqCst)).await;

        // The cancel is applied in owner memory at once and recorded once the
        // held start mark has landed: nothing commits before it.
        let runs = fixture.runs.clone();
        let cancel_principal = principal.clone();
        let cancel_workspace = workspace_id.clone();
        let cancel_run_id = accepted.run_id().clone();
        let cancel = tokio::spawn(async move {
            runs.cancel(
                &cancel_principal,
                &cancel_workspace,
                &cancel_run_id,
                &immediate(None, 100),
            )
            .await
        });
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert!(
            !cancel.is_finished(),
            "{caller:?}: answered only once recorded"
        );
        claim_gate.release.store(true, Ordering::SeqCst);
        let canceled = cancel
            .await
            .expect("join cancellation")
            .expect("cancel queued Run");
        assert_eq!(canceled.state(), RunState::Canceled, "{caller:?}");
        assert!(
            retained_events(&fixture.runs, &user, &workspace_id, canceled.run_id())
                .await
                .iter()
                .any(|event| matches!(
                    event.kind(),
                    RunEventKind::CancellationRequested { principal: requester }
                        if requester == principal.principal()
                )),
            "{caller:?}"
        );
        assert_eq!(engine.executions.load(Ordering::SeqCst), 0, "{caller:?}");
        wait_until(|| claim_gate.finished.load(Ordering::SeqCst) == 1).await;
    }
}

#[tokio::test]
async fn queued_deadline_expires_without_starting_javascript() {
    let engine = Arc::new(ImmediateEngine::default());
    let claim_gate = Arc::new(ClaimGate::default());
    let fixture = TestModules::with_execution_adapters(
        engine.clone(),
        Arc::new(RejectingUpstream),
        Arc::new(NoopInvocationObserver),
        claim_gate.clone(),
    )
    .await;
    let (workspace_id, principal) = fixture
        .user_scope("user:queued-timeout", "Time out queued Run")
        .await;
    let accepted = fixture
        .runs
        .start(
            &principal,
            &workspace_id,
            RunSubmission::new(
                ProgramSource::parse("queued-timeout").expect("Program source"),
                RunTimeout::from_millis(25).expect("Run timeout"),
            ),
        )
        .await
        .expect("accept queued Program");
    wait_until(|| claim_gate.entered.load(Ordering::SeqCst)).await;
    // The deadline reaches the owner's mailbox before its start mark lands:
    // the Program never starts, and the timeout is recorded.
    tokio::time::sleep(Duration::from_millis(50)).await;
    claim_gate.release.store(true, Ordering::SeqCst);

    let terminal =
        wait_for_terminal(&fixture.runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert_eq!(engine.executions.load(Ordering::SeqCst), 0);
    wait_until(|| claim_gate.finished.load(Ordering::SeqCst) == 1).await;
}

/// A cancel and the Run deadline: the first the owner applies decides the
/// terminal kind, and the record shows exactly one. The cancel first: the
/// Run ends Canceled with `cancellation.requested`, and the deadline's
/// timer, which fires on the host clock later, changes nothing. The
/// deadline's timer first: the Run ends `TimedOut`, the cancel joins no chunk
/// and writes no Event, and the requester is answered from the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_and_the_run_deadline_record_the_first_applied_kind() {
    for deadline_first in [false, true] {
        let (engine, _release) = BlockingEngine::new();
        let fixture = TestModules::new(Arc::new(engine.clone())).await;
        let (workspace_id, principal) = fixture
            .user_scope("user:cancel-vs-deadline", "Cancel against the deadline")
            .await;
        // The deadline's timer fires on the host clock: far enough for the
        // cancel to come first, soon enough to wait for when it comes second.
        let run_timeout = if deadline_first { 300 } else { 5_000 };
        let accepted = fixture
            .runs
            .start(
                &principal,
                &workspace_id,
                RunSubmission::new(
                    ProgramSource::parse("blocked").expect("Program source"),
                    RunTimeout::from_millis(run_timeout).expect("Run timeout"),
                ),
            )
            .await
            .expect("accept the Program");
        let run_id = accepted.run_id().clone();
        wait_until(|| engine.started.load(Ordering::SeqCst)).await;
        let claim_id = fixture
            .run_record(&workspace_id, &run_id)
            .await
            .execution_claim_id
            .expect("started Run");
        let owner = fixture
            .runs
            .owners
            .find(&run_id, &claim_id)
            .expect("the owner is live");
        if deadline_first {
            // The deadline's timer stops the Program; the clock stays
            // before the deadline, so only the timer's order decides.
            wait_until(|| owner.is_stopping()).await;
        }
        let terminal = fixture
            .runs
            .cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
            .await
            .expect("cancel");
        assert_first_applied_kind(
            &fixture.runs,
            &principal,
            &workspace_id,
            &run_id,
            terminal,
            deadline_first,
        )
        .await;
        assert_eq!(engine.executions.load(Ordering::SeqCst), 1);
    }
}

/// A cancel and the Run deadline in one mailbox drain, against the
/// database: the owner is held at the message it dequeues next while the
/// deadline's timer fires and the cancel arrives, in the order the arm
/// chooses, and then applies both in one drain with it. The first in the
/// mailbox decides the kind, and the record shows exactly one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_and_the_run_deadline_in_one_mailbox_drain_record_the_first_applied_kind() {
    for deadline_first in [false, true] {
        let (engine, gates) = BurstEngine::new(vec!["\"one\"".to_string()]);
        let probe = CommitProbe::new();
        let fixture = TestModules::with_execution_adapters(
            engine.clone(),
            Arc::new(RejectingUpstream),
            Arc::new(NoopInvocationObserver),
            probe.clone(),
        )
        .await;
        let (workspace_id, principal) = fixture
            .user_scope("user:one-drain", "Cancel and deadline in one drain")
            .await;
        let run_timeout = Duration::from_millis(300);
        let accepted = fixture
            .runs
            .start(
                &principal,
                &workspace_id,
                RunSubmission::new(
                    ProgramSource::parse("one-drain").expect("Program source"),
                    RunTimeout::from_millis(run_timeout.as_millis().try_into().unwrap())
                        .expect("Run timeout"),
                ),
            )
            .await
            .expect("accept the Program");
        let run_id = accepted.run_id().clone();
        wait_until_started(&fixture, &workspace_id, &run_id).await;
        // The owner is held at the next message it dequeues (the Emission,
        // or its start if that is still queued); the deadline's timer and
        // the cancel reach the mailbox while it is held.
        let release = probe.hold_next_message();
        gates.go.send(()).expect("start the burst");
        wait_until(|| probe.message_held()).await;
        let cancel = {
            let runs = fixture.runs.clone();
            let (principal, workspace_id, run_id) =
                (principal.clone(), workspace_id.clone(), run_id.clone());
            move || async move {
                runs.cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
                    .await
            }
        };
        // Twice the timeout on the host clock: the timer has fired into the
        // held mailbox by then.
        let cancelling = if deadline_first {
            tokio::time::sleep(run_timeout * 2).await;
            tokio::spawn(cancel())
        } else {
            let cancelling = tokio::spawn(cancel());
            tokio::time::sleep(run_timeout * 2).await;
            cancelling
        };
        release.send(()).expect("release the owner");
        gates.release.send(()).expect("release the Program");
        let terminal = cancelling.await.expect("the cancel task").expect("cancel");
        assert_first_applied_kind(
            &fixture.runs,
            &principal,
            &workspace_id,
            &run_id,
            terminal,
            deadline_first,
        )
        .await;
    }
}

/// The record a cancel and the Run deadline leave: the first the owner
/// applied decides the one terminal kind, and `cancellation.requested` is
/// written only when the cancel came first. `terminal` is what the cancel
/// returned.
async fn assert_first_applied_kind(
    runs: &RunModule,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    run_id: &crate::RunId,
    terminal: crate::RunUpdate,
    deadline_first: bool,
) {
    let terminal = if terminal.state().is_terminal() {
        terminal
    } else {
        wait_for_terminal(runs, principal, workspace_id, run_id).await
    };
    let expected = if deadline_first {
        RunState::TimedOut
    } else {
        RunState::Canceled
    };
    assert_eq!(
        terminal.state(),
        expected,
        "deadline first: {deadline_first}"
    );
    let events = retained_events(runs, principal, workspace_id, run_id).await;
    let ended: Vec<_> = events
        .iter()
        .filter(|event| matches!(event.kind(), RunEventKind::Ended { .. }))
        .collect();
    assert!(
        matches!(ended.as_slice(), [ended] if ended.kind() == &RunEventKind::Ended { state: expected }),
        "exactly one terminal kind: {events:?}"
    );
    assert_eq!(
        events
            .iter()
            .filter(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
            .count(),
        usize::from(!deadline_first),
        "a cancel applied after the timeout writes no Event: {events:?}"
    );
}

/// What ends the Run while its Invocation is in flight.
#[derive(Debug, Clone, Copy)]
enum Termination {
    Cancel(CancellationCaller),
    Deadline,
}

impl Termination {
    /// Ends the Run and returns its terminal update.
    async fn apply(
        self,
        fixture: &TestModules,
        principal: &AuthenticatedPrincipal,
        operation: &OperationFixture,
        run: &crate::RunId,
    ) -> crate::RunUpdate {
        let workspace = &operation.workspace_id;
        match self {
            Self::Cancel(_) => fixture
                .runs
                .cancel(principal, workspace, run, &immediate(None, 100))
                .await
                .expect("cancel in-flight work"),
            // The owner's own timer applies the deadline on the host clock.
            Self::Deadline => {
                wait_until_async_within(Duration::from_secs(5), async || {
                    fixture.run_record(workspace, run).await.state.is_terminal()
                })
                .await;
                wait_for_terminal(&fixture.runs, principal, workspace, run).await
            }
        }
    }

    /// The Run timeout: the deadline's is short enough to wait out.
    fn timeout(self) -> RunTimeout {
        let millis = match self {
            Self::Cancel(_) => 5_000,
            Self::Deadline => 1_000,
        };
        RunTimeout::from_millis(millis).expect("Run timeout")
    }
}

/// An Invocation held before its dispatch fence or inside its upstream send.
enum InFlight {
    Waiting {
        gate: Arc<FenceGate>,
        release: oneshot::Sender<()>,
        upstream: Arc<ScriptedUpstream>,
        engine: Arc<CompletionAttemptingEngine>,
    },
    Dispatching {
        upstream: Arc<BlockingUpstream>,
    },
}

impl InFlight {
    async fn arrange(dispatching: bool) -> (Self, TestModules) {
        let result = || serde_json::json!({"temperature": 21});
        if dispatching {
            let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(200, result())));
            let fixture = TestModules::with_upstream(
                Arc::new(
                    crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                        .expect("V8 engine"),
                ),
                upstream.clone(),
            )
            .await;
            (Self::Dispatching { upstream }, fixture)
        } else {
            let (gate, release) = FenceGate::blocking();
            let upstream = Arc::new(ScriptedUpstream::json(200, result()));
            let engine = Arc::new(CompletionAttemptingEngine::new(
                "coral.providers.weather.getForecast",
            ));
            let fixture = TestModules::with_invocation_adapters(
                engine.clone(),
                upstream.clone(),
                gate.clone(),
            )
            .await;
            (
                Self::Waiting {
                    gate,
                    release,
                    upstream,
                    engine,
                },
                fixture,
            )
        }
    }

    async fn entered(&self) {
        match self {
            Self::Waiting { gate, .. } => wait_until(|| gate.entered()).await,
            Self::Dispatching { upstream } => {
                wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
            }
        }
    }

    fn sends(&self) -> usize {
        match self {
            Self::Waiting { upstream, .. } => upstream.sends.load(Ordering::SeqCst),
            Self::Dispatching { upstream } => upstream.sends.load(Ordering::SeqCst),
        }
    }

    /// Lets the held work finish late, after the Run is terminal.
    async fn finish_late(self) {
        match self {
            Self::Waiting {
                release, engine, ..
            } => {
                release.send(()).expect("release dispatch fence");
                wait_until(|| engine.finished.load(Ordering::SeqCst)).await;
            }
            Self::Dispatching { upstream } => {
                upstream.release.store(true, Ordering::SeqCst);
                wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
            }
        }
    }
}

/// Cancellation and the Run deadline end an Invocation still waiting at its fence
/// without a send. One already sent waits out the child shutdown grace and ends
/// indeterminate; its late result changes nothing.
#[tokio::test]
async fn termination_ends_waiting_invocations_and_marks_unfinished_dispatches_indeterminate() {
    let terminations = CancellationCaller::ALL
        .map(Termination::Cancel)
        .into_iter()
        .chain([Termination::Deadline]);
    for termination in terminations {
        for dispatching in [false, true] {
            let case = format!("{termination:?}, dispatching: {dispatching}");
            let (in_flight, fixture) = InFlight::arrange(dispatching).await;
            let operation = fixture
                .user_operation("user:terminate-in-flight", "Terminate in-flight work")
                .await;
            let principal = match termination {
                Termination::Cancel(caller) => {
                    fixture
                        .cancellation_caller(caller, &operation.workspace_id, &operation.principal)
                        .await
                }
                Termination::Deadline => operation.principal.clone(),
            };
            let accepted = fixture
                .runs
                .start(
                    &principal,
                    &operation.workspace_id,
                    RunSubmission::new(
                        ProgramSource::parse("await coral.providers.weather.getForecast();")
                            .unwrap(),
                        termination.timeout(),
                    ),
                )
                .await
                .expect("accept Program");
            in_flight.entered().await;

            let terminating = std::time::Instant::now();
            let terminal = termination
                .apply(&fixture, &principal, &operation, accepted.run_id())
                .await;
            let terminating = terminating.elapsed();
            let (invocation_end, sends) = if dispatching {
                (crate::InvocationResultKind::Indeterminate, 1)
            } else {
                (crate::InvocationResultKind::RunTerminated, 0)
            };
            let state = match termination {
                Termination::Cancel(_) => RunState::Canceled,
                Termination::Deadline => RunState::TimedOut,
            };
            assert_eq!(terminal.state(), state, "{case}");
            assert!(
                retained_events(
                    &fixture.runs,
                    &operation.principal,
                    &operation.workspace_id,
                    terminal.run_id()
                )
                .await
                .iter()
                .any(|event| matches!(
                    event.kind(),
                    RunEventKind::InvocationEnded { result_kind, .. } if *result_kind == invocation_end
                )),
                "{case}"
            );
            assert_eq!(in_flight.sends(), sends, "{case}");
            if dispatching {
                assert!(
                    terminating >= Duration::from_millis(90),
                    "{case}: child shutdown grace was skipped: {terminating:?}"
                );
            }
            in_flight.finish_late().await;
            wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;
            let after_late_work = fixture
                .runs
                .observe(
                    &principal,
                    &operation.workspace_id,
                    accepted.run_id(),
                    &immediate(None, 100),
                )
                .await
                .expect("observe after late work");
            assert_eq!(
                after_late_work, terminal,
                "{case}: late work must not change terminal evidence"
            );
        }
    }
}

#[tokio::test]
async fn complete_outcome_or_known_failure_can_win_during_cancellation_shutdown() {
    for result_too_large in [false, true] {
        let upstream = Arc::new(BlockingUpstream {
            result_too_large,
            ..BlockingUpstream::new(HttpOutcome::new(
                200,
                serde_json::json!({"temperature": 21}),
            ))
        });
        let mut fixture = TestModules::with_upstream(
            Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                    .expect("V8 engine"),
            ),
            upstream.clone(),
        )
        .await;
        // The upstream result must land inside the shutdown grace. The 100 ms
        // test grace is wall-clock time, so a loaded machine could expire it
        // first and record the dispatch as indeterminate.
        fixture.runs.execution_policy =
            crate::execution::ExecutionPolicy::testing_with_shutdown_grace(Duration::from_secs(30));
        let operation = fixture
            .user_operation("user:cancel-dispatch", "Cancel dispatching Invocation")
            .await;
        let accepted = fixture
            .runs
            .start(
                &operation.principal,
                &operation.workspace_id,
                submission("await coral.providers.weather.getForecast();"),
            )
            .await
            .expect("accept Program");
        wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;

        let runs = fixture.runs.clone();
        let principal = operation.principal.clone();
        let workspace_id = operation.workspace_id.clone();
        let run_id = accepted.run_id().clone();
        let cancel = tokio::spawn(async move {
            runs.cancel(&principal, &workspace_id, &run_id, &immediate(None, 100))
                .await
        });
        let mut intent_observed = false;
        for _ in 0..200 {
            let update = fixture
                .runs
                .observe(
                    &operation.principal,
                    &operation.workspace_id,
                    accepted.run_id(),
                    &immediate(None, 100),
                )
                .await
                .expect("observe cancellation intent");
            if retained_events(
                &fixture.runs,
                &operation.principal,
                &operation.workspace_id,
                update.run_id(),
            )
            .await
            .iter()
            .any(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
            {
                intent_observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(
            intent_observed,
            "cancellation intent must be durable before shutdown"
        );
        // Give V8 time to stop before the dispatched Invocation settles.
        tokio::time::sleep(Duration::from_millis(20)).await;
        upstream.release.store(true, Ordering::SeqCst);
        let canceled = cancel
            .await
            .expect("join cancellation")
            .expect("cancel dispatching Run");

        assert_eq!(canceled.state(), RunState::Canceled);
        let expected = if result_too_large {
            crate::InvocationResultKind::CoralFailure
        } else {
            crate::InvocationResultKind::UpstreamOutcome
        };
        assert!(
            retained_events(
                &fixture.runs,
                &operation.principal,
                &operation.workspace_id,
                canceled.run_id()
            )
            .await
            .iter()
            .any(|event| matches!(
                event.kind(),
                RunEventKind::InvocationEnded { result_kind, .. } if *result_kind == expected
            )),
            "{canceled:?}"
        );
        assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn deadline_terminates_noncooperative_javascript_without_replay() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:v8-deadline", "Terminate infinite JavaScript")
        .await;
    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            RunSubmission::new(
                ProgramSource::parse("while (true) {}").expect("Program source"),
                RunTimeout::from_millis(500).expect("Run timeout"),
            ),
        )
        .await
        .expect("accept infinite Program");

    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::TimedOut)
    ));
    assert_eq!(
        retained_events(&runs, &principal, &workspace_id, terminal.run_id())
            .await
            .iter()
            .filter(|event| event.kind()
                == &RunEventKind::Ended {
                    state: RunState::TimedOut
                })
            .count(),
        1
    );
}

/// The process that boots next sweeps the Run this one stopped renewing:
/// one Run recovered, and the Run as it ended.
async fn swept_by_the_next_process(
    fixture: &TestModules,
    operation: &OperationFixture,
    run_id: &crate::RunId,
) -> crate::RunUpdate {
    assert_eq!(
        fixture
            .next_process()
            .sweep_expired_leases()
            .await
            .expect("sweep the lapsed lease"),
        1
    );
    wait_for_terminal(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        run_id,
    )
    .await
}

#[tokio::test]
async fn recovery_revokes_a_started_claim_before_late_program_work() {
    let (engine, release) = StaleClaimEngine::new("coral.providers.weather.getForecast");
    let engine = Arc::new(engine);
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let fixture = TestModules::with_upstream(engine.clone(), upstream.clone()).await;
    let operation = fixture
        .user_operation("user:stale-claim", "Recover lost execution")
        .await;
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("stale-claim"),
        )
        .await
        .expect("accept Program");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    let accepted_at = retained_events(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await
    .first()
    .expect("run.started")
    .occurred_at_unix_nanos();
    fixture
        .expire_worker_claim(&operation.workspace_id, accepted.run_id(), accepted_at)
        .await;
    fixture
        .clock
        .next
        .store(accepted_at + 2_000_000_000, Ordering::SeqCst);

    let interrupted = swept_by_the_next_process(&fixture, &operation, accepted.run_id()).await;
    assert_eq!(interrupted.state(), RunState::Interrupted);
    assert!(matches!(
        interrupted.terminal_outcome(),
        Some(RunTerminalOutcome::Interrupted)
    ));

    release.send(()).expect("release stale execution");
    wait_until(|| engine.attempted_late_work.load(Ordering::SeqCst)).await;
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;
    assert!(!engine.emission_succeeded.load(Ordering::SeqCst));
    assert!(!engine.invocation_succeeded.load(Ordering::SeqCst));
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    let unchanged = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("observe immutable interruption");
    assert_eq!(unchanged.state(), RunState::Interrupted);
    assert!(
        retained_events(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            unchanged.run_id()
        )
        .await
        .iter()
        .all(|event| {
            !matches!(
                event.kind(),
                RunEventKind::ProgramEmitted { .. } | RunEventKind::InvocationStarted { .. }
            )
        })
    );
}

#[tokio::test]
async fn recovery_ends_a_pending_invocation_without_dispatch() {
    let (observer, release_fence) = FenceGate::blocking();
    let upstream = Arc::new(ScriptedUpstream::json(
        200,
        serde_json::json!({"temperature": 21}),
    ));
    let engine = Arc::new(CompletionAttemptingEngine::new(
        "coral.providers.weather.getForecast",
    ));
    let fixture =
        TestModules::with_invocation_adapters(engine.clone(), upstream.clone(), observer.clone())
            .await;
    let operation = fixture
        .user_operation("user:recover-pending", "Recover pending Invocation")
        .await;
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("recover-pending"),
        )
        .await
        .expect("accept Program");
    wait_until(|| observer.entered()).await;
    let accepted_at = retained_events(
        &fixture.runs,
        &operation.principal,
        &operation.workspace_id,
        accepted.run_id(),
    )
    .await
    .first()
    .expect("run.started")
    .occurred_at_unix_nanos();
    fixture
        .expire_worker_claim(&operation.workspace_id, accepted.run_id(), accepted_at)
        .await;
    fixture
        .clock
        .next
        .store(accepted_at + 2_000_000_000, Ordering::SeqCst);

    let interrupted = swept_by_the_next_process(&fixture, &operation, accepted.run_id()).await;
    assert_eq!(interrupted.state(), RunState::Interrupted);
    assert!(
        retained_events(
            &fixture.runs,
            &operation.principal,
            &operation.workspace_id,
            interrupted.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                result_kind: crate::InvocationResultKind::RunTerminated,
                ..
            }
        ))
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    release_fence.send(()).expect("release dispatch fence");
    wait_until(|| engine.finished.load(Ordering::SeqCst)).await;
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
    let unchanged = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("observe interruption after stale fence and completion attempts");
    assert_eq!(unchanged.state(), RunState::Interrupted);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one cancellation scenario proves the terminal race and all User, Owner, and Agent visibility outcomes"
)]
async fn durable_cancellation_wins_completion_and_preserves_principal_confidentiality() {
    let (engine, release) = BlockingEngine::new();
    let fixture = TestModules::new(Arc::new(engine.clone())).await;
    let owner_user = organization_user(
        fixture.database.clone(),
        "user:workspace-owner",
        OrganizationRole::Member,
    )
    .await;
    let member_user = organization_user(
        fixture.database.clone(),
        "user:workspace-member",
        OrganizationRole::Member,
    )
    .await;
    let organization_owner_user = organization_user(
        fixture.database.clone(),
        "user:organization-owner",
        OrganizationRole::Owner,
    )
    .await;
    let workspace = fixture
        .workspaces
        .create(
            &change(owner_user.clone(), 10, '3'),
            WorkspaceDisplayName::parse("Cancellation").expect("display name"),
        )
        .await
        .expect("create Workspace");
    fixture
        .workspaces
        .set_membership(
            &change(owner_user.clone(), 11, '4'),
            workspace.id(),
            &member_user,
            WorkspaceRole::Member,
        )
        .await
        .expect("add member");
    let second_owner = AuthenticatedPrincipal::user(
        fixture
            .workspace_user(
                workspace.id(),
                &owner_user,
                "user:second-owner",
                WorkspaceRole::Owner,
            )
            .await,
    );
    let owner = AuthenticatedPrincipal::user(owner_user);
    let member = AuthenticatedPrincipal::user(member_user);
    let organization_owner = AuthenticatedPrincipal::user(organization_owner_user);
    let agent = registered_agent(&fixture.workspaces, &owner, workspace.id()).await;
    let accepted = fixture
        .runs
        .start(&agent, workspace.id(), submission("blocked-agent"))
        .await
        .expect("accept Agent Run");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;

    assert_run_concealed(&fixture.runs, &member, workspace.id(), accepted.run_id()).await;
    let different_agent = registered_agent(&fixture.workspaces, &owner, workspace.id()).await;
    assert_run_concealed(
        &fixture.runs,
        &different_agent,
        workspace.id(),
        accepted.run_id(),
    )
    .await;

    // An Organization Owner without a Membership is denied like any non-Member.
    assert!(matches!(
        fixture
            .runs
            .cancel(
                &organization_owner,
                workspace.id(),
                accepted.run_id(),
                &immediate(None, 100),
            )
            .await,
        Err(Error::RunNotFound)
    ));
    assert_run_concealed(
        &fixture.runs,
        &organization_owner,
        workspace.id(),
        accepted.run_id(),
    )
    .await;
    assert_eq!(
        fixture
            .runs
            .observe(
                &owner,
                workspace.id(),
                accepted.run_id(),
                &immediate(None, 100),
            )
            .await
            .expect("Workspace Owner observes Agent Run")
            .state(),
        RunState::Running,
        "the denied cancellation changed nothing"
    );

    let canceled = fixture
        .runs
        .cancel(
            &second_owner,
            workspace.id(),
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("another designated Owner cancels Agent Run");
    assert_eq!(canceled.state(), RunState::Canceled);
    assert!(matches!(
        canceled.terminal_outcome(),
        Some(RunTerminalOutcome::Canceled)
    ));
    assert!(
        retained_events(
            &fixture.runs,
            &second_owner,
            workspace.id(),
            canceled.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::CancellationRequested { principal }
                if principal == second_owner.principal()
        ))
    );

    release
        .send(ExecutionCompletion::Succeeded(FinalValue::Absent))
        .expect("release losing completion");
    wait_until(|| engine.finished.load(Ordering::SeqCst)).await;
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;
    let still_canceled = fixture
        .runs
        .observe(
            &owner,
            workspace.id(),
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("Workspace Owner observes Agent Run");
    assert_eq!(still_canceled.state(), RunState::Canceled);

    let immediate_runs = RunModule::with_adapters(
        fixture.database.clone(),
        fixture.workspaces.clone(),
        fixture.providers.clone(),
        Arc::new(ImmediateEngine::default()),
        fixture.upstream.clone(),
        fixture.clock.clone(),
        crate::TestObservers::new(
            Arc::new(NoopInvocationObserver),
            Arc::new(NoopExecutionObserver),
        ),
    );
    let member_run = immediate_runs
        .start(&member, workspace.id(), submission("absent"))
        .await
        .expect("start member Run");
    for outsider in [&owner, &second_owner, &organization_owner] {
        assert_run_concealed(
            &immediate_runs,
            outsider,
            workspace.id(),
            member_run.run_id(),
        )
        .await;
    }
}

/// What a Run whose holder died carries into the expired-lease sweep.
#[derive(Debug, Clone, Copy)]
enum Abandoned {
    /// No deadline passed: the Run is interrupted.
    Nothing,
    /// Its Approval Request's deadline passed: the request expires first.
    ApprovalDeadline,
    /// Its Run deadline passed too: the Run times out.
    RunDeadline,
    /// Its owner recorded a cancellation and died before it settled.
    RecordedCancel,
}

/// The expired-lease sweep recreates the owner of a Run whose holder died,
/// with the timers that passed queued in deadline order, and that owner ends
/// the Run in one chunk. An Approval Request whose deadline passed expires,
/// dated at its deadline, and its Invocation fails `approval_expired` before
/// the Run ends; a passed Run deadline records `timed_out`, not
/// `interrupted`; a recorded termination is finished as recorded; otherwise
/// the Run is interrupted, its waiting Invocation ends `run_terminated` and
/// its request closes. The Run is taken over with a new claim.
#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one table of abandoned Runs, each arranged and checked against its one chunk"
)]
async fn the_sweep_ends_an_abandoned_run_by_its_passed_timers_or_its_recorded_termination() {
    use crate::InvocationResultKind::{CoralFailure, RunTerminated};
    use crate::approval::ApprovalState;
    // (what passed, the Run's end, its request's end, the Invocation's end)
    let cases = [
        (
            Abandoned::Nothing,
            RunState::Interrupted,
            ApprovalState::Closed,
            RunTerminated,
        ),
        (
            Abandoned::ApprovalDeadline,
            RunState::Interrupted,
            ApprovalState::Expired,
            CoralFailure,
        ),
        (
            Abandoned::RunDeadline,
            RunState::TimedOut,
            ApprovalState::Expired,
            CoralFailure,
        ),
        (
            Abandoned::RecordedCancel,
            RunState::Canceled,
            ApprovalState::Closed,
            RunTerminated,
        ),
    ];
    for (case, run_end, request_end, invocation_end) in cases {
        // The request waits 20 s at most, well inside the Run's 60 s.
        let held =
            HeldApproval::expiring(&format!("user:abandoned-{case:?}"), Duration::from_secs(20))
                .await;
        let (fixture, workspace_id, run_id) =
            (&held.fixture, &held.operation.workspace_id, &held.run);
        let request = held
            .approvals
            .list_for_run(&held.actor, workspace_id, run_id)
            .await
            .unwrap()
            .remove(0);
        let mut handed = fixture
            .hand_to(workspace_id, run_id, "dead-holder", None)
            .await;
        match case {
            Abandoned::Nothing => {}
            Abandoned::ApprovalDeadline => fixture
                .clock
                .next
                .store(request.deadline_at_unix_nanos, Ordering::SeqCst),
            Abandoned::RunDeadline => fixture
                .clock
                .next
                .store(handed.deadline_at_unix_nanos, Ordering::SeqCst),
            Abandoned::RecordedCancel => {
                fixture
                    .inject_fault(&format!(
                        "UPDATE program_run_records SET termination_kind = 'canceled', \
                         termination_started_at_unix_nanos = accepted_at_unix_nanos + 1, \
                         termination_deadline_at_unix_nanos = \
                         accepted_at_unix_nanos + 1 + shutdown_grace_nanos, \
                         cancellation_requested_at_unix_nanos = accepted_at_unix_nanos + 1, \
                         cancellation_principal_id = principal_id, \
                         cancellation_principal_kind = principal_kind \
                         WHERE id = '{run_id}'"
                    ))
                    .await;
                handed = fixture.run_record(workspace_id, run_id).await;
            }
        }

        let swept = fixture.next_process().sweep_expired_leases().await;
        assert_eq!(swept.unwrap(), 1, "{case:?}");
        let ended = fixture.terminal_record(workspace_id, run_id).await;

        assert_eq!(ended.state, run_end, "{case:?}");
        assert_eq!(
            ended.chunk_sequence,
            handed.chunk_sequence + 1,
            "{case:?}: one chunk"
        );
        assert_eq!(ended.worker_claim_id, None, "{case:?}");
        assert!(
            ended
                .execution_claim_id
                .as_deref()
                .is_some_and(|claim| claim != "dead-holder"),
            "{case:?}: taken over with a new claim"
        );
        let events = fixture
            .events_after(workspace_id, run_id, last_sequence(&handed))
            .await;
        assert!(
            matches!(
                events.iter().map(RunEvent::kind).collect::<Vec<_>>().as_slice(),
                [
                    RunEventKind::InvocationEnded { result_kind, .. },
                    RunEventKind::Ended { state, .. },
                ] if *result_kind == invocation_end && *state == run_end
            ),
            "{case:?}: {events:?}"
        );
        let request_after = held
            .approvals
            .get(&held.actor, workspace_id, &request.id)
            .await
            .unwrap();
        assert_eq!(request_after.state, request_end, "{case:?}");
        if request_end == ApprovalState::Expired {
            assert_eq!(
                request_after.decided_at_unix_nanos,
                Some(request.deadline_at_unix_nanos),
                "{case:?}: dated at its deadline"
            );
            let rows = fixture.invocation_rows(workspace_id, run_id).await;
            assert!(
                matches!(rows.as_slice(), [row] if row.failure_code.as_deref() == Some("approval_expired")),
                "{case:?}"
            );
        }
    }
}

/// A Run whose holder died with one Invocation dispatching and another
/// waiting on its approval ends in one chunk: the dispatching one ends
/// `indeterminate` and is never sent again, the waiting one `run_terminated`
/// with its Approval Request closed, then `run.ended`; the head moves once.
/// The dead holder's late upstream result changes nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[expect(
    clippy::too_many_lines,
    reason = "one journey: two Invocations brought to dispatching and waiting, then one chunk"
)]
async fn an_abandoned_runs_dispatching_and_waiting_invocations_end_in_one_chunk() {
    use crate::InvocationResultKind::{Indeterminate, RunTerminated};
    use crate::approval::ApprovalState;
    let (engine, calls) = ScriptedCallsEngine::new();
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    )));
    let fixture = TestModules::with_upstream(Arc::new(engine), upstream.clone()).await;
    let operation = fixture
        .user_operation("user:abandoned-in-flight", "Recover in-flight work")
        .await;
    fixture.require_approval(&operation).await;
    let workspace_id = &operation.workspace_id;
    let approvals = fixture.approvals();
    let accepted = fixture
        .runs
        .start(&operation.principal, workspace_id, submission("calls"))
        .await
        .unwrap();
    let run_id = accepted.run_id();
    calls.invoke(&operation.call_path);
    calls.invoke(&operation.call_path);
    wait_for_actions(
        &fixture.runs,
        &operation.principal,
        workspace_id,
        run_id,
        2,
        2,
    )
    .await;
    let requests = approvals
        .list_for_run(&operation.principal, workspace_id, run_id)
        .await
        .unwrap();
    let [approved, waiting] = requests.as_slice() else {
        panic!("two Approval Requests: {requests:?}");
    };
    approvals
        .decide(
            &operation.principal,
            workspace_id,
            &approved.id,
            crate::approval::ApprovalDecision::Approve,
            None,
            &TraceId::parse(&"d".repeat(32)).unwrap(),
        )
        .await
        .unwrap();
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
    let handed = fixture
        .hand_to(workspace_id, run_id, "dead-holder", None)
        .await;

    assert_eq!(
        fixture.next_process().sweep_expired_leases().await.unwrap(),
        1
    );
    let ended = fixture.terminal_record(workspace_id, run_id).await;

    assert_eq!(ended.state, RunState::Interrupted);
    assert_eq!(ended.chunk_sequence, handed.chunk_sequence + 1, "one chunk");
    let events = fixture
        .events_after(workspace_id, run_id, last_sequence(&handed))
        .await;
    let mut ends: Vec<_> = events
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::InvocationEnded { result_kind, .. } => Some(result_kind.as_storage_str()),
            _ => None,
        })
        .collect();
    ends.sort_unstable();
    assert_eq!(
        ends,
        [
            Indeterminate.as_storage_str(),
            RunTerminated.as_storage_str()
        ]
    );
    assert!(
        matches!(
            events.last().map(RunEvent::kind),
            Some(RunEventKind::Ended {
                state: RunState::Interrupted,
                ..
            })
        ),
        "{events:?}"
    );
    let states: Vec<_> = approvals
        .list_for_run(&operation.principal, workspace_id, run_id)
        .await
        .unwrap()
        .into_iter()
        .map(|request| (request.id, request.state))
        .collect();
    assert!(states.contains(&(approved.id.clone(), ApprovalState::Approved)));
    assert!(states.contains(&(waiting.id.clone(), ApprovalState::Closed)));
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1, "never replayed");

    upstream.release.store(true, Ordering::SeqCst);
    wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
    wait_until(|| !fixture.runs.has_owner(run_id)).await;
    assert_eq!(
        fixture.run_record(workspace_id, run_id).await,
        ended,
        "the dead holder's late result changes nothing"
    );
}

/// A recovered owner takes no outside request: until its chunk lands, a
/// cancel and a decision for the Run are answered `OwnedElsewhere`, as for
/// any Run with no owner here, and write nothing. Once the chunk landed, the
/// retried requests are answered from the record.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_request_for_a_run_being_recovered_is_owned_elsewhere_until_its_chunk_lands() {
    use crate::approval::{ApprovalDecision, ApprovalState};
    let held = HeldApproval::new("user:recovered-request").await;
    let (fixture, workspace_id, run_id) = (&held.fixture, &held.operation.workspace_id, &held.run);
    let request = held
        .approvals
        .list_for_run(&held.actor, workspace_id, run_id)
        .await
        .unwrap()
        .remove(0);
    let handed = fixture
        .hand_to(workspace_id, run_id, "dead-holder", None)
        .await;
    let (gate, release) = CommitGate::blocking();
    let next = fixture.next_process_observed(gate.clone());
    assert_eq!(next.sweep_expired_leases().await.unwrap(), 1);
    wait_until(|| gate.entered()).await;
    let cancel = async || {
        next.cancel(&held.actor, workspace_id, run_id, &immediate(None, 100))
            .await
    };
    let decide = async || {
        next.approvals()
            .decide(
                &held.operation.principal,
                workspace_id,
                &request.id,
                ApprovalDecision::Approve,
                None,
                &TraceId::parse(&"d".repeat(32)).unwrap(),
            )
            .await
    };

    let refused = cancel().await;
    assert!(matches!(refused, Err(Error::OwnedElsewhere)), "{refused:?}");
    let refused = decide().await;
    assert!(matches!(refused, Err(Error::OwnedElsewhere)), "{refused:?}");
    assert_eq!(
        fixture.run_record(workspace_id, run_id).await,
        handed,
        "a refused request writes nothing"
    );

    release
        .send(())
        .expect("release the recovered owner's chunk");
    let ended = fixture.terminal_record(workspace_id, run_id).await;
    assert_eq!(ended.chunk_sequence, handed.chunk_sequence + 1, "one chunk");
    let canceled = cancel()
        .await
        .expect("the record answers the retried cancel");
    assert_eq!(canceled.state(), RunState::Interrupted);
    let decided = decide()
        .await
        .expect("the record answers the retried decision");
    assert_eq!(decided.state, ApprovalState::Closed);
    assert!(
        fixture
            .events_after(workspace_id, run_id, last_sequence(&handed))
            .await
            .iter()
            .all(|event| !matches!(event.kind(), RunEventKind::CancellationRequested { .. })),
        "the cancel recorded nothing of its own"
    );
}

/// A `running` Run is another process's while its lease lives: one with no
/// claim (as the previous release left one between acceptance and its claim)
/// for one lease after acceptance, one another process holds until its lease
/// ends. The sweep leaves it alone, then interrupts it, unstarted, in one
/// chunk once the lease lapsed.
#[tokio::test]
async fn the_sweep_leaves_a_run_alone_until_its_lease_lapses() {
    for foreign in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
        let (workspace_id, principal) = fixture
            .user_scope("user:live-lease", "Leave a live lease alone")
            .await;
        let run_id = fixture
            .staged_run(&principal, &workspace_id, "live-lease")
            .await;
        let lease = fixture.runs.execution_policy.claim_lease_nanos();
        let (held, lapsed_at) = if foreign {
            let accepted_at = fixture
                .run_record(&workspace_id, &run_id)
                .await
                .accepted_at_unix_nanos;
            let lease_until = accepted_at + 3 * lease;
            let held = fixture
                .hand_to(&workspace_id, &run_id, "foreign-process", Some(lease_until))
                .await;
            (held, lease_until)
        } else {
            let unclaimed = fixture.release_worker_claim(&workspace_id, &run_id).await;
            let lapsed_at = unclaimed.accepted_at_unix_nanos + lease + 1;
            (unclaimed, lapsed_at)
        };

        assert_eq!(fixture.runs.sweep_expired_leases().await.unwrap(), 0);
        assert_eq!(
            fixture.run_record(&workspace_id, &run_id).await,
            held,
            "foreign {foreign}: the sweep writes nothing while the lease lives"
        );

        fixture.clock.next.store(lapsed_at, Ordering::SeqCst);
        assert_eq!(fixture.runs.sweep_expired_leases().await.unwrap(), 1);
        let ended = fixture.terminal_record(&workspace_id, &run_id).await;
        assert_eq!(ended.state, RunState::Interrupted, "foreign {foreign}");
        assert_eq!(ended.chunk_sequence, held.chunk_sequence + 1, "one chunk");
        assert_eq!(ended.execution_started_at_unix_nanos, None, "never started");
        assert!(matches!(
            fixture
                .events_after(&workspace_id, &run_id, last_sequence(&held))
                .await
                .iter()
                .map(RunEvent::kind)
                .collect::<Vec<_>>()
                .as_slice(),
            [RunEventKind::Ended {
                state: RunState::Interrupted,
                ..
            }]
        ));
    }
}

/// A Run accepted before shutdown began and launched only after it is not
/// lost by the drain: shutdown waits for that admission, and the owner it
/// registers late is interrupted with the rest.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shutdown_waits_for_an_admission_in_flight_and_interrupts_its_late_owner() {
    let engine = Arc::new(StopAwareEngine::default());
    let mut fixture = TestModules::new(engine.clone()).await;
    fixture.runs = fixture
        .runs
        .clone()
        .with_shutdown_drain(crate::ShutdownDrain::new(Duration::ZERO));
    let (workspace_id, principal) = fixture
        .user_scope("user:drain-admission", "Drain an admission")
        .await;
    let prepared = fixture
        .runs
        .prepare(&principal, &workspace_id, submission("late-launch"))
        .await
        .unwrap();
    let mut transaction = fixture.database.begin().await.unwrap();
    let staged = fixture
        .runs
        .accept_in_transaction(&mut transaction, prepared)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let run_id = staged.run_id().clone();

    let shutdown = tokio::spawn({
        let runs = fixture.runs.clone();
        async move { runs.shutdown().await }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !shutdown.is_finished(),
        "shutdown waits for the admission in flight"
    );
    fixture.runs.launch(staged).await.unwrap();
    shutdown.await.unwrap();

    assert_eq!(
        fixture.run_record(&workspace_id, &run_id).await.state,
        RunState::Interrupted,
        "the late owner is interrupted with the rest"
    );
    assert!(!fixture.runs.has_owner(&run_id));
}

/// During the Shutdown Drain a new Run is refused with `ShuttingDown` and
/// nothing is written, a live Run that ends on its own keeps its outcome,
/// and the drain waits for the rest; once it is out, the Runs still live are
/// interrupted. A zero drain interrupts every live Run at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shutdown_drain_lets_live_runs_end_refuses_new_ones_and_interrupts_the_rest() {
    const DRAIN: Duration = Duration::from_secs(1);
    for drain in [DRAIN, Duration::ZERO] {
        let engine = Arc::new(StopAwareEngine::default());
        let mut fixture = TestModules::new(engine.clone()).await;
        fixture.runs = fixture
            .runs
            .clone()
            .with_shutdown_drain(crate::ShutdownDrain::new(drain));
        let (workspace_id, principal) = fixture
            .user_scope(&format!("user:drain-{}", drain.as_millis()), "Drain")
            .await;
        let finishing = fixture
            .runs
            .start(&principal, &workspace_id, submission("until-released"))
            .await
            .unwrap();
        let lingering = fixture
            .runs
            .start(&principal, &workspace_id, submission("lingering"))
            .await
            .unwrap();
        wait_until_started(&fixture, &workspace_id, finishing.run_id()).await;
        wait_until_started(&fixture, &workspace_id, lingering.run_id()).await;

        let started = std::time::Instant::now();
        let shutdown = tokio::spawn({
            let runs = fixture.runs.clone();
            async move { runs.shutdown().await }
        });
        wait_until(|| fixture.runs.admission.check().is_err()).await;
        let before = fixture.runs.count_runs().await.unwrap();
        let refused = fixture
            .runs
            .start(&principal, &workspace_id, submission("refused"))
            .await;
        assert!(matches!(refused, Err(Error::ShuttingDown)), "{refused:?}");
        assert_eq!(fixture.runs.count_runs().await.unwrap(), before);
        if !drain.is_zero() {
            engine.released.store(true, Ordering::SeqCst);
            let finished =
                wait_for_terminal(&fixture.runs, &principal, &workspace_id, finishing.run_id())
                    .await;
            assert_eq!(finished.state(), RunState::Succeeded, "its own outcome");
            assert!(!shutdown.is_finished(), "the drain waits for the live Run");
        }
        shutdown.await.unwrap();
        let elapsed = started.elapsed();

        if drain.is_zero() {
            assert_eq!(
                fixture
                    .run_record(&workspace_id, finishing.run_id())
                    .await
                    .state,
                RunState::Interrupted
            );
        }
        assert_eq!(
            fixture
                .run_record(&workspace_id, lingering.run_id())
                .await
                .state,
            RunState::Interrupted,
            "drain {drain:?}"
        );
        if drain.is_zero() {
            assert!(
                elapsed < DRAIN,
                "a zero drain interrupts at once: {elapsed:?}"
            );
        } else {
            assert!(elapsed >= DRAIN, "the drain is waited out: {elapsed:?}");
        }
        assert!(!fixture.runs.has_owner(lingering.run_id()));
    }
}
