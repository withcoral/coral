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

#[tokio::test]
async fn durable_cancellation_stops_execution_owned_by_an_independent_worker() {
    let engine = Arc::new(StopAwareEngine::default());
    let fixture = TestModules::new(engine.clone()).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:remote-cancel", "Cancel independent worker")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("remote-cancel"))
        .await
        .expect("accept Program");
    wait_until(|| engine.started.load(Ordering::SeqCst)).await;
    let canceling_worker = RunModule::with_adapters(
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

    let canceled = canceling_worker
        .cancel(
            &principal,
            &workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("cancel from independent worker");

    assert_eq!(canceled.state(), RunState::Canceled);
    // The canceling worker holds no owner for the Run: it ended the Run on
    // its own, and the owner stops at its next commit (an idle tick at the
    // latest), which finds the Run ended.
    tokio::time::timeout(Duration::from_secs(5), async {
        while !engine.stopped.load(Ordering::SeqCst) || fixture.runs.has_owner(accepted.run_id()) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the owner's next commit stops the Program and retires it");
    assert_eq!(engine.executions.load(Ordering::SeqCst), 1);
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

        let canceled = fixture
            .runs
            .cancel(
                &principal,
                &workspace_id,
                accepted.run_id(),
                &immediate(None, 100),
            )
            .await
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
        claim_gate.release.store(true, Ordering::SeqCst);
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

    let terminal =
        wait_for_terminal(&fixture.runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert_eq!(engine.executions.load(Ordering::SeqCst), 0);
    claim_gate.release.store(true, Ordering::SeqCst);
    wait_until(|| claim_gate.finished.load(Ordering::SeqCst) == 1).await;
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
        if let Self::Cancel(_) = self {
            return fixture
                .runs
                .cancel(principal, workspace, run, &immediate(None, 100))
                .await
                .expect("cancel in-flight work");
        }
        let accepted_at = retained_events(&fixture.runs, &operation.principal, workspace, run)
            .await
            .first()
            .expect("run.started")
            .occurred_at_unix_nanos();
        fixture
            .clock
            .next
            .store(accepted_at + 6_000_000_000, Ordering::SeqCst);
        fixture
            .runs
            .recover()
            .await
            .expect("apply the Run deadline");
        wait_for_terminal(&fixture.runs, principal, workspace, run).await
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
                        RunTimeout::from_millis(5_000).unwrap(),
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

    fixture.runs.recover().await.expect("recover stale claim");
    let interrupted = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("observe interrupted Run");
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

    fixture.runs.recover().await.expect("recover pending work");
    let interrupted = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("observe interrupted Run");
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
async fn recovery_marks_an_uncertain_dispatch_indeterminate_without_replay() {
    let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
        200,
        serde_json::json!({"temperature": 21}),
    )));
    let fixture = TestModules::with_upstream(
        Arc::new(InvokingEngine::new("coral.providers.weather.getForecast")),
        upstream.clone(),
    )
    .await;
    let operation = fixture
        .user_operation("user:recover-dispatching", "Recover dispatching Invocation")
        .await;
    let accepted = fixture
        .runs
        .start(
            &operation.principal,
            &operation.workspace_id,
            submission("recover-dispatching"),
        )
        .await
        .expect("accept Program");
    wait_until(|| upstream.entered.load(Ordering::SeqCst)).await;
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

    fixture
        .runs
        .recover()
        .await
        .expect("recover uncertain dispatch");
    let interrupted = fixture
        .runs
        .observe(
            &operation.principal,
            &operation.workspace_id,
            accepted.run_id(),
            &immediate(None, 100),
        )
        .await
        .expect("observe interrupted Run");
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
                result_kind: crate::InvocationResultKind::Indeterminate,
                ..
            }
        ))
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 1);
    upstream.release.store(true, Ordering::SeqCst);
    wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
    wait_until(|| !fixture.runs.has_owner(accepted.run_id())).await;
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
