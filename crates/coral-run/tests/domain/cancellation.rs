//! Cancellation and deadlines of running work.
use super::run_stream_reconnect::{finish_with, forecast};
use super::run_streams::{Scripted, ended, finish_terminated, fire, started, take, take_any};
use super::*;
use coral_run_executor::wire::v1::RunEnd as WireEnd;

/// A Run of `source` with `timeout_millis` as its time limit, taken and
/// started on a scripted stream.
async fn started_with(
    fixture: &TestModules,
    principal: &AuthenticatedPrincipal,
    workspace_id: &coral_workspace::WorkspaceId,
    timeout_millis: u64,
) -> (crate::RunId, Scripted) {
    let accepted = fixture
        .runs
        .start(
            principal,
            workspace_id,
            RunSubmission::new(
                ProgramSource::parse("return 1;").expect("Program source"),
                RunTimeout::from_millis(timeout_millis).expect("Run timeout"),
            ),
        )
        .await
        .expect("accept the Program");
    let claim = take(&fixture.runs).await;
    let mut stream = Scripted::open(&fixture.runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;
    (accepted.run_id().clone(), stream)
}

#[tokio::test]
async fn run_deadline_is_measured_from_the_start_mark() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, principal) = fixture.user_scope("user:deadline", "Deadline").await;
    let (run_id, mut stream) = started_with(&fixture, &principal, &workspace_id, 5_000).await;
    let started_at = fixture
        .run_record(&workspace_id, &run_id)
        .await
        .execution_started_at_unix_nanos
        .expect("the start mark");
    fixture
        .clock
        .next
        .store(started_at + 5_000_000_001, Ordering::SeqCst);
    stream.request(1, finish_with("null"));
    stream
        .until(|read| read.contains(&ended(WireEnd::TimedOut)))
        .await;

    let terminal = wait_for_terminal(&fixture.runs, &principal, &workspace_id, &run_id).await;
    assert_eq!(terminal.state(), RunState::TimedOut);
    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::TimedOut)
    ));
}

/// Catches: a cancel that lands while the stream's enforcement chunk waits
/// to commit, and that the chunk then overwrites or dispatches past. The
/// cancel ends the Run with both pending Invocations `run_terminated`; the
/// held chunk finds the Run ended, and nothing is sent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_cancel_during_an_in_flight_enforcement_check_is_applied_at_once_and_run_ended_follows_it()
 {
    let upstream = Arc::new(ScriptedUpstream::json(200, serde_json::json!({})));
    let latch = EnforcementLatch::holding_arrivals();
    let probe = CommitProbe::new();
    let fixture = TestModules::with_execution_adapters(
        Arc::new(ImmediateEngine),
        upstream.clone(),
        latch.clone(),
        probe.clone(),
    )
    .await;
    let tool = fixture
        .user_tool(
            "user:cancel-mid-enforcement",
            "Cancel during an enforcement check",
        )
        .await;
    let (run_id, mut stream) = started(&fixture.runs, &tool).await;
    stream.request(1, forecast(None));
    stream.request(2, forecast(None));
    latch.until_arrived(2).await;
    let release = probe.hold_next_commit();
    latch.release_arrivals();
    // The batch of both waits to commit.
    wait_until(|| probe.held()).await;
    let before = fixture.run_record(&tool.workspace_id, &run_id).await;
    let canceled = fixture
        .runs
        .cancel(
            &tool.principal,
            &tool.workspace_id,
            &run_id,
            &immediate(None, 100),
        )
        .await
        .expect("the cancellation is recorded at once");
    assert_eq!(canceled.state(), RunState::Canceled);
    release.send(()).expect("release the enforcement chunk");
    finish_terminated(&mut stream, 3).await;
    let events = fixture
        .events_after(&tool.workspace_id, &run_id, last_sequence(&before))
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
    let rows = fixture.invocation_rows(&tool.workspace_id, &run_id).await;
    assert_eq!(rows.len(), 2);
    assert!(
        rows.iter().all(
            |row| row.result_kind == Some(crate::InvocationResultKind::RunTerminated)
                && row.dispatch_started_at_unix_nanos.is_none()
        ),
        "{rows:?}"
    );
    assert_eq!(upstream.sends.load(Ordering::SeqCst), 0);
}

/// Catches: a second cancel, received by another Coral Instance once the
/// first is recorded, that moves the Run or records a second request: it
/// is answered from the record.
#[tokio::test]
async fn a_second_cancel_elsewhere_is_answered_from_the_record_and_records_nothing() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let tool = fixture
        .user_tool("user:second-cancel", "Second cancel")
        .await;
    let (run_id, _stream) = started(&fixture.runs, &tool).await;
    let cancel = async |runs: &RunModule| {
        runs.cancel(
            &tool.principal,
            &tool.workspace_id,
            &run_id,
            &immediate(None, 100),
        )
        .await
        .expect("cancel")
    };
    assert_eq!(cancel(&fixture.runs).await.state(), RunState::Canceled);
    let recorded = fixture.run_record(&tool.workspace_id, &run_id).await;

    let second = cancel(&fixture.next_process()).await;

    assert_eq!(second.state(), RunState::Canceled);
    assert_eq!(
        fixture.run_record(&tool.workspace_id, &run_id).await,
        recorded,
        "the repeat moves nothing"
    );
    let requested = retained_events(&fixture.runs, &tool.principal, &tool.workspace_id, &run_id)
        .await
        .into_iter()
        .filter(|event| matches!(event.kind(), RunEventKind::CancellationRequested { .. }))
        .count();
    assert_eq!(requested, 1);
}

/// Catches: a cancel of a Run no take has started that leaves it to a
/// start, or records another requester.
#[tokio::test]
async fn queued_user_and_agent_cancellation_record_intent_without_starting_javascript() {
    for caller in CancellationCaller::ALL {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
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
        let events = retained_events(&fixture.runs, &user, &workspace_id, canceled.run_id()).await;
        assert!(
            events.iter().any(|event| matches!(
                event.kind(),
                RunEventKind::CancellationRequested { principal: requester }
                    if requester == principal.principal()
            )),
            "{caller:?}"
        );
        assert_eq!(
            fixture
                .run_record(&workspace_id, accepted.run_id())
                .await
                .execution_started_at_unix_nanos,
            None,
            "{caller:?}"
        );
        assert_eq!(take_any(&fixture.runs).await, None, "{caller:?}");
    }
}

/// A Run waiting for its start mark has no deadline, so the wait never times
/// it out: the time limit counts from the start.
#[tokio::test]
async fn a_queued_run_never_times_out_before_its_start() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let (workspace_id, principal) = fixture
        .user_scope("user:queued-timeout", "Wait longer than the time limit")
        .await;
    let accepted = fixture
        .runs
        .start(&principal, &workspace_id, submission("queued-timeout"))
        .await
        .expect("accept queued Program");
    // Twice the one-minute time limit passes before the start mark, and a
    // sweep runs meanwhile.
    fixture
        .clock
        .next
        .fetch_add(120_000_000_000, Ordering::SeqCst);
    fixture.runs.sweep_runs().await.expect("sweep");
    let claim = take(&fixture.runs).await;
    let mut stream = Scripted::open(&fixture.runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;
    stream.request(1, finish_with("null"));
    stream
        .until(|read| read.contains(&ended(WireEnd::Succeeded)))
        .await;

    let terminal =
        wait_for_terminal(&fixture.runs, &principal, &workspace_id, accepted.run_id()).await;
    assert_eq!(terminal.state(), RunState::Succeeded);
}

/// A cancel and the Run deadline: the first that lands decides the
/// terminal kind, and the record shows exactly one. The cancel first: the
/// Run ends Canceled with `cancellation.requested`. The deadline's timer
/// first: the Run ends `TimedOut`, the cancel writes no Event, and the
/// requester is answered from the record.
///
/// The test sends the deadline's timer message itself: a real timer short
/// enough to wait for would race the Run's start.
#[tokio::test]
async fn a_cancel_and_the_run_deadline_record_the_first_applied_kind() {
    for deadline_first in [false, true] {
        let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
        let (workspace_id, principal) = fixture
            .user_scope("user:cancel-vs-deadline", "Cancel against the deadline")
            .await;
        // The real deadline never fires here.
        let (run_id, _stream) = started_with(&fixture, &principal, &workspace_id, 60_000).await;
        if deadline_first {
            // The clock stays before the deadline, so only the timer's
            // order decides.
            fire(
                &fixture.runs,
                &run_id,
                crate::run_stream::run_loop::Timer::RunDeadline,
            );
            fixture.terminal_record(&workspace_id, &run_id).await;
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
    }
}

/// The record a cancel and the Run deadline leave: the first that lands
/// decides the one terminal kind, and `cancellation.requested` is
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
        tool: &ToolFixture,
        run: &crate::RunId,
    ) -> crate::RunUpdate {
        let workspace = &tool.workspace_id;
        match self {
            Self::Cancel(_) => fixture
                .runs
                .cancel(principal, workspace, run, &immediate(None, 100))
                .await
                .expect("cancel in-flight work"),
            // The owner's own timer applies the deadline on the host clock.
            Self::Deadline => {
                wait_until_async(async || {
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

/// An Invocation held before its Policy Enforcement Point, on a scripted
/// stream, or inside its upstream send, on the test's Run Executor.
enum InFlight {
    Waiting {
        gate: Arc<EnforcementGate>,
        release: oneshot::Sender<()>,
        upstream: Arc<ScriptedUpstream>,
        stream: Option<Scripted>,
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
            let mut fixture = TestModules::with_upstream(v8_engine(), upstream.clone()).await;
            fixture.serve().await;
            (Self::Dispatching { upstream }, fixture)
        } else {
            let (gate, release) = EnforcementGate::blocking();
            let upstream = Arc::new(ScriptedUpstream::json(200, result()));
            let fixture = TestModules::with_invocation_adapters(
                Arc::new(ImmediateEngine),
                upstream.clone(),
                gate.clone(),
            )
            .await;
            (
                Self::Waiting {
                    gate,
                    release,
                    upstream,
                    stream: None,
                },
                fixture,
            )
        }
    }

    /// Starts the Run's one call and waits until it is held.
    async fn start(&mut self, runs: &RunModule, run_id: &crate::RunId) {
        match self {
            Self::Waiting { gate, stream, .. } => {
                let claim = take(runs).await;
                let mut opened = Scripted::open(runs, &run_id.to_string(), &claim);
                opened.start().await;
                opened.request(1, forecast(None));
                *stream = Some(opened);
                wait_until(|| gate.entered()).await;
            }
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

    /// Lets the held work finish late, after `termination` ended the Run.
    async fn finish_late(self, termination: Termination) {
        match self {
            Self::Waiting {
                release, stream, ..
            } => {
                release.send(()).expect("release Policy Enforcement Point");
                // The loop ends the stream itself once it wrote the deadline;
                // a Run another writer ended waits for the Program's end.
                if let Termination::Cancel(_) = termination {
                    finish_terminated(&mut stream.expect("the stream"), 2).await;
                }
            }
            Self::Dispatching { upstream } => {
                upstream.release();
                wait_until(|| upstream.exited.load(Ordering::SeqCst)).await;
            }
        }
    }
}

/// Cancellation and the Run deadline end an Invocation still waiting at its
/// enforcement point without a send, and one already sent `indeterminate`;
/// its late result changes nothing.
#[tokio::test]
async fn termination_ends_waiting_invocations_and_marks_unfinished_dispatches_indeterminate() {
    let terminations = CancellationCaller::ALL
        .map(Termination::Cancel)
        .into_iter()
        .chain([Termination::Deadline]);
    for termination in terminations {
        for dispatching in [false, true] {
            let case = format!("{termination:?}, dispatching: {dispatching}");
            let (mut in_flight, fixture) = InFlight::arrange(dispatching).await;
            let tool = fixture
                .user_tool("user:terminate-in-flight", "Terminate in-flight work")
                .await;
            let principal = match termination {
                Termination::Cancel(caller) => {
                    fixture
                        .cancellation_caller(caller, &tool.workspace_id, &tool.principal)
                        .await
                }
                Termination::Deadline => tool.principal.clone(),
            };
            let accepted = fixture
                .runs
                .start(
                    &principal,
                    &tool.workspace_id,
                    RunSubmission::new(
                        ProgramSource::parse(INVOKE_WEATHER).unwrap(),
                        termination.timeout(),
                    ),
                )
                .await
                .expect("accept Program");
            in_flight.start(&fixture.runs, accepted.run_id()).await;

            let terminal = termination
                .apply(&fixture, &principal, &tool, accepted.run_id())
                .await;
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
                    &tool.principal,
                    &tool.workspace_id,
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
            in_flight.finish_late(termination).await;
            wait_until(|| !fixture.runs.has_loop(accepted.run_id())).await;
            let after_late_work = fixture
                .runs
                .observe(
                    &principal,
                    &tool.workspace_id,
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

#[tokio::test(flavor = "multi_thread")]
async fn deadline_terminates_noncooperative_javascript_without_replay() {
    coral_test_support::database_only!(Sqlite);
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
    let runs = fixture.live(None).await;
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
#[expect(
    clippy::too_many_lines,
    reason = "one cancellation scenario proves the terminal race and all User, Owner, and Agent visibility outcomes"
)]
async fn durable_cancellation_wins_completion_and_preserves_principal_confidentiality() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine)).await;
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
    let claim = take(&fixture.runs).await;
    let mut stream = Scripted::open(&fixture.runs, &accepted.run_id().to_string(), &claim);
    stream.start().await;

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

    // The Program's completion crosses the cancellation and loses.
    stream.request(1, finish_with("null"));
    wait_until(|| !fixture.runs.has_loop(accepted.run_id())).await;
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
        Arc::new(ImmediateEngine),
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
