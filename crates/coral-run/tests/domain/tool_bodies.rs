//! Private Tool calls whose body Coral performs in the Program, over the
//! real V8 engine: the outer call holds no slot under the Fan-out Limit, and
//! a Run that ends during a body ends the outer call as `run_terminated`
//! without waiting out the shutdown grace, while the calls the body made
//! keep their own outcomes. The owner's in-memory slot rule has its own row
//! in the `apply` table (`owner.rs`).
use super::*;

use crate::InvocationResultKind;

const WEATHER: &str = "coral.providers.weather.getForecast";

/// A Workspace whose `weather` Tool sends through an upstream that holds
/// each send until released, and whose Runs call its Private Tools. Its
/// shutdown grace is longer than any wait here, so a Run that waits it out
/// fails the wait.
struct BodyScope {
    fixture: TestModules,
    upstream: Arc<BlockingUpstream>,
    tool: ToolFixture,
}

impl BodyScope {
    async fn new(user: &str) -> Self {
        let upstream = Arc::new(BlockingUpstream::new(HttpOutcome::new(
            200,
            serde_json::json!({ "temperature": 21 }),
        )));
        let mut fixture = TestModules::with_upstream(
            Arc::new(
                crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                    .expect("V8 engine"),
            ),
            upstream.clone(),
        )
        .await;
        fixture.runs.execution_policy =
            crate::execution::ExecutionPolicy::testing_with_shutdown_grace(Duration::from_secs(30));
        let index =
            coral_search::SearchIndex::new(fixture.database.clone(), Duration::from_secs(3_600));
        fixture.runs = fixture
            .runs
            .clone()
            .with_saved_functions(crate::SavedFunctions::new(Self::functions(&fixture), index));
        let tool = fixture.user_tool(user, "Tool bodies").await;
        assert_eq!(tool.call_path, WEATHER);
        Self {
            fixture,
            upstream,
            tool,
        }
    }

    fn functions(fixture: &TestModules) -> SavedFunctionModule {
        SavedFunctionModule::new(
            fixture.database.clone(),
            fixture.workspaces.clone(),
            crate::IsolateLimits::default(),
            coral_test_support::TEST_COMPILATION_TIMEOUT,
        )
        .expect("the Private Tool module")
    }

    /// Saves the User's Private Tool at `path`, whose module exports
    /// `body` as its default.
    async fn save(&self, path: &str, body: &str) {
        self.save_module(path, &format!("export default {body};"))
            .await;
    }

    /// Saves the User's Private Tool at `path` with the module `source`.
    async fn save_module(&self, path: &str, source: &str) {
        Self::functions(&self.fixture)
            .save(
                &SavedFunctionChangeContext::new(
                    self.tool.principal.clone(),
                    60,
                    coral_workspace::TraceId::current_or_new(),
                ),
                &self.tool.workspace_id,
                SavedFunctionSave::new(
                    SavedFunctionPath::parse(path).expect("a path"),
                    SavedFunctionDescription::parse("A tool body").expect("a description"),
                    SavedFunctionSource::parse(source).expect("a source"),
                ),
            )
            .await
            .expect("save the Private Tool");
    }

    async fn start(&self, runs: &RunModule, run: RunSubmission) -> crate::RunId {
        runs.start(&self.tool.principal, &self.tool.workspace_id, run)
            .await
            .expect("accept the Program")
            .run_id()
            .clone()
    }

    async fn terminal(&self, run: &crate::RunId) -> crate::RunUpdate {
        wait_for_terminal(
            &self.fixture.runs,
            &self.tool.principal,
            &self.tool.workspace_id,
            run,
        )
        .await
    }

    async fn rows(&self, run: &crate::RunId) -> Vec<InvocationRow> {
        self.fixture
            .invocation_rows(&self.tool.workspace_id, run)
            .await
    }

    /// Ends `run` as `ending` says, while its body's call waits upstream.
    /// For a cancellation, returns the cancel call, which answers once the
    /// Run has ended.
    async fn end(
        &self,
        ending: Ending,
        run: &crate::RunId,
    ) -> Option<tokio::task::JoinHandle<Result<crate::RunUpdate, Error>>> {
        match ending {
            Ending::Canceled => {
                let (runs, principal, workspace_id, run) = (
                    self.fixture.runs.clone(),
                    self.tool.principal.clone(),
                    self.tool.workspace_id.clone(),
                    run.clone(),
                );
                Some(tokio::spawn(async move {
                    runs.cancel(&principal, &workspace_id, &run, &immediate(None, 100))
                        .await
                }))
            }
            Ending::Interrupted => {
                let claim = self
                    .fixture
                    .run_record(&self.tool.workspace_id, run)
                    .await
                    .execution_claim_id
                    .expect("a started Run");
                self.fixture
                    .runs
                    .owners
                    .find(run, &claim)
                    .expect("the owner is live")
                    .request_stop();
                None
            }
            Ending::TimedOut | Ending::CpuLimitExceeded => None,
            Ending::LostInstance => {
                let accepted_at = self
                    .fixture
                    .run_record(&self.tool.workspace_id, run)
                    .await
                    .accepted_at_unix_nanos;
                self.fixture
                    .expire_worker_claim(&self.tool.workspace_id, run, accepted_at)
                    .await;
                self.fixture
                    .clock
                    .next
                    .store(accepted_at + 2_000_000_000, Ordering::SeqCst);
                self.fixture
                    .next_process()
                    .sweep_expired_leases()
                    .await
                    .expect("sweep the lost instance's lapsed lease");
                None
            }
        }
    }

    async fn termination_is_recorded(&self, run: &crate::RunId) {
        wait_until_async(async || {
            self.fixture
                .run_record(&self.tool.workspace_id, run)
                .await
                .termination
                .is_some()
        })
        .await;
    }
}

/// The ended rows of `run` as (Call Path, result kind), sorted.
fn outcomes(rows: &[InvocationRow]) -> Vec<(&str, Option<InvocationResultKind>)> {
    let mut outcomes: Vec<_> = rows
        .iter()
        .map(|row| {
            assert_eq!(row.state, "ended", "{rows:?}");
            (row.call_path.as_str(), row.result_kind)
        })
        .collect();
    outcomes.sort_unstable_by_key(|(call_path, _)| *call_path);
    outcomes
}

// Catches: an outer Private Tool call that holds its slot under the Fan-out
// Limit while its body runs, so that the body's own calls wait for that slot
// and the Run deadlocks at the limit.
#[tokio::test]
async fn tool_bodies_that_make_more_concurrent_calls_than_the_fan_out_limit_complete() {
    let mut scope = BodyScope::new("user:tool-bodies-fan-out").await;
    scope.upstream.release.store(true, Ordering::SeqCst);
    scope.fixture.runs = scope
        .fixture
        .runs
        .clone()
        .with_fan_out_limit(crate::FanOutLimit::new(1).expect("a limit"));
    scope
        .save(
            "weather.inner",
            "async () => (await coral.providers.weather.getForecast()).data",
        )
        .await;
    scope
        .save(
            "weather.outer",
            "async () => Promise.all([
                coral.providers.weather.getForecast().then((result) => result.data),
                coral.providers.weather.getForecast().then((result) => result.data),
                coral.private.weather.inner(),
            ])",
        )
        .await;

    let run = scope
        .start(
            &scope.fixture.runs,
            submission(
                "return await Promise.all([coral.private.weather.outer(), coral.private.weather.outer()]);",
            ),
        )
        .await;

    let terminal = scope.terminal(&run).await;
    let forecast = serde_json::json!({ "temperature": 21 });
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!([
            [forecast, forecast, forecast],
            [forecast, forecast, forecast],
        ])),
        "{terminal:?}"
    );
    let rows = scope.rows(&run).await;
    let upstream = InvocationResultKind::UpstreamOutcome;
    assert_eq!(
        outcomes(&rows),
        [
            ("coral.private.weather.inner", Some(upstream)),
            ("coral.private.weather.inner", Some(upstream)),
            ("coral.private.weather.outer", Some(upstream)),
            ("coral.private.weather.outer", Some(upstream)),
            (WEATHER, Some(upstream)),
            (WEATHER, Some(upstream)),
            (WEATHER, Some(upstream)),
            (WEATHER, Some(upstream)),
            (WEATHER, Some(upstream)),
            (WEATHER, Some(upstream)),
        ]
    );
}

/// How a Run ends while a Private Tool's body runs.
#[derive(Debug, Clone, Copy)]
enum Ending {
    Canceled,
    TimedOut,
    /// Its owner is stopped, as the end of a Shutdown Drain does.
    Interrupted,
    /// The body spends the Run CPU Budget.
    CpuLimitExceeded,
    /// Its Coral Instance is lost and the next one's expired-lease sweep
    /// ends it.
    LostInstance,
}

// Catches: an open code-performed Invocation that the end of its Run leaves
// to wait out the shutdown grace, or ends as anything but `run_terminated`
// (an `indeterminate` dispatch, in particular), whether the Run is canceled,
// times out, is interrupted, spends its CPU budget in the body, or is ended by
// the recovery of a lost Coral Instance; or a termination that changes the
// outcome of a call the body made.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_ends_during_a_tool_body_ends_its_call_as_run_terminated() {
    for ending in [
        Ending::Canceled,
        Ending::TimedOut,
        Ending::Interrupted,
        Ending::CpuLimitExceeded,
        Ending::LostInstance,
    ] {
        let mut scope = BodyScope::new(&format!("user:tool-bodies-{ending:?}")).await;
        // The body's call returns at once unless the Run ends while it waits.
        if matches!(ending, Ending::CpuLimitExceeded) {
            scope.upstream.release.store(true, Ordering::SeqCst);
            scope.fixture.runs = scope.fixture.runs.clone().with_cpu_budget(
                crate::RunCpuBudget::new(Duration::from_millis(200)).expect("a budget"),
            );
        }
        scope
            .save(
                "weather.spin",
                "async () => { await coral.providers.weather.getForecast(); while (true) {} }",
            )
            .await;
        let timeout = if matches!(ending, Ending::TimedOut) {
            1_000
        } else {
            60_000
        };
        let run = scope
            .start(
                &scope.fixture.runs,
                RunSubmission::new(
                    ProgramSource::parse("return await coral.private.weather.spin();")
                        .expect("Program source"),
                    RunTimeout::from_millis(timeout).expect("Run timeout"),
                ),
            )
            .await;
        wait_until(|| scope.upstream.entered.load(Ordering::SeqCst)).await;

        let cancel = scope.end(ending, &run).await;
        if !matches!(ending, Ending::CpuLimitExceeded | Ending::LostInstance) {
            // The body's call ends with its own outcome while the Run ends.
            scope.termination_is_recorded(&run).await;
            scope.upstream.release.store(true, Ordering::SeqCst);
        }

        let terminal = scope.terminal(&run).await;
        let (state, body_call) = match ending {
            Ending::Canceled => (RunState::Canceled, InvocationResultKind::UpstreamOutcome),
            Ending::TimedOut => (RunState::TimedOut, InvocationResultKind::UpstreamOutcome),
            Ending::Interrupted => (RunState::Interrupted, InvocationResultKind::UpstreamOutcome),
            Ending::CpuLimitExceeded => (RunState::Failed, InvocationResultKind::UpstreamOutcome),
            // Nothing tells the next instance how the lost send ended.
            Ending::LostInstance => (RunState::Interrupted, InvocationResultKind::Indeterminate),
        };
        assert_eq!(terminal.state(), state, "{ending:?}: {terminal:?}");
        assert_eq!(
            outcomes(&scope.rows(&run).await),
            [
                (
                    "coral.private.weather.spin",
                    Some(InvocationResultKind::RunTerminated)
                ),
                (WEATHER, Some(body_call)),
            ],
            "{ending:?}"
        );
        if let Some(cancel) = cancel {
            let canceled = cancel.await.expect("join").expect("cancel the Run");
            assert_eq!(canceled.state(), RunState::Canceled);
        }
        scope.upstream.release.store(true, Ordering::SeqCst);
        wait_until(|| !scope.fixture.runs.has_owner(&run)).await;
    }
}

// Catches: a Program that ends while a tool body still waits on nothing
// Coral holds, leaving the outer Invocation `dispatching` so that the
// terminal write refuses and the Run stays `running`; or a Program that
// awaits it failing with a message that does not say why.
#[tokio::test]
async fn a_program_that_ends_before_a_tool_body_settles_ends_its_call_as_run_terminated() {
    let scope = BodyScope::new("user:tool-bodies-unsettled").await;
    scope
        .save("stuck.hang", "async () => { await new Promise(() => {}); }")
        .await;
    for (source, state) in [
        ("return await coral.private.stuck.hang();", RunState::Failed),
        ("coral.private.stuck.hang(); return 1;", RunState::Succeeded),
    ] {
        let run = scope.start(&scope.fixture.runs, submission(source)).await;
        let terminal = scope.terminal(&run).await;
        assert_eq!(terminal.state(), state, "{source}: {terminal:?}");
        if let Some(RunTerminalOutcome::Failed(failure)) = terminal.terminal_outcome() {
            assert_eq!(
                (failure.code(), failure.message()),
                (
                    RunFailureCode::ProgramExecutionFailed,
                    "Program awaited a Promise that nothing can settle"
                ),
                "{source}"
            );
        }
        assert_eq!(
            outcomes(&scope.rows(&run).await),
            [(
                "coral.private.stuck.hang",
                Some(InvocationResultKind::RunTerminated)
            )],
            "{source}"
        );
    }
}

// Catches: a call that ends with its Program, before its body did, traced
// as `indeterminate` (an error) though its row records `run_terminated`, or
// with a span that ends when the call was permitted instead of when it ended.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_that_ends_with_its_program_is_traced_as_run_terminated_when_it_ends() {
    super::trace::capture();
    let scope = BodyScope::new("user:tool-bodies-traced").await;
    scope.upstream.release.store(true, Ordering::SeqCst);
    scope
        .save(
            "weather.spin",
            "async () => { await coral.providers.weather.getForecast(); while (true) {} }",
        )
        .await;
    let run = scope
        .start(
            &scope.fixture.runs,
            submission("return await coral.private.weather.spin();"),
        )
        .await;
    wait_until(|| scope.upstream.sends.load(Ordering::SeqCst) == 1).await;
    let spinning = Duration::from_millis(300);
    tokio::time::sleep(spinning).await;
    let canceled = scope
        .fixture
        .runs
        .cancel(
            &scope.tool.principal,
            &scope.tool.workspace_id,
            &run,
            &immediate(None, 100),
        )
        .await
        .expect("cancel the Run");
    assert_eq!(canceled.state(), RunState::Canceled);
    wait_until(|| {
        super::trace::named(&super::trace::of_run(&run), "coral.invocation")
            .iter()
            .any(|span| {
                coral_telemetry::testing::attribute(span, "coral.tool.call_path").as_deref()
                    == Some("coral.private.weather.spin")
            })
    })
    .await;

    let spans = super::trace::of_run(&run);
    let call = super::trace::named(&spans, "coral.invocation")
        .into_iter()
        .find(|span| {
            coral_telemetry::testing::attribute(span, "coral.tool.call_path").as_deref()
                == Some("coral.private.weather.spin")
        })
        .expect("the call's span");
    assert_eq!(
        coral_telemetry::testing::attribute(call, "coral.invocation.result_kind").as_deref(),
        Some("run_terminated")
    );
    assert_eq!(call.status, opentelemetry::trace::Status::Unset);
    let traced = call
        .end_time
        .duration_since(call.start_time)
        .expect("the span ends after it starts");
    assert!(
        traced >= spinning,
        "the span ends when the call did, not when it was permitted: {traced:?}"
    );
}

/// The Call Path of `row`'s parent, another of `rows`.
fn parent_path<'a>(rows: &'a [InvocationRow], row: &InvocationRow) -> Option<&'a str> {
    let parent = row.parent_id.as_ref()?;
    rows.iter()
        .find(|row| &row.id == parent)
        .map(|row| row.call_path.as_str())
        .or_else(|| panic!("the parent is a row of the Run: {rows:?}"))
}

/// Each row of `rows` as (Call Path, its parent's Call Path, its children's
/// Call Paths), sorted.
fn tree(rows: &[InvocationRow]) -> Vec<(&str, Option<&str>, Vec<&str>)> {
    let mut tree: Vec<_> = rows
        .iter()
        .map(|row| {
            let mut children: Vec<_> = rows
                .iter()
                .filter(|child| child.parent_id.as_ref() == Some(&row.id))
                .map(|child| child.call_path.as_str())
                .collect();
            children.sort_unstable();
            (row.call_path.as_str(), parent_path(rows, row), children)
        })
        .collect();
    tree.sort();
    tree
}

// Catches: a call a tool body makes that is recorded with no parent, or
// with the wrong one: after an await, inside another tool's body, or when
// two calls of one tool run at once; a module's top-level call recorded
// under a call's Invocation, or not named for its module; or a child whose
// span is not its parent's child in the trace.
#[tokio::test]
async fn each_call_a_tool_body_makes_is_a_child_of_the_call_it_runs_for() {
    super::trace::capture();
    let scope = BodyScope::new("user:tool-bodies-children").await;
    scope.upstream.release.store(true, Ordering::SeqCst);
    scope
        .save(
            "weather.inner",
            "async () => (await coral.providers.weather.getForecast()).data",
        )
        .await;
    scope
        .save_module(
            "weather.outer",
            "await coral.providers.weather.getForecast();
            export default async function outer() {
                await Promise.resolve();
                return Promise.all([
                    coral.providers.weather.getForecast().then((result) => result.data),
                    coral.private.weather.inner(),
                ]);
            }",
        )
        .await;

    let run = scope
        .start(
            &scope.fixture.runs,
            submission(
                "return await Promise.all([coral.private.weather.outer(), coral.private.weather.outer()]);",
            ),
        )
        .await;

    let terminal = scope.terminal(&run).await;
    assert_eq!(terminal.state(), RunState::Succeeded, "{terminal:?}");
    let rows = scope.rows(&run).await;
    let (outer, inner) = ("coral.private.weather.outer", "coral.private.weather.inner");
    let mut expected = vec![
        // The module's top-level call.
        (WEATHER, None, vec![]),
        (outer, None, vec![inner, WEATHER]),
        (outer, None, vec![inner, WEATHER]),
        (WEATHER, Some(outer), vec![]),
        (WEATHER, Some(outer), vec![]),
        (inner, Some(outer), vec![WEATHER]),
        (inner, Some(outer), vec![WEATHER]),
        (WEATHER, Some(inner), vec![]),
        (WEATHER, Some(inner), vec![]),
    ];
    expected.sort();
    assert_eq!(tree(&rows), expected);

    // The top-level call names the module that made it: the one whose
    // body's calls the outer calls' are, not the inner tool's.
    let module_of = |parent: Option<&str>| {
        let sources: std::collections::BTreeSet<_> = rows
            .iter()
            .filter(|row| row.call_path == WEATHER && parent_path(&rows, row) == parent)
            .map(|row| row.source_saved_function_id.clone())
            .collect();
        assert_eq!(sources.len(), 1, "{parent:?}: {sources:?}");
        sources.into_iter().next().flatten().expect("a module")
    };
    assert_eq!(module_of(None), module_of(Some(outer)));
    assert_ne!(module_of(None), module_of(Some(inner)));

    spans_nest_as_rows_do(&run, &rows).await;
}

// Catches: a parent link that blocks deleting a retained Run, so that its
// nested Invocations outlive it or the deletion fails.
#[tokio::test]
async fn retention_deletes_a_run_with_its_nested_invocations() {
    let scope = BodyScope::new("user:tool-bodies-retention").await;
    scope.upstream.release.store(true, Ordering::SeqCst);
    scope
        .save(
            "weather.inner",
            "async () => (await coral.providers.weather.getForecast()).data",
        )
        .await;
    scope
        .save("weather.outer", "async () => coral.private.weather.inner()")
        .await;
    let run = scope
        .start(
            &scope.fixture.runs,
            submission("return await coral.private.weather.outer();"),
        )
        .await;
    assert_eq!(scope.terminal(&run).await.state(), RunState::Succeeded);
    assert_eq!(scope.rows(&run).await.len(), 3);

    let mut cleanup = scope.fixture.database.begin().await.expect("begin");
    crate::retention::delete_in_transaction(
        &mut cleanup,
        &scope.tool.workspace_id,
        &run,
        scope.tool.principal.principal(),
    )
    .await
    .expect("delete the retained Run");
    cleanup.commit().await.expect("commit the deletion");

    assert_eq!(scope.rows(&run).await, []);
}

// Pins: a call whose body left a nested tool call running whose own body
// never settles waits for that child, so its Run runs until its deadline and
// times out, and both calls end as `run_terminated`. Without the wait, the
// Program would end at once; a change to that is a deliberate one.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_call_waiting_on_a_child_that_never_settles_holds_its_run_until_the_deadline() {
    let scope = BodyScope::new("user:tool-bodies-held").await;
    scope
        .save("stuck.hang", "async () => { await new Promise(() => {}); }")
        .await;
    scope
        .save(
            "stuck.leave",
            "async () => { void coral.private.stuck.hang(); return 1; }",
        )
        .await;
    let run = scope
        .start(
            &scope.fixture.runs,
            RunSubmission::new(
                ProgramSource::parse("return await coral.private.stuck.leave();")
                    .expect("Program source"),
                RunTimeout::from_millis(1_000).expect("Run timeout"),
            ),
        )
        .await;

    let terminal = scope.terminal(&run).await;
    assert_eq!(terminal.state(), RunState::TimedOut, "{terminal:?}");
    let terminated = Some(InvocationResultKind::RunTerminated);
    assert_eq!(
        outcomes(&scope.rows(&run).await),
        [
            ("coral.private.stuck.hang", terminated),
            ("coral.private.stuck.leave", terminated),
        ]
    );
}

/// Asserts that each Invocation's span in `run`'s trace is a child of its
/// parent's span, or of the Run's execute span when it has no parent.
async fn spans_nest_as_rows_do(run: &crate::RunId, rows: &[InvocationRow]) {
    wait_until(|| {
        super::trace::named(&super::trace::of_run(run), "coral.invocation").len() == rows.len()
    })
    .await;
    let spans = super::trace::of_run(run);
    let span_of = |id: &crate::InvocationId| {
        super::trace::named(&spans, "coral.invocation")
            .into_iter()
            .find(|span| {
                coral_telemetry::testing::attribute(span, "coral.invocation.id").as_deref()
                    == Some(id.to_string().as_str())
            })
            .unwrap_or_else(|| panic!("the span of {id}"))
    };
    let [execute] = super::trace::named(&spans, "coral.run.execute")[..] else {
        panic!("one execute span");
    };
    for row in rows {
        let expected = row
            .parent_id
            .as_ref()
            .map_or(execute.span_context.span_id(), |parent| {
                span_of(parent).span_context.span_id()
            });
        assert_eq!(
            span_of(&row.id).parent_span_id,
            expected,
            "{} under {:?}",
            row.call_path,
            row.parent_id
        );
    }
}

// Catches: a call that a tool body's leftover work makes after the body
// ended, admitted as the ended call's child or refused under any code but
// `parent_invocation_ended`; or a call that ends before a child its body
// left running.
#[tokio::test]
async fn a_call_made_after_its_tool_body_ended_is_refused() {
    let scope = BodyScope::new("user:tool-bodies-late").await;
    scope.upstream.release.store(true, Ordering::SeqCst);
    scope
        .save(
            "stray.fire",
            "async () => {
                void (async () => {
                    await coral.providers.weather.getForecast();
                    try {
                        await coral.providers.weather.getForecast();
                    } catch (error) {
                        await coral.emit((error as { code: string }).code);
                    }
                })();
                return 1;
            }",
        )
        .await;

    let run = scope
        .start(
            &scope.fixture.runs,
            submission("return await coral.private.stray.fire();"),
        )
        .await;

    let terminal = scope.terminal(&run).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(1)),
        "{terminal:?}"
    );
    let emitted: Vec<_> = terminal
        .events()
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::ProgramEmitted { value } => Some(value.as_json().clone()),
            _ => None,
        })
        .collect();
    assert_eq!(emitted, [serde_json::json!("parent_invocation_ended")]);
    let rows = scope.rows(&run).await;
    let upstream = Some(InvocationResultKind::UpstreamOutcome);
    assert_eq!(
        outcomes(&rows),
        [("coral.private.stray.fire", upstream), (WEATHER, upstream)]
    );
    let [fire, child] = rows.as_slice() else {
        panic!("two rows: {rows:?}");
    };
    assert_eq!(child.parent_id.as_ref(), Some(&fire.id));
    assert!(
        fire.ended_at_unix_nanos >= child.ended_at_unix_nanos,
        "{rows:?}"
    );
}

// Catches: a call whose body ended but whose child still runs that the
// Run's cancellation leaves waiting for the child, or ends as anything but
// `run_terminated`; or a cancellation that changes the child's outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_run_that_ends_while_a_call_waits_for_its_children_ends_it_as_run_terminated() {
    let scope = BodyScope::new("user:tool-bodies-waiting").await;
    scope
        .save(
            "weather.leave",
            "async () => { void coral.providers.weather.getForecast(); return 1; }",
        )
        .await;
    let run = scope
        .start(
            &scope.fixture.runs,
            submission("return await coral.private.weather.leave();"),
        )
        .await;
    wait_until(|| scope.upstream.entered.load(Ordering::SeqCst)).await;

    let cancel = scope.end(Ending::Canceled, &run).await.expect("a cancel");
    scope.termination_is_recorded(&run).await;
    scope.upstream.release.store(true, Ordering::SeqCst);

    let terminal = scope.terminal(&run).await;
    assert_eq!(terminal.state(), RunState::Canceled, "{terminal:?}");
    assert_eq!(
        outcomes(&scope.rows(&run).await),
        [
            (
                "coral.private.weather.leave",
                Some(InvocationResultKind::RunTerminated)
            ),
            (WEATHER, Some(InvocationResultKind::UpstreamOutcome)),
        ]
    );
    let canceled = cancel.await.expect("join").expect("cancel the Run");
    assert_eq!(canceled.state(), RunState::Canceled);
}
