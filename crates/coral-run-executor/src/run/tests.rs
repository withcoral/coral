use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use tokio::sync::{Notify, mpsc, oneshot};

use super::*;
use crate::link::HeldUnit;
use crate::testing::{Answer, PullCall, RunCall, ScriptedCoral, package, start_run_executor};
use crate::wire::v1::{
    Committed, CoralFailure, Emit, Ended, Fatal, Finish, FinishEnd, InvocationResult, Invoke,
    Loaded, Location, Log, LogLevel, Request, RunEnd, Started, Terminate, TerminateReason,
    UpstreamOutcome, finish, invocation_result, request,
};
use crate::{CompilerClient as _, InProcessCompiler};

/// A scripted Coral and a Run Executor that holds two Runs, linked, with
/// its pull stream. The Run Executor retires when the sender drops.
async fn linked(applied_cap_bytes: usize) -> (ScriptedCoral, PullCall, oneshot::Sender<()>) {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let limits = RunLimits {
        applied_cap_bytes,
        ..RunLimits::default()
    };
    let retire = start_run_executor(&coral, 2, limits, None);
    let call = coral.next_call().await;
    (coral, call, retire)
}

fn compiled(source: &str) -> RunPackage {
    let compiler = InProcessCompiler::new(coral_v8::IsolateLimits::defaults()).unwrap();
    let program = compiler
        .compile(
            TEST_COMPILATION_TIMEOUT,
            source,
            &[],
            &coral_v8::BuiltinTools::default(),
            None,
        )
        .unwrap();
    package(program.javascript(), program.source_map())
}

/// Takes Run `run_id` and answers its Open with `package`, which loads.
async fn loaded(coral: &mut ScriptedCoral, run_id: &str, package: RunPackage) -> RunCall {
    coral.take(run_id, "claim-1");
    let mut run = coral.next_run().await;
    assert_eq!(
        run.open,
        Open {
            run_id: run_id.to_owned(),
            claim_id: "claim-1".to_owned(),
            protocol_level: PROTOCOL_LEVEL,
        }
    );
    run.send(run_response::Message::Package(package)).await;
    assert_eq!(
        run.next().await,
        Some(run_request::Message::Loaded(Loaded {}))
    );
    run
}

/// Takes one Run, answers its Open with `package` and its Loaded with
/// Started.
async fn started(
    coral: &mut ScriptedCoral,
    package: RunPackage,
    cpu_budget_ms: Option<u64>,
) -> RunCall {
    let run = loaded(coral, "run-1", package).await;
    let started = Started {
        deadline_ms: 60_000,
        cpu_budget_ms,
    };
    run.send(run_response::Message::Started(started)).await;
    run
}

async fn next_request(run: &mut RunCall) -> (u64, request::Kind) {
    match run.next().await {
        Some(run_request::Message::Request(Request {
            number,
            kind: Some(kind),
        })) => (number, kind),
        other => panic!("expected a request, got {other:?}"),
    }
}

fn finish(completion: finish::Completion) -> request::Kind {
    request::Kind::Finish(Finish {
        completion: Some(completion),
    })
}

fn finish_end(end: FinishEnd) -> request::Kind {
    finish(finish::Completion::End(end.into()))
}

fn emit(value_json: &str, line: u32) -> request::Kind {
    request::Kind::Emit(Emit {
        value_json: value_json.to_owned(),
        location: Some(Location {
            tool_id: String::new(),
            line,
            column: 13,
            ..Location::default()
        }),
    })
}

fn result(number: u64, bytes: u64, outcome: invocation_result::Outcome) -> run_response::Message {
    run_response::Message::InvocationResult(InvocationResult {
        number,
        bytes,
        outcome: Some(outcome),
    })
}

/// Catches: requests numbered out of the Program's order, an `InvocationResult`
/// that settles the wrong Invoke, or a unit that `Ended` never frees.
#[tokio::test]
async fn a_program_invokes_emits_logs_and_returns_in_numbered_order() {
    let (mut coral, mut call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let source = r#"const [issue, missing] = await Promise.all([
  coral.providers.tracker.issues.get({ id: 7 }),
  coral.providers.tracker.issues.get({ id: 8 }).catch((error) => error.code),
]);
await coral.emit(issue.title);
console.warn("seen", missing);
return issue.id;"#;
    let mut run = started(&mut coral, compiled(source), None).await;
    for (expected, id, line) in [(1, 7, 2), (2, 8, 3)] {
        let (number, kind) = next_request(&mut run).await;
        let request::Kind::Invoke(Invoke {
            call_path,
            arguments_json,
            location,
        }) = kind
        else {
            panic!("expected an Invoke, got {kind:?}");
        };
        assert_eq!(
            (
                number,
                call_path.as_str(),
                arguments_json,
                location.map(|at| at.line)
            ),
            (
                expected,
                "coral.providers.tracker.issues.get",
                format!(r#"{{"id":{id}}}"#),
                Some(line)
            )
        );
    }
    // Answered in the other order: each settles the Invoke it names.
    let rejection = CoralFailure {
        code: "tool_not_found".to_owned(),
        message: "Tool Call Path was not found".to_owned(),
        trace_id: String::new(),
        suggestions: Vec::new(),
    };
    run.send(result(
        2,
        0,
        invocation_result::Outcome::CoralFailure(rejection),
    ))
    .await;
    let issue = r#"{"id":7,"title":"Ship it"}"#.to_owned();
    run.send(result(1, 26, invocation_result::Outcome::ResultJson(issue)))
        .await;
    assert_eq!(next_request(&mut run).await, (3, emit(r#""Ship it""#, 5)));
    let log = request::Kind::Log(Log {
        level: LogLevel::Warn.into(),
        message: "seen tool_not_found".to_owned(),
        location: Some(Location {
            tool_id: String::new(),
            line: 6,
            column: 9,
            ..Location::default()
        }),
    });
    assert_eq!(next_request(&mut run).await, (4, log));
    let value = finish(finish::Completion::ValueJson("7".to_owned()));
    assert_eq!(next_request(&mut run).await, (5, value));
    run.send(run_response::Message::Committed(Committed {
        request_number: 5,
    }))
    .await;
    let ended = Ended {
        end: RunEnd::Succeeded.into(),
    };
    run.send(run_response::Message::Ended(ended)).await;
    assert_eq!(
        call.rooms.recv().await,
        Some(2),
        "Ended frees the Run's unit"
    );
}

/// Catches: an Upstream Outcome that does not reach the Program as a
/// catchable `ToolError`, or a host failure that does.
#[tokio::test]
async fn an_invocation_result_settles_its_invoke_by_its_outcome() {
    let source = r"try { return await coral.providers.tracker.issues.get({ id: 7 }); }
catch (error) { return error.code; }";
    let upstream = UpstreamOutcome {
        code: "upstream_not_found".to_owned(),
        message: "The Tool's upstream answered 404".to_owned(),
        outcome_json: r#"{"status":404,"data":{},"metadata":{}}"#.to_owned(),
    };
    let caught = finish(finish::Completion::ValueJson(
        r#""upstream_not_found""#.to_owned(),
    ));
    for (case, outcome, end) in [
        (
            "an Upstream Outcome",
            invocation_result::Outcome::UpstreamOutcome(upstream),
            caught,
        ),
        (
            "a host failure",
            invocation_result::Outcome::Fatal(Fatal {}),
            finish_end(FinishEnd::HostTerminated),
        ),
    ] {
        let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
        let mut run = started(&mut coral, compiled(source), None).await;
        let (number, kind) = next_request(&mut run).await;
        assert!(
            matches!((number, &kind), (1, request::Kind::Invoke(_))),
            "{case}: {kind:?}"
        );
        run.send(result(1, 0, outcome)).await;
        assert_eq!(next_request(&mut run).await, (2, end), "{case}");
    }
}

/// Catches: an Emission or Log over the package's limits that reaches Coral,
/// or one the Program cannot catch.
#[tokio::test]
async fn a_value_over_its_limit_throws_at_the_call() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let source = r#"const caught: string[] = [];
try { await coral.emit("x".repeat(100)); } catch (error) { caught.push((error as Error).name); }
try { console.log("y".repeat(70_000)); } catch (error) { caught.push((error as Error).name); }
return caught;"#;
    let mut package = compiled(source);
    package.value_limits.as_mut().unwrap().host_bytes = 64;
    let mut run = started(&mut coral, package, None).await;
    let value = finish(finish::Completion::ValueJson(
        r#"["RangeError","RangeError"]"#.to_owned(),
    ));
    assert_eq!(next_request(&mut run).await, (1, value));
}

/// Catches: an Output Budget that leaves out an `InvocationResult`'s bytes,
/// or an end the Program can catch.
#[tokio::test]
async fn output_over_the_run_output_budget_ends_the_run() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let source = r#"await coral.providers.tracker.issues.list({});
await coral.emit("12345");
try { await coral.emit("1234567890"); } catch { return "caught"; }
return "kept";"#;
    let mut package = compiled(source);
    package.value_limits.as_mut().unwrap().run_output_bytes = 20;
    let mut run = started(&mut coral, package, None).await;
    assert_eq!(next_request(&mut run).await.0, 1);
    run.send(result(
        1,
        5,
        invocation_result::Outcome::ResultJson("[]".to_owned()),
    ))
    .await;
    // 5 + 7 bytes fit in 20; 12 more do not, but without the result's 5
    // they would.
    assert_eq!(next_request(&mut run).await, (2, emit(r#""12345""#, 2)));
    let end = finish_end(FinishEnd::OutputBudget);
    assert_eq!(next_request(&mut run).await, (3, end));
}

/// Catches: a Final Value left out of the Run Output Budget, or one counted a
/// byte short.
#[tokio::test]
async fn a_final_value_counts_against_the_run_output_budget() {
    let program = compiled("await coral.emit(\"12345\");\nreturn \"1234567\";");
    let value = finish(finish::Completion::ValueJson(r#""1234567""#.to_owned()));
    // The Emission's 7 bytes and the Final Value's 9 fill 16 exactly.
    for (budget, expected) in [(16, value), (15, finish_end(FinishEnd::OutputBudget))] {
        let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
        let mut package = program.clone();
        package.value_limits.as_mut().unwrap().run_output_bytes = budget;
        let mut run = started(&mut coral, package, None).await;
        assert_eq!(next_request(&mut run).await, (1, emit(r#""12345""#, 1)));
        assert_eq!(
            next_request(&mut run).await,
            (2, expected),
            "budget {budget}"
        );
    }
}

/// Catches: an Applied cap that counts Invokes, a Program that sends past it
/// (its Finish included), or one that a Committed never releases.
#[tokio::test]
async fn the_applied_cap_holds_the_next_request_until_committed() {
    let (mut coral, _call, _retire) = linked(8).await;
    let source = r#"await Promise.all([1, 2, 3].map((id) =>
  coral.providers.tracker.issues.get({ id, note: "longer than the cap" })));
await coral.emit("123456789");
console.info("next");
await coral.emit("123456789");
return 1;"#;
    let mut run = started(&mut coral, compiled(source), None).await;
    // Each Invoke is longer than the cap of 8, but counts no output: none waits.
    for number in 1..=3 {
        let (sent, kind) = next_request(&mut run).await;
        assert_eq!(sent, number);
        assert!(matches!(kind, request::Kind::Invoke(_)), "{kind:?}");
    }
    for number in 1..=3 {
        run.send(result(
            number,
            0,
            invocation_result::Outcome::ResultJson("{}".to_owned()),
        ))
        .await;
    }
    assert_eq!(next_request(&mut run).await, (4, emit(r#""123456789""#, 3)));
    // 11 output bytes above the watermark, more than the cap: the Log waits.
    let held = tokio::time::timeout(Duration::from_millis(300), run.next()).await;
    assert!(held.is_err(), "the next request waits, got {held:?}");
    run.send(run_response::Message::Committed(Committed {
        request_number: 4,
    }))
    .await;
    let (number, kind) = next_request(&mut run).await;
    assert!(
        matches!((number, &kind), (5, request::Kind::Log(log)) if log.message == "next"),
        "{kind:?}"
    );
    assert_eq!(next_request(&mut run).await, (6, emit(r#""123456789""#, 5)));
    // Finish waits for the cap like any other request.
    let held = tokio::time::timeout(Duration::from_millis(300), run.next()).await;
    assert!(held.is_err(), "Finish waits, got {held:?}");
    run.send(run_response::Message::Committed(Committed {
        request_number: 6,
    }))
    .await;
    let value = finish(finish::Completion::ValueJson("1".to_owned()));
    assert_eq!(next_request(&mut run).await, (7, value));
}

/// Catches: a Terminate that leaves a waiting Program blocked, or one that
/// lets Program code run after it.
#[tokio::test]
async fn terminate_ends_a_program_that_waits_on_a_request() {
    for (waits_on, source) in [
        (
            "an Invoke",
            r#"try { await coral.providers.tracker.issues.list({}); }
finally { await coral.emit("finally"); }"#,
        ),
        (
            "the Applied cap",
            r#"await coral.emit("123456789");
try { console.info("held"); } finally { await coral.emit("finally"); }"#,
        ),
    ] {
        let (mut coral, _call, _retire) = linked(8).await;
        let mut run = started(&mut coral, compiled(source), None).await;
        assert_eq!(next_request(&mut run).await.0, 1, "{waits_on}");
        let terminate = Terminate {
            reason: TerminateReason::Canceled.into(),
        };
        run.send(run_response::Message::Terminate(terminate)).await;
        let end = finish_end(FinishEnd::HostTerminated);
        assert_eq!(next_request(&mut run).await, (2, end), "{waits_on}");
    }
}

/// Catches: a `Loaded` for a package that did not load, or a Program that
/// runs without `Started`.
#[tokio::test]
async fn a_run_that_never_starts_finishes_as_request_one() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;

    // A script the isolate cannot compile: Finish in place of Loaded.
    let mut broken = compiled("return 1;");
    broken.script = "return (".to_owned();
    broken.sha256 =
        crate::compile_wire::package_sha256(&broken.script, &broken.source_map).to_vec();
    coral.take("run-1", "claim-1");
    let mut run = coral.next_run().await;
    run.send(run_response::Message::Package(broken)).await;
    assert!(
        matches!(
            next_request(&mut run).await,
            (
                1,
                request::Kind::Finish(Finish {
                    completion: Some(finish::Completion::Failure(_))
                })
            )
        ),
        "the load failure is request 1, with no Loaded before it"
    );

    // A Terminate after Loaded: Finish in place of Started.
    let mut run = loaded(&mut coral, "run-2", compiled(r#"await coral.emit("ran");"#)).await;
    let terminate = Terminate {
        reason: TerminateReason::Canceled.into(),
    };
    run.send(run_response::Message::Terminate(terminate)).await;
    let end = finish_end(FinishEnd::HostTerminated);
    assert_eq!(
        next_request(&mut run).await,
        (1, end),
        "the Program never ran"
    );
}

/// Catches: a Run stream task that panics and leaves its Program waiting
/// forever, or a unit of room it never frees.
#[tokio::test]
async fn a_panicked_stream_task_stops_its_program() {
    let held = Arc::new(AtomicU32::new(0));
    let freed = Arc::new(Notify::new());
    let unit = HeldUnit::take(&held, &freed);
    let (outbound, mut requests) = mpsc::unbounded_channel();
    let (host, exited) = load(compiled("return 1;"), outbound, RunLimits::default(), unit)
        .expect("the package loads");
    let loaded = requests.recv().await.and_then(|request| request.message);
    assert_eq!(loaded, Some(run_request::Message::Loaded(Loaded {})));
    // The Program waits for a Started that never comes.
    let task = tokio::spawn(async move {
        let _host = StopsOnDrop(host);
        panic!("the Run stream task panics");
    });
    assert!(task.await.unwrap_err().is_panic());
    // The thread drops its sender as it ends.
    let _ended = tokio::time::timeout(Duration::from_secs(10), exited)
        .await
        .expect("the Program's thread ends");
    tokio::time::timeout(Duration::from_secs(10), freed.notified())
        .await
        .expect("the unit frees");
    assert_eq!(held.load(Ordering::SeqCst), 0);
}

/// How a case breaks a loadable package.
type Breaks = fn(&mut RunPackage);

/// Catches: a package loaded although it breaks the protocol, or a closed
/// stream that keeps its unit.
#[tokio::test]
async fn a_package_that_breaks_the_protocol_closes_the_stream() {
    let (mut coral, mut call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let loadable = compiled("return 1;");
    let cases: [(&str, Breaks); 5] = [
        ("a digest that is not the package's", |package| {
            package.script.insert_str(0, "globalThis.changed = true; ");
        }),
        ("a source map the isolate cannot read", |package| {
            package.source_map = "not a source map".to_owned();
            package.sha256 =
                crate::compile_wire::package_sha256(&package.script, &package.source_map).to_vec();
        }),
        ("no value limits", |package| package.value_limits = None),
        ("a value limit of zero", |package| {
            package.value_limits.as_mut().unwrap().host_bytes = 0;
        }),
        ("a level two apart", |package| {
            package.protocol_level = PROTOCOL_LEVEL + 2;
        }),
    ];
    for (run_id, (case, breaks)) in cases.into_iter().enumerate() {
        let mut package = loadable.clone();
        breaks(&mut package);
        coral.take(&format!("run-{run_id}"), "claim-1");
        let mut run = coral.next_run().await;
        run.send(run_response::Message::Package(package)).await;
        assert_eq!(run.next().await, None, "{case}: nothing loads");
        assert_eq!(call.rooms.recv().await, Some(2), "{case}: the unit frees");
    }
    // The package each case breaks loads as it is.
    loaded(&mut coral, "run-loadable", loadable).await;
}

/// Catches: a Program over its Run CPU Budget that ends any other way.
#[cfg(unix)]
#[tokio::test]
async fn a_spent_cpu_budget_finishes_with_the_cpu_end() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let mut run = started(&mut coral, compiled("while (true) {}"), Some(100)).await;
    let end = finish_end(FinishEnd::CpuBudget);
    assert_eq!(next_request(&mut run).await, (1, end));
}
