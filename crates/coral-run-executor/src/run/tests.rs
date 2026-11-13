use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use coral_v8::LinkedToolSource;
use tokio::sync::{Notify, mpsc, oneshot};

use super::*;
use crate::link::HeldRun;
use crate::testing::{Answer, PullCall, RunCall, ScriptedCoral, package, start_run_executor};
use crate::wire::v1::{
    Committed, Complete, CoralFailure, Emit, Ended, Fatal, Finish, FinishEnd, InvocationResult,
    Invoke, LinkedSnapshot, LinkedToolRoot, Loaded, Location, Log, LogLevel, Permitted, Request,
    RunEnd, Started, Terminate, TerminateReason, UpstreamOutcome, complete, finish,
    invocation_result, request,
};
use crate::{CompilerClient as _, InProcessCompiler};

/// The number of the next request that parks on the Applied cap, within 10 s.
async fn parks(parked: &mut mpsc::UnboundedReceiver<u64>) -> u64 {
    tokio::time::timeout(Duration::from_secs(10), parked.recv())
        .await
        .expect("a request parks on the Applied cap")
        .expect("the Run Executor reports each park")
}

/// A scripted Coral and a Run Executor that holds two Runs, linked, with
/// its pull stream. The Run Executor retires when the sender drops.
async fn linked(applied_cap_bytes: usize) -> (ScriptedCoral, PullCall, oneshot::Sender<()>) {
    let (coral, call, retire, _parked) = parking(applied_cap_bytes).await;
    (coral, call, retire)
}

/// [`linked`], with the number of each request that parks on the Applied
/// cap.
async fn parking(
    applied_cap_bytes: usize,
) -> (
    ScriptedCoral,
    PullCall,
    oneshot::Sender<()>,
    mpsc::UnboundedReceiver<u64>,
) {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let (on_parked, parked) = mpsc::unbounded_channel();
    let limits = RunLimits {
        applied_cap_bytes,
        on_parked: Some(Box::leak(Box::new(on_parked))),
        ..RunLimits::default()
    };
    let retire = start_run_executor(&coral, 2, limits, None);
    let call = coral.next_call().await;
    (coral, call, retire, parked)
}

async fn compiled(source: &str) -> RunPackage {
    compiled_with(source, &[]).await
}

/// `source` compiled with each linked Tool, each named by its snapshot.
async fn compiled_with(source: &str, linked: &[(LinkedToolSource, LinkedSnapshot)]) -> RunPackage {
    let compiler = InProcessCompiler::new(coral_v8::IsolateLimits::defaults()).unwrap();
    let sources: Vec<_> = linked.iter().map(|(source, _)| source.clone()).collect();
    let program = compiler
        .compile(
            TEST_COMPILATION_TIMEOUT,
            source,
            &sources,
            &coral_v8::BuiltinTools::default(),
            None,
        )
        .await
        .unwrap();
    RunPackage {
        linked_snapshots: linked
            .iter()
            .map(|(_, snapshot)| snapshot.clone())
            .collect(),
        ..package(program.javascript(), program.source_map())
    }
}

/// The Private Tool at `path`, linked as snapshot `tool_id`.
fn private_tool(path: &str, tool_id: &str, source: &str) -> (LinkedToolSource, LinkedSnapshot) {
    let segments = path.split('.').map(str::to_owned).collect();
    let snapshot = LinkedSnapshot {
        path: path.to_owned(),
        tool_id: tool_id.to_owned(),
        root: LinkedToolRoot::Private.into(),
    };
    (
        LinkedToolSource::private(segments, source.to_owned()),
        snapshot,
    )
}

/// A tracker upstream's 404, as Coral relays it.
fn upstream_not_found() -> UpstreamOutcome {
    UpstreamOutcome {
        code: "upstream_not_found".to_owned(),
        message: "The Tool's upstream answered 404".to_owned(),
        outcome_json: r#"{"status":404,"data":{},"metadata":{}}"#.to_owned(),
    }
}

/// The Private Tool `notes.title`, snapshot `tool-7`, which reads an
/// issue's title.
fn notes_title() -> (LinkedToolSource, LinkedSnapshot) {
    private_tool(
        "notes.title",
        "tool-7",
        r"export default async function title({ id }: { id: number }) {
  const issue = await coral.providers.tracker.issues.get({ id });
  return issue.title;
}",
    )
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
    let mut run = started(&mut coral, compiled(source).await, None).await;
    for (expected, id, line) in [(1, 7, 2), (2, 8, 3)] {
        let (number, kind) = next_request(&mut run).await;
        let request::Kind::Invoke(Invoke {
            call_path,
            arguments_json,
            location,
            parent: None,
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
/// catchable `ToolError`, a host failure that does, or a rejected Perform
/// whose body runs.
#[tokio::test]
async fn an_invocation_result_settles_its_request_by_its_outcome() {
    let invokes = compiled(
        r"try { return await coral.providers.tracker.issues.get({ id: 7 }); }
catch (error) { return error.code; }",
    )
    .await;
    let performs = compiled_with(
        r"try { return await coral.private.notes.title({ id: 7 }); }
catch (error) { return error.code; }",
        &[notes_title()],
    )
    .await;
    let denied = CoralFailure {
        code: "policy_denied".to_owned(),
        message: "Policy denied the call".to_owned(),
        trace_id: String::new(),
        suggestions: Vec::new(),
    };
    let upstream = upstream_not_found();
    let caught = |code: &str| finish(finish::Completion::ValueJson(format!(r#""{code}""#)));
    let kind_of = |kind: &request::Kind| match kind {
        request::Kind::Invoke(_) => "an Invoke",
        request::Kind::Perform(_) => "a Perform",
        _ => "another request",
    };
    for (case, package, first, outcome, end) in [
        (
            "an Upstream Outcome",
            &invokes,
            "an Invoke",
            invocation_result::Outcome::UpstreamOutcome(upstream),
            caught("upstream_not_found"),
        ),
        (
            "a host failure",
            &invokes,
            "an Invoke",
            invocation_result::Outcome::Fatal(Fatal {}),
            finish_end(FinishEnd::HostTerminated),
        ),
        (
            "a rejected Perform",
            &performs,
            "a Perform",
            invocation_result::Outcome::CoralFailure(denied),
            caught("policy_denied"),
        ),
    ] {
        let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
        let mut run = started(&mut coral, package.clone(), None).await;
        let (number, kind) = next_request(&mut run).await;
        assert_eq!((number, kind_of(&kind)), (1, first), "{case}");
        run.send(result(1, 0, outcome)).await;
        // The next request is the Program's end: a rejected body never ran.
        assert_eq!(next_request(&mut run).await, (2, end), "{case}");
    }
}

/// Catches: a body call sent without its parent Perform or its Tool's call
/// site, a Complete for another Perform, or a Complete's value that does not
/// reach the caller.
#[tokio::test]
async fn a_private_tool_call_is_permitted_then_its_body_runs_and_completes() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    // `notes.loud` calls `notes.title`, whose body calls the tracker.
    let loud = private_tool(
        "notes.loud",
        "tool-8",
        r"export default async function loud({ id }: { id: number }) {
  return (await coral.private.notes.title({ id })).toUpperCase();
}",
    );
    let source = "return await coral.private.notes.loud({ id: 7 });";
    let package = compiled_with(source, &[notes_title(), loud]).await;
    let mut run = started(&mut coral, package, None).await;
    let permitted = || invocation_result::Outcome::Permitted(Permitted {});
    let (number, kind) = next_request(&mut run).await;
    let request::Kind::Perform(perform) = kind else {
        panic!("expected a Perform, got {kind:?}");
    };
    assert_eq!(
        (
            number,
            perform.call_path.as_str(),
            perform.arguments_json.as_str(),
            perform.parent
        ),
        (1, "coral.private.notes.loud", r#"{"id":7}"#, None)
    );
    run.send(result(1, 0, permitted())).await;
    let (number, kind) = next_request(&mut run).await;
    let request::Kind::Perform(perform) = kind else {
        panic!("expected a Perform, got {kind:?}");
    };
    let at = perform.location.map(|at| (at.line, at.tool_id));
    assert_eq!(
        (number, perform.call_path.as_str(), perform.parent, at),
        (
            2,
            "coral.private.notes.title",
            Some(1),
            Some((2, "tool-8".to_owned()))
        )
    );
    run.send(result(2, 0, permitted())).await;
    let invoke = request::Kind::Invoke(Invoke {
        call_path: "coral.providers.tracker.issues.get".to_owned(),
        arguments_json: r#"{"id":7}"#.to_owned(),
        location: Some(Location {
            tool_id: "tool-7".to_owned(),
            line: 2,
            column: 54,
        }),
        parent: Some(2),
    });
    assert_eq!(next_request(&mut run).await, (3, invoke));
    let issue = r#"{"id":7,"title":"Ship it"}"#.to_owned();
    run.send(result(3, 26, invocation_result::Outcome::ResultJson(issue)))
        .await;
    let completes = |perform, json: &str| {
        request::Kind::Complete(Complete {
            perform,
            outcome: Some(complete::Outcome::ReturnedJson(json.to_owned())),
        })
    };
    assert_eq!(
        next_request(&mut run).await,
        (4, completes(2, r#""Ship it""#))
    );
    // The caller receives Coral's answer, not the body's return value.
    let title = r#""Ship this""#.to_owned();
    run.send(result(4, 0, invocation_result::Outcome::ResultJson(title)))
        .await;
    assert_eq!(
        next_request(&mut run).await,
        (5, completes(1, r#""SHIP THIS""#))
    );
    let value = r#""SHIP IT""#.to_owned();
    run.send(result(
        5,
        0,
        invocation_result::Outcome::ResultJson(value.clone()),
    ))
    .await;
    assert_eq!(
        next_request(&mut run).await,
        (6, finish(finish::Completion::ValueJson(value)))
    );
}

/// Catches: a permit taken as an Invoke's value, or a value taken as a
/// Perform's permit, instead of closing the stream.
#[tokio::test]
async fn an_answer_of_the_wrong_kind_closes_the_stream() {
    let cases = [
        (
            "a value for a Perform",
            "return await coral.private.notes.title({ id: 7 });",
            vec![notes_title()],
            invocation_result::Outcome::ResultJson("1".to_owned()),
        ),
        (
            "an Upstream Outcome for a Perform",
            "return await coral.private.notes.title({ id: 7 });",
            vec![notes_title()],
            invocation_result::Outcome::UpstreamOutcome(upstream_not_found()),
        ),
        (
            "a permit for an Invoke",
            "return await coral.providers.tracker.issues.get({ id: 7 });",
            Vec::new(),
            invocation_result::Outcome::Permitted(Permitted {}),
        ),
    ];
    for (case, source, tools, answer) in cases {
        let (mut coral, mut call, _retire) = linked(APPLIED_CAP_BYTES).await;
        let package = compiled_with(source, &tools).await;
        let mut run = started(&mut coral, package, None).await;
        let (number, _kind) = next_request(&mut run).await;
        run.send(result(number, 0, answer)).await;
        // No Ended: the Run Executor closes the stream itself, maybe after
        // the stopped Program's Finish. `next` fails on a stream that stays
        // open.
        while run.next().await.is_some() {}
        assert_eq!(call.rooms.recv().await, Some(2), "{case}: the unit frees");
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
    let mut package = compiled(source).await;
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
    let mut package = compiled(source).await;
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
    let program = compiled("await coral.emit(\"12345\");\nreturn \"1234567\";").await;
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
    let (mut coral, _call, _retire, mut parked) = parking(8).await;
    let source = r#"await Promise.all([1, 2, 3].map((id) =>
  coral.providers.tracker.issues.get({ id, note: "longer than the cap" })));
await coral.emit("123456789");
console.info("next");
await coral.emit("123456789");
return 1;"#;
    let mut run = started(&mut coral, compiled(source).await, None).await;
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
    assert_eq!(parks(&mut parked).await, 5, "the Log waits");
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
    assert_eq!(parks(&mut parked).await, 7, "Finish waits");
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
    for (waits_on, source, waits) in [
        (
            "an Invoke",
            r#"try { await coral.providers.tracker.issues.list({}); }
finally { await coral.emit("finally"); }"#,
            None,
        ),
        (
            "the Applied cap",
            r#"await coral.emit("123456789");
try { console.info("held"); } finally { await coral.emit("finally"); }"#,
            Some(2),
        ),
    ] {
        let (mut coral, _call, _retire, mut parked) = parking(8).await;
        let mut run = started(&mut coral, compiled(source).await, None).await;
        assert_eq!(next_request(&mut run).await.0, 1, "{waits_on}");
        if let Some(number) = waits {
            assert_eq!(
                parks(&mut parked).await,
                number,
                "{waits_on}: the Log waits"
            );
        }
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
    let mut broken = compiled("return 1;").await;
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
    let mut run = loaded(
        &mut coral,
        "run-2",
        compiled(r#"await coral.emit("ran");"#).await,
    )
    .await;
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
    let unit = HeldRun::take(&held, &freed);
    let (outbound, mut requests) = mpsc::unbounded_channel();
    let (host, exited) = load(
        compiled("return 1;").await,
        outbound,
        RunLimits::default(),
        unit,
    )
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
    let loadable = compiled("return 1;").await;
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
    let mut run = started(&mut coral, compiled("while (true) {}").await, Some(100)).await;
    let end = finish_end(FinishEnd::CpuBudget);
    assert_eq!(next_request(&mut run).await, (1, end));
}
