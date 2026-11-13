use std::sync::Arc;
use std::time::{Duration, Instant};

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use coral_v8::LinkedToolSource;
use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use super::*;
use crate::link::{HeldRun, Runs};
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
    let (coral, call, retire, _parked) = parking(applied_cap_bytes, HELD_CAP_REQUESTS).await;
    (coral, call, retire)
}

/// [`linked`] with a held cap, and the number of each request that parks on
/// a cap.
async fn parking(
    applied_cap_bytes: usize,
    held_cap: usize,
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
        held_cap,
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
    start(&run, 60_000, cpu_budget_ms).await;
    run
}

/// A Program that waits on an Invoke.
const WAITS: &str = "await coral.providers.tracker.issues.list({});";

/// Sends `Started` with the time to the deadline and the CPU budget.
async fn start(run: &RunCall, deadline_ms: u64, cpu_budget_ms: Option<u64>) {
    let started = Started {
        deadline_ms,
        cpu_budget_ms,
    };
    run.send(run_response::Message::Started(started)).await;
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
    run.send(committed(5)).await;
    run.send(ended()).await;
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
    let (mut coral, _call, _retire, mut parked) = parking(8, HELD_CAP_REQUESTS).await;
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
    run.send(committed(4)).await;
    let (number, kind) = next_request(&mut run).await;
    assert!(
        matches!((number, &kind), (5, request::Kind::Log(log)) if log.message == "next"),
        "{kind:?}"
    );
    assert_eq!(next_request(&mut run).await, (6, emit(r#""123456789""#, 5)));
    // Finish waits for the cap like any other request.
    assert_eq!(parks(&mut parked).await, 7, "Finish waits");
    run.send(committed(6)).await;
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
        let (mut coral, _call, _retire, mut parked) = parking(8, HELD_CAP_REQUESTS).await;
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
    let runs = Arc::new(Runs::default());
    let HeldRun { unit, stream } = HeldRun::take(&runs);
    let (outbound, mut requests) = mpsc::unbounded_channel();
    let Program { host, stopped, .. } = load(
        compiled("return 1;").await,
        outbound,
        RunLimits::default(),
        unit,
        &RunId::new("run-1"),
        stream,
    )
    .expect("the package loads");
    let loaded = requests.recv().await.and_then(|request| request.message);
    assert_eq!(loaded, Some(run_request::Message::Loaded(Loaded {})));
    // The Program waits for a Started that never comes.
    let task = tokio::spawn(async move {
        let _host = host;
        panic!("the Run stream task panics");
    });
    assert!(task.await.unwrap_err().is_panic());
    // The thread drops its sender as it ends.
    let _ended = tokio::time::timeout(Duration::from_secs(10), stopped)
        .await
        .expect("the Program's thread ends");
    let mut holding = runs.holding.subscribe();
    let freed = holding.wait_for(|holding| holding.held() == 0);
    tokio::time::timeout(Duration::from_secs(10), freed)
        .await
        .expect("the unit frees")
        .unwrap();
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

fn committed(request_number: u64) -> run_response::Message {
    run_response::Message::Committed(Committed { request_number })
}

fn ended() -> run_response::Message {
    run_response::Message::Ended(Ended {
        end: RunEnd::Succeeded.into(),
    })
}

fn title(number: u64, title: &str) -> run_response::Message {
    let json = format!(r#"{{"title":"{title}"}}"#);
    result(number, 0, invocation_result::Outcome::ResultJson(json))
}

/// Catches: a reopen that resends what `Committed` covers, sends a request
/// twice or out of order, loses the `Invoke` in flight or the `Finish`, or
/// closes the stream on Coral's repeat answer to a resent request.
#[tokio::test]
async fn a_lost_stream_reopens_and_resends_what_is_above_the_watermark_once() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let source = r"const first = await coral.providers.tracker.issues.get({ id: 1 });
await coral.emit(first.title);
const second = await coral.providers.tracker.issues.get({ id: 2 });
return second.title;";
    let mut run = started(&mut coral, compiled(source).await, None).await;
    let open = run.open.clone();
    assert_eq!(next_request(&mut run).await.0, 1);
    run.send(title(1, "a")).await;
    assert_eq!(next_request(&mut run).await, (2, emit(r#""a""#, 2)));
    let (number, invoke) = next_request(&mut run).await;
    assert_eq!(number, 3);
    // The Invoke is in flight when the stream fails.
    run.fail(Status::unavailable("lost")).await;
    let mut run = coral.next_run().await;
    assert_eq!(run.open, open, "the same Open");
    run.send(committed(0)).await;
    assert!(matches!(
        next_request(&mut run).await,
        (1, request::Kind::Invoke(_))
    ));
    assert_eq!(next_request(&mut run).await, (2, emit(r#""a""#, 2)));
    assert_eq!(next_request(&mut run).await, (3, invoke.clone()));
    // Coral answers the resent Invoke 1 again: the stream stays open.
    run.send(title(1, "a")).await;
    run.send(title(3, "b")).await;
    let value = finish(finish::Completion::ValueJson(r#""b""#.to_owned()));
    assert_eq!(next_request(&mut run).await, (4, value.clone()));
    // A clean end moves the stream: it reopens at once.
    drop(run);
    let mut run = coral.next_run().await;
    run.send(committed(2)).await;
    assert_eq!(next_request(&mut run).await, (3, invoke));
    assert_eq!(next_request(&mut run).await, (4, value));
    run.send(committed(4)).await;
    run.send(ended()).await;
    assert_eq!(
        run.next().await,
        None,
        "nothing more, and the stream closes"
    );
}

/// Catches: a reopen that loads the package again instead of naming it by
/// its digest, or one that never sends `Loaded` again.
#[tokio::test]
async fn a_reopen_skips_the_load_by_the_digest() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let package = compiled(r#"await coral.emit("ran");"#).await;
    let run = loaded(&mut coral, "run-1", package.clone()).await;
    drop(run);
    let mut run = coral.next_run().await;
    // Not this script's digest: a load would close the stream.
    let again = RunPackage {
        script: "return 2;".to_owned(),
        ..package
    };
    run.send(run_response::Message::Package(again)).await;
    assert_eq!(
        run.next().await,
        Some(run_request::Message::Loaded(Loaded {}))
    );
    start(&run, 60_000, None).await;
    assert_eq!(next_request(&mut run).await, (1, emit(r#""ran""#, 1)));
}

/// How a case answers the reopen of a Run.
type Reanswer = fn(RunPackage) -> run_response::Message;

/// Catches: a Program that outlives `Ended` on a reopen, or a reopen
/// answered by a package that breaks the protocol and keeps its Program.
#[tokio::test]
async fn a_reopen_answered_by_ended_or_a_wrong_package_ends_the_program() {
    let cases: [(&str, bool, Reanswer); 3] = [
        ("Ended", true, |_package| ended()),
        ("a package for a Run that started", true, |package| {
            run_response::Message::Package(package)
        }),
        ("a package that is not the one loaded", false, |package| {
            let mut other = package.clone();
            other.sha256 = vec![0; 32];
            run_response::Message::Package(other)
        }),
    ];
    let source = "await coral.providers.tracker.issues.list({});";
    for (case, starts, reanswer) in cases {
        let (mut coral, mut call, _retire) = linked(APPLIED_CAP_BYTES).await;
        let package = compiled(source).await;
        let mut run = loaded(&mut coral, "run-1", package.clone()).await;
        if starts {
            start(&run, 60_000, None).await;
            assert_eq!(next_request(&mut run).await.0, 1, "{case}");
        }
        run.fail(Status::unavailable("lost")).await;
        let mut run = coral.next_run().await;
        run.send(reanswer(package)).await;
        assert_eq!(run.next().await, None, "{case}: the stream closes");
        assert_eq!(call.rooms.recv().await, Some(2), "{case}: the unit frees");
    }
}

/// Catches: a lost Run that stops redialing, redials without the back-off,
/// or ends its Program before the deadline, or a `Finish` at the deadline
/// that does not wait for the next stream.
#[tokio::test]
async fn a_lost_run_redials_with_the_backoff_and_ends_at_its_deadline() {
    let (mut coral, _call, _retire) = linked(APPLIED_CAP_BYTES).await;
    let source = "await coral.providers.tracker.issues.list({});";
    let mut run = loaded(&mut coral, "run-1", compiled(source).await).await;
    let deadline = Duration::from_secs(1);
    start(&run, 1_000, None).await;
    let started_at = Instant::now();
    assert_eq!(next_request(&mut run).await.0, 1);
    run.fail(Status::unavailable("lost")).await;
    // Each reopen that Coral ends before it answers waits longer: at least
    // half of 200 and 400 ms after the first.
    let mut reopens = Vec::new();
    let mut run = loop {
        let run = coral.next_run().await;
        if run.at.duration_since(started_at) > deadline + Duration::from_millis(500) {
            break run;
        }
        reopens.push(run.at);
    };
    assert!(reopens.len() >= 3, "several back-offs: {reopens:?}");
    let gaps = reopens.iter().zip(reopens.iter().skip(1));
    for ((earlier, later), floor) in gaps.zip([100, 200]) {
        assert!(
            later.duration_since(*earlier) >= Duration::from_millis(floor),
            "{reopens:?}"
        );
    }
    run.send(committed(1)).await;
    let end = finish_end(FinishEnd::DeadlineReached);
    assert_eq!(
        next_request(&mut run).await,
        (2, end),
        "kept for this stream"
    );
    run.send(committed(2)).await;
    run.send(ended()).await;
    assert_eq!(run.next().await, None);
}

/// Catches: a lost Run that runs past a cap, or one whose parked request
/// never goes once `Committed` makes room on the reopened stream.
#[tokio::test]
async fn a_lost_run_keeps_running_up_to_a_cap_then_waits() {
    let cases = [
        (
            "the Applied cap",
            8,
            HELD_CAP_REQUESTS,
            r#"await coral.emit("123456789"); console.info("next");"#,
            2,
        ),
        (
            "the held cap",
            APPLIED_CAP_BYTES,
            2,
            r#"await coral.emit("a"); await coral.emit("b"); console.info("next");"#,
            3,
        ),
    ];
    for (case, applied_cap_bytes, held_cap, source, parks_at) in cases {
        let (mut coral, _call, _retire, mut parked) = parking(applied_cap_bytes, held_cap).await;
        let run = loaded(&mut coral, "run-1", compiled(source).await).await;
        start(&run, 60_000, None).await;
        run.fail(Status::unavailable("lost")).await;
        assert_eq!(parks(&mut parked).await, parks_at, "{case}");
        let mut run = coral.next_run().await;
        run.send(committed(0)).await;
        for number in 1..parks_at {
            assert_eq!(next_request(&mut run).await.0, number, "{case}: resent");
        }
        run.send(committed(parks_at - 1)).await;
        let (number, kind) = next_request(&mut run).await;
        assert!(
            matches!((number, &kind), (n, request::Kind::Log(_)) if n == parks_at),
            "{case}: {kind:?}"
        );
    }
}

/// Catches: a Run Executor that waits forever for an isolate that ignores
/// termination, or one that leaves its other Runs behind when it retires.
#[tokio::test]
async fn an_isolate_that_misses_its_stop_retires_the_run_executor() {
    for with_another_run in [false, true] {
        let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
        let stall = &*Box::leak(Box::new(std::sync::RwLock::new(())));
        // Held for good: the stopped isolate never goes.
        Box::leak(Box::new(stall.write().unwrap()));
        let limits = RunLimits {
            stop_grace: Duration::from_millis(100),
            stall: Some(stall),
            ..RunLimits::default()
        };
        let (_retire, task) = crate::testing::spawn_run_executor(&coral, 2, limits, None);
        let mut call = coral.next_call().await;
        let waits = compiled(WAITS).await;
        let mut other = None;
        if with_another_run {
            let mut run = loaded(&mut coral, "run-2", waits.clone()).await;
            start(&run, 60_000, None).await;
            assert_eq!(next_request(&mut run).await.0, 1);
            other = Some(run);
        }
        let mut stuck = started(&mut coral, waits, None).await;
        assert_eq!(next_request(&mut stuck).await.0, 1);
        let reason = TerminateReason::Canceled.into();
        let terminate = run_response::Message::Terminate(Terminate { reason });
        stuck.send(terminate).await;

        // Alone, the stuck Run leaves nothing to wait for: it reports none,
        // then closes the stream at once.
        assert_eq!(call.rooms.recv().await, Some(0), "it retires");
        if let Some(mut run) = other {
            assert!(!task.is_finished(), "it waits for its other Run");
            let json = invocation_result::Outcome::ResultJson("[]".to_owned());
            run.send(result(1, 2, json)).await;
            let end = finish_end(FinishEnd::Absent);
            assert_eq!(next_request(&mut run).await, (2, end));
            run.send(run_response::Message::Ended(Ended {
                end: RunEnd::Succeeded.into(),
            }))
            .await;
        }
        crate::testing::retired(task).await;
    }
}
