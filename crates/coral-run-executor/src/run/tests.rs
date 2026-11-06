use std::time::Duration;

use coral_test_support::TEST_COMPILATION_TIMEOUT;
use tokio::sync::oneshot;

use super::*;
use crate::testing::{Answer, RunCall, ScriptedCoral, package, start_run_executor};
use crate::wire::v1::{
    Committed, Emit, Ended, Finish, FinishEnd, Invoke, Loaded, Location, Log, LogLevel, Rejection,
    Request, RunEnd, Started, Terminate, TerminateReason, ToolResult, finish, request, tool_result,
};
use crate::{CompilerClient as _, InProcessCompiler};

/// A scripted Coral and a Run Executor that holds two Runs, linked. The Run
/// Executor retires when the sender drops.
async fn linked(applied_cap_bytes: usize) -> (ScriptedCoral, oneshot::Sender<()>) {
    let mut coral = ScriptedCoral::start([Answer::Welcome(PROTOCOL_LEVEL)]).await;
    let limits = RunLimits {
        applied_cap_bytes,
        ..RunLimits::default()
    };
    let retire = start_run_executor(&coral, 2, limits, None);
    coral.next_call().await;
    (coral, retire)
}

fn compiled(source: &str) -> Package {
    let compiler = InProcessCompiler::new(coral_v8::IsolateLimits::defaults()).unwrap();
    let program = compiler
        .compile(TEST_COMPILATION_TIMEOUT, source, &[])
        .unwrap();
    package(program.javascript(), program.source_map())
}

/// Takes one Run, answers its Open with `package` and its Loaded with
/// Started.
async fn started(
    coral: &mut ScriptedCoral,
    package: Package,
    cpu_budget_ms: Option<u64>,
) -> RunCall {
    coral.take("run-1", "claim-1");
    let mut run = coral.next_run().await;
    assert_eq!(
        run.open,
        Open {
            run_id: "run-1".to_owned(),
            claim_id: "claim-1".to_owned(),
            protocol_level: PROTOCOL_LEVEL,
        }
    );
    run.send(run_response::Message::Package(package)).await;
    assert_eq!(
        run.next().await,
        Some(run_request::Message::Loaded(Loaded {}))
    );
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
            saved_function_id: String::new(),
            line,
            column: 13,
        }),
    })
}

fn result(number: u64, bytes: u64, outcome: tool_result::Outcome) -> run_response::Message {
    run_response::Message::ToolResult(ToolResult {
        number,
        bytes,
        outcome: Some(outcome),
    })
}

/// Catches: requests numbered out of the Program's order, a `ToolResult` that
/// settles the wrong Invoke, or a unit that `Ended` never frees.
#[tokio::test]
async fn a_program_invokes_emits_logs_and_returns_in_numbered_order() {
    let (mut coral, _retire) = linked(APPLIED_CAP_BYTES).await;
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
    let rejection = Rejection {
        code: "tool_not_found".to_owned(),
        message: "Tool Call Path was not found".to_owned(),
        trace_id: String::new(),
        suggestions: Vec::new(),
    };
    run.send(result(2, 0, tool_result::Outcome::Rejection(rejection)))
        .await;
    let issue = r#"{"id":7,"title":"Ship it"}"#.to_owned();
    run.send(result(1, 26, tool_result::Outcome::ResultJson(issue)))
        .await;
    assert_eq!(next_request(&mut run).await, (3, emit(r#""Ship it""#, 5)));
    let log = request::Kind::Log(Log {
        level: LogLevel::Warn.into(),
        message: "seen tool_not_found".to_owned(),
        location: Some(Location {
            saved_function_id: String::new(),
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
    assert_eq!(coral.next_room().await, 2, "Ended frees the Run's unit");
}

/// Catches: an Emission or Log over the package's limits that reaches Coral,
/// or one the Program cannot catch.
#[tokio::test]
async fn a_value_over_its_limit_throws_at_the_call() {
    let (mut coral, _retire) = linked(APPLIED_CAP_BYTES).await;
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

/// Catches: an Output Budget that leaves out a `ToolResult`'s bytes, or an end
/// the Program can catch.
#[tokio::test]
async fn output_over_the_run_output_budget_ends_the_run() {
    let (mut coral, _retire) = linked(APPLIED_CAP_BYTES).await;
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
        tool_result::Outcome::ResultJson("[]".to_owned()),
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
        let (mut coral, _retire) = linked(APPLIED_CAP_BYTES).await;
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

/// Catches: a Program that sends past the Applied cap, or one that a
/// Committed never releases.
#[tokio::test]
async fn the_applied_cap_holds_the_next_request_until_committed() {
    let (mut coral, _retire) = linked(8).await;
    let source = r#"await coral.emit("123456789");
console.info("next");
return 1;"#;
    let mut run = started(&mut coral, compiled(source), None).await;
    assert_eq!(next_request(&mut run).await, (1, emit(r#""123456789""#, 1)));
    // 11 bytes above the watermark, more than the cap of 8: the Log waits.
    let held = tokio::time::timeout(Duration::from_millis(300), run.next()).await;
    assert!(held.is_err(), "the next request waits, got {held:?}");
    run.send(run_response::Message::Committed(Committed {
        request_number: 1,
    }))
    .await;
    let (number, kind) = next_request(&mut run).await;
    assert!(
        matches!((number, &kind), (2, request::Kind::Log(log)) if log.message == "next"),
        "{kind:?}"
    );
    let value = finish(finish::Completion::ValueJson("1".to_owned()));
    assert_eq!(next_request(&mut run).await, (3, value));
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
        let (mut coral, _retire) = linked(8).await;
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

/// How a case breaks a loadable package.
type Breaks = fn(&mut Package);

/// Catches: a package loaded although it breaks the protocol, or a closed
/// stream that keeps its unit.
#[tokio::test]
async fn a_package_that_breaks_the_protocol_closes_the_stream() {
    let (mut coral, _retire) = linked(APPLIED_CAP_BYTES).await;
    let loadable = compiled("return 1;");
    let cases: [(&str, Breaks); 5] = [
        ("a digest that is not the package's", |package| {
            package.script.insert_str(0, "globalThis.changed = true; ");
        }),
        ("a source map the isolate cannot read", |package| {
            package.source_map = "not a source map".to_owned();
            package.sha256 = package_digest(&package.script, &package.source_map);
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
        assert_eq!(coral.next_room().await, 2, "{case}: the unit frees");
    }
}

/// Catches: a Program over its Run CPU Budget that ends any other way.
#[cfg(unix)]
#[tokio::test]
async fn a_spent_cpu_budget_finishes_with_the_cpu_end() {
    let (mut coral, _retire) = linked(APPLIED_CAP_BYTES).await;
    let mut run = started(&mut coral, compiled("while (true) {}"), Some(100)).await;
    let end = finish_end(FinishEnd::CpuBudget);
    assert_eq!(next_request(&mut run).await, (1, end));
}
