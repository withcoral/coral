//! Program emissions, logs and invalid emitted values.
use super::*;

#[tokio::test]
async fn binary_output_validation_and_cumulative_limits_preserve_earlier_emissions() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture
        .live_runs(None)
        .with_value_limits(coral_value::ValueLimits::new(5, 1024, 4096, 160).expect("limits"));
    let (workspace, principal) = fixture
        .user_scope("user:binary-output", "Bound output")
        .await;
    let accepted = runs.start(&principal, &workspace, submission(r#"
        const binary = {_tag:"BinaryValue", encoding:"base64", mimeType:"image/png", data:"AP+ADQo=", byteLength:5};
        try { await coral.emit({...binary, byteLength:4}); throw Error("accepted invalid"); }
        catch (error) { if (!(error instanceof TypeError)) throw error; }
        await coral.emit(binary);
        await coral.emit(binary);
    "#)).await.expect("accepted");
    let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Failed(failure)) if failure.code() == RunFailureCode::ResourceLimitExceeded)
    );
    let emitted = terminal
        .events()
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::ProgramEmitted { value } => Some(value.as_json()),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        emitted,
        vec![
            &serde_json::json!({"_tag":"BinaryValue", "encoding":"base64", "mimeType":"image/png", "data":"AP+ADQo=", "byteLength":5})
        ]
    );
    assert_eq!(terminal.value_limits(), runs.value_limits());
}

#[tokio::test]
async fn emissions_and_logs_are_durable_ordered_and_separate_after_program_failure() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:program-output", "Publish Program output")
        .await;
    let accepted = runs
        .start(
            &principal,
            &workspace_id,
            submission(
                r#"
                    await coral.emit({ step: 1, status: "durable" });
                    if (console.debug("debug", 1) !== undefined) {
                        throw new Error("console.debug did not return undefined");
                    }
                    console.info("info");
                    console.log("log maps", "to info");
                    console.warn("warn");
                    console.error("error does not fail the Run");
                    throw new Error("fail after output");
                "#,
            ),
        )
        .await
        .expect("accept Program with output");
    let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;

    assert!(matches!(
        terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Failed(failure))
            if failure.code() == RunFailureCode::ProgramRejected
    ));
    assert!(matches!(
        terminal.events(),
        [emitted, debug, info, log, warn, error]
            if matches!(
                    emitted.kind(),
                    RunEventKind::ProgramEmitted { value }
                        if value.as_json()
                            == &serde_json::json!({"step": 1, "status": "durable"})
                )
                && debug.kind() == &RunEventKind::ProgramLogged {
                    level: ProgramLogLevel::Debug,
                    message: "debug 1".to_string(),
                }
                && info.kind() == &RunEventKind::ProgramLogged {
                    level: ProgramLogLevel::Info,
                    message: "info".to_string(),
                }
                && log.kind() == &RunEventKind::ProgramLogged {
                    level: ProgramLogLevel::Info,
                    message: "log maps to info".to_string(),
                }
                && warn.kind() == &RunEventKind::ProgramLogged {
                    level: ProgramLogLevel::Warn,
                    message: "warn".to_string(),
                }
                && error.kind() == &RunEventKind::ProgramLogged {
                    level: ProgramLogLevel::Error,
                    message: "error does not fail the Run".to_string(),
                }
    ));
}

/// An invalid emitted value is never recorded. Unless the Program catches the
/// rejection, the Run fails with the invalid-value code.
#[tokio::test]
async fn invalid_emissions_fail_the_run_unless_the_program_catches_them() {
    const CAUGHT_INTRINSIC_TYPE_ERROR: &str = r#"
        const IntrinsicTypeError = TypeError;
        globalThis.TypeError = class ProgramTypeError extends Error {};
        let caughtIntrinsic = false;
        try {
            await coral.emit(Symbol());
        } catch (error) {
            caughtIntrinsic = error instanceof IntrinsicTypeError
                && !(error instanceof globalThis.TypeError);
        }
        if (!caughtIntrinsic) {
            throw new Error("Emission did not reject with intrinsic TypeError");
        }
        await coral.emit({ after: "caught invalid output" });
        return "continued";
    "#;
    let cases: [(&str, &str, Option<serde_json::Value>, &[serde_json::Value]); 5] = [
        ("uncaught", "await coral.emit(1n);", None, &[]),
        ("unobserved", "coral.emit(Symbol()); return 1;", None, &[]),
        (
            "unobserved during Final Value serialization",
            "return { toJSON() { coral.emit(Symbol()).then(); return 1; } };",
            None,
            &[],
        ),
        (
            "caught during Final Value serialization",
            "return { toJSON() { coral.emit(Symbol()).catch(() => undefined); return 1; } };",
            Some(serde_json::json!(1)),
            &[],
        ),
        (
            "caught as the intrinsic TypeError",
            CAUGHT_INTRINSIC_TYPE_ERROR,
            Some(serde_json::json!("continued")),
            &[serde_json::json!({"after": "caught invalid output"})],
        ),
    ];
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let (workspace_id, principal) = fixture
        .user_scope("user:invalid-emission", "Reject invalid Program output")
        .await;
    for (case, source, final_value, emitted) in cases {
        let accepted = runs
            .start(&principal, &workspace_id, submission(source))
            .await
            .expect("accept Program before Emission validation");
        let terminal = wait_for_terminal(&runs, &principal, &workspace_id, accepted.run_id()).await;
        match &final_value {
            None => assert!(
                matches!(
                    terminal.terminal_outcome(),
                    Some(RunTerminalOutcome::Failed(failure))
                        if failure.code() == RunFailureCode::InvalidProgramValue
                ),
                "{case}: {terminal:?}"
            ),
            Some(expected) => assert!(
                matches!(
                    terminal.terminal_outcome(),
                    Some(RunTerminalOutcome::Succeeded {
                        final_value: FinalValue::Present(value)
                    }) if value.as_json() == expected
                ),
                "{case}: {terminal:?}"
            ),
        }
        let recorded = terminal
            .events()
            .iter()
            .filter_map(|event| match event.kind() {
                RunEventKind::ProgramEmitted { value } => Some(value.as_json().clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(recorded, emitted, "{case}");
    }
}
