//! The Run CPU Budget through Run execution and the recorded outcome. Only a
//! platform with a thread CPU clock enforces it.
use super::*;

/// Catches: a spent budget recorded as anything but its own failure, a
/// Program that catches its end, or a CPU end that ends another Run.
#[tokio::test]
async fn a_spent_cpu_budget_fails_only_its_own_run() {
    let fixture = TestModules::new(Arc::new(
        crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap(),
    ))
    .await;
    let runs = fixture
        .runs
        .clone()
        .with_cpu_budget(crate::RunCpuBudget::new(Duration::from_millis(200)).expect("a budget"));
    let (workspace, principal) = fixture.user_scope("user:cpu", "Spend the budget").await;
    let busy = runs
        .start(
            &principal,
            &workspace,
            submission(r#"try { while (true) {} } catch { return "caught"; }"#),
        )
        .await
        .expect("accept busy Run");
    let healthy = runs
        .start(&principal, &workspace, submission("return 42;"))
        .await
        .expect("accept healthy Run");

    let busy = wait_for_terminal(&runs, &principal, &workspace, busy.run_id()).await;
    let Some(RunTerminalOutcome::Failed(failure)) = busy.terminal_outcome() else {
        panic!("the CPU end bypasses the Program's catch: {busy:?}");
    };
    assert_eq!(
        (failure.code(), failure.message()),
        (
            RunFailureCode::CpuLimitExceeded,
            "Program CPU limit exceeded"
        )
    );
    let healthy = wait_for_terminal(&runs, &principal, &workspace, healthy.run_id()).await;
    assert!(
        matches!(healthy.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
            final_value: FinalValue::Present(value)
        }) if value.as_json() == &serde_json::json!(42)),
        "{healthy:?}"
    );
}
