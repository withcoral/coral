//! One memory-limit journey through Run execution, observation, and retained history.
use super::*;

#[derive(Default)]
struct PendingMemoryUpstream(tokio::sync::Notify);

#[async_trait]
impl UpstreamSender for PendingMemoryUpstream {
    async fn send(
        &self,
        dispatch: ReadyDispatch,
        _stop: Arc<ExecutionStop>,
    ) -> Result<UpstreamOutcome, crate::invocation::UpstreamSendError> {
        let ReadyDispatch::Http(dispatch) = dispatch else {
            panic!("HTTP fixture")
        };
        if dispatch
            .url()
            .query_pairs()
            .any(|(key, value)| key == "city" && value == "pending")
        {
            self.0.notify_one();
            std::future::pending().await
        } else {
            self.0.notified().await;
            Ok(UpstreamOutcome::Http(HttpOutcome::new(
                200,
                serde_json::json!({"temperature": 21}),
            )))
        }
    }
}

#[tokio::test]
async fn memory_termination_preserves_output_and_dispatched_uncertainty_without_poisoning_later_runs()
 {
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(
                coral_v8::IsolateLimits::new(16 * 1024 * 1024, 512 * 1024 * 1024).expect("limits"),
            )
            .expect("runtime"),
        ),
        Arc::new(PendingMemoryUpstream::default()),
    )
    .await;
    let tool = fixture
        .user_tool("user:memory", "Stop outstanding work")
        .await;
    let accepted = fixture
        .runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                r#"
            await coral.emit("before memory limit");
            coral.providers.weather.getForecast({ city: "pending" });
            await coral.providers.weather.getForecast({ city: "ready" });
            try {
                const retained = [];
                for (let i = 0; i < 128; i++) retained.push(new Array(131072).fill(i));
            } catch (error) {
                return "caught memory termination";
            }
            return "unexpected success";
        "#,
            ),
        )
        .await
        .expect("accepted");
    let terminal = wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    let Some(RunTerminalOutcome::Failed(failure)) = terminal.terminal_outcome() else {
        panic!("memory termination must bypass Program catch: {terminal:?}");
    };
    assert_eq!(failure.code(), RunFailureCode::MemoryLimitExceeded);
    assert_eq!(failure.message(), "Program memory limit exceeded");
    assert!(
        failure.error().is_none(),
        "host failure exposes no private V8 diagnostics"
    );
    assert!(matches!(terminal.events(), [emitted]
        if matches!(emitted.kind(), RunEventKind::ProgramEmitted { value }
            if value.as_json() == &serde_json::json!("before memory limit"))));
    assert!(
        retained_events(
            &fixture.runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                result_kind: crate::InvocationResultKind::Indeterminate,
                ..
            }
        )),
        "dispatched work retains uncertainty after termination"
    );

    let (other_workspace, other_principal) = fixture
        .user_scope("user:memory-other", "Continue in another Workspace")
        .await;
    let healthy = fixture
        .runs
        .start(&other_principal, &other_workspace, submission("return 42;"))
        .await
        .expect("accept healthy Run");
    let healthy = wait_for_terminal(
        &fixture.runs,
        &other_principal,
        &other_workspace,
        healthy.run_id(),
    )
    .await;
    assert!(
        matches!(healthy.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json() == &serde_json::json!(42))
    );
}
