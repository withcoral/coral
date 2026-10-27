//! Cumulative output limits through the Run Module's start-and-observe surface.

use std::sync::Arc;

use coral_value::ValueLimits;
use serde_json::json;

use super::{ImmediateEngine, ScriptedUpstream, TestModules, submission, wait_for_terminal};
use crate::{FinalValue, InvocationResultKind, RunEventKind, RunFailureCode, RunTerminalOutcome};

#[tokio::test]
async fn context_impact_counts_utf8_payloads_once_across_child_pages() {
    let fixture = TestModules::with_upstream(
        Arc::new(crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default()).unwrap()),
        Arc::new(ConcurrentUpstream(tokio::sync::Barrier::new(2))),
    )
    .await;
    let tool = fixture.user_tool("context-impact", "Context impact").await;
    let accepted = fixture.runs.start(&tool.principal, &tool.workspace_id, submission(
        "console.log('é'); await coral.emit('é'); await Promise.all([1,2].map(() => coral.providers.weather.getForecast({city:'Paris'}))); return 'é';"
    )).await.unwrap();
    wait_for_terminal(
        &fixture.runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    let mut window = crate::RunEntryWindow::Start;
    let mut impacts = Vec::new();
    loop {
        let page = fixture
            .runs
            .activity_entries(
                &tool.principal,
                &tool.workspace_id,
                accepted.run_id(),
                &window,
                1,
                None,
            )
            .await
            .unwrap();
        // Two 150-byte Tool Results; log 2, emission 4, final value 4.
        assert_eq!(page.run.context_impact_bytes, Some(-290));
        assert_eq!(
            page.total, 5,
            "a full child page still reports changes outside its window"
        );
        for entry in page.entries {
            impacts.push(match entry {
                crate::RunEntry::Invocation(row) => row.context_impact_bytes.unwrap(),
                crate::RunEntry::Event {
                    context_impact_bytes,
                    ..
                } => context_impact_bytes,
            });
        }
        if !page.has_more {
            break;
        }
        window = crate::RunEntryWindow::After(page.next_after);
    }
    assert_eq!(impacts, vec![2, 4, -150, -150, 4]);
    let page = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &crate::RunEntryWindow::Viewport {
                offset: 2,
                anchor: None,
                preceding: 0,
            },
            2,
            None,
        )
        .await
        .unwrap();
    assert_eq!(page.offset, 2);
    assert_eq!(page.entries.len(), 2);
    let crate::RunEntry::Invocation(invocation) = page.entries.first().unwrap() else {
        panic!("tool row")
    };
    let anchored = fixture
        .runs
        .activity_entries(
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
            &crate::RunEntryWindow::Viewport {
                offset: 0,
                anchor: Some(crate::RunEntryAnchor::Invocation(invocation.id.clone())),
                preceding: 1,
            },
            3,
            None,
        )
        .await
        .unwrap();
    assert_eq!(anchored.offset, 1);
    assert_eq!(anchored.total, 5);
    assert!(
        matches!(anchored.entries.get(1), Some(crate::RunEntry::Invocation(value)) if value.id == invocation.id)
    );
}

#[tokio::test]
async fn many_emissions_share_a_byte_budget_with_logs_and_survive_limit_failure() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let (workspace, principal) = fixture
        .user_scope("user:output-budget", "Bound cumulative output")
        .await;
    // The log retains two UTF-8 bytes; each JSON string retains four, including quotes.
    for (budget, retained_emissions) in [(258, 64), (257, 63)] {
        let runs = fixture
            .live_runs(None)
            .with_value_limits(ValueLimits::new(5, 1024, 4096, budget).expect("limits"));
        let accepted = runs
            .start(
                &principal,
                &workspace,
                submission("console.log('é'); for (let i = 0; i < 64; i++) await coral.emit('é');"),
            )
            .await
            .expect("accepted");
        let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
        if budget == 258 {
            assert!(matches!(
                terminal.terminal_outcome(),
                Some(RunTerminalOutcome::Succeeded {
                    final_value: FinalValue::Absent
                })
            ));
        } else {
            assert!(matches!(terminal.terminal_outcome(),
                Some(RunTerminalOutcome::Failed(failure))
                    if failure.code() == RunFailureCode::ResourceLimitExceeded));
        }
        let values = terminal
            .events()
            .iter()
            .filter_map(|event| match event.kind() {
                RunEventKind::ProgramEmitted { value } => Some(value.as_json().clone()),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(values, vec![json!("é"); retained_emissions]);
        assert!(terminal.events().iter().any(|event| matches!(event.kind(),
            RunEventKind::ProgramLogged { message, .. } if message == "é")));
    }
}

/// Two Invocation results reach the Run Owner before its next commit: the
/// owner decides each against its exact in-memory total, so the second is
/// admitted or refused by the byte, and the recorded total is exactly what
/// was admitted. Tool arguments never count.
#[tokio::test]
async fn invocation_results_share_the_output_budget_by_the_byte_but_arguments_do_not() {
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        Arc::new(ConcurrentUpstream(tokio::sync::Barrier::new(2))),
    )
    .await;
    let tool = fixture
        .user_tool("user:invocation-output-budget", "Bound Invocation output")
        .await;
    // Each scripted HTTP result retains 150 JSON bytes, including its metadata.
    // The log and Emission retain six more bytes. Arguments exceed the entire budget.
    // (budget, both results admitted, recorded output bytes)
    let cases = [(306, true, 306), (305, false, 156)];
    for (budget, both_admitted, recorded_bytes) in cases {
        let runs = fixture
            .runs
            .clone()
            .with_value_limits(ValueLimits::new(5, 1024, 4096, budget).expect("limits"));
        let accepted = runs
            .start(
                &tool.principal,
                &tool.workspace_id,
                submission(
                    r"
                        console.log('é');
                        await coral.emit('é');
                        await Promise.all([1, 2].map(() =>
                            coral.providers.weather.getForecast({city: 'x'.repeat(512)})));
                    ",
                ),
            )
            .await
            .expect("accepted");
        let terminal = wait_for_terminal(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            accepted.run_id(),
        )
        .await;
        let results = super::retained_events(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id(),
        )
        .await
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::InvocationEnded { result_kind, .. } => Some(*result_kind),
            _ => None,
        })
        .collect::<Vec<_>>();
        if both_admitted {
            assert!(
                matches!(
                    terminal.terminal_outcome(),
                    Some(RunTerminalOutcome::Succeeded {
                        final_value: FinalValue::Absent
                    })
                ),
                "{budget}: {terminal:?}"
            );
            assert_eq!(
                results,
                vec![InvocationResultKind::UpstreamOutcome; 2],
                "{budget}"
            );
        } else {
            assert!(
                matches!(terminal.terminal_outcome(),
                Some(RunTerminalOutcome::Failed(failure))
                    if failure.code() == RunFailureCode::ProgramRejected
                        && failure.message() == "ToolError: Run output exceeds its limit"),
                "{budget}: {terminal:?}"
            );
            assert_eq!(
                results,
                vec![
                    InvocationResultKind::UpstreamOutcome,
                    InvocationResultKind::CoralFailure
                ],
                "{budget}"
            );
        }
        assert_eq!(
            fixture
                .run_record(&tool.workspace_id, terminal.run_id())
                .await
                .accumulated_output_bytes,
            recorded_bytes,
            "{budget}: the recorded total is exactly the admitted bytes"
        );
        assert!(terminal.events().iter().any(|event| matches!(event.kind(),
            RunEventKind::ProgramEmitted { value } if value.as_json() == &json!("é"))));
        assert!(terminal.events().iter().any(|event| matches!(event.kind(),
            RunEventKind::ProgramLogged { message, .. } if message == "é")));
    }
}

struct ConcurrentUpstream(tokio::sync::Barrier);

#[async_trait::async_trait]
impl crate::invocation::UpstreamSender for ConcurrentUpstream {
    async fn send(
        &self,
        _dispatch: coral_upstream::ReadyDispatch,
        _stop: Arc<crate::execution::ExecutionStop>,
    ) -> Result<coral_upstream::UpstreamOutcome, crate::invocation::UpstreamSendError> {
        self.0.wait().await;
        Ok(coral_upstream::UpstreamOutcome::Http(
            coral_upstream::HttpOutcome::new(200, json!(null)),
        ))
    }
}

#[tokio::test]
async fn rejected_invocation_output_leaves_the_remaining_budget_for_program_output() {
    let fixture = TestModules::with_upstream(
        Arc::new(
            crate::engine::V8ProgramEngine::new(crate::IsolateLimits::default())
                .expect("V8 engine"),
        ),
        Arc::new(ScriptedUpstream::json(200, json!(null))),
    )
    .await;
    let tool = fixture
        .user_tool(
            "user:remaining-output-budget",
            "Retain admitted output only",
        )
        .await;
    // One HTTP result (150), a log (2), an Emission (4), and a Final Value (1).
    let runs = fixture
        .runs
        .clone()
        .with_value_limits(ValueLimits::new(5, 1024, 4096, 157).expect("limits"));
    let accepted = runs
        .start(
            &tool.principal,
            &tool.workspace_id,
            submission(
                r"
                    await coral.providers.weather.getForecast({city: 'London'});
                    try {
                        await coral.providers.weather.getForecast({city: 'London'});
                        throw Error('accepted oversized result');
                    } catch (error) {
                        if (error.code !== 'upstream_result_too_large'
                            || error.message !== 'Run output exceeds its limit') throw error;
                    }
                    console.log('é');
                    await coral.emit('é');
                    return 0;
                ",
            ),
        )
        .await
        .expect("accepted");
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(
        matches!(terminal.terminal_outcome(),
        Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) })
            if value.as_json() == &json!(0)),
        "{terminal:?}"
    );
    assert!(
        super::retained_events(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .any(|event| matches!(
            event.kind(),
            RunEventKind::InvocationEnded {
                result_kind: InvocationResultKind::CoralFailure,
                ..
            }
        ))
    );
    assert!(terminal.events().iter().any(|event| matches!(event.kind(),
        RunEventKind::ProgramEmitted { value } if value.as_json() == &json!("é"))));
    assert!(terminal.events().iter().any(|event| matches!(event.kind(),
        RunEventKind::ProgramLogged { message, .. } if message == "é")));
}
