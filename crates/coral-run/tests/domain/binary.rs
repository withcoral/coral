//! Binary host-crossing checks through the Run Module and real V8/HTTP Adapters.

use std::sync::Arc;

use coral_value::ValueLimits;
use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{ImmediateEngine, TestModules, retained_events, submission, wait_for_terminal};
use crate::{FinalValue, InvocationResultKind, RunEventKind, RunFailureCode, RunTerminalOutcome};

#[tokio::test]
async fn binary_metadata_and_base64_are_revalidated_for_emissions_and_final_values() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let limits = ValueLimits::new(5, 1024, 4096, 4096).expect("limits");
    let runs = fixture.live_runs(None).with_value_limits(limits);
    let (workspace, principal) = fixture
        .user_scope("user:binary-validation", "Validate binary crossings")
        .await;
    let accepted = runs.start(&principal, &workspace, submission(r#"
        const binary = {_tag:"BinaryValue", encoding:"base64", mimeType:"image/png", data:"AP+ADQo=", byteLength:5};
        const invalid = [
            {...binary, extra:true}, {...binary, encoding:"BASE64"}, {...binary, byteLength:4},
            {...binary, mimeType:"IMAGE/PNG"}, {...binary, mimeType:"image/png; charset=utf-8"},
            {...binary, mimeType:"image/*"}, {...binary, mimeType:"image"},
            {...binary, data:"AP+ADQo"}, {...binary, data:"AP-ADQo="},
            {...binary, data:"AP+ADQp="}, {...binary, name:".."}, {...binary, name:"."},
            {...binary, name:"a/b"}, {...binary, name:"a\\b"}, {...binary, name:"a\nb"},
            {...binary, name:"é".repeat(128)}, {...binary, name:null}
        ];
        for (const value of invalid) {
            try { await coral.emit({nested:value}); throw Error("invalid accepted"); }
            catch (error) { if (!(error instanceof TypeError)) throw error; }
        }
        try { await coral.emit({...binary, data:"AAAAAAAB", byteLength:6}); throw Error("oversized accepted"); }
        catch (error) { if (!(error instanceof RangeError)) throw error; }
        return {...binary, name:"é".repeat(127)+"a"};
    "#)).await.expect("accepted");
    let changed_deployment = runs.clone().with_value_limits(ValueLimits::default());
    let terminal = wait_for_terminal(
        &changed_deployment,
        &principal,
        &workspace,
        accepted.run_id(),
    )
    .await;
    assert_eq!(terminal.value_limits(), limits);
    let Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value),
    }) = terminal.terminal_outcome()
    else {
        panic!("valid final binary: {terminal:?}")
    };
    assert_eq!(value.as_json()["data"], "AP+ADQo=");
    assert_eq!(
        terminal.events().len(),
        0,
        "invalid Emissions create no output occurrences"
    );

    for (source, expected) in [
        (
            "return {_tag:'BinaryValue', encoding:'base64', mimeType:'image/png', data:'AA==', byteLength:2};",
            RunFailureCode::InvalidProgramValue,
        ),
        (
            "return 'x'.repeat(1023);",
            RunFailureCode::ResourceLimitExceeded,
        ),
    ] {
        let accepted = runs
            .start(&principal, &workspace, submission(source))
            .await
            .expect("accepted");
        let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
        assert!(
            matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Failed(failure)) if failure.code() == expected)
        );
    }
}

#[tokio::test]
async fn ordinary_json_fields_are_not_reserved_as_mcp_binary_payloads() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture.live_runs(None);
    let (workspace, principal) = fixture
        .user_scope("user:ordinary-json", "Preserve ordinary JSON")
        .await;
    let accepted = runs.start(&principal, &workspace, submission(r#"return {uri:"record:1", blob:"ordinary text", nested:{type:"image", data:"ordinary text"}, image:{type:"image", mimeType:"image/png", data:123}, resource:{type:"resource", resource:{blob:123}}};"#)).await.expect("accepted");
    let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &json!({"uri":"record:1", "blob":"ordinary text", "nested":{"type":"image", "data":"ordinary text"}, "image":{"type":"image", "mimeType":"image/png", "data":123}, "resource":{"type":"resource", "resource":{"blob":123}}}))
    );
}

#[tokio::test]
async fn final_value_cannot_exceed_the_remaining_durable_output_budget() {
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture
        .live_runs(None)
        .with_value_limits(ValueLimits::new(5, 1024, 4096, 10).expect("limits"));
    let (workspace, principal) = fixture
        .user_scope("user:final-budget", "Bound final output")
        .await;
    // Encoded JSON uses seven bytes for the Emission and three for the Final Value.
    let boundary = runs
        .start(
            &principal,
            &workspace,
            submission("await coral.emit('12345'); return '6';"),
        )
        .await
        .expect("accepted at the output boundary");
    let boundary = wait_for_terminal(&runs, &principal, &workspace, boundary.run_id()).await;
    assert!(
        matches!(boundary.terminal_outcome(), Some(RunTerminalOutcome::Succeeded {
        final_value: FinalValue::Present(value)
    }) if value.as_json() == &json!("6"))
    );

    let accepted = runs
        .start(
            &principal,
            &workspace,
            submission("await coral.emit('12345'); return '678';"),
        )
        .await
        .expect("accepted");
    let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Failed(failure)) if failure.code() == RunFailureCode::ResourceLimitExceeded)
    );
    assert!(terminal.events().iter().any(|event| matches!(event.kind(), RunEventKind::ProgramEmitted { value } if value.as_json() == &json!("12345"))));
    let accepted = runs
        .start(
            &principal,
            &workspace,
            submission("return {toJSON() { console.log('too much output'); return null; }};"),
        )
        .await
        .expect("accepted");
    let terminal = wait_for_terminal(&runs, &principal, &workspace, accepted.run_id()).await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Failed(failure)) if failure.code() == RunFailureCode::ResourceLimitExceeded)
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn oversized_http_output_ends_one_invocation_without_a_partial_result_or_retry() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/forecast"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_raw(vec![0, 255, 128, 13, 10, 0], "application/octet-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let fixture = TestModules::new(Arc::new(ImmediateEngine::default())).await;
    let runs = fixture
        .live_runs(None)
        .with_value_limits(ValueLimits::new(5, 1024, 4096, 4096).expect("limits"));
    let tool = fixture
        .user_tool_at(
            "user:oversized-result",
            "Bound upstream output",
            &server.uri(),
        )
        .await;
    let accepted = runs.start(&tool.principal, &tool.workspace_id, submission(r#"
        try { await coral.providers.weather.getForecast({city:"London"}); return "unexpected partial result"; }
        catch (error) { return error.message; }
    "#)).await.expect("accepted");
    let terminal = wait_for_terminal(
        &runs,
        &tool.principal,
        &tool.workspace_id,
        accepted.run_id(),
    )
    .await;
    assert!(
        matches!(terminal.terminal_outcome(), Some(RunTerminalOutcome::Succeeded { final_value: FinalValue::Present(value) }) if value.as_json() == &json!("Upstream result exceeds its limit"))
    );
    assert_eq!(
        retained_events(
            &runs,
            &tool.principal,
            &tool.workspace_id,
            terminal.run_id()
        )
        .await
        .iter()
        .filter_map(|event| match event.kind() {
            RunEventKind::InvocationEnded { result_kind, .. } => Some(*result_kind),
            _ => None,
        })
        .collect::<Vec<_>>(),
        vec![InvocationResultKind::CoralFailure]
    );
    server.verify().await;
}
