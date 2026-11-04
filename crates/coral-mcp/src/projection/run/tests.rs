use super::*;

#[test]
fn javascript_preserves_required_action_nanoseconds_through_json() {
    let mut action = RequiredAction {
        request: RequiredActionRequest::InvocationApproval(
            coral_run::approval::ApprovalId::parse("12345678-1234-4123-8123-123456789abc")
                .expect("approval identity"),
        ),
        required_actor: RequiredActor::InvokingUser,
        deadline_at_unix_nanos: 1_767_225_600_123_456_789,
        action_path: "/workspaces/example/approvals?request=workspaces/example/approvals/example".into(),
        call_path: Some("coral.providers.weather.getForecast".into()),
        provider_label: Some("Weather".into()),
    };
    let cases = [
        (1_767_225_600_123_456_789, "2026-01-01T00:00:00.123456789Z"),
        (1_767_225_600_123_000_000, "2026-01-01T00:00:00.123000000Z"),
        (0, "1970-01-01T00:00:00.000000000Z"),
        (-1, "1969-12-31T23:59:59.999999999Z"),
        (i64::MIN, "1677-09-21T00:12:43.145224192Z"),
        (i64::MAX, "2262-04-11T23:47:16.854775807Z"),
    ]
    .map(|(nanos, expected)| {
        action.deadline_at_unix_nanos = nanos;
        json!({"action": required_action(&action, &crate::HumanInterfaceUrl::parse("https://reef.example.test").unwrap()), "expected": expected})
    });
    let output = std::process::Command::new("node")
        .args([
            "--input-type=module",
            "-e",
            r#"
                import assert from 'node:assert/strict';
                // The former wire representation demonstrably loses nanoseconds.
                const legacy = JSON.parse('{"deadline_at_unix_nanos":1767225600123456789}');
                assert.notEqual(BigInt(legacy.deadline_at_unix_nanos), 1767225600123456789n);
                const cases = JSON.parse(process.argv[1]);
                for (const {action, expected} of cases) {
                    assert.equal(action.deadline_at, expected);
                    assert.equal('deadline_at_unix_nanos' in action, false);
                    assert.equal(JSON.parse(JSON.stringify(action)).deadline_at, expected);
                }
                const {action} = cases[0];
                // Date is a lossy display value, never a replacement for the exact text.
                const display = new Date(action.deadline_at).toISOString();
                assert.equal(display, '2026-01-01T00:00:00.123Z');
                assert.equal(action.deadline_at, '2026-01-01T00:00:00.123456789Z');
            "#,
            &serde_json::to_string(&cases).expect("projected fixtures"),
        ])
        .output()
        .expect("Node is required for the JavaScript MCP precision regression");
    assert!(
        output.status.success(),
        "JavaScript JSON regression: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// Catches: an MCP failure code that differs from the stable code Coral
/// stores and exports for the same failure.
#[test]
fn every_failure_code_reaches_mcp_as_its_stable_code() {
    for code in [
        RunFailureCode::ProgramRejected,
        RunFailureCode::InvalidProgramValue,
        RunFailureCode::ResourceLimitExceeded,
        RunFailureCode::MemoryLimitExceeded,
        RunFailureCode::CpuLimitExceeded,
        RunFailureCode::ProgramExecutionFailed,
        RunFailureCode::RunAuthorityLost,
    ] {
        assert_eq!(
            serde_json::to_value(FailureCode::from(code)).unwrap(),
            json!(code.code())
        );
    }
}
