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
        action_path: "/workspaces/example/approvals/example".into(),
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

fn projected<T>(result: Result<T, ToolFailure>) -> T {
    result.unwrap_or_else(|failure| panic!("projection failed: {}", failure.code()))
}

/// Catches: a page measured as another size than its encoded result has, so
/// that it passes the page target, or leaves out an Event that fitted.
#[test]
fn a_page_is_measured_as_its_encoded_result() {
    let emitted = [
        json!({"quote":"say \"hi\"", "path":"C:\\temp", "control":"line\nbreak\u{1}", "wide":"é漢😀"}),
        json!({"type":"text", "text":"rich \"text\"\\"}),
        json!([1, 2.5, null, true, {"deep":["\\\""]}]),
        json!({"_tag":"BinaryValue", "mimeType":"image/png", "data":"AP+ADQo="}),
        json!(""),
    ];
    let final_values = [
        None,
        Some(json!({"answer":"a \"plain\" value"})),
        Some(json!({"type":"text", "text":"a \"rich\" value"})),
    ];
    let events = || {
        let mut events = PageEvents::default();
        for (value, sequence) in emitted.iter().zip(2_u64..) {
            let index = events.rich.len() + 1;
            let (value, block) = projected(output_value(value, "urn:coral:output:run:2", index));
            let event = Event::Emitted {
                sequence: sequence.to_string(),
                occurred_at: timestamp(UnixNanos::new(0)),
                value,
            };
            projected(events.push(event, block));
            let log = Event::Logged {
                sequence: sequence.to_string(),
                occurred_at: timestamp(UnixNanos::new(0)),
                level: LogLevel::Warn,
                message: "a \"quoted\" log\\\n",
            };
            projected(events.push(log, None));
        }
        events
    };
    for final_value in &final_values {
        for count in 0..=events().len() {
            let events = events();
            let (value, block) = final_value
                .as_ref()
                .map(|value| {
                    let index = events.end(count).rich + 1;
                    projected(output_value(value, "urn:coral:output:run:final", index))
                })
                .unzip();
            let result = RunUpdateResult {
                task: NoTask {},
                run_id: "height-word-database-library".into(),
                state: State::Succeeded,
                required_actions: Vec::new(),
                events: Vec::new(),
                next_cursor: "cursor".into(),
                has_more: value.is_none(),
                final_value: value,
                failure: None,
                trace_id: None,
            };
            let page = projected(Page::new(result, block.flatten(), &events, count));
            let measured = page.bytes;
            let encoded = serde_json::to_vec(&projected(page.into_result(events)))
                .expect("encoded result")
                .len();
            assert_eq!(measured, encoded, "{count} Events, then {final_value:?}");
        }
    }
}

/// Catches: a value that the SDK reads as a content block and Coral keeps as
/// a plain value, or the reverse, because Coral judged its type on its own.
#[test]
fn a_value_is_rich_content_exactly_when_the_sdk_reads_it_as_a_block() {
    for value in [
        json!({"type":"text", "text":"hi"}),
        json!({"type":"image", "data":"AA==", "mimeType":"image/png"}),
        json!({"type":"audio", "data":"AA==", "mimeType":"audio/wav"}),
        json!({"type":"resource", "resource":{"uri":"file:///a", "text":"a"}}),
        json!({"type":"resource_link", "uri":"file:///a", "name":"a"}),
        // The SDK also reads a block from an array of its type and fields.
        json!(["text", "hi", null, null]),
        json!(["text", "hi"]),
        json!({"type":"text"}),
        json!({"type":"User", "login":"hi"}),
        json!({"type":0, "text":"hi"}),
        json!({"text":"hi"}),
        json!([0, "hi"]),
        json!([]),
        json!("text"),
        json!(7),
        json!(null),
    ] {
        let (_, block) = projected(output_value(&value, "urn:coral:output:run:2", 1));
        assert_eq!(
            block.is_some(),
            serde_json::from_value::<ContentBlock>(value.clone()).is_ok(),
            "{value}"
        );
    }
    // The SDK lists its block types when it rejects one. Coral copies a
    // value for the SDK to read only when the value names one of them.
    let rejection = serde_json::from_value::<ContentBlock>(json!({"type":"none"}))
        .expect_err("no such block type")
        .to_string();
    let block_types = rejection.split('`').skip(3).step_by(2).collect::<Vec<_>>();
    assert!(block_types.contains(&"text"), "{rejection}");
    for block_type in block_types {
        assert!(
            names_content_block(&json!({"type":block_type})),
            "{block_type}"
        );
    }
}
