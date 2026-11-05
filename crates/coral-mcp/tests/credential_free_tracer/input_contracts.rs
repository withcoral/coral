use serde_json::{Value, json};

use super::RawSession;

fn property<'a>(tools: &'a [Value], name: &str, field: &str) -> &'a Value {
    tools
        .iter()
        .find(|tool| tool["name"] == name)
        .and_then(|tool| tool.pointer(&format!("/inputSchema/properties/{field}")))
        .unwrap_or_else(|| panic!("missing advertised {name}.{field}"))
}

fn validator(schema: &Value) -> jsonschema::Validator {
    jsonschema::draft202012::options()
        .should_validate_formats(true)
        .should_ignore_unknown_formats(false)
        .build(schema)
        .expect("portable advertised property schema")
}

pub(super) async fn check(session: &RawSession, tools: &[Value]) {
    let started = session
        .tool(
            "exec",
            json!({
                "goal":"Check advertised input constraints", "intent":"Start the Task",
                "program":"return null;"
            }),
        )
        .await;
    let task = started
        .pointer("/structuredContent/task_id")
        .and_then(Value::as_str)
        .expect("Task id");
    check_task_choice(session, task).await;
    check_identities(session, tools).await;
    check_text(session, tools).await;
    check_sources(session, task, tools).await;
    check_cursors(session, task, tools).await;
    session
        .tool("end_task", json!({"task_id":task, "outcome":"success"}))
        .await;
}

/// An `exec` names its Task or states the goal of a new one. Coral never
/// guesses which Task a call belongs to, so neither and both are refused
/// with the same message, which names both options.
async fn check_task_choice(session: &RawSession, task: &str) {
    for (arguments, case) in [
        (
            json!({"intent":"Name no Task", "program":"return null;"}),
            "neither",
        ),
        (
            json!({
                "task_id":task, "goal":"Start another Task", "intent":"Name both",
                "program":"return null;"
            }),
            "both",
        ),
    ] {
        let error = session
            .result("tools/call", json!({"name":"exec", "arguments":arguments}))
            .await
            .expect_err(case);
        assert_eq!(error.get("code"), Some(&json!(-32602)), "{case}");
        assert_eq!(
            error.get("message"),
            Some(&json!(
                "exec takes exactly one of `task_id` or `goal`: pass `task_id` to continue a Task, or `goal` to start a new one"
            )),
            "{case}"
        );
    }
}

async fn check_text(session: &RawSession, tools: &[Value]) {
    for tool in tools {
        let name = tool.get("name").and_then(Value::as_str).expect("tool name");
        for field in ["intent", "goal", "feedback"] {
            let Some(schema) = tool.pointer(&format!("/inputSchema/properties/{field}")) else {
                continue;
            };
            let validator = validator(schema);
            assert!(
                !validator.is_valid(&json!("")),
                "{name}.{field} is nonempty"
            );
            // Whitespace and NUL rules are domain-validated, not duplicated in the schema.
            for valid in [
                " \t\r\n",
                "\u{85}\u{a0}\u{2003}",
                "a\0b",
                "\0\n",
                "  café 🪸\n",
                "\u{feff}",
            ] {
                assert!(
                    validator.is_valid(&json!(valid)),
                    "{name}.{field}: {valid:?}"
                );
            }
        }
    }
    for value in ["  café 🪸\n", "\u{feff}"] {
        assert_eq!(
            coral_task::TaskIntent::parse(value)
                .expect("valid Unicode intent")
                .as_str(),
            value
        );
    }
    let intent_schema = validator(property(tools, "exec", "goal"));
    for (value, valid) in [("🪸".repeat(80), true), ("🪸".repeat(81), false)] {
        assert_eq!(intent_schema.is_valid(&json!(value)), valid);
        assert_eq!(coral_task::TaskIntent::parse(&value).is_ok(), valid);
    }
    for invalid in [" \t\n", "\u{85}", "bad\0text"] {
        let error = session
            .result(
                "tools/call",
                json!({"name":"exec", "arguments":{
                    "goal":invalid, "intent":"Check the goal", "program":"return null;"
                }}),
            )
            .await
            .expect_err("intent remains validated at the handler");
        assert_eq!(error.get("code"), Some(&json!(-32602)));
        // exec also takes the call's own `intent`, which fails with the same
        // text, so the refusal names `goal`.
        let message = error.get("message").and_then(Value::as_str);
        assert!(
            message.is_some_and(|it| it.starts_with("`goal`: ")),
            "{error:?}"
        );
    }
    check_feedback(session, tools).await;
}

async fn check_feedback(session: &RawSession, tools: &[Value]) {
    for feedback in [
        None,
        Some(Value::Null),
        Some(json!("  café 🪸\n")),
        Some(json!("🪸".repeat(4096))),
    ] {
        let id = session.start_task("Check completion feedback").await;
        for invalid in [" \n".to_string(), "a\0b".to_string(), "🪸".repeat(4097)] {
            let rejected = session
                .tool(
                    "end_task",
                    json!({"task_id":id, "outcome":"success", "feedback":invalid}),
                )
                .await;
            assert_eq!(
                rejected.pointer("/structuredContent/code"),
                Some(&json!("invalid_input"))
            );
        }
        let mut arguments = json!({"task_id":id, "outcome":"success"});
        if let Some(feedback) = feedback {
            assert!(validator(property(tools, "end_task", "feedback")).is_valid(&feedback));
            arguments
                .as_object_mut()
                .expect("arguments")
                .insert("feedback".into(), feedback);
        }
        let ended = session.tool("end_task", arguments).await;
        assert_eq!(
            ended.pointer("/structuredContent/outcome"),
            Some(&json!("success"))
        );
    }
}

/// A Program's byte bound and NUL check are domain-owned, as its schema
/// leaves them. coral-functions owns a saved source's own rules.
async fn check_sources(session: &RawSession, task: &str, tools: &[Value]) {
    let program = format!("//{}", "é".repeat(32767));
    assert_eq!(program.len(), 65536);
    let schema = validator(property(tools, "exec", "program"));
    assert!(schema.is_valid(&json!("")));
    for (value, valid) in [
        (program.clone(), true),
        (format!("{program}é"), false),
        ("a\0b".to_string(), false),
    ] {
        assert!(
            schema.is_valid(&json!(value)),
            "byte and NUL constraints are domain-owned"
        );
        let result = session
            .tool(
                "exec",
                json!({"task_id":task, "intent":"Check Unicode source bytes", "program":value}),
            )
            .await;
        assert_eq!(
            result.get("isError") == Some(&json!(true)),
            !valid,
            "exec: {result}"
        );
        if !valid {
            assert_eq!(
                result.pointer("/structuredContent/code"),
                Some(&json!("invalid_input"))
            );
        }
    }
}

async fn check_cursors(session: &RawSession, task: &str, tools: &[Value]) {
    for name in ["wait", "cancel"] {
        let schema = property(tools, name, "cursor");
        assert!(
            schema.get("pattern").is_none(),
            "private cursor encoding is not an authoring contract"
        );
        assert!(validator(schema).is_valid(&Value::Null));
        assert!(!validator(schema).is_valid(&json!("")));
    }
    let accepted = session
        .tool(
            "exec",
            json!({"task_id":task, "intent":"Check Run cursors", "program":""}),
        )
        .await;
    let update = accepted.get("structuredContent").expect("Run Update");
    for name in ["wait", "cancel"] {
        let replay = session
            .tool(
                name,
                json!({"run_id":update["run_id"], "cursor":update["next_cursor"]}),
            )
            .await;
        assert_ne!(replay.get("isError"), Some(&json!(true)));
        let error = session
            .result(
                "tools/call",
                json!({"name":name, "arguments":{"run_id":update["run_id"], "cursor":"!"}}),
            )
            .await
            .expect_err("malformed Run cursor is a protocol rejection");
        assert_eq!(error.get("code"), Some(&json!(-32602)));
    }
}

async fn check_identities(session: &RawSession, tools: &[Value]) {
    let canonical = "abcdef12-1234-4123-8123-123456789abc";
    for tool in tools {
        let name = tool.get("name").and_then(Value::as_str).expect("tool name");
        for field in ["task_id", "run_id"] {
            let Some(schema) = tool.pointer(&format!("/inputSchema/properties/{field}")) else {
                continue;
            };
            assert_eq!(
                schema["pattern"],
                coral_identifier::SCHEMA_PATTERN,
                "{name}.{field}"
            );
            let validator = validator(schema);
            // Four or five words, the shapes Coral accepts, or the UUID of an
            // identifier issued before Coral used words.
            for valid in [
                "height-word-database-library",
                "small-title-work-image-power",
                canonical,
            ] {
                assert!(validator.is_valid(&json!(valid)), "{name}.{field}: {valid}");
            }
            // Only `exec` may leave its Task out, when it states a goal.
            let optional = (name, field) == ("exec", "task_id");
            assert_eq!(validator.is_valid(&Value::Null), optional, "{name}.{field}");
            for invalid in [
                "height-word-database".to_string(),
                "Height-word-database-library".to_string(),
                "height-word-database-library-power-table".to_string(),
                "abcdefghijklmnopqrstu-word-database-library".to_string(),
                "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxx1".to_string(),
                canonical.to_uppercase(),
                canonical.replace('-', ""),
            ] {
                assert!(
                    !validator.is_valid(&json!(invalid)),
                    "{name}.{field}: {invalid}"
                );
            }
        }
    }
    for invalid in ["not-a-uuid", "ABCDEF12-1234-4123-8123-123456789ABC"] {
        coral_task::TaskId::parse(invalid).expect_err("invalid Task id");
        coral_run::RunId::parse(invalid).expect_err("invalid Run id");
        for (name, arguments) in [
            (
                "end_task",
                json!({"task_id": invalid, "outcome": "success"}),
            ),
            ("wait", json!({"run_id": invalid})),
            ("cancel", json!({"run_id": invalid})),
        ] {
            let error = session
                .result("tools/call", json!({"name":name, "arguments":arguments}))
                .await
                .expect_err("identity parsing remains a protocol rejection");
            assert_eq!(error.get("code"), Some(&json!(-32602)));
        }
    }
}
