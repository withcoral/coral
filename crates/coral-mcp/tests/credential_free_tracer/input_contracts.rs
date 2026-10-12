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

fn guidance(schema: &Value) -> &str {
    schema
        .get("description")
        .and_then(Value::as_str)
        .expect("advertised guidance")
}

pub(super) async fn check(session: &RawSession, tools: &[Value]) {
    let started = session
        .tool(
            "start_task",
            json!({"intent":"Check advertised input constraints"}),
        )
        .await;
    let task = started
        .pointer("/structuredContent/task_id")
        .and_then(Value::as_str)
        .expect("Task id");
    check_identities(session, task, tools).await;
    check_text(session, task, tools).await;
    check_authoring_guidance(tools);
    check_paths(session, task, tools).await;
    check_sources(session, task, tools).await;
    check_cursors(session, task, tools).await;
    session
        .tool("end_task", json!({"task_id":task, "outcome":"success"}))
        .await;
}

async fn check_text(session: &RawSession, task: &str, tools: &[Value]) {
    for tool in tools {
        let name = tool.get("name").and_then(Value::as_str).expect("tool name");
        for field in ["intent", "query", "feedback"] {
            let Some(schema) = tool.pointer(&format!("/inputSchema/properties/{field}")) else {
                continue;
            };
            let validator = validator(schema);
            assert!(
                !validator.is_valid(&json!("")),
                "{name}.{field} is nonempty"
            );
            // Whitespace and NUL rules are described, not duplicated in the schema.
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
            assert!(guidance(schema).contains("not trimmed"));
        }
    }
    for value in ["  café 🪸\n", "\u{feff}"] {
        assert_eq!(
            coral_task::TaskIntent::parse(value)
                .expect("valid Unicode intent")
                .as_str(),
            value
        );
        assert_eq!(
            coral_search::SearchQuery::parse(value)
                .expect("valid Unicode query")
                .as_str(),
            value
        );
    }
    let intent_schema = validator(property(tools, "start_task", "intent"));
    for (value, valid) in [("🪸".repeat(80), true), ("🪸".repeat(81), false)] {
        assert_eq!(intent_schema.is_valid(&json!(value)), valid);
        assert_eq!(coral_task::TaskIntent::parse(&value).is_ok(), valid);
    }
    for invalid in [" \t\n", "\u{85}", "bad\0text"] {
        let error = session
            .result(
                "tools/call",
                json!({"name":"start_task", "arguments":{"intent":invalid}}),
            )
            .await
            .expect_err("intent remains validated at the handler");
        assert_eq!(error.get("code"), Some(&json!(-32602)));
        let rejected = session
            .tool(
                "search",
                json!({
                    "task_id":task, "intent":"Check invalid query", "query":invalid
                }),
            )
            .await;
        assert_eq!(
            rejected.pointer("/structuredContent/code"),
            Some(&json!("invalid_input"))
        );
    }
    assert!(guidance(property(tools, "search", "query")).contains("4096 UTF-8 bytes"));
    let query_schema = validator(property(tools, "search", "query"));
    assert!(query_schema.is_valid(&Value::Null));
    let listed = session
        .tool(
            "search",
            json!({"task_id":task, "intent":"List with null query", "query":null}),
        )
        .await;
    assert_ne!(listed.get("isError"), Some(&json!(true)));
    for (value, valid) in [("🪸".repeat(1024), true), ("🪸".repeat(1025), false)] {
        // JSON Schema character counts cannot enforce the domain's UTF-8 byte limit.
        assert!(query_schema.is_valid(&json!(value)));
        assert_eq!(coral_search::SearchQuery::parse(&value).is_ok(), valid);
        let result = session
            .tool(
                "search",
                json!({
                    "task_id":task, "intent":"Check Unicode query bytes", "query":value
                }),
            )
            .await;
        assert_eq!(result.get("isError") == Some(&json!(true)), !valid);
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
        let started = session
            .tool("start_task", json!({"intent":"Check completion feedback"}))
            .await;
        let id = started
            .pointer("/structuredContent/task_id")
            .expect("Task id");
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

fn check_authoring_guidance(tools: &[Value]) {
    for name in ["save_function", "delete_function", "describe"] {
        let schema = property(tools, name, "path");
        let guidance = guidance(schema);
        for term in [
            "JavaScript identifier",
            "__proto__",
            "prototype",
            "constructor",
            "then",
            "16",
            "256 UTF-8 bytes",
        ] {
            assert!(guidance.contains(term), "{name}.path omits {term}");
        }
        let validator = validator(schema);
        let valid = if name == "describe" {
            "coral.functions.éclair.$value"
        } else {
            "éclair.$value"
        };
        assert!(validator.is_valid(&json!(valid)));
        assert!(!validator.is_valid(&json!("")));
        for domain_invalid in ["a..b", "a\0b"] {
            assert!(validator.is_valid(&json!(domain_invalid)));
        }
    }
    let describe_path = property(tools, "describe", "path");
    assert!(guidance(describe_path).contains("1024 UTF-8 bytes"));
    assert!(
        validator(describe_path).is_valid(&json!(format!(
            "coral.providers.x.{}",
            "a".repeat(1025 - "coral.providers.x.".len())
        ))),
        "the Call Path byte bound is domain-validated"
    );
    // Only describe asks what the search result did not show.
    for tool in tools {
        let name = tool["name"].as_str().expect("tool name");
        if let Some(intent) = tool.pointer("/inputSchema/properties/intent") {
            assert_eq!(
                guidance(intent).contains("the search result did not show"),
                name == "describe",
                "{name}.intent"
            );
        }
    }
    let description = guidance(property(tools, "save_function", "description"));
    for term in [
        "4096 UTF-8 bytes",
        "surrounding whitespace",
        "control characters",
    ] {
        assert!(description.contains(term), "description omits {term}");
    }
    let description_schema = validator(property(tools, "save_function", "description"));
    for invalid in ["a\0b", "line\nbreak", "text\n", "a\u{85}b"] {
        assert!(
            description_schema.is_valid(&json!(invalid)),
            "control characters are domain-validated"
        );
    }
    for value in ["🪸".repeat(1024), "🪸".repeat(1025)] {
        assert!(
            description_schema.is_valid(&json!(value)),
            "byte limits remain domain-owned"
        );
    }
    for (name, field) in [("save_function", "source"), ("exec", "program")] {
        let schema = property(tools, name, field);
        let guidance = guidance(schema);
        assert!(guidance.contains("65536 UTF-8 bytes") && guidance.contains("null"));
        assert!(
            validator(schema).is_valid(&json!("a\0b")),
            "NUL is domain-validated"
        );
    }
    assert!(validator(property(tools, "exec", "program")).is_valid(&json!("")));
    assert!(!validator(property(tools, "save_function", "source")).is_valid(&json!("")));
}

async fn check_paths(session: &RawSession, task: &str, tools: &[Value]) {
    use coral_functions::SavedFunctionPath;

    for path in [
        "a/b".to_string(),
        "a..b".to_string(),
        "a.1b".to_string(),
        "a.__proto__".to_string(),
        "prototype".to_string(),
        "a.constructor".to_string(),
        "a.then".to_string(),
        "a.".repeat(16) + "a",
        "é".repeat(129),
    ] {
        SavedFunctionPath::parse(&path).expect_err("invalid or reserved path");
        for name in ["save_function", "delete_function"] {
            let mut arguments =
                json!({"task_id":task, "intent":"Check rejected function path", "path":path});
            if name == "save_function" {
                let arguments = arguments.as_object_mut().expect("arguments");
                arguments.insert("description".into(), json!("Valid description"));
                arguments.insert("source".into(), json!("export default () => 1;"));
            }
            let rejected = session.tool(name, arguments).await;
            assert_eq!(
                rejected.pointer("/structuredContent/code"),
                Some(&json!("invalid_input"))
            );
        }
    }
    for path in [
        "a/b",
        "coral.providers.onlyNamespace",
        "coral.functions.a..b",
        "coral.functions.1b",
        "coral.functions.a\0b",
    ] {
        coral_search::CallPath::parse(path).expect_err("invalid complete path");
        let rejected = session
            .tool(
                "describe",
                json!({"task_id":task, "intent":"Check complete path", "path":path}),
            )
            .await;
        assert_eq!(
            rejected.pointer("/structuredContent/code"),
            Some(&json!("invalid_input"))
        );
    }
    // The schema describes, rather than reimplements, Unicode identifier and reserved-word semantics.
    assert!(validator(property(tools, "save_function", "path")).is_valid(&json!("a.then")));
    assert!(validator(property(tools, "save_function", "path")).is_valid(&json!("a/b")));
    for path in [
        "é".repeat(128),
        "a.".repeat(15) + "a",
        "éclair.$value".to_string(),
    ] {
        SavedFunctionPath::parse(&path).expect("valid Unicode path at bounds");
    }
}

async fn check_sources(session: &RawSession, task: &str, tools: &[Value]) {
    use coral_functions::{SavedFunctionDescription, SavedFunctionSource};
    use coral_run::ProgramSource;

    for invalid in [
        " text ".to_string(),
        "line\nbreak".to_string(),
        "a\0b".to_string(),
        "a\u{85}b".to_string(),
        "🪸".repeat(1025),
    ] {
        SavedFunctionDescription::parse(&invalid).expect_err("invalid description");
        let rejected = session
            .tool(
                "save_function",
                json!({
                    "task_id":task, "intent":"Check description rejection", "path":"inputContract",
                    "description":invalid, "source":"export default () => 1;"
                }),
            )
            .await;
        assert_eq!(
            rejected.pointer("/structuredContent/code"),
            Some(&json!("invalid_input"))
        );
    }
    let source = format!("export default () => 1;/*{}*/", "🪸".repeat(16377));
    assert_eq!(source.len(), 65535);
    let source = format!("{source} ");
    let program = format!("//{}", "é".repeat(32767));
    assert_eq!(source.len(), 65536);
    assert_eq!(program.len(), 65536);
    for (name, field, valid_source) in [
        ("save_function", "source", source),
        ("exec", "program", program),
    ] {
        let schema = validator(property(tools, name, field));
        for (value, valid) in [
            (valid_source.clone(), true),
            (format!("{valid_source}é"), false),
            ("a\0b".to_string(), false),
        ] {
            assert!(
                schema.is_valid(&json!(value)),
                "byte and NUL constraints are domain-owned"
            );
            let mut arguments =
                json!({"task_id":task, "intent":"Check Unicode source bytes", field:value});
            if name == "save_function" {
                assert_eq!(SavedFunctionSource::parse(&value).is_ok(), valid);
                let arguments = arguments.as_object_mut().expect("arguments");
                arguments.insert("path".into(), json!("éclair.$value"));
                arguments.insert("description".into(), json!("🪸".repeat(1024)));
            } else {
                assert_eq!(ProgramSource::parse(&value).is_ok(), valid);
            }
            let result = session.tool(name, arguments).await;
            assert_eq!(
                result.get("isError") == Some(&json!(true)),
                !valid,
                "{name}: {result}"
            );
            if !valid {
                assert_eq!(
                    result.pointer("/structuredContent/code"),
                    Some(&json!("invalid_input"))
                );
            }
        }
    }
    let described = session.tool("describe", json!({"task_id":task, "intent":"Describe valid Unicode path", "path":"coral.functions.éclair.$value"})).await;
    assert_ne!(described.get("isError"), Some(&json!(true)));
    let deleted = session
        .tool(
            "delete_function",
            json!({"task_id":task, "intent":"Delete valid Unicode path", "path":"éclair.$value"}),
        )
        .await;
    assert_ne!(deleted.get("isError"), Some(&json!(true)));
}

async fn check_cursors(session: &RawSession, task: &str, tools: &[Value]) {
    for name in ["search", "wait", "cancel"] {
        let schema = property(tools, name, "cursor");
        let guidance = guidance(schema);
        assert!(
            guidance.contains("opaque")
                && guidance.contains("Reuse")
                && guidance.contains("unchanged")
        );
        assert!(
            schema.get("pattern").is_none(),
            "private cursor encoding is not an authoring contract"
        );
        assert!(validator(schema).is_valid(&Value::Null));
        assert!(!validator(schema).is_valid(&json!("")));
    }
    // Malformed opaque tokens remain domain rejections, not a new schema prevalidation layer.
    let rejected = session
        .tool(
            "search",
            json!({"task_id":task, "intent":"Check opaque cursor", "cursor":"!"}),
        )
        .await;
    assert_eq!(
        rejected.pointer("/structuredContent/code"),
        Some(&json!("invalid_cursor"))
    );
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

async fn check_identities(session: &RawSession, task: &str, tools: &[Value]) {
    let canonical = "abcdef12-1234-4123-8123-123456789abc";
    for tool in tools {
        let name = tool.get("name").and_then(Value::as_str).expect("tool name");
        for field in ["task_id", "run_id"] {
            let Some(schema) = tool.pointer(&format!("/inputSchema/properties/{field}")) else {
                continue;
            };
            assert_eq!(schema["format"], "uuid", "{name}.{field}");
            let description = guidance(schema);
            assert!(description.contains("lowercase") && description.contains("hyphenated"));
            let validator = validator(schema);
            assert!(validator.is_valid(&json!(canonical)));
            // Standard UUID format permits uppercase; canonical lowercase is domain-owned.
            assert!(validator.is_valid(&json!(canonical.to_uppercase())));
            assert!(!validator.is_valid(&Value::Null));
            for invalid in [
                "xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx".to_string(),
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
                "search",
                json!({"task_id": invalid, "intent": "Check Task identity"}),
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
    // `provider` is a namespace, the second segment of a Call Path; an id is not one.
    let provider_schema = validator(property(tools, "search", "provider"));
    assert!(provider_schema.is_valid(&json!("weather")));
    for invalid in [
        canonical,
        "not-a-uuid",
        "ABCDEF12-1234-4123-8123-123456789ABC",
    ] {
        assert!(!provider_schema.is_valid(&json!(invalid)), "{invalid}");
        let rejected = session
            .tool(
                "search",
                json!({
                    "task_id": task, "intent": "Check Provider namespace", "provider": invalid
                }),
            )
            .await;
        assert_eq!(
            rejected.pointer("/structuredContent/code"),
            Some(&json!("invalid_input")),
            "{invalid}"
        );
    }
    for provider in [None, Some(Value::Null), Some(json!("weather"))] {
        let mut arguments = json!({"task_id": task, "intent": "Check optional Provider namespace"});
        if let Some(provider) = provider {
            arguments
                .as_object_mut()
                .expect("arguments")
                .insert("provider".into(), provider);
        }
        let result = session.tool("search", arguments).await;
        assert_ne!(result.get("isError"), Some(&json!(true)));
    }
    // Also assert the optional type itself, not just format-validation behavior.
    assert_eq!(
        property(tools, "search", "provider")["type"],
        json!(["string", "null"])
    );
}
