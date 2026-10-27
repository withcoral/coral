//! One nested-result contract checker for both raw and SDK journeys.
//! Official MCP envelope validation remains separate: it cannot validate these payloads.
use std::collections::BTreeMap;

use serde_json::{Value, json};

#[derive(Default)]
pub(super) struct OutputContracts(BTreeMap<String, jsonschema::Validator>);

impl OutputContracts {
    pub(super) fn new(tools: &[Value]) -> Self {
        let contracts = tools
            .iter()
            .map(|tool| {
                let schema = tool
                    .get("outputSchema")
                    .expect("every tool advertises an output schema");
                assert_eq!(schema["type"], "object");
                assert_eq!(
                    schema["$schema"],
                    "https://json-schema.org/draft/2020-12/schema"
                );
                let validator = jsonschema::draft202012::options()
                    .should_validate_formats(true)
                    .should_ignore_unknown_formats(false)
                    .build(schema)
                    .expect("portable output schema with locally resolvable references");
                (tool["name"].as_str().expect("tool name").into(), validator)
            })
            .collect();
        let schema = |name| {
            tools
                .iter()
                .find(|tool| tool["name"] == name)
                .expect("Run tool")["outputSchema"]
                .clone()
        };
        assert_eq!(schema("exec"), schema("wait"));
        assert_eq!(schema("wait"), schema("cancel"));
        Self(contracts)
    }

    pub(super) fn check(&self, name: &str, result: &Value) {
        let validator = self.0.get(name).expect("advertised tool");
        let structured = result.get("structuredContent").expect("structured result");
        Self::check_content(name, result, structured);
        let violations = validator
            .iter_errors(structured)
            .map(|error| error.to_string())
            .collect::<Vec<_>>();
        assert!(
            violations.is_empty(),
            "{name} output contract violated: {violations:?}\n{structured}"
        );

        Self::reject_missing_fields(name, result, validator, structured);
        Self::reject_malformed_fields(name, validator, structured);
        if result["isError"] == true {
            for (tool, contract) in &self.0 {
                assert!(
                    contract.is_valid(structured),
                    "{tool} must accommodate safe tool failures"
                );
            }
        } else if matches!(name, "exec" | "wait" | "cancel") {
            assert_eq!(
                result["isError"], false,
                "every valid Run state is a tool success"
            );
            Self::check_run(validator, structured);
        }
    }

    /// A content-only client reads the same JSON from `content[0]` that a
    /// structured client reads from `structuredContent`. Rich blocks follow it,
    /// each referenced once, in order, and their payloads stay out of the text.
    fn check_content(name: &str, result: &Value, structured: &Value) {
        let content = result
            .get("content")
            .and_then(Value::as_array)
            .expect("content");
        let text = content
            .first()
            .filter(|block| block["type"] == "text")
            .and_then(|block| block["text"].as_str())
            .unwrap_or_else(|| panic!("{name} content[0] is JSON text: {result}"));
        assert_eq!(
            text,
            serde_json::to_string(structured).expect("compact JSON"),
            "{name} content[0] is the compact JSON of structuredContent"
        );
        let mut indices = Vec::new();
        let events = structured.get("events").and_then(Value::as_array);
        for value in events
            .into_iter()
            .flatten()
            .filter_map(|event| event.get("value"))
            .chain(structured.get("final_value"))
        {
            if let Some(index) = value.get("content_index") {
                indices.push(index.as_u64().expect("content index"));
            }
        }
        assert_eq!(
            indices,
            (1..u64::try_from(content.len()).expect("small content")).collect::<Vec<_>>(),
            "{name} references each rich block after the JSON text once, in order"
        );
        for block in content.iter().skip(1) {
            for payload in ["/data", "/resource/blob"] {
                if let Some(payload) = block.pointer(payload).and_then(Value::as_str) {
                    assert!(
                        !text.contains(payload),
                        "{name} rich payload appears only in its block"
                    );
                }
            }
        }
    }

    fn reject_missing_fields(
        name: &str,
        result: &Value,
        validator: &jsonschema::Validator,
        structured: &Value,
    ) {
        // Independent contract expectations, not required lists read back from the schema.
        let required: &[&str] = if result["isError"] == true {
            &["code", "message", "trace_id"]
        } else {
            match name {
                "start_task" => &["task_id", "providers"],
                "search" => &["hits", "has_more"],
                // A Saved Function's source and diagnostics are absent from a
                // description cut to `types`.
                "describe" => &["kind", "call_path", "description", "typescript_declaration"],
                "save_function" => &[
                    "call_path",
                    "description",
                    "declaration",
                    "diagnostics",
                    "diagnostics_truncated",
                ],
                "delete_function" => &["call_path"],
                "exec" | "wait" | "cancel" => &[
                    "run_id",
                    "state",
                    "events",
                    "required_actions",
                    "next_cursor",
                    "has_more",
                ],
                "end_task" => &["task_id", "outcome"],
                _ => panic!("unexpected public tool {name}"),
            }
        };
        for key in required {
            let mut malformed = structured.clone();
            malformed
                .as_object_mut()
                .expect("object result")
                .remove(*key);
            assert!(!validator.is_valid(&malformed), "{name} must require {key}");
        }
        for pointer in [
            "/hits/0/call_path",
            "/hits/0/signature_line",
            "/events/0/occurred_at",
            "/events/0/sequence",
            "/events/0/message",
            "/events/0/value/value",
            "/events/0/value/content_index",
            "/required_actions/0/request_id",
            "/required_actions/0/required_actor",
            "/required_actions/0/deadline_at",
            "/required_actions/0/run_status_url",
            "/diagnostics/0/message",
            "/diagnostics/0/line",
            "/failure/error/message",
            "/failure/code",
            "/failure/error",
        ] {
            if structured.pointer(pointer).is_none() {
                continue;
            }
            let (parent, key) = pointer.rsplit_once('/').expect("property pointer");
            let mut malformed = structured.clone();
            malformed
                .pointer_mut(parent)
                .and_then(Value::as_object_mut)
                .expect("parent object")
                .remove(key);
            assert!(
                !validator.is_valid(&malformed),
                "{name} must require {pointer}"
            );
        }
    }

    fn reject_malformed_fields(name: &str, validator: &jsonschema::Validator, structured: &Value) {
        for (pointer, invalid) in [
            ("/task_id", json!("not-a-uuid")),
            ("/state", json!("unknown_state")),
            ("/outcome", json!("running")),
            ("/kind", json!("provider_schema")),
            ("/hits/0/signature_line", json!({})),
            ("/events/0/kind", json!("invocation.started")),
            (
                "/events/0",
                json!({"kind":"unknown", "sequence":"1", "occurred_at":"2026-01-01T00:00:00.000000000Z"}),
            ),
            ("/events/0/sequence", json!(1)),
            (
                "/events/0/occurred_at",
                json!("2026-02-30T00:00:00.000000000Z"),
            ),
            ("/events/0/value", json!({"kind":"value"})),
            (
                "/events/0/value",
                json!({"kind":"content", "content_index":-1}),
            ),
            (
                "/events/0/value",
                json!({"kind":"content", "content_index":0}),
            ),
            (
                "/events/0/value",
                json!({"kind":"content", "content_index":9_007_199_254_740_992_u64}),
            ),
            ("/required_actions/0/kind", json!("unknown_action")),
            ("/required_actions/0/required_actor", json!("agent")),
            ("/required_actions/0/deadline_at", json!(0)),
            ("/final_value", Value::Null),
            ("/final_value", json!({"kind":"value"})),
            ("/failure", Value::Null),
            (
                "/final_value",
                json!({"kind":"content", "content_index":1.5}),
            ),
            ("/failure/code", json!("unknown_failure")),
            (
                "/failure/error/frames/0/line",
                json!(9_007_199_254_740_992_u64),
            ),
            ("/diagnostics/0/code", json!(9_007_199_254_740_992_u64)),
            ("/diagnostics/0/category", json!("unknown_category")),
        ] {
            let mut malformed = structured.clone();
            if let Some(target) = malformed.pointer_mut(pointer) {
                *target = invalid;
                assert!(
                    !validator.is_valid(&malformed),
                    "{name} accepted malformed {pointer}: {malformed}"
                );
            }
        }
    }

    fn check_run(validator: &jsonschema::Validator, update: &Value) {
        if matches!(
            update["state"].as_str(),
            Some("canceled" | "timed_out" | "interrupted")
        ) {
            let mut malformed = update.clone();
            malformed
                .as_object_mut()
                .expect("Run Update")
                .insert("final_value".into(), json!({"kind":"value", "value":null}));
            assert!(
                !validator.is_valid(&malformed),
                "terminated Runs cannot contain a Final Value"
            );
        }
        if update.pointer("/required_actions/0").is_some() {
            for (kind, actor, valid) in [
                ("account_connection", "invoking_user", true),
                ("invocation_approval", "workspace_owner", true),
                ("account_connection", "workspace_owner", false),
            ] {
                let mut observation = update.clone();
                *observation
                    .pointer_mut("/required_actions/0/kind")
                    .expect("kind") = json!(kind);
                *observation
                    .pointer_mut("/required_actions/0/required_actor")
                    .expect("actor") = json!(actor);
                assert_eq!(validator.is_valid(&observation), valid, "{kind} / {actor}");
            }
        }
        for field in ["final_value", "failure"] {
            if update.get(field).is_some() {
                let mut malformed = update.clone();
                *malformed.get_mut("has_more").expect("has_more") = json!(true);
                assert!(
                    !validator.is_valid(&malformed),
                    "terminal data must wait for the final page"
                );
                *malformed.get_mut("has_more").expect("has_more") = json!(false);
                *malformed.get_mut("state").expect("state") = json!("running");
                assert!(
                    !validator.is_valid(&malformed),
                    "running observations cannot contain {field}"
                );
            }
        }
        if update["state"] == "failed" && update["has_more"] == false {
            let mut malformed = update.clone();
            malformed
                .as_object_mut()
                .expect("Run Update")
                .remove("failure");
            assert!(
                !validator.is_valid(&malformed),
                "final failed page requires failure details"
            );
        }
    }
}
