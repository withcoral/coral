use serde_json::{Value, json};

pub(super) async fn check(
    session: &super::RawSession,
    tools: &[Value],
    index: &coral_search::SearchIndex,
) {
    check_schema(tools);
    let started = session
        .tool(
            "start_task",
            json!({"intent":"Check optional search defaults"}),
        )
        .await;
    let task = started
        .pointer("/structuredContent/task_id")
        .and_then(Value::as_str)
        .expect("Task id");
    search_defaults_return_ten_visible_descriptions(session, task, index).await;
    session
        .tool("end_task", json!({"task_id":task, "outcome":"success"}))
        .await;
}

async fn search_defaults_return_ten_visible_descriptions(
    session: &super::RawSession,
    task: &str,
    index: &coral_search::SearchIndex,
) {
    for index in 0..12 {
        let saved = session
            .tool(
                "save_function",
                json!({
                    "task_id":task, "intent":"Prepare default limit evidence",
                    "path":format!("optionalDefaults.item{index}"),
                    "description":"optionalDefaults coverage",
                    "source":if index == 0 { "export default function item(): number { return 'advisory'; }" } else { "export default function item() { return 1; }" }
                }),
            )
            .await;
        assert_ne!(saved.get("isError"), Some(&json!(true)));
    }
    index.reconcile().await;
    let exact = session
        .tool(
            "describe",
            json!({
                "task_id":task, "intent":"Read saved source and advisory diagnostics",
                "path":"coral.functions.optionalDefaults.item0"
            }),
        )
        .await;
    assert!(
        !exact
            .pointer("/structuredContent/diagnostics")
            .and_then(Value::as_array)
            .expect("advisory diagnostics")
            .is_empty()
    );
    assert!(
        exact
            .pointer("/structuredContent/source")
            .and_then(Value::as_str)
            .expect("saved source")
            .contains("advisory")
    );
    let mut default_page = None;
    for (limit, count) in [
        (None, 10),
        (Some(Value::Null), 10),
        (Some(json!(3)), 3),
        (Some(json!(12)), 12),
    ] {
        let mut arguments = json!({
            "task_id":task, "intent":"Observe the search result limit", "query":"optionalDefaults"
        });
        if let Some(limit) = limit {
            arguments
                .as_object_mut()
                .expect("arguments")
                .insert("limit".into(), limit);
        }
        let result = session.tool("search", arguments).await;
        assert_ne!(result.get("isError"), Some(&json!(true)));
        let page = result.get("structuredContent").expect("search page");
        for hit in page["hits"].as_array().expect("hits") {
            assert!(hit.get("source").is_none());
            assert!(hit.get("diagnostics").is_none());
            assert!(
                hit["signature_line"]
                    .as_str()
                    .expect("signature line")
                    .contains("number")
            );
        }
        assert_eq!(
            page.get("hits")
                .and_then(Value::as_array)
                .expect("hits")
                .len(),
            count
        );
        assert_eq!(
            page.get("next_cursor").is_some_and(Value::is_string),
            count < 12
        );
        if count == 10 {
            if let Some(expected) = &default_page {
                assert_eq!(page, expected, "omission and null return the same page");
            } else {
                default_page = Some(page.clone());
            }
        }
    }
    for index in 0..12 {
        let deleted = session
            .tool(
                "delete_function",
                json!({
                    "task_id":task, "intent":"Remove default limit fixtures",
                    "path":format!("optionalDefaults.item{index}")
                }),
            )
            .await;
        assert_ne!(deleted.get("isError"), Some(&json!(true)));
    }
}

pub(super) async fn check_exec_defaults(
    client: &super::SdkClient,
    runs: &coral_run::RunModule,
    user: &coral_workspace::AuthenticatedUser,
    workspace: &coral_workspace::WorkspaceId,
) {
    let started = super::call(
        client,
        "start_task",
        json!({"intent":"Check optional execution defaults"}),
    )
    .await;
    let task = super::structured(&started)
        .get("task_id")
        .and_then(Value::as_str)
        .expect("Task id")
        .to_string();
    initial_wait_batches_until_terminal_unless_explicitly_zero(client, &task).await;
    let actor = coral_workspace::AuthenticatedPrincipal::user(user.clone());
    for (timeout, expected_nanos) in [
        (None, 900_000_000_000_i64),
        (Some(Value::Null), 900_000_000_000),
        (Some(json!(120_000)), 120_000_000_000),
    ] {
        let mut arguments = json!({
            "task_id":task, "intent":"Observe the accepted Run deadline",
            "program":"return await coral.providers.weather.getForecast({});", "wait_ms":1000
        });
        if let Some(timeout) = timeout {
            arguments
                .as_object_mut()
                .expect("arguments")
                .insert("run_timeout_ms".into(), timeout);
        }
        // Approval exposes the retained deadline without waiting for it to expire.
        let result = super::call(client, "exec", arguments).await;
        let update = super::structured(&result);
        assert_eq!(update.get("state"), Some(&json!("running")));
        let deadline = chrono::DateTime::parse_from_rfc3339(
            update
                .pointer("/required_actions/0/deadline_at")
                .and_then(Value::as_str)
                .expect("approval deadline"),
        )
        .expect("RFC 3339 deadline")
        .timestamp_nanos_opt()
        .expect("nanosecond deadline");
        let run_id = coral_run::RunId::parse(
            update
                .get("run_id")
                .and_then(Value::as_str)
                .expect("Run id"),
        )
        .expect("valid Run id");
        let detail = runs
            .inspect_run(
                &actor,
                workspace,
                &run_id,
                &coral_run::RunInspection {
                    event_limit: 1,
                    ..Default::default()
                },
            )
            .await
            .expect("accepted Run detail");
        assert_eq!(deadline - detail.summary.started_at, expected_nanos);
        let canceled = super::call(client, "cancel", json!({"run_id":run_id.to_string()})).await;
        assert_eq!(
            super::structured(&canceled).get("state"),
            Some(&json!("canceled"))
        );
        assert_eq!(
            super::structured(&canceled).get("required_actions"),
            Some(&json!([]))
        );
    }
    super::call(
        client,
        "end_task",
        json!({"task_id":task, "outcome":"success"}),
    )
    .await;
}

async fn initial_wait_batches_until_terminal_unless_explicitly_zero(
    client: &super::SdkClient,
    task: &str,
) {
    // Terminal completion is the signal: no production-duration sleeps.
    for wait in [None, Some(Value::Null), Some(json!(30_000)), Some(json!(0))] {
        let immediate = wait == Some(json!(0));
        let mut arguments = json!({
            "task_id":task, "intent":"Observe initial output batching",
            "program":"coral.emit(1); console.log('two'); return 3;"
        });
        if let Some(wait) = wait {
            arguments
                .as_object_mut()
                .expect("arguments")
                .insert("wait_ms".into(), wait);
        }
        let result = super::call(client, "exec", arguments).await;
        let update = super::structured(&result);
        if immediate {
            assert_eq!(update.get("state"), Some(&json!("running")));
            assert_eq!(update.get("events"), Some(&json!([])));
            assert!(update.get("final_value").is_none());
            let finished = super::call(
                client,
                "wait",
                json!({
                    "run_id":update.get("run_id"), "wait_ms":30_000
                }),
            )
            .await;
            assert_ne!(finished.is_error, Some(true));
        } else {
            assert_eq!(update.get("state"), Some(&json!("succeeded")));
            assert_eq!(
                update
                    .get("events")
                    .and_then(Value::as_array)
                    .expect("authored output")
                    .len(),
                2
            );
            assert_eq!(
                update.get("final_value"),
                Some(&json!({"kind":"value", "value":3}))
            );
        }
    }
}

fn property<'a>(tools: &'a [Value], name: &str, field: &str) -> &'a Value {
    tools
        .iter()
        .find(|tool| tool.get("name") == Some(&json!(name)))
        .and_then(|tool| tool.pointer(&format!("/inputSchema/properties/{field}")))
        .expect("advertised optional parameter")
}

fn check_schema(tools: &[Value]) {
    for (name, field, default) in [
        ("search", "limit", 10),
        ("exec", "run_timeout_ms", 900_000),
        ("exec", "wait_ms", 10_000),
        ("wait", "wait_ms", 0),
    ] {
        let property = property(tools, name, field);
        assert_eq!(
            property.get("default"),
            Some(&json!(default)),
            "{name}.{field}"
        );
        assert_eq!(property.get("type"), Some(&json!(["integer", "null"])));
        let required = tools
            .iter()
            .find(|tool| tool.get("name") == Some(&json!(name)))
            .and_then(|tool| tool.pointer("/inputSchema/required"))
            .and_then(Value::as_array)
            .expect("required parameters");
        assert!(!required.contains(&json!(field)));
    }
}
