use coral_engine::{CoralQuery, CoreError, StatusCode};
use serde_json::{Value, json};
use wiremock::matchers::{header, method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{
    TestRuntime, build_source, build_source_with_secrets, execution_to_rows, users_rows,
};

fn base_http_manifest(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "users",
            "description": "HTTP users",
            "request": {
                "method": "GET",
                "path": "/api/users"
            },
            "response": {
                "rows_path": ["data"]
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" },
                { "name": "email", "type": "Utf8" }
            ]
        }]
    })
}

#[tokio::test]
async fn select_all_from_http_source() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_users", &server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name, email FROM http_users.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, users_rows());
}

#[tokio::test]
async fn select_with_column_projection() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_projection", &server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT name, email FROM http_projection.users ORDER BY name",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"name": "Ada", "email": "ada@example.com"}),
            json!({"name": "Grace", "email": "grace@example.com"}),
            json!({"name": "Linus", "email": "linus@example.com"}),
        ]
    );
}

#[tokio::test]
async fn select_with_order_by() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_order", &server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT name FROM http_order.users ORDER BY name DESC",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"name": "Linus"}),
            json!({"name": "Grace"}),
            json!({"name": "Ada"})
        ]
    );
}

#[tokio::test]
async fn select_with_limit() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_limit", &server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT * FROM http_limit.users LIMIT 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["id"], 1);
    assert_eq!(rows[1]["id"], 2);
}

#[tokio::test]
async fn select_with_where_filter_pushdown() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("id", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({ "data": [json!({"id": 2, "name": "Grace", "email": "grace@example.com"})] }),
        ))
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_filter", &server.uri());
    let table = &mut manifest["tables"][0];
    table["filters"] = json!([{ "name": "id" }]);
    table["request"]["query"] = json!([
        { "name": "id", "from": "filter", "key": "id" }
    ]);
    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name FROM http_filter.users WHERE id = 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 2, "name": "Grace"})]);
}

#[tokio::test]
async fn select_count_aggregation() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_count", &server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT COUNT(*) AS n FROM http_count.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn pagination_page_mode() {
    let server = MockServer::start().await;
    let rows = users_rows();
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": &rows[..2] })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": &rows[2..] })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("page", "3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_page", &server.uri());
    manifest["tables"][0]["pagination"] = json!({
        "mode": "page",
        "page_param": "page",
        "page_start": 1
    });
    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name, email FROM http_page.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, users_rows());
}

#[tokio::test]
async fn pagination_offset_mode() {
    let server = MockServer::start().await;
    let rows = users_rows();
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": &rows[..2] })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("offset", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": &rows[2..] })))
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("offset", "4"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_offset", &server.uri());
    manifest["tables"][0]["pagination"] = json!({
        "mode": "offset",
        "offset_param": "offset",
        "offset_step": 2
    });
    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name, email FROM http_offset.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, users_rows());
}

#[tokio::test]
async fn pagination_link_header() {
    let server = MockServer::start().await;
    let rows = users_rows();
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param_is_missing("page"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header("Link", "</api/users?page=2>; rel=\"next\"")
                .set_body_json(json!({ "data": &rows[..2] })),
        )
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": &rows[2..] })))
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_link", &server.uri());
    manifest["tables"][0]["pagination"] = json!({
        "mode": "link_header"
    });
    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT id, name, email FROM http_link.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, users_rows());
}

#[tokio::test]
async fn auth_headers_sent_correctly() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(header("authorization", "Bearer secret-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_auth", &server.uri());
    manifest["inputs"] = json!({
        "API_TOKEN": { "kind": "secret" }
    });
    manifest["auth"] = json!({
        "headers": [{
            "name": "Authorization",
            "from": "template",
            "template": "Bearer {{input.API_TOKEN}}"
        }]
    });
    let source = build_source_with_secrets(manifest, [("API_TOKEN", "secret-token")]);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT COUNT(*) AS n FROM http_auth.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn api_returns_500() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(500).set_body_string("boom"))
        .expect(3)
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_500", &server.uri()));

    let error = CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM http_500.users")
        .await
        .expect_err("500 should fail");

    assert_eq!(error.status_code(), StatusCode::Unavailable);
    match &error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert!(sqe.retryable());
            assert_eq!(sqe.metadata().get("http_status").unwrap(), "500");
            assert_eq!(sqe.metadata().get("source").unwrap(), "http_500");
            assert!(sqe.detail().contains("boom"));
        }
        other => panic!("unexpected 500 error variant: {other:?}"),
    }
}

#[tokio::test]
async fn api_returns_401() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(401).set_body_string("unauthorized"))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_401", &server.uri()));

    let error = CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM http_401.users")
        .await
        .expect_err("401 should fail");

    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match &error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert!(!sqe.retryable());
            assert_eq!(sqe.metadata().get("http_status").unwrap(), "401");
            assert_eq!(sqe.metadata().get("source").unwrap(), "http_401");
            assert!(sqe.hint().unwrap().contains("coral source add http_401"));
            assert!(sqe.detail().contains("unauthorized"));
        }
        other => panic!("unexpected 401 error variant: {other:?}"),
    }
}

fn slack_messages_manifest(base_url: &str) -> Value {
    json!({
        "name": "slack_ts",
        "version": "2.0.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "messages",
            "description": "Slack messages",
            "request": {
                "method": "GET",
                "path": "/api/conversations.history",
                "query": [
                    { "name": "channel", "from": "filter", "key": "channel" }
                ]
            },
            "response": {
                "ok_path": ["ok"],
                "error_path": ["error"],
                "rows_path": ["messages"]
            },
            "columns": [
                {
                    "name": "channel",
                    "type": "Utf8",
                    "nullable": false,
                    "expr": { "kind": "from_filter", "key": "channel" }
                },
                {
                    "name": "user_id",
                    "type": "Utf8",
                    "nullable": true,
                    "expr": { "kind": "path", "path": ["user"] }
                },
                {
                    "name": "text",
                    "type": "Utf8",
                    "nullable": true,
                    "expr": { "kind": "path", "path": ["text"] }
                },
                {
                    "name": "ts",
                    "type": "Timestamp",
                    "nullable": false,
                    "expr": {
                        "kind": "format_timestamp",
                        "input": "seconds",
                        "expr": { "kind": "path", "path": ["ts"] }
                    }
                },
                {
                    "name": "permalink",
                    "type": "Utf8",
                    "nullable": false,
                    "expr": {
                        "kind": "template",
                        "template": "https://slack.com/archives/{{filter.channel}}/p{{expr.ts_id}}",
                        "values": {
                            "ts_id": {
                                "kind": "replace",
                                "expr": { "kind": "path", "path": ["ts"] },
                                "from": ".",
                                "to": ""
                            }
                        }
                    }
                }
            ],
            "filters": [
                { "name": "channel", "required": true }
            ]
        }]
    })
}

/// Regression test for DATA-366: Slack message timestamps must be returned as
/// human-readable ISO-8601 dates (not raw Slack ts strings), and each message
/// should include a Slack permalink.
#[tokio::test]
async fn slack_messages_have_formatted_ts_and_permalink() {
    let server = MockServer::start().await;

    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .and(query_param("channel", "C123456"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "messages": [
                { "user": "U001", "text": "Hello world", "ts": "1609459200.000100" },
                { "user": "U002", "text": "Hi there", "ts": "1609459300.000200" }
            ]
        })))
        .mount(&server)
        .await;

    let source = build_source(slack_messages_manifest(&server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            &TestRuntime,
            "SELECT ts, permalink, user_id, text FROM slack_ts.messages WHERE channel = 'C123456' ORDER BY ts",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0]["ts"], "2021-01-01T00:00:00.000100Z");
    assert_eq!(rows[1]["ts"], "2021-01-01T00:01:40.000200Z");
    assert_eq!(
        rows[0]["permalink"],
        "https://slack.com/archives/C123456/p1609459200000100"
    );
    assert_eq!(
        rows[1]["permalink"],
        "https://slack.com/archives/C123456/p1609459300000200"
    );
}

#[tokio::test]
async fn missing_required_filter_surfaces_structured_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(0)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_required", &server.uri());
    let table = &mut manifest["tables"][0];
    table["filters"] = json!([{ "name": "id", "required": true }]);
    table["request"]["query"] = json!([
        { "name": "id", "from": "filter", "key": "id" }
    ]);
    let source = build_source(manifest);

    let error =
        CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM http_required.users")
            .await
            .expect_err("query without the required filter should fail");

    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match &error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "MISSING_REQUIRED_FILTER");
            assert!(!sqe.retryable());
            assert_eq!(sqe.metadata().get("schema").unwrap(), "http_required");
            assert_eq!(sqe.metadata().get("table").unwrap(), "users");
            assert_eq!(sqe.metadata().get("column").unwrap(), "id");
            assert!(sqe.summary().contains("WHERE id"));
            assert!(sqe.hint().unwrap().contains("coral.columns"));
        }
        other => panic!("unexpected missing-filter error variant: {other:?}"),
    }
}

#[tokio::test]
async fn api_returns_malformed_json() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_string("not-json"))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_bad_json", &server.uri()));

    let error =
        CoralQuery::execute_sql(&[source], &TestRuntime, "SELECT * FROM http_bad_json.users")
            .await
            .expect_err("malformed json should fail");

    assert_eq!(error.status_code(), StatusCode::Internal);
    match error {
        CoreError::Internal(detail) => assert!(detail.contains("response decoding failed")),
        other => panic!("unexpected malformed-json error variant: {other:?}"),
    }
}
