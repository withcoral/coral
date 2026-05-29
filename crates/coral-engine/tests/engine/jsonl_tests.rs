#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::{fs, path::Path};

use coral_engine::{CoralQuery, CoreError, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    assert_row_count, assert_table_not_found, build_source, dir_url, execution_to_rows,
    test_runtime, users_rows, write_jsonl_file,
};

fn jsonl_manifest(name: &str, dir: &Path, glob: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": glob
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" },
                { "name": "email", "type": "Utf8" }
            ]
        }]
    })
}

fn jsonl_segment_partition_manifest(name: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl",
                "partitions": [
                    {
                        "name": "year",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    },
                    {
                        "name": "month",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 1 }
                    },
                    {
                        "name": "day",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 2 }
                    }
                ]
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" },
                { "name": "email", "type": "Utf8" }
            ]
        }]
    })
}

fn jsonl_hive_partition_manifest(name: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "users",
            "description": "Users fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl",
                "partitions": [
                    { "name": "year", "type": "Int64" },
                    { "name": "month", "type": "Int64" }
                ]
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
async fn select_all_from_jsonl_source() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_users", temp.path(), "**/*.jsonl"));

    let execution = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, name, email FROM jsonl_users.users ORDER BY id",
    )
    .await
    .expect("query should succeed");

    assert_row_count(&execution, 3);
    assert_eq!(execution_to_rows(&execution), users_rows());
}

#[tokio::test]
async fn quoted_fully_qualified_table_reference_reports_sql_reference_hint() {
    let temp = TempDir::new().expect("temp dir");
    let source = github_pulls_source(temp.path());

    let error =
        CoralQuery::execute_sql(&[source], test_runtime(), "SELECT * FROM \"github.pulls\"")
            .await
            .expect_err("whole-reference quoted table should fail");

    assert_quoted_fully_qualified_table_reference_hint(error);
}

#[tokio::test]
async fn explain_sql_quoted_fully_qualified_table_reference_reports_sql_reference_hint() {
    let temp = TempDir::new().expect("temp dir");
    let source = github_pulls_source(temp.path());

    let error =
        CoralQuery::explain_sql(&[source], test_runtime(), "SELECT * FROM \"github.pulls\"")
            .await
            .expect_err("whole-reference quoted table should fail during explanation");

    assert_quoted_fully_qualified_table_reference_hint(error);
}

fn assert_quoted_fully_qualified_table_reference_hint(error: CoreError) {
    assert_eq!(error.status_code(), StatusCode::NotFound);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "TABLE_NOT_FOUND");
            assert_eq!(sqe.metadata().get("schema"), None);
            assert_eq!(
                sqe.metadata().get("table").map(String::as_str),
                Some("github.pulls")
            );
            let hint = sqe.hint().expect("hint should be present");
            assert!(
                hint.contains("`\"github.pulls\"` is one quoted identifier"),
                "hint should explain the quoted-qualified mistake, got: {hint}"
            );
            assert!(
                hint.contains("`github.pulls`"),
                "hint should suggest the list_tables sql_reference form, got: {hint}"
            );
            assert!(
                hint.contains("`\"github\".\"pulls\"`"),
                "hint should show per-identifier quoting as valid SQL, got: {hint}"
            );
        }
        other => panic!("expected CoreError::QueryFailure, got {other:?}"),
    }
}

#[tokio::test]
async fn explain_sql_returns_logical_and_physical_plans() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_plan", temp.path(), "**/*.jsonl"));

    let plan = CoralQuery::explain_sql(
        &[source],
        test_runtime(),
        "SELECT id, name FROM jsonl_plan.users WHERE id > 1 ORDER BY name",
    )
    .await
    .expect("query should explain");

    assert!(plan.unoptimized_logical_plan().contains("jsonl_plan.users"));
    assert!(plan.optimized_logical_plan().contains("jsonl_plan.users"));
    assert!(plan.physical_plan().contains("Exec"));
}

fn github_pulls_source(dir: &Path) -> coral_engine::QuerySource {
    write_jsonl_file(
        dir,
        "pulls.jsonl",
        &[json!({"id": 1, "title": "Fix table hint"})],
    );
    build_source(json!({
        "name": "github",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "pulls",
            "description": "Pull requests fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "title", "type": "Utf8" }
            ]
        }]
    }))
}

#[tokio::test]
async fn select_with_column_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest(
        "jsonl_projection",
        temp.path(),
        "**/*.jsonl",
    ));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT name FROM jsonl_projection.users ORDER BY name DESC",
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
async fn select_with_where_filter() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_filter", temp.path(), "**/*.jsonl"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, name FROM jsonl_filter.users WHERE id = 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 2, "name": "Grace"})]);
}

#[tokio::test]
async fn select_with_order_by_and_limit() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_order", temp.path(), "**/*.jsonl"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT name FROM jsonl_order.users ORDER BY name DESC LIMIT 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({"name": "Linus"}), json!({"name": "Grace"})]
    );
}

#[tokio::test]
async fn select_with_limit_returns_rows() {
    let temp = TempDir::new().expect("temp dir");
    fs::write(
        temp.path().join("users.jsonl"),
        b"{\"id\":1,\"name\":\"Ada\",\"email\":\"ada@example.com\"}\n{\"id\":2,\"name\":\"Grace\",\"email\":\"grace@example.com\"}\n",
    )
    .expect("jsonl fixture should be written");
    let source = build_source(jsonl_manifest(
        "jsonl_stream_limit",
        temp.path(),
        "**/*.jsonl",
    ));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id FROM jsonl_stream_limit.users LIMIT 1",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 1})]);
}

#[tokio::test]
async fn select_count_aggregation() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "users.jsonl", &users_rows());
    let source = build_source(jsonl_manifest("jsonl_count", temp.path(), "**/*.jsonl"));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT COUNT(*) AS n FROM jsonl_count.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn malformed_jsonl_rows_return_error() {
    let temp = TempDir::new().expect("temp dir");
    std::fs::write(
        temp.path().join("users.jsonl"),
        r#"{"id":1,"name":"Ada","email":"ada@example.com"}
not-json
{"id":2,"name":"Grace","email":"grace@example.com"}
"#,
    )
    .expect("jsonl fixture should write");
    let source = build_source(jsonl_manifest(
        "jsonl_count_malformed",
        temp.path(),
        "**/*.jsonl",
    ));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT COUNT(*) AS n FROM jsonl_count_malformed.users",
    )
    .await
    .expect_err("malformed JSONL should fail");

    assert!(
        error.to_string().contains("failed to parse") || error.to_string().contains("Json error"),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn segment_partitions_are_projected_from_relative_path() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "2026/05/14/users.jsonl", &users_rows()[..2]);
    write_jsonl_file(temp.path(), "2026/05/13/users.jsonl", &users_rows()[2..]);
    let source = build_source(jsonl_segment_partition_manifest(
        "jsonl_segment_project",
        temp.path(),
    ));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, year, month, day FROM jsonl_segment_project.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "year": 2026, "month": 5, "day": 14}),
            json!({"id": 2, "year": 2026, "month": 5, "day": 14}),
            json!({"id": 3, "year": 2026, "month": 5, "day": 13}),
        ]
    );
}

#[tokio::test]
async fn segment_partitions_reject_files_without_declared_layout() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "2026/05/14/users.jsonl", &users_rows()[..1]);
    write_jsonl_file(
        temp.path(),
        "users.jsonl",
        &[json!({
            "id": 2,
            "name": "Grace",
            "email": "grace@example.com",
            "year": 2026,
            "month": 5,
            "day": 14
        })],
    );
    let source = build_source(jsonl_segment_partition_manifest(
        "jsonl_segment_strict",
        temp.path(),
    ));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, year, month, day FROM jsonl_segment_strict.users ORDER BY id",
    )
    .await
    .expect_err("file outside declared partition layout should fail");

    assert!(
        error
            .to_string()
            .contains("does not match partitioned table layout"),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn segment_partition_values_override_payload_fields() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "2026/05/14/users.jsonl",
        &[json!({
            "id": 1,
            "name": "Ada",
            "email": "ada@example.com",
            "year": 2025,
            "month": 1,
            "day": 1
        })],
    );
    let source = build_source(jsonl_segment_partition_manifest(
        "jsonl_segment_collision",
        temp.path(),
    ));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, year, month, day FROM jsonl_segment_collision.users \
             WHERE year = 2026 AND month = 5 AND day = 14",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({"id": 1, "year": 2026, "month": 5, "day": 14})]
    );
}

#[tokio::test]
async fn codex_session_style_segment_partitions_and_json_payload_are_queryable() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "2026/05/14/rollout-2026-05-14T12-34-33.jsonl",
        &[
            json!({
                "timestamp": "2026-05-14T12:34:33Z",
                "type": "event_msg",
                "payload": {
                    "id": "evt_1",
                    "cwd": "/Users/james/src/withcoral/coral"
                }
            }),
            json!({
                "timestamp": "2026-05-14T12:35:00Z",
                "type": "response_item",
                "payload": {
                    "id": "evt_2",
                    "cwd": "/Users/james/src/withcoral/coral"
                }
            }),
        ],
    );
    let source = build_source(json!({
        "name": "codex_sessions_fixture",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "Codex session events",
            "format": "jsonl",
            "source": {
                "location": dir_url(temp.path()),
                "glob": "**/*.jsonl",
                "partitions": [
                    {
                        "name": "year",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 0 }
                    },
                    {
                        "name": "month",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 1 }
                    },
                    {
                        "name": "day",
                        "type": "Int64",
                        "path": { "kind": "segment", "index": 2 }
                    }
                ]
            },
            "columns": [
                { "name": "timestamp", "type": "Utf8" },
                { "name": "type", "type": "Utf8" },
                { "name": "payload", "type": "Json" }
            ]
        }]
    }));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT year, month, day, type, json_get_str(payload, 'id') AS payload_id \
             FROM codex_sessions_fixture.events \
             WHERE year = 2026 AND month = 5 AND day = 14 \
             ORDER BY payload_id",
        )
        .await
        .expect("codex-style sessions should query"),
    );

    assert_eq!(
        rows,
        vec![
            json!({
                "year": 2026,
                "month": 5,
                "day": 14,
                "type": "event_msg",
                "payload_id": "evt_1"
            }),
            json!({
                "year": 2026,
                "month": 5,
                "day": 14,
                "type": "response_item",
                "payload_id": "evt_2"
            }),
        ]
    );
}

#[tokio::test]
async fn matching_partition_layout_with_invalid_value_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "year=bad/month=05/users.jsonl",
        &users_rows()[..1],
    );
    let source = build_source(jsonl_hive_partition_manifest(
        "jsonl_hive_bad_value",
        temp.path(),
    ));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id FROM jsonl_hive_bad_value.users",
    )
    .await
    .expect_err("invalid partition value should fail");

    assert!(
        error
            .to_string()
            .contains("partition 'year' value 'bad' is not Int64"),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn segment_partition_filters_prune_unrelated_files_before_counting() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "2026/05/14/users.jsonl", &users_rows()[..2]);
    let bad_dir = temp.path().join("2026/05/13");
    std::fs::create_dir_all(&bad_dir).expect("bad partition dir should exist");
    std::fs::write(bad_dir.join("users.jsonl"), [0xff]).expect("bad jsonl should write");
    let sources = vec![build_source(jsonl_segment_partition_manifest(
        "jsonl_segment_count",
        temp.path(),
    ))];

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT COUNT(*) AS n FROM jsonl_segment_count.users \
             WHERE year = 2026 AND month = 5 AND day = 14",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 2})]);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &sources,
            test_runtime(),
            "SELECT COUNT(*) AS n FROM jsonl_segment_count.users \
             WHERE year = 2026 AND year = 2025",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 0})]);
}

#[tokio::test]
async fn glob_matches_multiple_files() {
    let temp = TempDir::new().expect("temp dir");
    let rows = users_rows();
    write_jsonl_file(temp.path(), "nested/one.jsonl", &rows[..2]);
    write_jsonl_file(temp.path(), "nested/deeper/two.jsonl", &rows[2..]);
    let source = build_source(jsonl_manifest("jsonl_glob", temp.path(), "**/*.jsonl"));

    let execution = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, name, email FROM jsonl_glob.users ORDER BY id",
    )
    .await
    .expect("query should succeed");

    assert_eq!(execution_to_rows(&execution), rows);
}

#[tokio::test]
async fn missing_file_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(jsonl_manifest("jsonl_missing", &missing_dir, "**/*.jsonl"));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT * FROM jsonl_missing.users",
    )
    .await
    .expect_err("missing jsonl source should fail");

    assert_table_not_found(error, "jsonl_missing", "users");
}
