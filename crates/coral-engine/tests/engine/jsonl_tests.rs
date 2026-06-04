#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::path::Path;

use coral_engine::{CoralQuery, CoreError, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    assert_table_not_found, assert_users_query_matrix, build_source, dir_url, file_table_manifest,
    query_rows, source_error, source_rows, test_runtime, users_jsonl_manifest, users_jsonl_source,
    users_rows, write_jsonl_file,
};

fn jsonl_partition_manifest(name: &str, dir: &Path, partitions: &Value) -> Value {
    file_table_manifest(
        name,
        "users",
        "Users fixture",
        "jsonl",
        &json!({
                "location": dir_url(dir),
                "glob": "**/*.jsonl",
                "partitions": partitions
        }),
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "name", "type": "Utf8" }),
            json!({ "name": "email", "type": "Utf8" }),
        ],
    )
}

fn segment_partitions() -> Value {
    json!([
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
    ])
}

fn jsonl_segment_partition_manifest(name: &str, dir: &Path) -> Value {
    jsonl_partition_manifest(name, dir, &segment_partitions())
}

fn jsonl_hive_partition_manifest(name: &str, dir: &Path) -> Value {
    jsonl_partition_manifest(
        name,
        dir,
        &json!([
            { "name": "year", "type": "Int64" },
            { "name": "month", "type": "Int64" }
        ]),
    )
}

fn partition_source(
    name: &str,
    files: &[(&str, &[Value])],
    manifest: impl FnOnce(&str, &Path) -> Value,
) -> (TempDir, coral_engine::QuerySource) {
    let temp = TempDir::new().expect("temp dir");
    for (path, rows) in files {
        write_jsonl_file(temp.path(), path, rows);
    }
    let source = build_source(manifest(name, temp.path()));
    (temp, source)
}

fn segment_partition_source(
    name: &str,
    files: &[(&str, &[Value])],
) -> (TempDir, coral_engine::QuerySource) {
    partition_source(name, files, jsonl_segment_partition_manifest)
}

fn hive_partition_source(
    name: &str,
    files: &[(&str, &[Value])],
) -> (TempDir, coral_engine::QuerySource) {
    partition_source(name, files, jsonl_hive_partition_manifest)
}

fn assert_error_contains(error: &CoreError, expected: &str) {
    assert!(
        error.to_string().contains(expected),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn jsonl_users_query_matrix() {
    assert_users_query_matrix("jsonl", users_jsonl_source).await;
}

#[tokio::test]
async fn quoted_fully_qualified_table_reference_reports_sql_reference_hint() {
    let temp = TempDir::new().expect("temp dir");
    let source = github_pulls_source(temp.path());

    let error = source_error(&source, "SELECT * FROM \"github.pulls\"").await;

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
            for (fragment, reason) in [
                (
                    "`\"github.pulls\"` is one quoted identifier",
                    "explain the quoted-qualified mistake",
                ),
                (
                    "`github.pulls`",
                    "suggest the list_tables sql_reference form",
                ),
                (
                    "`\"github\".\"pulls\"`",
                    "show per-identifier quoting as valid SQL",
                ),
            ] {
                assert!(hint.contains(fragment), "hint should {reason}, got: {hint}");
            }
        }
        other => panic!("expected CoreError::QueryFailure, got {other:?}"),
    }
}

#[tokio::test]
async fn explain_sql_returns_logical_and_physical_plans() {
    let (_temp, source) = users_jsonl_source("jsonl_plan");

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
    build_source(file_table_manifest(
        "github",
        "pulls",
        "Pull requests fixture",
        "jsonl",
        &json!({
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
        }),
        &[
            json!({ "name": "id", "type": "Int64" }),
            json!({ "name": "title", "type": "Utf8" }),
        ],
    ))
}

#[tokio::test]
async fn select_with_limit_returns_rows() {
    let temp = TempDir::new().expect("temp dir");
    let rows = users_rows();
    write_jsonl_file(temp.path(), "users.jsonl", &rows[..2]);
    let source = build_source(users_jsonl_manifest(
        "jsonl_stream_limit",
        temp.path(),
        "**/*.jsonl",
    ));

    let rows = source_rows(&source, "SELECT id FROM jsonl_stream_limit.users LIMIT 1").await;

    assert_eq!(rows, vec![json!({"id": 1})]);
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
    let source = build_source(users_jsonl_manifest(
        "jsonl_count_malformed",
        temp.path(),
        "**/*.jsonl",
    ));

    let error = source_error(
        &source,
        "SELECT COUNT(*) AS n FROM jsonl_count_malformed.users",
    )
    .await;

    assert!(
        error.to_string().contains("failed to parse") || error.to_string().contains("Json error"),
        "unexpected error: {error:?}"
    );
}

#[tokio::test]
async fn segment_partitions_are_projected_from_relative_path() {
    let rows = users_rows();
    let (_temp, source) = segment_partition_source(
        "jsonl_segment_project",
        &[
            ("2026/05/14/users.jsonl", &rows[..2]),
            ("2026/05/13/users.jsonl", &rows[2..]),
        ],
    );

    let rows = source_rows(
        &source,
        "SELECT id, year, month, day FROM jsonl_segment_project.users ORDER BY id",
    )
    .await;

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
    let rows = users_rows();
    let unpartitioned = vec![json!({
            "id": 2,
            "name": "Grace",
            "email": "grace@example.com",
            "year": 2026,
            "month": 5,
            "day": 14
    })];
    let (_temp, source) = segment_partition_source(
        "jsonl_segment_strict",
        &[
            ("2026/05/14/users.jsonl", &rows[..1]),
            ("users.jsonl", &unpartitioned),
        ],
    );

    let error = source_error(
        &source,
        "SELECT id, year, month, day FROM jsonl_segment_strict.users ORDER BY id",
    )
    .await;

    assert_error_contains(&error, "does not match partitioned table layout");
}

#[tokio::test]
async fn segment_partition_values_override_payload_fields() {
    let rows = vec![json!({
            "id": 1,
            "name": "Ada",
            "email": "ada@example.com",
            "year": 2025,
            "month": 1,
            "day": 1
    })];
    let (_temp, source) = segment_partition_source(
        "jsonl_segment_collision",
        &[("2026/05/14/users.jsonl", &rows)],
    );

    let rows = source_rows(
        &source,
        "SELECT id, year, month, day FROM jsonl_segment_collision.users \
         WHERE year = 2026 AND month = 5 AND day = 14",
    )
    .await;

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
    let source = build_source(file_table_manifest(
        "codex_sessions_fixture",
        "events",
        "Codex session events",
        "jsonl",
        &json!({
            "location": dir_url(temp.path()),
            "glob": "**/*.jsonl",
            "partitions": segment_partitions(),
            "metadata": [
                { "name": "session_path", "kind": "relative_path" },
                { "name": "session_file", "kind": "file_stem" },
                { "name": "event_index", "kind": "line_number" }
            ]
        }),
        &[
            json!({ "name": "timestamp", "type": "Utf8" }),
            json!({ "name": "type", "type": "Utf8" }),
            json!({ "name": "payload", "type": "Json" }),
        ],
    ));

    let rows = source_rows(
        &source,
        "SELECT year, month, day, session_path, session_file, event_index, \
                type, json_get_str(payload, 'id') AS payload_id \
         FROM codex_sessions_fixture.events \
         WHERE year = 2026 AND month = 5 AND day = 14 \
         ORDER BY event_index",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({
                "year": 2026,
                "month": 5,
                "day": 14,
                "session_path": "2026/05/14/rollout-2026-05-14T12-34-33.jsonl",
                "session_file": "rollout-2026-05-14T12-34-33",
                "event_index": 1,
                "type": "event_msg",
                "payload_id": "evt_1"
            }),
            json!({
                "year": 2026,
                "month": 5,
                "day": 14,
                "session_path": "2026/05/14/rollout-2026-05-14T12-34-33.jsonl",
                "session_file": "rollout-2026-05-14T12-34-33",
                "event_index": 2,
                "type": "response_item",
                "payload_id": "evt_2"
            }),
        ]
    );
}

#[tokio::test]
async fn jsonl_metadata_columns_are_queryable_and_discoverable() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "2026/06/02/session.with.dots.jsonl",
        &[
            json!({
                "id": 1,
                "file_path": "payload/file.jsonl",
                "file_name": "payload-file.jsonl",
                "file_stem": "payload-file",
                "event_index": -1
            }),
            json!({"id": 2}),
        ],
    );
    let source = build_source(json!({
        "name": "jsonl_metadata_fixture",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "JSONL events with metadata",
            "format": "jsonl",
            "source": {
                "location": dir_url(temp.path()),
                "glob": "**/*.jsonl",
                "metadata": [
                    { "name": "file_path", "kind": "relative_path" },
                    { "name": "file_name", "kind": "file_name" },
                    { "name": "file_stem", "kind": "file_stem" },
                    { "name": "event_index", "kind": "line_number" }
                ]
            },
            "columns": [{ "name": "id", "type": "Int64" }]
        }]
    }));
    let sources = [source];

    let rows = query_rows(
        &sources,
        "SELECT id, file_path, file_name, file_stem, event_index \
         FROM jsonl_metadata_fixture.events \
         ORDER BY event_index",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({
                "id": 1,
                "file_path": "2026/06/02/session.with.dots.jsonl",
                "file_name": "session.with.dots.jsonl",
                "file_stem": "session.with.dots",
                "event_index": 1
            }),
            json!({
                "id": 2,
                "file_path": "2026/06/02/session.with.dots.jsonl",
                "file_name": "session.with.dots.jsonl",
                "file_stem": "session.with.dots",
                "event_index": 2
            }),
        ]
    );

    let rows = query_rows(
        &sources,
        "SELECT column_name, data_type \
         FROM coral.columns \
         WHERE schema_name = 'jsonl_metadata_fixture' \
           AND table_name = 'events' \
           AND column_name IN ('file_path', 'file_name', 'file_stem', 'event_index') \
         ORDER BY column_name",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({"column_name": "event_index", "data_type": "Int64"}),
            json!({"column_name": "file_name", "data_type": "Utf8"}),
            json!({"column_name": "file_path", "data_type": "Utf8"}),
            json!({"column_name": "file_stem", "data_type": "Utf8"}),
        ]
    );
}

#[tokio::test]
async fn jsonl_metadata_columns_work_for_single_file_locations() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "events.jsonl", &[json!({"id": 1})]);
    let file_url = url::Url::from_file_path(temp.path().join("events.jsonl"))
        .expect("file path should convert to URL")
        .to_string();
    let source = build_source(json!({
        "name": "jsonl_single_file_metadata",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "Single JSONL file with metadata",
            "format": "jsonl",
            "source": {
                "location": file_url,
                "metadata": [
                    { "name": "file_path", "kind": "relative_path" },
                    { "name": "file_name", "kind": "file_name" },
                    { "name": "file_stem", "kind": "file_stem" },
                    { "name": "event_index", "kind": "line_number" }
                ]
            },
            "columns": [{ "name": "id", "type": "Int64" }]
        }]
    }));
    let sources = [source];

    let rows = query_rows(
        &sources,
        "SELECT id, file_path, file_name, file_stem, event_index \
         FROM jsonl_single_file_metadata.events",
    )
    .await;

    assert_eq!(
        rows,
        vec![json!({
            "id": 1,
            "file_path": "events.jsonl",
            "file_name": "events.jsonl",
            "file_stem": "events",
            "event_index": 1
        })]
    );
}

#[tokio::test]
async fn jsonl_metadata_preserves_object_store_path_text() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "L%3ABC.jsonl", &[json!({"id": 1})]);
    write_jsonl_file(temp.path(), "bad%ZZ.jsonl", &[json!({"id": 2})]);
    let source = build_source(json!({
        "name": "jsonl_percent_metadata",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "JSONL files with literal percent path text",
            "format": "jsonl",
            "source": {
                "location": dir_url(temp.path()),
                "glob": "*.jsonl",
                "metadata": [
                    { "name": "file_path", "kind": "relative_path" },
                    { "name": "file_name", "kind": "file_name" },
                    { "name": "file_stem", "kind": "file_stem" }
                ]
            },
            "columns": [{ "name": "id", "type": "Int64" }]
        }]
    }));
    let sources = [source];

    let rows = query_rows(
        &sources,
        "SELECT id, file_path, file_name, file_stem \
         FROM jsonl_percent_metadata.events \
         ORDER BY id",
    )
    .await;

    assert_eq!(
        rows,
        vec![
            json!({
                "id": 1,
                "file_path": "L%3ABC.jsonl",
                "file_name": "L%3ABC.jsonl",
                "file_stem": "L%3ABC"
            }),
            json!({
                "id": 2,
                "file_path": "bad%ZZ.jsonl",
                "file_name": "bad%ZZ.jsonl",
                "file_stem": "bad%ZZ"
            }),
        ]
    );
}

#[tokio::test]
async fn matching_partition_layout_with_invalid_value_returns_error() {
    let rows = users_rows();
    let (_temp, source) = hive_partition_source(
        "jsonl_hive_bad_value",
        &[("year=bad/month=05/users.jsonl", &rows[..1])],
    );

    let error = source_error(&source, "SELECT id FROM jsonl_hive_bad_value.users").await;

    assert_error_contains(&error, "partition 'year' value 'bad' is not Int64");
}

#[tokio::test]
async fn segment_partition_filters_prune_unrelated_files_before_counting() {
    let rows = users_rows();
    let (temp, source) = segment_partition_source(
        "jsonl_segment_count",
        &[("2026/05/14/users.jsonl", &rows[..2])],
    );
    let bad_dir = temp.path().join("2026/05/13");
    std::fs::create_dir_all(&bad_dir).expect("bad partition dir should exist");
    std::fs::write(bad_dir.join("users.jsonl"), [0xff]).expect("bad jsonl should write");
    let sources = vec![source];

    let rows = query_rows(
        &sources,
        "SELECT COUNT(*) AS n FROM jsonl_segment_count.users \
         WHERE year = 2026 AND month = 5 AND day = 14",
    )
    .await;

    assert_eq!(rows, vec![json!({"n": 2})]);

    let rows = query_rows(
        &sources,
        "SELECT COUNT(*) AS n FROM jsonl_segment_count.users \
         WHERE year = 2026 AND year = 2025",
    )
    .await;

    assert_eq!(rows, vec![json!({"n": 0})]);
}

#[tokio::test]
async fn glob_matches_multiple_files() {
    let temp = TempDir::new().expect("temp dir");
    let rows = users_rows();
    write_jsonl_file(temp.path(), "nested/one.jsonl", &rows[..2]);
    write_jsonl_file(temp.path(), "nested/deeper/two.jsonl", &rows[2..]);
    let source = build_source(users_jsonl_manifest(
        "jsonl_glob",
        temp.path(),
        "**/*.jsonl",
    ));

    assert_eq!(
        source_rows(
            &source,
            "SELECT id, name, email FROM jsonl_glob.users ORDER BY id",
        )
        .await,
        rows
    );
}

#[tokio::test]
async fn missing_file_returns_error() {
    let temp = TempDir::new().expect("temp dir");
    let missing_dir = temp.path().join("missing");
    let source = build_source(users_jsonl_manifest(
        "jsonl_missing",
        &missing_dir,
        "**/*.jsonl",
    ));

    let error = source_error(&source, "SELECT * FROM jsonl_missing.users").await;

    assert_table_not_found(error, "jsonl_missing", "users");
}
