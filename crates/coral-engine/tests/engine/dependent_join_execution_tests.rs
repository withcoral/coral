use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use coral_engine::{
    CoralQuery, CoreError, DependentJoinConfig, DependentJoinSourceConfig, QueryRuntimeConfig,
    QuerySource, StatusCode,
};
use coral_spec::{ValidatedSourceManifest, parse_source_manifest_yaml};
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

#[tokio::test]
async fn sql_join_fetches_http_dependent_rows_per_distinct_binding_tuple() {
    assert_dependent_join_query(
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await;
}

#[tokio::test]
async fn sql_join_fetches_when_http_dependent_table_is_on_left() {
    assert_dependent_join_query(
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM github.pull_requests AS pr
        JOIN issues.items AS i
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await;
}

#[tokio::test]
async fn sql_join_fetches_when_http_dependent_table_is_projected() {
    assert_dependent_join_query(
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN (
          SELECT state, owner, repo, number
          FROM github.pull_requests
        ) AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await;
}

#[tokio::test]
async fn dpp_and_naive_paths_agree_on_duplicate_and_null_join_rows() {
    assert_dpp_and_naive_rows_agree(
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
        RowComparison::Unordered,
    )
    .await;
}

#[tokio::test]
async fn dpp_and_naive_paths_agree_with_order_by() {
    assert_dpp_and_naive_rows_agree(
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY pr.state, i.title
        ",
        RowComparison::Exact,
    )
    .await;
}

#[tokio::test]
async fn dpp_and_naive_paths_agree_on_join_aggregation() {
    assert_dpp_and_naive_rows_agree(
        "
        SELECT COUNT(*) AS row_count
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
        RowComparison::Exact,
    )
    .await;
}

#[tokio::test]
async fn duplicate_resolver_rows_for_one_binding_share_join_batch() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            issue_row("First", "withcoral", "coral", 123),
            issue_row("Duplicate tuple", "withcoral", "coral", 123),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
    )
    .await
    .expect("query should succeed");

    assert_eq!(execution.batches().len(), 1);
    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({ "issue_title": "First", "pr_state": "open" }),
            json!({ "issue_title": "Duplicate tuple", "pr_state": "open" }),
        ]
    );
}

#[tokio::test]
async fn dependent_join_accepts_safe_casts_on_join_keys() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let sql = "
        WITH input AS (
          SELECT 'Null repo' AS issue_title,
                 'withcoral' AS github_owner,
                 CAST(NULL AS VARCHAR) AS github_repo,
                 CAST(123 AS INTEGER) AS github_pr_number
          UNION ALL
          SELECT 'First', 'withcoral', 'coral', CAST(123 AS INTEGER)
        )
        SELECT input.issue_title AS issue_title, pr.state AS pr_state
        FROM input
        JOIN github.pull_requests AS pr
          ON pr.owner = input.github_owner
         AND pr.repo = input.github_repo
         AND pr.number = input.github_pr_number
        ORDER BY input.issue_title
        ";

    let explain_sql = format!("EXPLAIN {sql}");
    let explain = CoralQuery::execute_sql(
        &[build_source(github_required_manifest(&server.uri()))],
        test_runtime(),
        &explain_sql,
    )
    .await
    .expect("explain should succeed");
    let explain = execution_text(&explain);
    assert!(explain.contains("DependentJoinExec"), "{explain}");
    assert!(
        explain.contains(
            "binding_keys=[owner <- input.github_owner, repo <- input.github_repo, number <- input.github_pr_number]"
        ),
        "{explain}"
    );

    let execution = CoralQuery::execute_sql(
        &[build_source(github_required_manifest(&server.uri()))],
        test_runtime(),
        sql,
    )
    .await
    .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn sql_join_reads_all_resolver_partitions() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            issue_row("First", "withcoral", "coral", 123),
            issue_row("Second", "apache", "arrow-datafusion", 42),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 1,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/apache/arrow-datafusion/pulls/42"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "apache",
                "repo": "arrow-datafusion",
                "number": 42,
                "state": "closed"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM (
          SELECT title, github_owner, github_repo, github_pr_number
          FROM issues.items
          WHERE title = 'First'
          UNION ALL
          SELECT title, github_owner, github_repo, github_pr_number
          FROM issues.items
          WHERE title = 'Second'
        ) AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("dependent join should consume every resolver partition");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({ "issue_title": "First", "pr_state": "open" }),
            json!({ "issue_title": "Second", "pr_state": "closed" }),
        ]
    );
}

#[tokio::test]
async fn sql_join_rewrites_when_resolver_side_is_also_http() {
    let resolver_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/channels"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "channels": [
                { "name": "general", "id": "C-general" },
                { "name": "random", "id": "C-random" }
            ]
        })))
        .expect(1)
        .mount(&resolver_server)
        .await;

    let dependent_server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .and(query_param("channel", "C-general"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "messages": [
                { "channel": "C-general", "user": "U1", "text": "hello" }
            ]
        })))
        .expect(1)
        .mount(&dependent_server)
        .await;

    let sources = [
        build_source(slack_channels_http_manifest(&resolver_server.uri())),
        build_source(slack_messages_manifest(&dependent_server.uri())),
    ];
    let sql = "
    SELECT c.name AS channel_name, m.text
    FROM slack_channels.channels AS c
    JOIN slack.messages AS m
      ON m.channel = c.id
    WHERE c.name = 'general'
    ";

    let explain = execution_text(
        &CoralQuery::execute_sql(&sources, test_runtime(), &format!("EXPLAIN {sql}"))
            .await
            .expect("explain should succeed"),
    );
    assert!(explain.contains("DependentJoinExec"), "{explain}");
    assert!(explain.contains("channel <- c.id"), "{explain}");

    let execution = CoralQuery::execute_sql(&sources, test_runtime(), sql)
        .await
        .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "channel_name": "general", "text": "hello" })]
    );
}

#[tokio::test]
async fn sql_join_uses_qualified_resolver_binding_when_column_names_collide() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "channels.jsonl",
        &[json!({ "name": "general", "id": "C-general" })],
    );
    write_jsonl_file(
        temp.path(),
        "resolver_ids.jsonl",
        &[json!({ "id": "wrong-channel", "channel_name": "general" })],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .and(query_param("channel", "wrong-channel"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "messages": [
                { "channel": "wrong-channel", "user": "U-bad", "text": "wrong" }
            ]
        })))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .and(query_param("channel", "C-general"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "messages": [
                { "channel": "C-general", "user": "U1", "text": "hello" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(slack_channels_manifest(temp.path())),
            build_source(resolver_ids_manifest(temp.path())),
            build_source(slack_messages_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT r.id AS resolver_id, c.id AS channel_id, m.text
        FROM resolver_ids.items AS r
        JOIN slack_channels.channels AS c
          ON c.name = r.channel_name
        JOIN slack.messages AS m
          ON m.channel = c.id
        ",
    )
    .await
    .expect("dependent join should bind c.id, not the earlier r.id column");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "resolver_id": "wrong-channel",
            "channel_id": "C-general",
            "text": "hello"
        })]
    );
}

#[tokio::test]
async fn literal_filters_and_join_bindings_together_satisfy_required_dependent_filters() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 1,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_required_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.number = i.github_pr_number
        WHERE pr.owner = 'withcoral'
          AND pr.repo = 'coral'
        ORDER BY i.title
        ",
    )
    .await
    .expect("literal filters plus join bindings should satisfy required dependent filters");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn literal_filter_values_are_available_to_dependent_output_mapping() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("state", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "number": 123
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_filter_backed_columns_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT pr.state
        FROM issues.items i
        JOIN github.pull_requests pr
          ON pr.number = i.github_pr_number
        WHERE pr.owner = 'withcoral'
          AND pr.repo = 'coral'
          AND pr.state = 'open'
        ",
    )
    .await
    .expect("literal filters should be available to from_filter output columns");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "state": "open" })]
    );
}

#[tokio::test]
async fn null_binding_rows_do_not_fetch_and_do_not_emit() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            json!({
                "title": "Has PR",
                "github_owner": "withcoral",
                "github_repo": "coral",
                "github_pr_number": 123
            }),
            json!({
                "title": "Missing owner",
                "github_owner": null,
                "github_repo": "coral",
                "github_pr_number": 123
            }),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{ "owner": "withcoral", "repo": "coral", "number": 123, "state": "open" }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "Has PR", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn too_many_distinct_bindings_returns_cap_error() {
    let temp = TempDir::new().expect("temp dir");
    let issues = (1..=501)
        .map(|number| issue_row(&format!("Issue {number}"), "withcoral", "coral", number))
        .collect::<Vec<_>>();
    write_jsonl_file(temp.path(), "issues.jsonl", &issues);

    let server = MockServer::start().await;
    let error = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        dependent_join_sql(),
    )
    .await
    .expect_err("query should fail when distinct binding cap is exceeded");

    assert_error_contains(
        &error,
        "Your query produced 501 distinct combinations of join-key values for github.pull_requests",
    );
    assert_dependent_join_limit_error(
        &error,
        "DEPENDENT_JOIN_BINDING_LIMIT_EXCEEDED",
        "501",
        "500",
    );
    let CoreError::QueryFailure(query_error) = &error else {
        panic!("expected structured query failure, got {error:?}");
    };
    assert_eq!(
        query_error.metadata().get("binding_filters").unwrap(),
        "owner,repo,number"
    );
    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
}

#[tokio::test]
async fn too_many_resolver_rows_for_one_binding_returns_cap_error() {
    let temp = TempDir::new().expect("temp dir");
    let issues = (1..=1001)
        .map(|idx| issue_row(&format!("Issue {idx}"), "withcoral", "coral", 123))
        .collect::<Vec<_>>();
    write_jsonl_file(temp.path(), "issues.jsonl", &issues);

    let server = MockServer::start().await;
    // The 1001 JSONL rows above are resolver-side rows for one binding tuple.
    // Overflow must be detected before dispatching any dependent HTTP fetch.
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(500).set_body_string("unreachable dependent fetch"))
        .expect(0)
        .mount(&server)
        .await;

    let error = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        dependent_join_sql(),
    )
    .await
    .expect_err("query should fail when one binding has too many resolver rows");

    assert_error_contains(
        &error,
        "One join-key combination for github.pull_requests matched 1001 rows",
    );
    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
}

#[tokio::test]
async fn single_fetch_too_many_rows_returns_cap_error() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    let rows = (1..=1001)
        .map(|number| {
            json!({
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": format!("state-{number}")
            })
        })
        .collect::<Vec<_>>();

    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": rows })))
        .expect(1)
        .mount(&server)
        .await;

    let error = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        dependent_join_sql(),
    )
    .await
    .expect_err("query should fail when one dependent fetch exceeds its row cap");

    assert_error_contains(
        &error,
        "The upstream API for github.pull_requests returned 1001 rows for one join-key combination",
    );
    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
}

#[tokio::test]
async fn rows_per_binding_cap_stops_paginated_fetch_after_overflow_is_known() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    let first_page = (1..=1000)
        .map(|number| {
            json!({
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": format!("state-{number}")
            })
        })
        .collect::<Vec<_>>();

    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "1"))
        .and(query_param("per_page", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": first_page })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "2"))
        .and(query_param("per_page", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "closed"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "3"))
        .and(query_param("per_page", "1000"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(0)
        .mount(&server)
        .await;

    let mut github = github_paginated_manifest(&server.uri());
    first_table_object_mut(&mut github).insert(
        "pagination".to_string(),
        json!({
            "mode": "page",
            "page_param": "page",
            "page_start": 1,
            "page_size": {
                "default": 1000,
                "max": 1000,
                "query_param": "per_page"
            }
        }),
    );

    let error = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
    )
    .await
    .expect_err("query should fail once the dependent fetch exceeds its row cap");

    assert_error_contains(
        &error,
        "The upstream API for github.pull_requests returned 1001 rows for one join-key combination",
    );
    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
}

#[tokio::test]
async fn resolver_rows_cap_retries_original_query_without_dependent_join_rewrite() {
    let temp = TempDir::new().expect("temp dir");
    let issues = (1..=10_001)
        .map(|idx| {
            let number = ((idx - 1) % 11) + 1;
            issue_row(&format!("Issue {idx}"), "withcoral", "coral", number)
        })
        .collect::<Vec<_>>();
    write_jsonl_file(temp.path(), "issues.jsonl", &issues);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param("owner", "withcoral"))
        .and(query_param("repo", "coral"))
        .and(query_param("number", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 1,
                "state": "open"
            }]
        })))
        .expect(0)
        .mount(&server)
        .await;
    let fallback_rows = (1..=11)
        .map(|number| {
            json!({
                "owner": "withcoral",
                "repo": "coral",
                "number": number,
                "state": "open"
            })
        })
        .collect::<Vec<_>>();

    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param_is_missing("owner"))
        .and(query_param_is_missing("repo"))
        .and(query_param_is_missing("number"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": fallback_rows })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_query_manifest(&server.uri())),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            enabled: false,
            per_source: BTreeMap::from([(
                "github".to_string(),
                DependentJoinSourceConfig {
                    enabled: Some(true),
                    ..DependentJoinSourceConfig::default()
                },
            )]),
            ..DependentJoinConfig::default()
        }),
        "
        SELECT COUNT(*) AS row_count
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
    )
    .await
    .expect("resolver-row overflow should retry the original query without dependent join rewrite");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "row_count": 10001 })]
    );
}

#[tokio::test]
async fn resolver_rows_cap_preserves_cap_error_when_required_filter_fallback_fails() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            issue_row("First", "withcoral", "coral", 1),
            issue_row("Second", "withcoral", "coral", 2),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/1"))
        .respond_with(ResponseTemplate::new(500).set_body_string("unreachable dependent fetch"))
        .expect(0)
        .mount(&server)
        .await;

    let error = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_required_manifest(&server.uri())),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            max_resolver_rows: 1,
            ..DependentJoinConfig::default()
        }),
        dependent_join_sql(),
    )
    .await
    .expect_err("required-filter fallback should preserve resolver row cap error");

    assert_error_contains(
        &error,
        "The side of the join that supplies keys for github.pull_requests produced 2 rows",
    );
    assert_dependent_join_limit_error(
        &error,
        "DEPENDENT_JOIN_RESOLVER_ROW_LIMIT_EXCEEDED",
        "2",
        "1",
    );
}

#[tokio::test]
async fn parent_limit_caps_dependent_row_limit() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "1"))
        .and(query_param("per_page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "2"))
        .and(query_param("per_page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "closed"
            }]
        })))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "3"))
        .and(query_param("per_page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(0)
        .mount(&server)
        .await;

    let sources = [
        build_source(issues_manifest(temp.path())),
        build_source(github_paginated_manifest(&server.uri())),
    ];
    let sql = "
    SELECT i.title AS issue_title, pr.state AS pr_state
    FROM issues.items AS i
    JOIN github.pull_requests AS pr
      ON pr.owner = i.github_owner
     AND pr.repo = i.github_repo
     AND pr.number = i.github_pr_number
    LIMIT 1
    ";

    let explain = execution_text(
        &CoralQuery::execute_sql(&sources, test_runtime(), &format!("EXPLAIN {sql}"))
            .await
            .expect("explain should succeed"),
    );
    assert!(explain.contains("page_hint=1"), "{explain}");

    let execution = CoralQuery::execute_sql(&sources, test_runtime(), sql)
        .await
        .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn parent_limit_caps_owner_repo_dependent_list_route() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param("owner", "withcoral"))
        .and(query_param("repo", "coral"))
        .and(query_param("page", "1"))
        .and(query_param("per_page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                { "owner": "withcoral", "repo": "coral", "number": 1, "state": "open" },
                { "owner": "withcoral", "repo": "coral", "number": 2, "state": "closed" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param("owner", "withcoral"))
        .and(query_param("repo", "coral"))
        .and(query_param("page", "2"))
        .and(query_param("per_page", "2"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                { "owner": "withcoral", "repo": "coral", "number": 3, "state": "draft" },
                { "owner": "withcoral", "repo": "coral", "number": 4, "state": "merged" }
            ]
        })))
        .expect(0)
        .mount(&server)
        .await;

    let mut github = github_broad_query_manifest(&server.uri());
    first_table_object_mut(&mut github).insert(
        "pagination".to_string(),
        json!({
            "mode": "page",
            "page_param": "page",
            "page_start": 1,
            "page_size": {
                "default": 100,
                "max": 100,
                "query_param": "per_page"
            }
        }),
    );

    let sources = [
        build_source(issues_manifest(temp.path())),
        build_source(github),
    ];
    let sql = "
    SELECT pr.number, pr.state
    FROM issues.items AS i
    JOIN github.pull_requests AS pr
      ON pr.owner = i.github_owner
     AND pr.repo = i.github_repo
    LIMIT 2
    ";

    let explain = execution_text(
        &CoralQuery::execute_sql(&sources, test_runtime(), &format!("EXPLAIN {sql}"))
            .await
            .expect("explain should succeed"),
    );
    assert!(explain.contains("page_hint=2"), "{explain}");

    let execution = CoralQuery::execute_sql(&sources, test_runtime(), sql)
        .await
        .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({ "number": 1, "state": "open" }),
            json!({ "number": 2, "state": "closed" }),
        ]
    );
}

#[tokio::test]
async fn fetch_limit_default_does_not_truncate_dependent_fetches() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                {
                    "owner": "withcoral",
                    "repo": "coral",
                    "number": 123,
                    "state": "open"
                },
                {
                    "owner": "withcoral",
                    "repo": "coral",
                    "number": 123,
                    "state": "closed"
                }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest_with_fetch_limit_default(&server.uri(), 1)),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY pr.state DESC
        ",
    )
    .await
    .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({ "issue_title": "First", "pr_state": "open" }),
            json!({ "issue_title": "First", "pr_state": "closed" }),
        ]
    );
}

#[tokio::test]
async fn first_dependent_fetch_error_fails_query_and_stops_dispatching_new_bindings() {
    let temp = TempDir::new().expect("temp dir");
    let issues = (1..=9)
        .map(|number| issue_row(&format!("Issue {number}"), "withcoral", "coral", number))
        .collect::<Vec<_>>();
    write_jsonl_file(temp.path(), "issues.jsonl", &issues);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/1"))
        .respond_with(ResponseTemplate::new(500).set_body_string("dependent fetch failed"))
        .expect(3)
        .mount(&server)
        .await;

    for number in 2..=8 {
        Mock::given(method("GET"))
            .and(path(format!("/repos/withcoral/coral/pulls/{number}")))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_delay(Duration::from_secs(30))
                    .set_body_json(json!({
                        "data": [{
                            "owner": "withcoral",
                            "repo": "coral",
                            "number": number,
                            "state": "open"
                        }]
                    })),
            )
            .expect(1)
            .mount(&server)
            .await;
    }

    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/9"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 9,
                "state": "open"
            }]
        })))
        .expect(0)
        .mount(&server)
        .await;

    let error = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        dependent_join_sql(),
    )
    .await
    .expect_err("first terminal dependent fetch error should fail the query");

    assert_eq!(error.status_code(), StatusCode::Unavailable);
    assert_error_contains(&error, "dependent fetch failed");
}

#[tokio::test]
async fn dependent_fetches_honor_config_max_concurrency() {
    let temp = TempDir::new().expect("temp dir");
    let issues = (1..=5)
        .map(|number| issue_row(&format!("Issue {number}"), "withcoral", "coral", number))
        .collect::<Vec<_>>();
    write_jsonl_file(temp.path(), "issues.jsonl", &issues);

    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let delay = Duration::from_millis(250);

    let (server_uri, server_task) = spawn_concurrency_tracking_github_server(
        delay,
        Arc::clone(&active),
        Arc::clone(&max_active),
    )
    .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server_uri)),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            max_concurrency: 2,
            ..DependentJoinConfig::default()
        }),
        dependent_join_sql(),
    )
    .await
    .expect("dependent join query should succeed");

    assert_eq!(execution_to_rows(&execution).len(), 5);
    assert_eq!(max_active.load(Ordering::Acquire), 2);
    server_task.abort();
}

#[tokio::test]
async fn explain_analyze_reports_dependent_join_metrics() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            issue_row("First", "withcoral", "coral", 123),
            issue_row("Duplicate tuple", "withcoral", "coral", 123),
            issue_row("Second", "apache", "arrow-datafusion", 42),
            json!({
                "title": "Null owner",
                "github_owner": null,
                "github_repo": "coral",
                "github_pr_number": 123
            }),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("state", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/apache/arrow-datafusion/pulls/42"))
        .and(query_param("state", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        EXPLAIN ANALYZE
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain analyze should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec"));
    assert!(explain.contains("binding_count=2"), "{explain}");
    assert!(explain.contains("fetch_count=2"), "{explain}");
    assert!(explain.contains("resolver_rows=4"), "{explain}");
    assert!(
        explain.contains("resolver_null_binding_rows=1"),
        "{explain}"
    );
    assert!(explain.contains("dependent_rows_returned=1"), "{explain}");
}

#[tokio::test]
async fn explain_shows_dependent_join_bindings_and_caps() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest("http://127.0.0.1:9")),
        ],
        test_runtime(),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec: table=github.pull_requests"));
    assert!(
        explain.contains(
            "binding_keys=[owner <- i.github_owner, repo <- i.github_repo, number <- i.github_pr_number]"
        ),
        "{explain}"
    );
    assert!(
        explain.contains("literal_filters={state=\"open\"}"),
        "{explain}"
    );
    assert!(explain.contains("max_bindings=500"), "{explain}");
    assert!(explain.contains("max_resolver_rows=10000"), "{explain}");
    assert!(explain.contains("max_rows_per_binding=1000"), "{explain}");
    assert!(
        explain.contains("max_resolver_rows_per_binding=1000"),
        "{explain}"
    );
    assert!(explain.contains("max_concurrency=8"), "{explain}");
    assert!(explain.contains("page_hint=None"), "{explain}");
}

#[tokio::test]
async fn dependent_join_config_disables_rewrite_globally() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_query_manifest("http://127.0.0.1:9")),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            enabled: false,
            ..DependentJoinConfig::default()
        }),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(!explain.contains("DependentJoinExec"), "{explain}");
}

#[tokio::test]
async fn dependent_join_config_sets_default_caps() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest("http://127.0.0.1:9")),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            enabled: true,
            max_bindings: 7,
            max_resolver_rows: 11,
            max_rows_per_binding: 13,
            max_resolver_rows_per_binding: 17,
            max_concurrency: 19,
            per_source: BTreeMap::new(),
        }),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec: table=github.pull_requests"));
    assert!(explain.contains("max_bindings=7"), "{explain}");
    assert!(explain.contains("max_resolver_rows=11"), "{explain}");
    assert!(explain.contains("max_rows_per_binding=13"), "{explain}");
    assert!(
        explain.contains("max_resolver_rows_per_binding=17"),
        "{explain}"
    );
    assert!(explain.contains("max_concurrency=19"), "{explain}");
}

#[tokio::test]
async fn dependent_join_config_clamps_explained_max_concurrency() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest("http://127.0.0.1:9")),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            max_concurrency: 0,
            ..DependentJoinConfig::default()
        }),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec: table=github.pull_requests"));
    assert!(explain.contains("max_concurrency=1"), "{explain}");
}

#[tokio::test]
async fn dependent_join_source_config_disables_rewrite_for_source() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_query_manifest("http://127.0.0.1:9")),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            per_source: BTreeMap::from([(
                "github".to_string(),
                DependentJoinSourceConfig {
                    enabled: Some(false),
                    ..DependentJoinSourceConfig::default()
                },
            )]),
            ..DependentJoinConfig::default()
        }),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(!explain.contains("DependentJoinExec"), "{explain}");
}

#[tokio::test]
async fn dependent_join_source_config_disables_one_mixed_lookup_key_side() {
    let execution = CoralQuery::execute_sql(
        &[
            build_source(github_broad_query_manifest("http://127.0.0.1:9")),
            build_source(source_named(
                github_broad_query_manifest("http://127.0.0.1:9"),
                "mirror",
            )),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            per_source: BTreeMap::from([(
                "github".to_string(),
                DependentJoinSourceConfig {
                    enabled: Some(false),
                    ..DependentJoinSourceConfig::default()
                },
            )]),
            ..DependentJoinConfig::default()
        }),
        "
        EXPLAIN
        SELECT pr.state AS github_state, mirror.state AS mirror_state
        FROM github.pull_requests AS pr
        JOIN mirror.pull_requests AS mirror
          ON mirror.owner = pr.owner
         AND mirror.repo = pr.repo
         AND mirror.number = pr.number
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(
        explain.contains("DependentJoinExec: table=mirror.pull_requests"),
        "{explain}"
    );
}

#[tokio::test]
async fn dependent_join_source_config_overrides_default_caps() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest("http://127.0.0.1:9")),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            max_bindings: 99,
            max_resolver_rows: 98,
            max_rows_per_binding: 97,
            max_resolver_rows_per_binding: 96,
            max_concurrency: 95,
            per_source: BTreeMap::from([(
                "github".to_string(),
                DependentJoinSourceConfig {
                    max_bindings: Some(7),
                    max_resolver_rows: Some(11),
                    max_rows_per_binding: Some(13),
                    max_resolver_rows_per_binding: Some(17),
                    max_concurrency: Some(19),
                    ..DependentJoinSourceConfig::default()
                },
            )]),
            ..DependentJoinConfig::default()
        }),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec: table=github.pull_requests"));
    assert!(explain.contains("max_bindings=7"), "{explain}");
    assert!(explain.contains("max_resolver_rows=11"), "{explain}");
    assert!(explain.contains("max_rows_per_binding=13"), "{explain}");
    assert!(
        explain.contains("max_resolver_rows_per_binding=17"),
        "{explain}"
    );
    assert!(explain.contains("max_concurrency=19"), "{explain}");
}

#[tokio::test]
async fn dependent_join_source_config_can_enable_one_source_when_default_is_disabled() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest("http://127.0.0.1:9")),
        ],
        runtime_with_dependent_join(DependentJoinConfig {
            enabled: false,
            per_source: BTreeMap::from([(
                "github".to_string(),
                DependentJoinSourceConfig {
                    enabled: Some(true),
                    ..DependentJoinSourceConfig::default()
                },
            )]),
            ..DependentJoinConfig::default()
        }),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ORDER BY i.title
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec: table=github.pull_requests"));
}

#[tokio::test]
async fn live_github_pulls_join_on_number_uses_dependent_per_pr_route() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls"))
        .respond_with(ResponseTemplate::new(500).set_body_string("broad route should not run"))
        .expect(0)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "number": 123,
            "state": "open"
        })))
        .expect(1)
        .mount(&server)
        .await;

    let github = core_http_manifest("sources/core/github/manifest.yaml");

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            QuerySource::new(
                github,
                BTreeMap::from([("GITHUB_API_BASE".to_string(), server.uri())]),
                BTreeMap::from([("GITHUB_TOKEN".to_string(), "test-token".to_string())]),
            ),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pulls AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn dependent_join_falls_back_when_route_does_not_consume_literal_filter() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let mut manifest = github_manifest("http://127.0.0.1:9");
    first_table_object_mut(&mut manifest).insert(
        "request".to_string(),
        json!({
            "method": "GET",
            "path": "/repos/{{filter.owner}}/{{filter.repo}}/pulls",
            "query": [
                { "name": "state", "from": "filter", "key": "state" }
            ]
        }),
    );
    first_table_object_mut(&mut manifest).insert(
        "requests".to_string(),
        json!([{
            "when_filters": ["owner", "repo", "number"],
            "method": "GET",
            "path": "/repos/{{filter.owner}}/{{filter.repo}}/pulls/{{filter.number}}"
        }]),
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(manifest),
        ],
        test_runtime(),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        WHERE pr.state = 'open'
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(!explain.contains("DependentJoinExec"), "{explain}");
}

#[tokio::test]
async fn dependent_join_falls_back_when_route_does_not_consume_binding_filter() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let mut manifest = github_manifest("http://127.0.0.1:9");
    first_table_object_mut(&mut manifest).insert(
        "requests".to_string(),
        json!([{
            "when_filters": ["owner", "repo", "number"],
            "method": "GET",
            "path": "/repos/{{filter.owner}}/pulls/{{filter.number}}"
        }]),
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(manifest),
        ],
        test_runtime(),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(!explain.contains("DependentJoinExec"), "{explain}");
}

#[tokio::test]
async fn dependent_join_accepts_binding_filter_consumed_by_source_header() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let mut manifest = github_manifest("http://127.0.0.1:9");
    manifest
        .as_object_mut()
        .expect("manifest should be an object")
        .insert(
            "request_headers".to_string(),
            json!([{
                "name": "X-Repo",
                "from": "filter",
                "key": "repo"
            }]),
        );
    first_table_object_mut(&mut manifest).insert(
        "requests".to_string(),
        json!([{
            "when_filters": ["owner", "repo", "number"],
            "method": "GET",
            "path": "/repos/{{filter.owner}}/pulls/{{filter.number}}"
        }]),
    );

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(manifest),
        ],
        test_runtime(),
        "
        EXPLAIN
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ",
    )
    .await
    .expect("explain should succeed");

    let explain = execution_text(&execution);
    assert!(explain.contains("DependentJoinExec"), "{explain}");
}

#[tokio::test]
async fn unsupported_join_shape_falls_back_to_regular_join_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[issue_row("First", "withcoral", "coral", 123)],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                { "owner": "withcoral", "repo": "coral", "number": 123, "state": "open" },
                { "owner": "apache", "repo": "arrow-datafusion", "number": 42, "state": "closed" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner != i.github_owner
        ORDER BY pr.state
        ",
    )
    .await
    .expect("unsupported dependent join shape should fall back to normal execution");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "closed" })]
    );
}

#[tokio::test]
async fn literal_and_join_binding_for_same_filter_falls_back_to_regular_join_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            issue_row("First", "withcoral", "coral", 123),
            issue_row("Second", "apache", "arrow-datafusion", 42),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                { "owner": "withcoral", "repo": "coral", "number": 123, "state": "open" },
                { "owner": "apache", "repo": "arrow-datafusion", "number": 42, "state": "closed" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
        WHERE pr.owner = 'withcoral'
        ORDER BY i.title
        ",
    )
    .await
    .expect("over-constrained dependent filter should fall back to normal execution");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn duplicate_join_binding_for_same_filter_falls_back_to_regular_join_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            json!({
                "title": "Same",
                "github_owner": "withcoral",
                "github_org": "withcoral",
                "github_repo": "coral",
                "github_pr_number": 123
            }),
            json!({
                "title": "Different",
                "github_owner": "withcoral",
                "github_org": "apache",
                "github_repo": "coral",
                "github_pr_number": 123
            }),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param_is_missing("owner"))
        .and(query_param_is_missing("repo"))
        .and(query_param_is_missing("number"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                { "owner": "withcoral", "repo": "coral", "number": 123, "state": "open" },
                { "owner": "apache", "repo": "arrow-datafusion", "number": 42, "state": "closed" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_with_org_manifest(temp.path())),
            build_source(github_broad_query_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.owner = i.github_org
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("duplicate binding for one dependent filter should fall back to normal execution");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "Same", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn null_equal_join_falls_back_to_regular_join_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[json!({
            "title": "Null owner",
            "github_owner": null,
            "github_repo": "coral",
            "github_pr_number": 123
        })],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": null,
                "repo": "coral",
                "number": 123,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let sql = "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner IS NOT DISTINCT FROM i.github_owner
        ";

    let explain = execution_text(
        &CoralQuery::execute_sql(
            &[
                build_source(issues_manifest(temp.path())),
                build_source(github_broad_manifest(&server.uri())),
            ],
            test_runtime(),
            &format!("EXPLAIN {sql}"),
        )
        .await
        .expect("explain should succeed"),
    );
    assert!(!explain.contains("DependentJoinExec"), "{explain}");

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_manifest(&server.uri())),
        ],
        test_runtime(),
        sql,
    )
    .await
    .expect("null-equality join should fall back to normal execution");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "Null owner", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn unsupported_resolver_binding_type_falls_back_to_regular_join_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[json!({
            "title": "First",
            "github_owner": "withcoral",
            "github_repo": "coral",
            "github_pr_number": 123.0
        })],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param_is_missing("owner"))
        .and(query_param_is_missing("repo"))
        .and(query_param_is_missing("number"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [
                { "owner": "withcoral", "repo": "coral", "number": 123, "state": "open" },
                { "owner": "apache", "repo": "arrow-datafusion", "number": 42, "state": "closed" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_float_binding_manifest(temp.path())),
            build_source(github_broad_query_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT i.title AS issue_title, pr.state AS pr_state
        FROM issues.items AS i
        JOIN github.pull_requests AS pr
          ON pr.owner = i.github_owner
         AND pr.repo = i.github_repo
         AND pr.number = i.github_pr_number
        ORDER BY i.title
        ",
    )
    .await
    .expect("unsupported resolver binding type should fall back to normal execution");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({ "issue_title": "First", "pr_state": "open" })]
    );
}

#[tokio::test]
async fn required_filter_satisfied_by_join_fetches_dependent_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "channels.jsonl",
        &[
            json!({ "name": "general", "id": "C123456" }),
            json!({ "name": "random", "id": "C999999" }),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .and(query_param("channel", "C123456"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "messages": [
                { "user": "U001", "text": "Hello from general" },
                { "user": "U002", "text": "Second message" }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(slack_channels_manifest(temp.path())),
            build_source(slack_messages_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT c.name AS channel_name, m.user_id, m.text
        FROM slack_channels.channels AS c
        JOIN slack.messages AS m
          ON m.channel = c.id
        WHERE c.name = 'general'
        ORDER BY m.user_id
        ",
    )
    .await
    .expect("join binding should satisfy required dependent filter");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({
                "channel_name": "general",
                "user_id": "U001",
                "text": "Hello from general"
            }),
            json!({
                "channel_name": "general",
                "user_id": "U002",
                "text": "Second message"
            }),
        ]
    );
}

#[tokio::test]
async fn required_filter_not_satisfied_by_join_surfaces_http_required_filter_error() {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "channels.jsonl",
        &[json!({ "name": "general", "id": "C123456" })],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/conversations.history"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "ok": true,
            "messages": []
        })))
        .expect(0)
        .mount(&server)
        .await;

    let error = CoralQuery::execute_sql(
        &[
            build_source(slack_channels_manifest(temp.path())),
            build_source(slack_messages_manifest(&server.uri())),
        ],
        test_runtime(),
        "
        SELECT c.name AS channel_name, m.text
        FROM slack_channels.channels AS c
        JOIN slack.messages AS m
          ON m.user_id = c.id
        WHERE c.name = 'general'
        ",
    )
    .await
    .expect_err("missing required dependent filter should surface HTTP provider error");

    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match &error {
        CoreError::QueryFailure(query_error) => {
            assert_eq!(query_error.reason(), "MISSING_REQUIRED_FILTER");
            assert_eq!(query_error.metadata().get("schema").unwrap(), "slack");
            assert_eq!(query_error.metadata().get("table").unwrap(), "messages");
            assert_eq!(query_error.metadata().get("column").unwrap(), "channel");
        }
        other => panic!("unexpected required-filter error variant: {other:?}"),
    }
}

async fn assert_dependent_join_query(sql: &str) {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "issues.jsonl",
        &[
            json!({
                "title": "First",
                "github_owner": "withcoral",
                "github_repo": "coral",
                "github_pr_number": 123
            }),
            json!({
                "title": "Duplicate tuple",
                "github_owner": "withcoral",
                "github_repo": "coral",
                "github_pr_number": 123
            }),
            json!({
                "title": "Second",
                "github_owner": "apache",
                "github_repo": "arrow-datafusion",
                "github_pr_number": 42
            }),
        ],
    );

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("state", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": [{
                "owner": "withcoral",
                "repo": "coral",
                "number": 123,
                "state": "open"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/apache/arrow-datafusion/pulls/42"))
        .and(query_param("state", "open"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": []
        })))
        .expect(1)
        .mount(&server)
        .await;

    let sources = vec![
        build_source(issues_manifest(temp.path())),
        build_source(github_manifest(&server.uri())),
    ];

    let execution = CoralQuery::execute_sql(&sources, test_runtime(), sql)
        .await
        .expect("query should succeed");

    assert_eq!(
        execution_to_rows(&execution),
        vec![
            json!({ "issue_title": "Duplicate tuple", "pr_state": "open" }),
            json!({ "issue_title": "First", "pr_state": "open" }),
        ]
    );
}

#[derive(Debug, Clone, Copy)]
enum RowComparison {
    Exact,
    Unordered,
}

async fn assert_dpp_and_naive_rows_agree(sql: &str, comparison: RowComparison) {
    let dpp_rows = execute_differential_join(sql, true).await;
    let naive_rows = execute_differential_join(sql, false).await;

    match comparison {
        RowComparison::Exact => assert_eq!(dpp_rows, naive_rows),
        RowComparison::Unordered => {
            assert_eq!(sort_rows(dpp_rows), sort_rows(naive_rows));
        }
    }
}

async fn execute_differential_join(sql: &str, lookup_key_enabled: bool) -> Vec<Value> {
    let temp = TempDir::new().expect("temp dir");
    write_jsonl_file(temp.path(), "issues.jsonl", &differential_issue_rows());

    let server = MockServer::start().await;
    mount_differential_github_mocks(&server, lookup_key_enabled).await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_broad_query_manifest_with_lookup_key(
                &server.uri(),
                lookup_key_enabled,
            )),
        ],
        test_runtime(),
        sql,
    )
    .await
    .expect("query should succeed");

    execution_to_rows(&execution)
}

async fn mount_differential_github_mocks(server: &MockServer, lookup_key_enabled: bool) {
    let pull_rows = differential_pull_rows();
    if lookup_key_enabled {
        mount_filtered_pull_response(server, &pull_rows, "withcoral", "coral", 123).await;
        mount_filtered_pull_response(server, &pull_rows, "apache", "arrow-datafusion", 42).await;
        mount_filtered_pull_response(server, &pull_rows, "ghost", "missing", 404).await;
    } else {
        Mock::given(method("GET"))
            .and(path("/pulls"))
            .and(query_param_is_missing("owner"))
            .and(query_param_is_missing("repo"))
            .and(query_param_is_missing("number"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": pull_rows })))
            .expect(1)
            .mount(server)
            .await;
    }
}

async fn mount_filtered_pull_response(
    server: &MockServer,
    pull_rows: &[Value],
    owner: &str,
    repo: &str,
    number: i64,
) {
    let rows = pull_rows
        .iter()
        .filter(|row| {
            row.get("owner").and_then(Value::as_str) == Some(owner)
                && row.get("repo").and_then(Value::as_str) == Some(repo)
                && row.get("number").and_then(Value::as_i64) == Some(number)
        })
        .cloned()
        .collect::<Vec<_>>();

    Mock::given(method("GET"))
        .and(path("/pulls"))
        .and(query_param("owner", owner))
        .and(query_param("repo", repo))
        .and(query_param("number", number.to_string()))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": rows })))
        .expect(1)
        .mount(server)
        .await;
}

fn differential_issue_rows() -> Vec<Value> {
    vec![
        issue_row("First", "withcoral", "coral", 123),
        issue_row("Duplicate tuple", "withcoral", "coral", 123),
        issue_row("Second", "apache", "arrow-datafusion", 42),
        issue_row("No match", "ghost", "missing", 404),
        json!({
            "title": "Null owner",
            "github_owner": null,
            "github_repo": "coral",
            "github_pr_number": 123
        }),
    ]
}

fn differential_pull_rows() -> Vec<Value> {
    vec![
        json!({
            "owner": "withcoral",
            "repo": "coral",
            "number": 123,
            "state": "open"
        }),
        json!({
            "owner": "withcoral",
            "repo": "coral",
            "number": 123,
            "state": "review"
        }),
        json!({
            "owner": "apache",
            "repo": "arrow-datafusion",
            "number": 42,
            "state": "merged"
        }),
        json!({
            "owner": null,
            "repo": "coral",
            "number": 123,
            "state": "null-owner"
        }),
        json!({
            "owner": "orphan",
            "repo": "unused",
            "number": 7,
            "state": "orphan"
        }),
    ]
}

fn sort_rows(mut rows: Vec<Value>) -> Vec<Value> {
    rows.sort_by_key(std::string::ToString::to_string);
    rows
}

fn dependent_join_sql() -> &'static str {
    "
    SELECT i.title AS issue_title, pr.state AS pr_state
    FROM issues.items AS i
    JOIN github.pull_requests AS pr
      ON pr.owner = i.github_owner
     AND pr.repo = i.github_repo
     AND pr.number = i.github_pr_number
    ORDER BY i.title
    "
}

fn runtime_with_dependent_join(config: DependentJoinConfig) -> QueryRuntimeConfig {
    let mut runtime = test_runtime();
    runtime.dependent_join = config;
    runtime
}

fn core_http_manifest(relative_path: &str) -> ValidatedSourceManifest {
    let raw = core_manifest_raw(relative_path);
    parse_source_manifest_yaml(&raw).expect("core source manifest should parse")
}

fn core_manifest_raw(relative_path: &str) -> String {
    let repo_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    std::fs::read_to_string(repo_root.join(relative_path))
        .expect("core source manifest should be readable")
}

fn issue_row(title: &str, owner: &str, repo: &str, number: i64) -> Value {
    json!({
        "title": title,
        "github_owner": owner,
        "github_repo": repo,
        "github_pr_number": number
    })
}

fn assert_error_contains(error: &CoreError, expected: &str) {
    let rendered = error.to_string();
    assert!(
        rendered.contains(expected),
        "expected error to contain {expected:?}, got {rendered:?}"
    );
}

fn assert_dependent_join_limit_error(
    error: &CoreError,
    expected_reason: &str,
    expected_observed: &str,
    expected_limit: &str,
) {
    let CoreError::QueryFailure(query_error) = error else {
        panic!("expected structured query failure, got {error:?}");
    };

    assert_eq!(query_error.reason(), expected_reason);
    assert_eq!(
        query_error.metadata().get("source").map(String::as_str),
        Some("github")
    );
    assert_eq!(
        query_error.metadata().get("table").map(String::as_str),
        Some("pull_requests")
    );
    assert_eq!(
        query_error.metadata().get("observed").map(String::as_str),
        Some(expected_observed)
    );
    assert_eq!(
        query_error.metadata().get("limit").map(String::as_str),
        Some(expected_limit)
    );
}

fn execution_text(execution: &coral_engine::QueryExecution) -> String {
    execution_to_rows(execution)
        .into_iter()
        .map(|row| {
            row.get("plan")
                .and_then(Value::as_str)
                .map_or_else(|| row.to_string(), ToString::to_string)
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn issues_manifest(dir: &Path) -> Value {
    json!({
        "name": "issues",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "items",
            "description": "Issue fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "title", "type": "Utf8" },
                { "name": "github_owner", "type": "Utf8" },
                { "name": "github_repo", "type": "Utf8" },
                { "name": "github_pr_number", "type": "Int64" }
            ]
        }]
    })
}

fn issues_float_binding_manifest(dir: &Path) -> Value {
    json!({
        "name": "issues",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "items",
            "description": "Issue fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "title", "type": "Utf8" },
                { "name": "github_owner", "type": "Utf8" },
                { "name": "github_repo", "type": "Utf8" },
                { "name": "github_pr_number", "type": "Float64" }
            ]
        }]
    })
}

fn issues_with_org_manifest(dir: &Path) -> Value {
    json!({
        "name": "issues",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "items",
            "description": "Issue fixture with an extra owner-like column",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "title", "type": "Utf8" },
                { "name": "github_owner", "type": "Utf8" },
                { "name": "github_org", "type": "Utf8" },
                { "name": "github_repo", "type": "Utf8" },
                { "name": "github_pr_number", "type": "Int64" }
            ]
        }]
    })
}

fn slack_channels_manifest(dir: &Path) -> Value {
    json!({
        "name": "slack_channels",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "channels",
            "description": "Slack channel fixture",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "name", "type": "Utf8" },
                { "name": "id", "type": "Utf8" }
            ]
        }]
    })
}

fn resolver_ids_manifest(dir: &Path) -> Value {
    json!({
        "name": "resolver_ids",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "items",
            "description": "Resolver table with an id column that can collide with joined resolver columns",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/resolver_ids.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Utf8" },
                { "name": "channel_name", "type": "Utf8" }
            ]
        }]
    })
}

fn slack_channels_http_manifest(base_url: &str) -> Value {
    json!({
        "name": "slack_channels",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "channels",
            "description": "Slack channel fixture",
            "request": {
                "method": "GET",
                "path": "/api/channels"
            },
            "response": {
                "rows_path": ["channels"]
            },
            "columns": [
                { "name": "name", "type": "Utf8" },
                { "name": "id", "type": "Utf8" }
            ]
        }]
    })
}

fn slack_messages_manifest(base_url: &str) -> Value {
    json!({
        "name": "slack",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "messages",
            "description": "Slack messages",
            "filters": [
                { "name": "channel", "required": true, "lookup_key": true }
            ],
            "request": {
                "method": "GET",
                "path": "/api/conversations.history",
                "query": [
                    { "name": "channel", "from": "filter", "key": "channel" }
                ]
            },
            "response": {
                "rows_path": ["messages"]
            },
            "columns": [
                {
                    "name": "channel",
                    "type": "Utf8",
                    "expr": { "kind": "from_filter", "key": "channel" }
                },
                {
                    "name": "user_id",
                    "type": "Utf8",
                    "expr": { "kind": "path", "path": ["user"] }
                },
                {
                    "name": "text",
                    "type": "Utf8",
                    "expr": { "kind": "path", "path": ["text"] }
                }
            ]
        }]
    })
}

fn github_manifest(base_url: &str) -> Value {
    github_manifest_with_filters(
        base_url,
        None,
        vec![
            json!({ "name": "owner", "lookup_key": true }),
            json!({ "name": "repo", "lookup_key": true }),
            json!({ "name": "number", "lookup_key": true }),
            json!({ "name": "state" }),
        ],
    )
}

fn github_required_manifest(base_url: &str) -> Value {
    github_manifest_with_filters(
        base_url,
        None,
        vec![
            json!({ "name": "owner", "required": true, "lookup_key": true }),
            json!({ "name": "repo", "required": true, "lookup_key": true }),
            json!({ "name": "number", "required": true, "lookup_key": true }),
            json!({ "name": "state" }),
        ],
    )
}

fn github_filter_backed_columns_manifest(base_url: &str) -> Value {
    let mut manifest = github_required_manifest(base_url);
    first_table_object_mut(&mut manifest).insert(
        "columns".to_string(),
        json!([
            {
                "name": "owner",
                "type": "Utf8",
                "expr": { "kind": "from_filter", "key": "owner" }
            },
            {
                "name": "repo",
                "type": "Utf8",
                "expr": { "kind": "from_filter", "key": "repo" }
            },
            { "name": "number", "type": "Int64" },
            {
                "name": "state",
                "type": "Utf8",
                "expr": { "kind": "from_filter", "key": "state" }
            }
        ]),
    );
    manifest
}

fn github_manifest_with_fetch_limit_default(base_url: &str, fetch_limit_default: usize) -> Value {
    let mut manifest = github_manifest(base_url);
    first_table_object_mut(&mut manifest).insert(
        "fetch_limit_default".to_string(),
        json!(fetch_limit_default),
    );
    manifest
}

fn github_paginated_manifest(base_url: &str) -> Value {
    let mut manifest = github_manifest(base_url);
    first_table_object_mut(&mut manifest).insert(
        "pagination".to_string(),
        json!({
            "mode": "page",
            "page_param": "page",
            "page_start": 1,
            "page_size": {
                "default": 100,
                "max": 100,
                "query_param": "per_page"
            }
        }),
    );
    manifest
}

fn first_table_object_mut(manifest: &mut Value) -> &mut serde_json::Map<String, Value> {
    manifest
        .get_mut("tables")
        .and_then(Value::as_array_mut)
        .and_then(|tables| tables.first_mut())
        .and_then(Value::as_object_mut)
        .expect("test manifest should contain one table object")
}

async fn spawn_concurrency_tracking_github_server(
    delay: Duration,
    active: Arc<AtomicUsize>,
    max_active: Arc<AtomicUsize>,
) -> (String, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind concurrency tracking github server");
    let addr = listener.local_addr().expect("server local addr");

    let task = tokio::spawn(async move {
        loop {
            let Ok((mut socket, _)) = listener.accept().await else {
                break;
            };
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);

            tokio::spawn(async move {
                let mut buffer = vec![0_u8; 4096];
                let Ok(read) = socket.read(&mut buffer).await else {
                    return;
                };
                let request_bytes = buffer
                    .get(..read)
                    .expect("read byte count should fit inside request buffer");
                let request = String::from_utf8_lossy(request_bytes);
                let request_path = request
                    .lines()
                    .next()
                    .and_then(|line| line.split_whitespace().nth(1))
                    .unwrap_or_default();
                let path_without_query = request_path.split('?').next().unwrap_or(request_path);
                let number = path_without_query
                    .strip_prefix("/repos/withcoral/coral/pulls/")
                    .and_then(|segment| segment.parse::<i64>().ok());

                let matched = matches!(number, Some(1..=5));
                let body = if let Some(number @ 1..=5) = number {
                    let in_flight = active.fetch_add(1, Ordering::AcqRel) + 1;
                    max_active.fetch_max(in_flight, Ordering::AcqRel);
                    tokio::time::sleep(delay).await;
                    active.fetch_sub(1, Ordering::AcqRel);

                    json!({
                        "data": [{
                            "owner": "withcoral",
                            "repo": "coral",
                            "number": number,
                            "state": "open"
                        }]
                    })
                    .to_string()
                } else {
                    json!({ "error": "not found" }).to_string()
                };
                let status = if matched { "200 OK" } else { "404 Not Found" };
                let response = format!(
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                );
                match socket.write_all(response.as_bytes()).await {
                    Ok(()) | Err(_) => {}
                }
            });
        }
    });

    (format!("http://{addr}"), task)
}

fn github_manifest_with_filters(
    base_url: &str,
    rate_limit: Option<Value>,
    filters: Vec<Value>,
) -> Value {
    let filters = Value::Array(filters);

    json!({
        "name": "github",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "rate_limit": rate_limit.unwrap_or_else(|| json!({})),
        "tables": [{
            "name": "pull_requests",
            "description": "Pull requests",
            "filters": filters,
            "request": {
                "method": "GET",
                "path": "/repos/{{filter.owner}}/{{filter.repo}}/pulls/{{filter.number}}",
                "query": [
                    { "name": "state", "from": "filter", "key": "state" }
                ]
            },
            "response": {
                "rows_path": ["data"]
            },
            "columns": [
                { "name": "owner", "type": "Utf8" },
                { "name": "repo", "type": "Utf8" },
                { "name": "number", "type": "Int64" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    })
}

fn github_broad_manifest(base_url: &str) -> Value {
    json!({
        "name": "github",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "pull_requests",
            "description": "Pull requests",
            "filters": [
                { "name": "owner", "lookup_key": true },
                { "name": "repo", "lookup_key": true },
                { "name": "number", "lookup_key": true }
            ],
            "request": {
                "method": "GET",
                "path": "/pulls"
            },
            "response": {
                "rows_path": ["data"]
            },
            "columns": [
                { "name": "owner", "type": "Utf8" },
                { "name": "repo", "type": "Utf8" },
                { "name": "number", "type": "Int64" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    })
}

fn github_broad_query_manifest(base_url: &str) -> Value {
    github_broad_query_manifest_with_lookup_key(base_url, true)
}

fn source_named(mut manifest: Value, name: &str) -> Value {
    manifest
        .as_object_mut()
        .expect("test manifest should be an object")
        .insert("name".to_string(), json!(name));
    manifest
}

fn github_broad_query_manifest_with_lookup_key(base_url: &str, lookup_key_enabled: bool) -> Value {
    json!({
        "name": "github",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "tables": [{
            "name": "pull_requests",
            "description": "Pull requests",
            "filters": [
                { "name": "owner", "lookup_key": lookup_key_enabled },
                { "name": "repo", "lookup_key": lookup_key_enabled },
                { "name": "number", "lookup_key": lookup_key_enabled }
            ],
            "request": {
                "method": "GET",
                "path": "/pulls",
                "query": [
                    { "name": "owner", "from": "filter", "key": "owner" },
                    { "name": "repo", "from": "filter", "key": "repo" },
                    { "name": "number", "from": "filter", "key": "number" }
                ]
            },
            "response": {
                "rows_path": ["data"]
            },
            "columns": [
                { "name": "owner", "type": "Utf8" },
                { "name": "repo", "type": "Utf8" },
                { "name": "number", "type": "Int64" },
                { "name": "state", "type": "Utf8" }
            ]
        }]
    })
}
