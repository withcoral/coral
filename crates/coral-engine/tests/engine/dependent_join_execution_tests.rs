use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use coral_engine::{CoralQuery, CoreError, StatusCode};
use serde_json::{Value, json};
use tempfile::TempDir;
use wiremock::matchers::{method, path, path_regex, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

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
async fn duplicate_resolver_rows_for_one_binding_emit_one_joined_batch() {
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
        "dependent join into 'github.pull_requests' produced 501 binding tuples, which exceeds cap 500",
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
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
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
        "dependent join resolver for 'github.pull_requests' produced 1001 rows for one binding, which exceeds max_resolver_rows_per_binding=1000",
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
        "dependent join fetch for 'github.pull_requests' returned 1001 rows for one binding, which exceeds max_rows_per_binding=1000",
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
        "dependent join fetch for 'github.pull_requests' returned 1001 rows for one binding, which exceeds max_rows_per_binding=1000",
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
        test_runtime(),
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
async fn parent_limit_is_page_hint_not_dependent_row_limit() {
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
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path("/repos/withcoral/coral/pulls/123"))
        .and(query_param("page", "3"))
        .and(query_param("per_page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(1)
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
async fn dependent_fetches_honor_source_max_concurrency() {
    let temp = TempDir::new().expect("temp dir");
    let issues = (1..=5)
        .map(|number| issue_row(&format!("Issue {number}"), "withcoral", "coral", number))
        .collect::<Vec<_>>();
    write_jsonl_file(temp.path(), "issues.jsonl", &issues);

    let active = Arc::new(AtomicUsize::new(0));
    let max_active = Arc::new(AtomicUsize::new(0));
    let delay = Duration::from_millis(250);

    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path_regex(r"^/repos/withcoral/coral/pulls/[1-5]$"))
        .respond_with({
            let active = Arc::clone(&active);
            let max_active = Arc::clone(&max_active);
            move |request: &Request| {
                let in_flight = active.fetch_add(1, Ordering::AcqRel) + 1;
                max_active.fetch_max(in_flight, Ordering::AcqRel);

                let active = Arc::clone(&active);
                tokio::spawn(async move {
                    tokio::time::sleep(delay).await;
                    active.fetch_sub(1, Ordering::AcqRel);
                });

                let number = request
                    .url
                    .path_segments()
                    .and_then(Iterator::last)
                    .and_then(|segment| segment.parse::<i64>().ok())
                    .expect("mocked pull request path should end with a number");

                ResponseTemplate::new(200)
                    .set_delay(delay)
                    .set_body_json(json!({
                        "data": [{
                            "owner": "withcoral",
                            "repo": "coral",
                            "number": number,
                            "state": "open"
                        }]
                    }))
            }
        })
        .expect(5)
        .mount(&server)
        .await;

    let execution = CoralQuery::execute_sql(
        &[
            build_source(issues_manifest(temp.path())),
            build_source(github_manifest_with_max_concurrency(&server.uri(), 2)),
        ],
        test_runtime(),
        dependent_join_sql(),
    )
    .await
    .expect("dependent join query should succeed");

    assert_eq!(execution_to_rows(&execution).len(), 5);
    assert_eq!(max_active.load(Ordering::Acquire), 2);
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
    assert!(explain.contains("resolver_rows=3"), "{explain}");
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
        "backend": "jsonl",
        "tables": [{
            "name": "items",
            "description": "Issue fixture",
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
        "backend": "jsonl",
        "tables": [{
            "name": "items",
            "description": "Issue fixture",
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
        "backend": "jsonl",
        "tables": [{
            "name": "items",
            "description": "Issue fixture with an extra owner-like column",
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
        "backend": "jsonl",
        "tables": [{
            "name": "channels",
            "description": "Slack channel fixture",
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
        "backend": "jsonl",
        "tables": [{
            "name": "items",
            "description": "Resolver table with an id column that can collide with joined resolver columns",
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
                { "name": "channel", "required": true, "bindable": true }
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
            json!({ "name": "owner", "bindable": true }),
            json!({ "name": "repo", "bindable": true }),
            json!({ "name": "number", "bindable": true }),
            json!({ "name": "state" }),
        ],
    )
}

fn github_required_manifest(base_url: &str) -> Value {
    github_manifest_with_filters(
        base_url,
        None,
        vec![
            json!({ "name": "owner", "required": true, "bindable": true }),
            json!({ "name": "repo", "required": true, "bindable": true }),
            json!({ "name": "number", "required": true, "bindable": true }),
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

fn github_manifest_with_max_concurrency(base_url: &str, max_concurrency: usize) -> Value {
    github_manifest_with_filters(
        base_url,
        Some(json!({ "max_concurrency": max_concurrency })),
        vec![
            json!({ "name": "owner", "bindable": true }),
            json!({ "name": "repo", "bindable": true }),
            json!({ "name": "number", "bindable": true }),
            json!({ "name": "state" }),
        ],
    )
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
                { "name": "owner", "bindable": true },
                { "name": "repo", "bindable": true },
                { "name": "number", "bindable": true }
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
                { "name": "owner", "bindable": true },
                { "name": "repo", "bindable": true },
                { "name": "number", "bindable": true }
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
