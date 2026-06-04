#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use coral_engine::{
    CoralQuery, CoreError, EngineExtensions, QueryRuntimeConfig, QueryRuntimeContext, QuerySource,
    RequestAuthenticator, RequestAuthenticatorError, StatusCode, StructuredQueryError,
};
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde::Serialize;
use serde_json::{Value, json};
use wiremock::matchers::{
    body_json, body_string, header, method, path, query_param, query_param_is_missing,
};
use wiremock::{Mock, MockServer, Request, ResponseTemplate};

use crate::harness::{
    assert_source_rows, build_source, build_source_with_secrets, execution_to_rows, source_error,
    source_rows, source_rows_with_runtime, test_runtime, users_rows,
};

const SEARCH_QUERY: &str = "flaky cleanup repo:withcoral/coral";

fn http_relation_manifest(
    name: &str,
    base_url: &str,
    relation_key: &str,
    relation: &Value,
) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        relation_key: [relation]
    })
}

fn http_table_manifest(name: &str, base_url: &str, table: &Value) -> Value {
    http_relation_manifest(name, base_url, "tables", table)
}

fn http_function_manifest(name: &str, base_url: &str, function: &Value) -> Value {
    http_relation_manifest(name, base_url, "functions", function)
}

fn column(name: &str, data_type: &str) -> Value {
    json!({ "name": name, "type": data_type })
}

fn users_columns() -> Vec<Value> {
    vec![
        column("id", "Int64"),
        column("name", "Utf8"),
        column("email", "Utf8"),
    ]
}

fn data_response(rows: impl Serialize) -> ResponseTemplate {
    ResponseTemplate::new(200).set_body_json(json!({ "data": rows }))
}

fn grace_data_response() -> ResponseTemplate {
    data_response([json!({"id": 2, "name": "Grace", "email": "grace@example.com"})])
}

struct TextBodyCase {
    name: &'static str,
    source_name: &'static str,
    path: &'static str,
    expected_content_type: Option<&'static str>,
    expected_body: &'static str,
    table: Value,
    response: ResponseTemplate,
    sql: &'static str,
    expected_rows: Vec<Value>,
}

fn base_http_manifest(name: &str, base_url: &str) -> Value {
    http_table_manifest(
        name,
        base_url,
        &json!({
            "name": "users",
            "description": "HTTP users",
            "request": {
                "method": "GET",
                "path": "/api/users"
            },
            "response": {
                "rows_path": ["data"]
            },
            "columns": users_columns()
        }),
    )
}

fn search_function_manifest(name: &str, base_url: &str) -> Value {
    http_function_manifest(
        name,
        base_url,
        &json!({
            "name": "search_issues",
            "kind": "search",
            "description": "Search issues",
            "search_limits": {
                "default_top_k": 10,
                "max_top_k": 100,
                "max_calls_per_query": 1
            },
            "args": [
                {
                    "name": "q",
                    "required": true,
                    "bind": { "arg": "q" }
                },
                {
                    "name": "mode",
                    "values": ["lexical", "semantic", "hybrid"],
                    "bind": { "arg": "search_type" }
                }
            ],
            "request": {
                "method": "GET",
                "path": "/api/search/issues",
                "query": [
                    { "name": "q", "from": "arg", "key": "q" },
                    { "name": "search_type", "from": "arg", "key": "search_type" }
                ]
            },
            "response": {
                "rows_path": ["items"]
            },
            "columns": [
                column("title", "Utf8"),
                column("score", "Float64")
            ]
        }),
    )
}

fn split_function_manifest(name: &str, base_url: &str) -> Value {
    http_function_manifest(
        name,
        base_url,
        &json!({
            "name": "issue_comments",
            "description": "Issue comments",
            "args": [{
                "name": "issue",
                "required": true,
                "bind": { "arg": "issue" }
            }],
            "request": {
                "method": "POST",
                "path": "/graphql",
                "body": [
                    {
                        "path": ["variables", "teamKey"],
                        "from": "arg_split",
                        "key": "issue",
                        "separator": "-",
                        "part": 0
                    },
                    {
                        "path": ["variables", "issueNumber"],
                        "from": "arg_split_int",
                        "key": "issue",
                        "separator": "-",
                        "part": 1
                    }
                ]
            },
            "response": {
                "rows_path": ["data", "comments"]
            },
            "columns": [column("body", "Utf8")]
        }),
    )
}

fn notionish_search_function_manifest(base_url: &str) -> Value {
    http_function_manifest(
        "notionish",
        base_url,
        &json!({
            "name": "search_objects",
            "kind": "search",
            "description": "Search objects",
            "search_limits": {
                "default_top_k": 10,
                "max_top_k": 100,
                "max_calls_per_query": 1
            },
            "args": [
                {
                    "name": "query",
                    "required": true,
                    "bind": { "arg": "query" }
                },
                {
                    "name": "object",
                    "values": ["page", "data_source"],
                    "bind": { "arg": "object" }
                }
            ],
            "request": {
                "method": "POST",
                "path": "/v1/search",
                "body": [
                    { "path": ["query"], "from": "arg", "key": "query" },
                    {
                        "path": ["filter", "property"],
                        "when_arg": "object",
                        "from": "literal",
                        "value": "object"
                    },
                    { "path": ["filter", "value"], "from": "arg", "key": "object" }
                ]
            },
            "response": {
                "rows_path": ["results"]
            },
            "columns": [
                { "name": "object", "type": "Utf8" },
                { "name": "id", "type": "Utf8" },
                {
                    "name": "requested_object",
                    "type": "Utf8",
                    "expr": { "kind": "from_arg", "key": "object" }
                }
            ]
        }),
    )
}

#[derive(Debug)]
struct TestRequestAuthenticator;

impl RequestAuthenticator for TestRequestAuthenticator {
    fn name(&self) -> &'static str {
        "test_signer"
    }

    fn authenticate(
        &self,
        auth: &coral_spec::CustomAuthSpec,
        request: &reqwest::Request,
        resolved_inputs: &BTreeMap<String, String>,
    ) -> Result<Vec<(HeaderName, HeaderValue)>, RequestAuthenticatorError> {
        let prefix = auth
            .config
            .get("prefix")
            .and_then(Value::as_str)
            .ok_or_else(|| RequestAuthenticatorError::invalid_input("missing auth prefix"))?;
        let token = resolved_inputs
            .get("API_TOKEN")
            .ok_or_else(|| RequestAuthenticatorError::failed_precondition("missing API_TOKEN"))?;
        Ok(vec![
            (
                AUTHORIZATION,
                HeaderValue::from_str(&format!("{prefix} {token}")).map_err(|error| {
                    RequestAuthenticatorError::failed_precondition(error.to_string())
                })?,
            ),
            (
                HeaderName::from_static("x-signed-path"),
                HeaderValue::from_str(request.url().path()).map_err(|error| {
                    RequestAuthenticatorError::failed_precondition(error.to_string())
                })?,
            ),
        ])
    }
}

fn test_auth_runtime() -> QueryRuntimeConfig {
    let mut extensions = EngineExtensions::default();
    extensions.request_authenticators.insert(
        "test_signer".to_string(),
        Arc::new(TestRequestAuthenticator),
    );
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions)
}

async fn http_users_source(name: &str) -> (MockServer, QuerySource) {
    let server = MockServer::start().await;
    mount_users_response(&server).await;
    let source = build_source(base_http_manifest(name, &server.uri()));
    (server, source)
}

async fn mount_users_response(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(data_response(users_rows()))
        .mount(server)
        .await;
}

async fn mount_users_query_response(
    server: &MockServer,
    query_name: &str,
    query_value: &str,
    rows: &[Value],
) {
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param(query_name, query_value))
        .respond_with(data_response(rows))
        .mount(server)
        .await;
}

async fn assert_users_pagination(
    source_name: &str,
    query_name: &str,
    requests: &[&str],
    pagination: Value,
) {
    let server = MockServer::start().await;
    let rows = users_rows();
    for (index, value) in requests.iter().enumerate() {
        let start = index * 2;
        let end = usize::min(start + 2, rows.len());
        let page_rows = if start < rows.len() {
            &rows[start..end]
        } else {
            &[]
        };
        mount_users_query_response(&server, query_name, value, page_rows).await;
    }

    let mut manifest = base_http_manifest(source_name, &server.uri());
    manifest["tables"][0]["pagination"] = pagination;
    let source = build_source(manifest);

    assert_source_rows(
        &source,
        &format!("SELECT id, name, email FROM {source_name}.users ORDER BY id"),
        users_rows(),
    )
    .await;
}

async fn mount_search_issues_response(server: &MockServer, search_type: Option<&str>) {
    let mock = Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", SEARCH_QUERY));
    let mock = if let Some(search_type) = search_type {
        mock.and(query_param("search_type", search_type))
    } else {
        mock.and(query_param_is_missing("search_type"))
    };
    mock.respond_with(ResponseTemplate::new(200).set_body_json(search_issues_response()))
        .expect(1)
        .mount(server)
        .await;
}

fn search_issues_response() -> Value {
    json!({ "items": search_issue_rows() })
}

fn search_issue_rows() -> Vec<Value> {
    vec![json!({
        "title": "Flaky workspace cleanup",
        "score": 9.5
    })]
}

fn assert_query_failure<'a>(
    error: &'a CoreError,
    status: StatusCode,
    reason: &str,
    retryable: bool,
) -> &'a StructuredQueryError {
    assert_eq!(error.status_code(), status);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), reason);
            assert_eq!(sqe.retryable(), retryable);
            sqe.as_ref()
        }
        other => panic!("unexpected query error variant: {other:?}"),
    }
}

fn assert_query_metadata(sqe: &StructuredQueryError, key: &str, expected: &str) {
    assert_eq!(sqe.metadata().get(key).map(String::as_str), Some(expected));
}

fn assert_error_contains(error: &CoreError, expected: &str) {
    assert!(
        error.to_string().contains(expected),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn basic_http_query_shapes() {
    for (case, schema, sql, expected) in [
        (
            "all",
            "http_all",
            "SELECT id, name, email FROM http_all.users ORDER BY id",
            users_rows(),
        ),
        (
            "projection",
            "http_projection",
            "SELECT name, email FROM http_projection.users ORDER BY name",
            vec![
                json!({"name": "Ada", "email": "ada@example.com"}),
                json!({"name": "Grace", "email": "grace@example.com"}),
                json!({"name": "Linus", "email": "linus@example.com"}),
            ],
        ),
        (
            "order",
            "http_order",
            "SELECT name FROM http_order.users ORDER BY name DESC",
            vec![
                json!({"name": "Linus"}),
                json!({"name": "Grace"}),
                json!({"name": "Ada"}),
            ],
        ),
        (
            "limit",
            "http_limit",
            "SELECT id FROM http_limit.users LIMIT 2",
            vec![json!({"id": 1}), json!({"id": 2})],
        ),
        (
            "count",
            "http_count",
            "SELECT COUNT(*) AS n FROM http_count.users",
            vec![json!({"n": 3})],
        ),
    ] {
        let (_server, source) = http_users_source(schema).await;

        assert_eq!(source_rows(&source, sql).await, expected, "{case}");
    }
}

#[tokio::test]
async fn select_with_where_filter_pushdown() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(query_param("id", "2"))
        .respond_with(grace_data_response())
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_filter", &server.uri());
    let table = &mut manifest["tables"][0];
    table["filters"] = json!([{ "name": "id" }]);
    table["request"]["query"] = json!([
        { "name": "id", "from": "filter", "key": "id" }
    ]);
    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT id, name FROM http_filter.users WHERE id = 2",
        vec![json!({"id": 2, "name": "Grace"})],
    )
    .await;
}

#[tokio::test]
async fn validate_source_accepts_function_only_http_source_and_runs_queries() {
    let server = MockServer::start().await;
    mount_search_issues_response(&server, None).await;

    let source = build_source(search_function_manifest("search", &server.uri()));
    let queries = vec![
        "SELECT title, score \
         FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral')"
            .to_string(),
    ];

    let report = CoralQuery::validate_source(&source, test_runtime(), &queries)
        .await
        .expect("function-only source should validate");

    assert!(report.tables.is_empty());
    assert_eq!(report.table_functions.len(), 1);
    assert_eq!(report.table_functions[0].schema_name, "search");
    assert_eq!(report.table_functions[0].function_name, "search_issues");
    assert_eq!(report.query_tests.len(), 1);
    assert!(report.query_tests[0].passed());
    assert_eq!(report.query_tests[0].row_count(), Some(1));
}

#[tokio::test]
async fn source_scoped_table_function_splits_argument_values() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .and(body_json(json!({
            "variables": {
                "teamKey": "SOURCE",
                "issueNumber": 496
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": {
                "comments": [{
                    "body": "Looks good"
                }]
            }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(split_function_manifest("linearish", &server.uri()));
    assert_source_rows(
        &source,
        "SELECT body FROM linearish.issue_comments(issue => 'SOURCE-496')",
        vec![json!({
            "body": "Looks good"
        })],
    )
    .await;
}

#[tokio::test]
async fn source_scoped_table_function_preserves_quoted_manifest_identifiers() {
    let server = MockServer::start().await;
    mount_search_issues_response(&server, Some("hybrid")).await;

    let mut manifest = search_function_manifest("Search", &server.uri());
    manifest["functions"][0]["name"] = json!("Search_Issues");
    manifest["functions"][0]["args"][0]["name"] = json!("Q");
    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT title, score \
         FROM \"Search\".\"Search_Issues\"(\"Q\" => 'flaky cleanup repo:withcoral/coral', mode => 'hybrid')",
        search_issue_rows(),
    )
    .await;
}

#[tokio::test]
async fn source_scoped_table_function_conditionally_emits_arg_body_fields() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/v1/search"))
        .and(body_json(json!({ "query": "Coral" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{
                "object": "page",
                "id": "page_1"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;
    Mock::given(method("POST"))
        .and(path("/v1/search"))
        .and(body_json(json!({
            "query": "Coral",
            "filter": {
                "property": "object",
                "value": "data_source"
            }
        })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "results": [{
                "object": "data_source",
                "id": "data_source_1"
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(notionish_search_function_manifest(&server.uri()));

    assert_source_rows(
        &source,
        "SELECT object, id, requested_object \
         FROM notionish.search_objects(query => 'Coral')",
        vec![json!({
            "object": "page",
            "id": "page_1"
        })],
    )
    .await;

    assert_source_rows(
        &source,
        "SELECT object, id, requested_object \
         FROM notionish.search_objects(query => 'Coral', object => 'data_source')",
        vec![json!({
            "object": "data_source",
            "id": "data_source_1",
            "requested_object": "data_source"
        })],
    )
    .await;
}

#[tokio::test]
async fn search_function_limit_is_capped_by_search_limits() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky"))
        .and(query_param("limit", "2"))
        .and(query_param("offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [
                { "title": "First", "score": 3.0 },
                { "title": "Second", "score": 2.0 },
                { "title": "Third", "score": 1.0 }
            ]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = search_function_manifest("capped_search", &server.uri());
    manifest["functions"][0]["search_limits"] = json!({
        "default_top_k": 1,
        "max_top_k": 2,
        "max_calls_per_query": 1
    });
    manifest["functions"][0]["pagination"] = json!({
        "mode": "offset",
        "offset_param": "offset",
        "page_size": {
            "default": 50,
            "max": 500,
            "query_param": "limit"
        }
    });

    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT title, score FROM capped_search.search_issues(q => 'flaky') LIMIT 3",
        vec![
            json!({ "title": "First", "score": 3.0 }),
            json!({ "title": "Second", "score": 2.0 }),
        ],
    )
    .await;
}

#[tokio::test]
async fn source_scoped_search_function_enforces_search_limits() {
    let server = MockServer::start().await;
    let items: Vec<Value> = (0..100)
        .map(|index| {
            json!({
                "title": format!("Issue {index}"),
                "score": f64::from(index)
            })
        })
        .collect();

    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param("search_type", "hybrid"))
        .and(query_param("per_page", "100"))
        .and(query_param("page", "1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": items
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = search_function_manifest("search", &server.uri());
    manifest["functions"][0]["pagination"] = json!({
        "mode": "page",
        "page_size": {
            "default": 10,
            "max": 500,
            "query_param": "per_page"
        },
        "page_param": "page",
        "page_start": 1,
        "page_step": 1
    });
    let source = build_source(manifest);
    let rows = source_rows(
        &source,
        "SELECT title, score \
         FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral', mode => 'hybrid') \
         LIMIT 250",
    )
    .await;

    assert_eq!(rows.len(), 100);
}

#[tokio::test]
async fn source_scoped_table_function_rejects_invalid_sql_shapes() {
    for (schema, sql, expected) in [
        (
            "search",
            "SELECT title FROM search.search_issues(q => 'flaky', q => 'cleanup')",
            "search.search_issues duplicate argument 'q'",
        ),
        (
            "search",
            "SELECT title FROM search.find_issues(q => 'flaky')",
            "unknown source table function search.find_issues; available functions: search.search_issues",
        ),
        (
            "bad_mode_search",
            "SELECT title FROM bad_mode_search.search_issues(q => 'flaky', mode => 'banana')",
            "bad_mode_search.search_issues argument 'mode' has invalid value 'banana'",
        ),
        (
            "conflict_search",
            "SELECT title FROM conflict_search.search_issues(q => 'flaky') WHERE q = 'raw'",
            "No column named `q`",
        ),
    ] {
        let error = search_function_error(schema, sql).await;
        assert_error_contains(&error, expected);
    }
}

#[tokio::test]
async fn source_scoped_table_function_query_shapes() {
    for (name, search_type, sql) in [
        (
            "builds request from named args",
            Some("hybrid"),
            "SELECT title, score \
             FROM search.search_issues(mode => 'hybrid', q => 'flaky cleanup repo:withcoral/coral')",
        ),
        (
            "normalizes unquoted identifiers",
            Some("hybrid"),
            "SELECT title, score \
             FROM SEARCH.SEARCH_ISSUES(MODE => 'hybrid', Q => 'flaky cleanup repo:withcoral/coral')",
        ),
        (
            "omits optional named arg",
            None,
            "SELECT title, score \
             FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral')",
        ),
        (
            "preserves table alias",
            Some("hybrid"),
            "SELECT issue.title, issue.score \
             FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral', mode => 'hybrid') AS issue",
        ),
        (
            "treats typed null as omitted optional arg",
            None,
            "SELECT title, score FROM search.search_issues(\
             q => 'flaky cleanup repo:withcoral/coral', mode => CAST(NULL AS VARCHAR))",
        ),
    ] {
        assert_search_function_query(name, sql, search_type).await;
    }
}

async fn assert_search_function_query(name: &str, sql: &str, search_type: Option<&str>) {
    let server = MockServer::start().await;
    mount_search_issues_response(&server, search_type).await;

    let source = build_source(search_function_manifest("search", &server.uri()));
    assert_eq!(
        source_rows(&source, sql).await,
        search_issue_rows(),
        "{name}"
    );
}

async fn search_function_error(schema: &str, sql: &str) -> CoreError {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(schema, &server.uri()));
    source_error(&source, sql).await
}

async fn http_users_error(
    source_name: &str,
    response: ResponseTemplate,
    expected_calls: u64,
    configure_manifest: impl FnOnce(&mut Value),
) -> CoreError {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(response)
        .expect(expected_calls)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest(source_name, &server.uri());
    configure_manifest(&mut manifest);
    let source = build_source(manifest);
    source_error(&source, &format!("SELECT * FROM {source_name}.users")).await
}

async fn http_users_structured_error(
    source_name: &str,
    response: ResponseTemplate,
    expected_calls: u64,
    configure_manifest: impl FnOnce(&mut Value),
    status: StatusCode,
    reason: &str,
    retryable: bool,
) -> StructuredQueryError {
    let error = http_users_error(source_name, response, expected_calls, configure_manifest).await;
    assert_query_failure(&error, status, reason, retryable).clone()
}

async fn assert_authenticated_count_query(
    source_name: &str,
    expected_headers: &[(&str, &str)],
    configure_manifest: impl FnOnce(&mut Value),
    secrets: &[(&'static str, &'static str)],
    runtime: QueryRuntimeConfig,
) {
    let server = MockServer::start().await;
    let mut mock = Mock::given(method("GET")).and(path("/api/users"));
    for (name, value) in expected_headers {
        mock = mock.and(header(*name, *value));
    }
    mock.respond_with(data_response(users_rows()))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest(source_name, &server.uri());
    configure_manifest(&mut manifest);
    let source = build_source_with_secrets(manifest, secrets.iter().copied());

    let rows = source_rows_with_runtime(
        &source,
        runtime,
        &format!("SELECT COUNT(*) AS n FROM {source_name}.users"),
    )
    .await;
    assert_eq!(rows, vec![json!({"n": 3})]);
}

async fn assert_text_body_case(case: TextBodyCase) {
    let server = MockServer::start().await;
    let mock = Mock::given(method("POST"))
        .and(path(case.path))
        .and(body_string(case.expected_body));
    let mock = if let Some(content_type) = case.expected_content_type {
        mock.and(header("content-type", content_type))
    } else {
        mock
    };
    mock.respond_with(case.response)
        .expect(1)
        .mount(&server)
        .await;

    let manifest = http_table_manifest(case.source_name, &server.uri(), &case.table);
    let source = build_source(manifest);

    assert_eq!(
        source_rows(&source, case.sql).await,
        case.expected_rows,
        "{}",
        case.name
    );
}

fn configure_bearer_auth(manifest: &mut Value) {
    manifest["inputs"] = json!({
        "API_TOKEN": { "kind": "secret" }
    });
    manifest["auth"] = json!({
        "type": "HeaderAuth",
        "headers": [{
            "name": "Authorization",
            "from": "bearer",
            "key": "API_TOKEN"
        }]
    });
}

fn configure_bearer_fallback_auth(manifest: &mut Value) {
    manifest["inputs"] = json!({
        "API_KEY": { "kind": "secret", "required": false },
        "OAUTH_TOKEN": { "kind": "secret", "required": false }
    });
    manifest["auth"] = json!({
        "type": "HeaderAuth",
        "headers": [{
            "name": "Authorization",
            "from": "one_of",
            "values": [
                { "from": "input", "key": "API_KEY" },
                { "from": "bearer", "key": "OAUTH_TOKEN" }
            ]
        }]
    });
}

fn configure_custom_auth(manifest: &mut Value) {
    manifest["inputs"] = json!({
        "API_TOKEN": { "kind": "secret" }
    });
    manifest["auth"] = json!({
        "type": "CustomAuth",
        "authenticator": "test_signer",
        "prefix": "Bearer"
    });
}

#[tokio::test]
async fn table_request_headers_do_not_resolve_args_from_filters() {
    let server = MockServer::start().await;
    let mut manifest = base_http_manifest("http_arg_header", &server.uri());
    manifest["request_headers"] = json!([{
        "name": "X-Request-Arg",
        "from": "template",
        "template": "{{arg.id}}"
    }]);
    manifest["tables"][0]["filters"] = json!([{ "name": "id" }]);
    manifest["tables"][0]["request"]["query"] = json!([
        { "name": "id", "from": "filter", "key": "id" }
    ]);

    let source = build_source(manifest);
    let error = source_error(&source, "SELECT id FROM http_arg_header.users WHERE id = 2").await;

    assert_error_contains(&error, "missing request argument 'id'");
}

#[tokio::test]
async fn table_function_request_headers_do_not_resolve_filters_from_args() {
    let server = MockServer::start().await;
    let mut manifest = search_function_manifest("function_filter_header", &server.uri());
    manifest["request_headers"] = json!([{
        "name": "X-Filter",
        "from": "template",
        "template": "{{filter.q}}"
    }]);
    let source = build_source(manifest);
    let sql = "SELECT title FROM function_filter_header.search_issues(q => 'flaky')";

    let error = source_error(&source, sql).await;

    assert_error_contains(&error, "missing filter 'q'");
}

#[tokio::test]
async fn boolean_filter_bool_is_predicate_sends_json_bool_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/users/search"))
        .and(body_json(json!({ "includeArchived": false })))
        .respond_with(grace_data_response())
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_bool_filter", &server.uri());
    let table = &mut manifest["tables"][0];
    table["filters"] = json!([{ "name": "include_archived" }]);
    table["request"] = json!({
        "method": "POST",
        "path": "/api/users/search",
        "body": [
            {
                "path": ["includeArchived"],
                "from": "filter_bool",
                "key": "include_archived"
            }
        ]
    });
    table["columns"].as_array_mut().unwrap().push(json!({
        "name": "include_archived",
        "type": "Boolean",
        "nullable": true,
        "virtual": true,
        "expr": { "kind": "from_filter", "key": "include_archived" }
    }));
    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT id, include_archived FROM http_bool_filter.users WHERE include_archived IS FALSE",
        vec![json!({"id": 2, "include_archived": false})],
    )
    .await;
}

#[tokio::test]
async fn pagination_parameter_modes() {
    for (source_name, query_name, requests, pagination) in [
        (
            "http_page",
            "page",
            ["1", "2", "3"],
            json!({
                "mode": "page",
                "page_param": "page",
                "page_start": 1
            }),
        ),
        (
            "http_offset",
            "offset",
            ["0", "2", "4"],
            json!({
                "mode": "offset",
                "offset_param": "offset",
                "offset_step": 2
            }),
        ),
    ] {
        assert_users_pagination(source_name, query_name, &requests, pagination).await;
    }
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
        .respond_with(data_response(&rows[2..]))
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_link", &server.uri());
    manifest["tables"][0]["pagination"] = json!({
        "mode": "link_header"
    });
    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT id, name, email FROM http_link.users ORDER BY id",
        users_rows(),
    )
    .await;
}

#[tokio::test]
async fn auth_header_cases() {
    for (source, headers, configure, secrets, runtime) in [
        (
            "http_auth",
            &[("authorization", "Bearer secret-token")][..],
            configure_bearer_auth as fn(&mut Value),
            &[("API_TOKEN", "secret-token")][..],
            test_runtime as fn() -> QueryRuntimeConfig,
        ),
        (
            "http_auth_fallback",
            &[("authorization", "Bearer oauth-token")][..],
            configure_bearer_fallback_auth,
            &[("OAUTH_TOKEN", "oauth-token")][..],
            test_runtime,
        ),
        (
            "http_custom_auth",
            &[
                ("authorization", "Bearer secret-token"),
                ("x-signed-path", "/api/users"),
            ][..],
            configure_custom_auth,
            &[("API_TOKEN", "secret-token")][..],
            test_auth_runtime,
        ),
    ] {
        assert_authenticated_count_query(source, headers, configure, secrets, runtime()).await;
    }
}

#[tokio::test]
async fn api_returns_500() {
    let sqe = http_users_structured_error(
        "http_500",
        ResponseTemplate::new(500).set_body_string("boom"),
        3,
        |_| {},
        StatusCode::Unavailable,
        "PROVIDER_REQUEST_FAILED",
        true,
    )
    .await;
    assert_query_metadata(&sqe, "http_status", "500");
    assert_query_metadata(&sqe, "source", "http_500");
    assert!(sqe.detail().contains("boom"));
}

#[tokio::test]
async fn api_returns_500_with_bad_link_header_still_reports_api_failure() {
    let sqe = http_users_structured_error(
        "http_500_bad_link",
        ResponseTemplate::new(500)
            .append_header(
                "Link",
                "<https://example.invalid/api/users?page=2>; rel=\"next\"",
            )
            .set_body_string("boom"),
        3,
        |manifest| {
            manifest["tables"][0]["pagination"] = json!({
                "mode": "link_header"
            });
        },
        StatusCode::Unavailable,
        "PROVIDER_REQUEST_FAILED",
        true,
    )
    .await;
    assert_query_metadata(&sqe, "http_status", "500");
    assert_query_metadata(&sqe, "source", "http_500_bad_link");
    assert_eq!(sqe.metadata().get("provider_failure_stage"), None);
}

#[tokio::test]
async fn api_returns_401() {
    let sqe = http_users_structured_error(
        "http_401",
        ResponseTemplate::new(401).set_body_string("unauthorized"),
        1,
        |_| {},
        StatusCode::FailedPrecondition,
        "PROVIDER_REQUEST_FAILED",
        false,
    )
    .await;
    assert_query_metadata(&sqe, "http_status", "401");
    assert_query_metadata(&sqe, "source", "http_401");
    assert!(sqe.hint().unwrap().contains("coral source add http_401"));
    assert!(sqe.detail().contains("unauthorized"));
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

    let rows = source_rows(
        &source,
        "SELECT ts, permalink, user_id, text FROM slack_ts.messages WHERE channel = 'C123456' ORDER BY ts",
    )
    .await;

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
        .respond_with(data_response(Vec::<Value>::new()))
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

    let error = source_error(&source, "SELECT * FROM http_required.users").await;

    let sqe = assert_query_failure(
        &error,
        StatusCode::FailedPrecondition,
        "MISSING_REQUIRED_FILTER",
        false,
    );
    assert_query_metadata(sqe, "schema", "http_required");
    assert_query_metadata(sqe, "table", "users");
    assert_query_metadata(sqe, "column", "id");
    assert!(sqe.summary().contains("WHERE id"));
    assert!(sqe.hint().unwrap().contains("coral.columns"));
}

#[tokio::test]
async fn api_returns_malformed_json() {
    let sqe = http_users_structured_error(
        "http_bad_json",
        ResponseTemplate::new(200).set_body_string("not-json"),
        1,
        |_| {},
        StatusCode::FailedPrecondition,
        "PROVIDER_REQUEST_FAILED",
        false,
    )
    .await;
    assert_eq!(sqe.summary(), "Source response decode failed");
    assert_query_metadata(&sqe, "source", "http_bad_json");
    assert_query_metadata(&sqe, "table", "users");
    assert_query_metadata(&sqe, "provider_failure_stage", "decode");
    assert!(sqe.detail().contains("response decoding failed"));
}

#[tokio::test]
async fn api_does_not_retry_empty_or_whitespace_json_response() {
    for (schema, body) in [("http_empty_json", ""), ("http_whitespace_json", " \n\t")] {
        let server = MockServer::start().await;
        let attempts = Arc::new(AtomicUsize::new(0));
        let responder_attempts = Arc::clone(&attempts);
        let body = body.to_string();
        Mock::given(method("GET"))
            .and(path("/api/users"))
            .respond_with(move |_request: &Request| {
                responder_attempts.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200).set_body_string(body.clone())
            })
            .expect(1)
            .mount(&server)
            .await;

        let source = build_source(base_http_manifest(schema, &server.uri()));
        let query = format!("SELECT id, name, email FROM {schema}.users ORDER BY id");

        let error = CoralQuery::execute_sql(&[source], test_runtime(), &query)
            .await
            .expect_err("stable empty JSON response should be a permanent decode failure");

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
        match error {
            CoreError::QueryFailure(sqe) => {
                assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
                assert_eq!(sqe.summary(), "Source response decode failed");
                assert!(!sqe.retryable());
                assert_eq!(
                    sqe.metadata().get("provider_failure_stage").unwrap(),
                    "decode"
                );
                assert!(
                    sqe.hint()
                        .expect("empty decode failures should include guidance")
                        .contains("source manifest")
                );
            }
            other => panic!("unexpected stable-empty-json error variant: {other:?}"),
        }
    }
}

#[tokio::test]
async fn api_retries_truncated_json_response() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let responder_attempts = Arc::clone(&attempts);
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(move |_request: &Request| {
            if responder_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                ResponseTemplate::new(200)
                    .set_body_string(r#"{"data":[{"id":1,"name":"Ada","email":"ada@example.com"#)
            } else {
                ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() }))
            }
        })
        .expect(2)
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest("http_truncated_json", &server.uri()));

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, name, email FROM http_truncated_json.users ORDER BY id",
        )
        .await
        .expect("truncated JSON EOF should be retried"),
    );

    assert_eq!(rows, users_rows());
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn api_reports_exhausted_truncated_get_json_as_retryable() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let responder_attempts = Arc::clone(&attempts);
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(move |_request: &Request| {
            responder_attempts.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_body_string(r#"{"data":[{"id":1,"name":"Ada","email":"ada@example.com"#)
        })
        .expect(3)
        .mount(&server)
        .await;

    let source = build_source(base_http_manifest(
        "http_exhausted_truncated_json",
        &server.uri(),
    ));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, name, email FROM http_exhausted_truncated_json.users ORDER BY id",
    )
    .await
    .expect_err("exhausted truncated GET JSON should surface as retryable");

    assert_eq!(attempts.load(Ordering::SeqCst), 3);
    assert_eq!(error.status_code(), StatusCode::Unavailable);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert_eq!(sqe.summary(), "Source response decode failed");
            assert!(sqe.retryable());
            assert_eq!(
                sqe.metadata().get("provider_failure_stage").unwrap(),
                "decode"
            );
            assert!(
                sqe.hint()
                    .expect("retryable decode failures should include guidance")
                    .contains("could not be fully decoded")
            );
        }
        other => panic!("unexpected exhausted truncated-json error variant: {other:?}"),
    }
}

#[tokio::test]
async fn api_does_not_retry_truncated_json_response_for_post() {
    let server = MockServer::start().await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let responder_attempts = Arc::clone(&attempts);
    Mock::given(method("POST"))
        .and(path("/api/users"))
        .respond_with(move |_request: &Request| {
            responder_attempts.fetch_add(1, Ordering::SeqCst);
            ResponseTemplate::new(200)
                .set_body_string(r#"{"data":[{"id":1,"name":"Ada","email":"ada@example.com"#)
        })
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_truncated_post_json", &server.uri());
    manifest["tables"][0]["request"]["method"] = json!("POST");
    let source = build_source(manifest);

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id, name, email FROM http_truncated_post_json.users ORDER BY id",
    )
    .await
    .expect_err("truncated JSON EOF should not retry non-idempotent requests");

    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert_eq!(sqe.summary(), "Source response decode failed");
            assert!(!sqe.retryable());
            assert_eq!(
                sqe.metadata().get("provider_failure_stage").unwrap(),
                "decode"
            );
        }
        other => panic!("unexpected truncated-json error variant: {other:?}"),
    }
}

#[tokio::test]
async fn pagination_link_header_cross_origin_surfaces_structured_error() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(
            ResponseTemplate::new(200)
                .append_header(
                    "Link",
                    "<https://example.invalid/api/users?page=2>; rel=\"next\"",
                )
                .set_body_json(json!({ "data": [] })),
        )
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_bad_pagination", &server.uri());
    manifest["tables"][0]["pagination"] = json!({
        "mode": "link_header"
    });
    let source = build_source(manifest);

    let error = source_error(&source, "SELECT * FROM http_bad_pagination.users").await;

    let sqe = assert_query_failure(
        &error,
        StatusCode::FailedPrecondition,
        "PROVIDER_REQUEST_FAILED",
        false,
    );
    assert_eq!(sqe.summary(), "Source pagination failed");
    assert_query_metadata(sqe, "source", "http_bad_pagination");
    assert_query_metadata(sqe, "table", "users");
    assert_query_metadata(sqe, "provider_failure_stage", "pagination");
    assert!(
        sqe.detail()
            .contains("pagination next link must stay on origin")
    );
}

#[tokio::test]
async fn text_body_request_cases() {
    let sql = "SELECT id, name, email FROM users WHERE id = 2 FORMAT JSONEachRow";
    for case in [
        TextBodyCase {
            name: "default content type",
            source_name: "http_text_body",
            path: "/query",
            expected_content_type: Some("text/plain"),
            expected_body: sql,
            table: json!({
                "name": "users",
                "description": "users via SQL",
                "request": {
                    "method": "POST",
                    "path": "/query",
                    "body": {
                        "format": "text",
                        "content": {
                            "from": "literal",
                            "value": "SELECT id, name, email FROM users WHERE id = 2 FORMAT JSONEachRow"
                        }
                    }
                },
                "response": {
                    "format": "json_each_row"
                },
                "columns": users_columns()
            }),
            response: ResponseTemplate::new(200)
                .set_body_string("{\"id\":2,\"name\":\"Grace\",\"email\":\"grace@example.com\"}\n"),
            sql: "SELECT id, name, email FROM http_text_body.users",
            expected_rows: vec![json!({"id": 2, "name": "Grace", "email": "grace@example.com"})],
        },
        TextBodyCase {
            name: "explicit content type",
            source_name: "http_ct_override",
            path: "/sql",
            expected_content_type: Some("application/sql"),
            expected_body: "SELECT 1",
            table: json!({
                "name": "items",
                "description": "items via SQL",
                "request": {
                    "method": "POST",
                    "path": "/sql",
                    "headers": [{
                        "name": "Content-Type",
                        "from": "literal",
                        "value": "application/sql"
                    }],
                    "body": {
                        "format": "text",
                        "content": { "from": "literal", "value": "SELECT 1" }
                    }
                },
                "response": {
                    "rows_path": ["data"]
                },
                "columns": [column("id", "Int64")]
            }),
            response: data_response(Vec::<Value>::new()),
            sql: "SELECT COUNT(*) AS n FROM http_ct_override.items",
            expected_rows: vec![json!({"n": 0})],
        },
        TextBodyCase {
            name: "omitted optional content",
            source_name: "http_optional_text_body",
            path: "/sql",
            expected_content_type: None,
            expected_body: "",
            table: json!({
                "name": "items",
                "description": "items via optional SQL",
                "filters": [{ "name": "sql" }],
                "request": {
                    "method": "POST",
                    "path": "/sql",
                    "body": {
                        "format": "text",
                        "content": { "from": "filter", "key": "sql" }
                    }
                },
                "response": {
                    "rows_path": ["data"]
                },
                "columns": [column("id", "Int64")]
            }),
            response: data_response(Vec::<Value>::new()),
            sql: "SELECT COUNT(*) AS n FROM http_optional_text_body.items",
            expected_rows: vec![json!({"n": 0})],
        },
    ] {
        assert_text_body_case(case).await;
    }
}

#[tokio::test]
async fn json_each_row_response_parses_newline_delimited_rows() {
    let server = MockServer::start().await;
    let body = "{\"id\":1,\"name\":\"Ada\"}\n\n\
                {\"id\":2,\"name\":\"Grace\"}\n\
                {\"id\":3,\"name\":\"Linus\"}\n";
    Mock::given(method("GET"))
        .and(path("/logs"))
        .respond_with(ResponseTemplate::new(200).set_body_string(body))
        .mount(&server)
        .await;

    let manifest = http_table_manifest(
        "http_ndjson",
        &server.uri(),
        &json!({
            "name": "logs",
            "description": "newline-delimited logs",
            "request": { "method": "GET", "path": "/logs" },
            "response": { "format": "json_each_row" },
            "columns": [
                column("id", "Int64"),
                column("name", "Utf8")
            ]
        }),
    );

    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT id, name FROM http_ndjson.logs ORDER BY id",
        vec![
            json!({"id": 1, "name": "Ada"}),
            json!({"id": 2, "name": "Grace"}),
            json!({"id": 3, "name": "Linus"}),
        ],
    )
    .await;
}

#[tokio::test]
async fn legacy_json_body_array_form_still_works() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/graphql"))
        .and(body_json(json!({ "query": "{ users { id name email } }" })))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "data": { "users": users_rows() }
        })))
        .expect(1)
        .mount(&server)
        .await;

    let manifest = http_table_manifest(
        "http_legacy_body",
        &server.uri(),
        &json!({
            "name": "users",
            "description": "graphql users",
            "request": {
                "method": "POST",
                "path": "/graphql",
                "body": [
                    { "path": ["query"], "from": "literal", "value": "{ users { id name email } }" }
                ]
            },
            "response": { "rows_path": ["data", "users"] },
            "columns": users_columns()
        }),
    );

    let source = build_source(manifest);

    assert_source_rows(
        &source,
        "SELECT id, name, email FROM http_legacy_body.users ORDER BY id",
        users_rows(),
    )
    .await;
}
