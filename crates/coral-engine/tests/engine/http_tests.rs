#![allow(
    clippy::indexing_slicing,
    clippy::string_slice,
    reason = "test code: assertion-style indexing is idiomatic in tests"
)]

use std::collections::BTreeMap;
use std::sync::Arc;

use coral_engine::{
    CoralQuery, CoreError, EngineExtensions, QueryRuntimeConfig, QueryRuntimeContext,
    RequestAuthenticator, RequestAuthenticatorError, StatusCode,
};
use reqwest::header::{AUTHORIZATION, HeaderName, HeaderValue};
use serde_json::{Value, json};
use wiremock::matchers::{
    body_json, body_string, header, method, path, query_param, query_param_is_missing,
};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{
    build_source, build_source_with_secrets, execution_to_rows, test_runtime, users_rows,
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

fn search_function_manifest(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "functions": [{
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
                { "name": "title", "type": "Utf8" },
                { "name": "score", "type": "Float64" }
            ]
        }]
    })
}

fn function_only_search_manifest(name: &str, base_url: &str) -> Value {
    let mut manifest = search_function_manifest(name, base_url);
    manifest
        .as_object_mut()
        .expect("manifest is an object")
        .remove("tables");
    manifest
}

fn split_function_manifest(name: &str, base_url: &str) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "functions": [{
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
            "columns": [
                { "name": "body", "type": "Utf8" }
            ]
        }]
    })
}

fn notionish_search_function_manifest(base_url: &str) -> Value {
    json!({
        "name": "notionish",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": base_url,
        "functions": [{
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
        }]
    })
}

fn internal_table_function_name(schema: &str, function: &str) -> String {
    // PR #306 only registers DataFusion's flat internal UDTF. The public
    // source-scoped planner in the next stack PR owns this mapping for users.
    format!(
        "__coral_udtf_{}_{}",
        hex_encode(schema),
        hex_encode(function)
    )
}

fn hex_encode(value: &str) -> String {
    use std::fmt::Write as _;

    let mut encoded = String::with_capacity(value.len() * 2);
    for byte in value.as_bytes() {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String never fails");
    }
    encoded
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
            test_runtime(),
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
            test_runtime(),
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
            test_runtime(),
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
            test_runtime(),
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
            test_runtime(),
            "SELECT id, name FROM http_filter.users WHERE id = 2",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 2, "name": "Grace"})]);
}

#[tokio::test]
async fn internal_table_function_builds_http_search_request() {
    let function_name = internal_table_function_name("search", "search_issues");
    assert_search_function_query(&format!(
        "SELECT title, score \
         FROM {function_name}('flaky cleanup repo:withcoral/coral', 'hybrid')"
    ))
    .await;
}

#[tokio::test]
async fn source_scoped_table_function_builds_http_search_request() {
    assert_search_function_query(
        "SELECT title, score \
         FROM search.search_issues(mode => 'hybrid', q => 'flaky cleanup repo:withcoral/coral')",
    )
    .await;
}

#[tokio::test]
async fn validate_source_accepts_function_only_http_source_and_runs_queries() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param_is_missing("search_type"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Flaky workspace cleanup",
                "score": 9.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(function_only_search_manifest("search", &server.uri()));
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
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT body FROM linearish.issue_comments(issue => 'SOURCE-496')",
        )
        .await
        .expect("function arg split query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "body": "Looks good"
        })]
    );
}

#[tokio::test]
async fn source_scoped_table_function_normalizes_unquoted_sql_identifiers() {
    assert_search_function_query(
        "SELECT title, score \
         FROM SEARCH.SEARCH_ISSUES(MODE => 'hybrid', Q => 'flaky cleanup repo:withcoral/coral')",
    )
    .await;
}

#[tokio::test]
async fn source_scoped_table_function_preserves_quoted_manifest_identifiers() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param("search_type", "hybrid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Flaky workspace cleanup",
                "score": 9.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = search_function_manifest("Search", &server.uri());
    manifest["functions"][0]["name"] = json!("Search_Issues");
    manifest["functions"][0]["args"][0]["name"] = json!("Q");
    let source = build_source(manifest);
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT title, score \
             FROM \"Search\".\"Search_Issues\"(\"Q\" => 'flaky cleanup repo:withcoral/coral', mode => 'hybrid')",
        )
        .await
        .expect("quoted exact manifest identifiers should resolve"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "title": "Flaky workspace cleanup",
            "score": 9.5
        })]
    );
}

#[tokio::test]
async fn source_scoped_table_function_omits_optional_named_arg() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param_is_missing("search_type"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Flaky workspace cleanup",
                "score": 9.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest("search", &server.uri()));
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT title, score \
             FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral')",
        )
        .await
        .expect("omitted optional named argument should be absent from the request"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "title": "Flaky workspace cleanup",
            "score": 9.5
        })]
    );
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

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            std::slice::from_ref(&source),
            test_runtime(),
            "SELECT object, id, requested_object \
             FROM notionish.search_objects(query => 'Coral')",
        )
        .await
        .expect("optional body fields should be omitted when the arg is absent"),
    );
    assert_eq!(
        rows,
        vec![json!({
            "object": "page",
            "id": "page_1"
        })]
    );

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT object, id, requested_object \
             FROM notionish.search_objects(query => 'Coral', object => 'data_source')",
        )
        .await
        .expect("optional body fields should be emitted when the arg is present"),
    );
    assert_eq!(
        rows,
        vec![json!({
            "object": "data_source",
            "id": "data_source_1",
            "requested_object": "data_source"
        })]
    );
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
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT title, score FROM capped_search.search_issues(q => 'flaky') LIMIT 3",
        )
        .await
        .expect("search function query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({ "title": "First", "score": 3.0 }),
            json!({ "title": "Second", "score": 2.0 })
        ]
    );
}

#[tokio::test]
async fn source_scoped_table_function_preserves_table_alias() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param("search_type", "hybrid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Flaky workspace cleanup",
                "score": 9.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest("search", &server.uri()));
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT issue.title, issue.score \
             FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral', mode => 'hybrid') AS issue",
        )
        .await
        .expect("source-scoped table function aliases should resolve"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "title": "Flaky workspace cleanup",
            "score": 9.5
        })]
    );
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
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT title, score \
             FROM search.search_issues(q => 'flaky cleanup repo:withcoral/coral', mode => 'hybrid') \
             LIMIT 250",
        )
        .await
        .expect("search limits should cap page size and total rows"),
    );

    assert_eq!(rows.len(), 100);
}

#[tokio::test]
async fn source_scoped_table_function_rejects_duplicate_args() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest("search", &server.uri()));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT title FROM search.search_issues(q => 'flaky', q => 'cleanup')",
    )
    .await
    .expect_err("duplicate function arguments should fail planning");

    assert!(
        error
            .to_string()
            .contains("search.search_issues duplicate argument 'q'"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn source_scoped_table_function_rejects_unknown_function_in_known_schema() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest("search", &server.uri()));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT title FROM search.find_issues(q => 'flaky')",
    )
    .await
    .expect_err("unknown source-scoped function should fail planning");

    assert!(
        error.to_string().contains(
            "unknown source table function search.find_issues; available functions: search.search_issues",
        ),
        "unexpected error: {error}"
    );
}

async fn assert_search_function_query(sql: &str) {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param("search_type", "hybrid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Flaky workspace cleanup",
                "score": 9.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest("search", &server.uri()));
    let rows = execution_to_rows(
        &CoralQuery::execute_sql(&[source], test_runtime(), sql)
            .await
            .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "title": "Flaky workspace cleanup",
            "score": 9.5
        })]
    );
}

#[tokio::test]
async fn table_function_treats_typed_null_as_omitted_optional_argument() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "flaky cleanup repo:withcoral/coral"))
        .and(query_param_is_missing("search_type"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Flaky workspace cleanup",
                "score": 9.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest("null_arg_search", &server.uri()));
    let function_name = internal_table_function_name("null_arg_search", "search_issues");

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            &format!(
                "SELECT title, score FROM {function_name}(\
                 'flaky cleanup repo:withcoral/coral', CAST(NULL AS VARCHAR))"
            ),
        )
        .await
        .expect("typed null optional argument should be omitted"),
    );

    assert_eq!(
        rows,
        vec![json!({
            "title": "Flaky workspace cleanup",
            "score": 9.5
        })]
    );
}

#[tokio::test]
async fn table_function_rejects_invalid_argument_values() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest("bad_mode_search", &server.uri()));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT title FROM bad_mode_search.search_issues(q => 'flaky', mode => 'banana')",
    )
    .await
    .expect_err("invalid function argument should fail planning");

    assert!(
        error
            .to_string()
            .contains("bad_mode_search.search_issues argument 'mode' has invalid value 'banana'"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn table_function_does_not_expose_request_args_as_columns() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest("conflict_search", &server.uri()));

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT title FROM conflict_search.search_issues(q => 'flaky') WHERE q = 'raw'",
    )
    .await
    .expect_err("request args should not be queryable as result columns");

    assert!(
        error.to_string().contains("No column named `q`"),
        "unexpected error: {error}"
    );
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
    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT id FROM http_arg_header.users WHERE id = 2",
    )
    .await
    .expect_err("table filters must not populate function arguments");

    assert!(
        error.to_string().contains("missing request argument 'id'"),
        "unexpected error: {error}"
    );
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
    let function_name = internal_table_function_name("function_filter_header", "search_issues");

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        &format!("SELECT title FROM {function_name}('flaky')"),
    )
    .await
    .expect_err("function args must not populate table filters");

    assert!(
        error.to_string().contains("missing filter 'q'"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn boolean_filter_bool_is_predicate_sends_json_bool_body() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/api/users/search"))
        .and(body_json(json!({ "includeArchived": false })))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({ "data": [json!({"id": 2, "name": "Grace", "email": "grace@example.com"})] }),
        ))
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

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, include_archived FROM http_bool_filter.users WHERE include_archived IS FALSE",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"id": 2, "include_archived": false})]);
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
            test_runtime(),
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
            test_runtime(),
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
            test_runtime(),
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
            test_runtime(),
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
        "type": "HeaderAuth",
        "headers": [{
            "name": "Authorization",
            "from": "bearer",
            "key": "API_TOKEN"
        }]
    });
    let source = build_source_with_secrets(manifest, [("API_TOKEN", "secret-token")]);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT COUNT(*) AS n FROM http_auth.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn auth_header_one_of_uses_bearer_fallback() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(header("authorization", "Bearer oauth-token"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_auth_fallback", &server.uri());
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
    let source = build_source_with_secrets(manifest, [("OAUTH_TOKEN", "oauth-token")]);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT COUNT(*) AS n FROM http_auth_fallback.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 3})]);
}

#[tokio::test]
async fn custom_authenticator_signs_final_request() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .and(header("authorization", "Bearer secret-token"))
        .and(header("x-signed-path", "/api/users"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": users_rows() })))
        .expect(1)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_custom_auth", &server.uri());
    manifest["inputs"] = json!({
        "API_TOKEN": { "kind": "secret" }
    });
    manifest["auth"] = json!({
        "type": "CustomAuth",
        "authenticator": "test_signer",
        "prefix": "Bearer"
    });
    let source = build_source_with_secrets(manifest, [("API_TOKEN", "secret-token")]);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_auth_runtime(),
            "SELECT COUNT(*) AS n FROM http_custom_auth.users",
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

    let error = CoralQuery::execute_sql(&[source], test_runtime(), "SELECT * FROM http_500.users")
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
async fn api_returns_500_with_bad_link_header_still_reports_api_failure() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/users"))
        .respond_with(
            ResponseTemplate::new(500)
                .append_header(
                    "Link",
                    "<https://example.invalid/api/users?page=2>; rel=\"next\"",
                )
                .set_body_string("boom"),
        )
        .expect(3)
        .mount(&server)
        .await;

    let mut manifest = base_http_manifest("http_500_bad_link", &server.uri());
    manifest["tables"][0]["pagination"] = json!({
        "mode": "link_header"
    });
    let source = build_source(manifest);

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT * FROM http_500_bad_link.users",
    )
    .await
    .expect_err("500 should fail as an API request error");

    assert_eq!(error.status_code(), StatusCode::Unavailable);
    match &error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert!(sqe.retryable());
            assert_eq!(sqe.metadata().get("http_status").unwrap(), "500");
            assert_eq!(sqe.metadata().get("source").unwrap(), "http_500_bad_link");
            assert_eq!(sqe.metadata().get("provider_failure_stage"), None);
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

    let error = CoralQuery::execute_sql(&[source], test_runtime(), "SELECT * FROM http_401.users")
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
            test_runtime(),
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

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT * FROM http_required.users",
    )
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

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT * FROM http_bad_json.users",
    )
    .await
    .expect_err("malformed json should fail");

    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert_eq!(sqe.summary(), "Source response decode failed");
            assert!(!sqe.retryable());
            assert_eq!(sqe.metadata().get("source").unwrap(), "http_bad_json");
            assert_eq!(sqe.metadata().get("table").unwrap(), "users");
            assert_eq!(
                sqe.metadata().get("provider_failure_stage").unwrap(),
                "decode"
            );
            assert!(sqe.detail().contains("response decoding failed"));
        }
        other => panic!("unexpected malformed-json error variant: {other:?}"),
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

    let error = CoralQuery::execute_sql(
        &[source],
        test_runtime(),
        "SELECT * FROM http_bad_pagination.users",
    )
    .await
    .expect_err("cross-origin pagination link should fail");

    assert_eq!(error.status_code(), StatusCode::FailedPrecondition);
    match error {
        CoreError::QueryFailure(sqe) => {
            assert_eq!(sqe.reason(), "PROVIDER_REQUEST_FAILED");
            assert_eq!(sqe.summary(), "Source pagination failed");
            assert!(!sqe.retryable());
            assert_eq!(sqe.metadata().get("source").unwrap(), "http_bad_pagination");
            assert_eq!(sqe.metadata().get("table").unwrap(), "users");
            assert_eq!(
                sqe.metadata().get("provider_failure_stage").unwrap(),
                "pagination"
            );
            assert!(
                sqe.detail()
                    .contains("pagination next link must stay on origin")
            );
        }
        other => panic!("unexpected pagination error variant: {other:?}"),
    }
}

#[tokio::test]
async fn text_body_sends_raw_sql_with_default_content_type() {
    let server = MockServer::start().await;
    let sql = "SELECT id, name, email FROM users WHERE id = 2 FORMAT JSONEachRow";
    Mock::given(method("POST"))
        .and(path("/query"))
        .and(header("content-type", "text/plain"))
        .and(body_string(sql))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_string("{\"id\":2,\"name\":\"Grace\",\"email\":\"grace@example.com\"}\n"),
        )
        .expect(1)
        .mount(&server)
        .await;

    let manifest = json!({
        "name": "http_text_body",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": &server.uri(),
        "tables": [{
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
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" },
                { "name": "email", "type": "Utf8" }
            ]
        }]
    });

    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, name, email FROM http_text_body.users",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![json!({"id": 2, "name": "Grace", "email": "grace@example.com"})]
    );
}

#[tokio::test]
async fn text_body_respects_explicit_content_type_override() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(header("content-type", "application/sql"))
        .and(body_string("SELECT 1"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(1)
        .mount(&server)
        .await;

    let manifest = json!({
        "name": "http_ct_override",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": &server.uri(),
        "tables": [{
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
            "columns": [{ "name": "id", "type": "Int64" }]
        }]
    });

    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT COUNT(*) AS n FROM http_ct_override.items",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 0})]);
}

#[tokio::test]
async fn text_body_omits_absent_optional_content() {
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/sql"))
        .and(body_string(""))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "data": [] })))
        .expect(1)
        .mount(&server)
        .await;

    let manifest = json!({
        "name": "http_optional_text_body",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": &server.uri(),
        "tables": [{
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
            "columns": [{ "name": "id", "type": "Int64" }]
        }]
    });

    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT COUNT(*) AS n FROM http_optional_text_body.items",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, vec![json!({"n": 0})]);
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

    let manifest = json!({
        "name": "http_ndjson",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": &server.uri(),
        "tables": [{
            "name": "logs",
            "description": "newline-delimited logs",
            "request": { "method": "GET", "path": "/logs" },
            "response": { "format": "json_each_row" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" }
            ]
        }]
    });

    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, name FROM http_ndjson.logs ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(
        rows,
        vec![
            json!({"id": 1, "name": "Ada"}),
            json!({"id": 2, "name": "Grace"}),
            json!({"id": 3, "name": "Linus"}),
        ]
    );
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

    let manifest = json!({
        "name": "http_legacy_body",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "http",
        "base_url": &server.uri(),
        "tables": [{
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
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" },
                { "name": "email", "type": "Utf8" }
            ]
        }]
    });

    let source = build_source(manifest);

    let rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT id, name, email FROM http_legacy_body.users ORDER BY id",
        )
        .await
        .expect("query should succeed"),
    );

    assert_eq!(rows, users_rows());
}
