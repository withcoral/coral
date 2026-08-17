use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use arrow::datatypes::{DataType, Schema};
use arrow::record_batch::RecordBatch;
use coral_engine::{
    CoralQuery, CoralSqlFunctionArgument, CoralSqlFunctionDefinition,
    CoralSqlFunctionInferenceDefinition, CoralSqlFunctionSignature, CoralSqlResultColumn,
    CoralSqlTableFunctionPublish, CoreError, EngineExtensions, QueryExecutionProvenance,
    QueryParameterValue, QueryParameters, QueryResultObserver, QueryResultObserverError,
    QueryRuntimeConfig, QueryRuntimeContext, QuerySource, RuntimeCatalogTarget,
    RuntimeSourcePackage, StatusCode,
};
use coral_spec::ManifestDataType;
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

const EVENTS_CALL: &str = "select * from udfs.min_id_events(min_id => 1)";
const REVIEW_QUERY: &str = "repo:withcoral/coral review";

#[derive(Debug, Default)]
struct RowCountObserver {
    calls: AtomicUsize,
}

impl RowCountObserver {
    fn calls(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

impl QueryResultObserver for RowCountObserver {
    fn name(&self) -> &'static str {
        "row_count"
    }

    fn observe_result(
        &self,
        _sql: &str,
        _schema: &Schema,
        _batches: &[RecordBatch],
        _provenance: &QueryExecutionProvenance,
    ) -> Result<(), QueryResultObserverError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

fn events_manifest(name: &str, dir: &Path) -> Value {
    json!({
        "name": name,
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [{
            "name": "events",
            "description": "Event rows",
            "format": "jsonl",
            "source": {
                "location": dir_url(dir),
                "glob": "**/*.jsonl"
            },
            "columns": [
                { "name": "id", "type": "Int64" }
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
            "description": "Search issues",
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
                },
                {
                    "name": "min_score",
                    "type": "Float64",
                    "bind": { "arg": "min_score" }
                },
                {
                    "name": "max_items",
                    "type": "Int64",
                    "bind": { "arg": "max_items" }
                },
                {
                    "name": "payload",
                    "type": "Json",
                    "bind": { "arg": "payload" }
                },
                {
                    "name": "since",
                    "type": "Timestamp",
                    "bind": { "arg": "since" }
                }
            ],
            "request": {
                "method": "GET",
                "path": "/api/search/issues",
                "query": [
                    { "name": "q", "from": "arg", "key": "q" },
                    { "name": "search_type", "from": "arg", "key": "search_type" },
                    { "name": "min_score", "from": "arg", "key": "min_score" },
                    { "name": "max_items", "from": "arg", "key": "max_items" },
                    { "name": "payload", "from": "arg", "key": "payload" },
                    { "name": "since", "from": "arg", "key": "since" }
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

async fn search_source_with_response(
    server: &MockServer,
    source_name: &str,
    mode: &str,
    title: &str,
    score: f64,
) -> coral_engine::QuerySource {
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", REVIEW_QUERY))
        .and(query_param("search_type", mode))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{ "title": title, "score": score }]
        })))
        .expect(1)
        .mount(server)
        .await;

    build_source(search_function_manifest(source_name, &server.uri()))
}

fn search_source(server: &MockServer, source_name: &str) -> coral_engine::QuerySource {
    build_source(search_function_manifest(source_name, &server.uri()))
}

fn events_source(source_name: &str) -> (tempfile::TempDir, coral_engine::QuerySource) {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(
        temp.path(),
        "events.jsonl",
        &[json!({"id": 1}), json!({"id": 2})],
    );
    let source = build_source(events_manifest(source_name, temp.path()));
    (temp, source)
}

fn udf_sql(name: &str, query: impl Into<String>) -> CoralSqlFunctionInferenceDefinition {
    CoralSqlFunctionInferenceDefinition {
        name: name.to_string(),
        query: query.into(),
    }
}

fn min_id_sql_udf(source_name: &str) -> CoralSqlFunctionInferenceDefinition {
    udf_sql(
        "min_id_events",
        format!("select id from {source_name}.events where id >= $min_id order by id"),
    )
}

fn review_queue_sql_udf(source_name: &str) -> CoralSqlFunctionInferenceDefinition {
    udf_sql(
        "review_queue",
        format!(
            "select title, score from {source_name}.search_issues(q => $query, min_score => $min_score, payload => $payload, since => $since)"
        ),
    )
}

fn udf_publish(name: &str) -> CoralSqlTableFunctionPublish {
    CoralSqlTableFunctionPublish {
        schema: "udfs".to_string(),
        name: name.to_string(),
        description: String::new(),
        guide: String::new(),
    }
}

fn udf_argument(name: &str, data_type: ManifestDataType) -> CoralSqlFunctionArgument {
    CoralSqlFunctionArgument {
        name: name.to_string(),
        data_type,
    }
}

fn udf_result_column(name: &str, data_type: DataType) -> CoralSqlResultColumn {
    udf_result_column_with_nullability(name, data_type, true)
}

fn udf_result_column_with_nullability(
    name: &str,
    data_type: DataType,
    nullable: bool,
) -> CoralSqlResultColumn {
    CoralSqlResultColumn {
        name: name.to_string(),
        data_type,
        nullable,
    }
}

fn min_id_udf(source_name: &str) -> CoralSqlFunctionDefinition {
    CoralSqlFunctionDefinition {
        name: "min_id_events".to_string(),
        description: "Events above an id".to_string(),
        arguments: vec![udf_argument("min_id", ManifestDataType::Int64)],
        query: format!("select id from {source_name}.events where id >= $min_id order by id"),
        publish: udf_publish("min_id_events"),
        result_columns: vec![udf_result_column("id", DataType::Int64)],
        source_names: vec![source_name.to_string()],
    }
}

fn min_id_udf_without_columns(source_name: &str) -> CoralSqlFunctionDefinition {
    let mut udf = min_id_udf(source_name);
    udf.result_columns.clear();
    udf
}

fn min_id_udf_with_result_column(
    source_name: &str,
    column_name: &str,
    data_type: DataType,
) -> CoralSqlFunctionDefinition {
    let mut udf = min_id_udf(source_name);
    udf.result_columns = vec![udf_result_column(column_name, data_type)];
    udf
}

fn min_id_udf_with_body(source_name: &str, body: impl Into<String>) -> CoralSqlFunctionDefinition {
    let mut udf = min_id_udf(source_name);
    udf.query = body.into();
    udf
}

fn min_id_udf_with_non_nullable_result(
    source_name: &str,
    body: impl Into<String>,
) -> CoralSqlFunctionDefinition {
    let mut udf = min_id_udf_with_body(source_name, body);
    udf.result_columns = vec![udf_result_column_with_nullability(
        "id",
        DataType::Int64,
        false,
    )];
    udf
}

fn mixed_case_published_min_id_udf(source_name: &str) -> CoralSqlFunctionDefinition {
    let mut udf = min_id_udf(source_name);
    udf.publish = CoralSqlTableFunctionPublish {
        schema: "Udfs".to_string(),
        name: "Min_Id_Events".to_string(),
        description: String::new(),
        guide: String::new(),
    };
    udf
}

fn published_limited_events_udf(source_name: &str) -> CoralSqlFunctionDefinition {
    CoralSqlFunctionDefinition {
        name: "limited_events".to_string(),
        description: "Limited events".to_string(),
        arguments: Vec::new(),
        query: format!("select id from {source_name}.events limit 1"),
        publish: udf_publish("limited_events"),
        result_columns: vec![udf_result_column("id", DataType::Int64)],
        source_names: vec![source_name.to_string()],
    }
}

fn argument_types(signature: &CoralSqlFunctionSignature) -> Vec<(&str, ManifestDataType)> {
    signature
        .arguments
        .iter()
        .map(|argument| (argument.name.as_str(), argument.data_type))
        .collect()
}

fn column_types(signature: &CoralSqlFunctionSignature) -> Vec<(&str, &DataType)> {
    signature
        .result_columns
        .iter()
        .map(|column| (column.name.as_str(), &column.data_type))
        .collect()
}

fn review_queue_udf(source_name: &str) -> CoralSqlFunctionDefinition {
    CoralSqlFunctionDefinition {
        name: "review_queue".to_string(),
        description: "Review queue".to_string(),
        arguments: vec![
            udf_argument("min_score", ManifestDataType::Float64),
            udf_argument("mode", ManifestDataType::Utf8),
            udf_argument("payload", ManifestDataType::Json),
            udf_argument("query", ManifestDataType::Utf8),
            udf_argument("since", ManifestDataType::Timestamp),
        ],
        query: format!(
            "select title, score from {source_name}.search_issues(q => $query, mode => $mode, min_score => $min_score, payload => $payload, since => $since)"
        ),
        publish: udf_publish("review_queue"),
        result_columns: Vec::new(),
        source_names: vec![source_name.to_string()],
    }
}

fn published_review_queue_udf(source_name: &str) -> CoralSqlFunctionDefinition {
    let mut udf = review_queue_udf(source_name);
    udf.result_columns = vec![
        udf_result_column("title", DataType::Utf8),
        udf_result_column("score", DataType::Float64),
    ];
    udf
}

fn review_queue_udf_published_as(source_name: &str, schema: &str) -> CoralSqlFunctionDefinition {
    review_queue_udf_published_at(source_name, schema, "review_queue")
}

fn review_queue_udf_published_at(
    source_name: &str,
    schema: &str,
    name: &str,
) -> CoralSqlFunctionDefinition {
    let mut udf = published_review_queue_udf(source_name);
    udf.publish = CoralSqlTableFunctionPublish {
        schema: schema.to_string(),
        name: name.to_string(),
        description: String::new(),
        guide: String::new(),
    };
    udf
}

fn runtime_with_observer(observer: Arc<dyn QueryResultObserver>) -> QueryRuntimeConfig {
    let mut extensions = EngineExtensions::default();
    extensions.query_result_observers.push(observer);
    QueryRuntimeConfig::new(QueryRuntimeContext::default(), extensions)
}

fn assert_invalid_input_contains(error: CoreError, expected: &str) {
    let CoreError::InvalidInput(detail) = error else {
        panic!("expected CoreError::InvalidInput, got {error:?}");
    };
    assert!(
        detail.contains(expected),
        "expected error detail to contain {expected:?}, got {detail:?}"
    );
}

async fn assert_udf_sql_error(
    source_name: &str,
    udfs: Vec<CoralSqlFunctionDefinition>,
    sql: &str,
    expected: &str,
) {
    let (_temp, source) = events_source(source_name);
    let runtime = test_runtime().with_udfs(udfs);

    let error = CoralQuery::execute_sql(&[source], runtime, sql)
        .await
        .expect_err("udf SQL should fail");

    assert_invalid_input_contains(error, expected);
}

async fn assert_source_udf_sql_error(
    source_name: &str,
    udfs: Vec<CoralSqlFunctionDefinition>,
    sql: &str,
    expected: &str,
) {
    let server = MockServer::start().await;
    let source = search_source(&server, source_name);
    let runtime = test_runtime().with_udfs(udfs);

    let error = CoralQuery::execute_sql(&[source], runtime, sql)
        .await
        .expect_err("udf SQL should fail");

    assert_invalid_input_contains(error, expected);
}

#[tokio::test]
async fn infer_udf_signature_uses_source_function_schema_with_parameters() {
    let source = build_source(search_function_manifest(
        "signature_param_search",
        "https://example.test",
    ));

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        review_queue_sql_udf("signature_param_search"),
    )
    .await
    .expect("udf signature inference should use source function schema without binding params");

    assert_eq!(
        argument_types(&signature),
        [
            ("min_score", ManifestDataType::Float64),
            ("payload", ManifestDataType::Json),
            ("query", ManifestDataType::Utf8),
            ("since", ManifestDataType::Timestamp),
        ]
    );
    assert_eq!(signature.source_names, ["signature_param_search"]);

    let columns = column_types(&signature);
    assert!(matches!(
        columns.as_slice(),
        [
            ("title", DataType::Utf8 | DataType::Utf8View),
            ("score", DataType::Float64)
        ]
    ));
}

#[tokio::test]
async fn infer_udf_signature_uses_explicit_cast_for_argument_type() {
    let signature = CoralQuery::infer_udf_signature(
        &[],
        test_runtime(),
        udf_sql("cast_label", "select cast($label as VARCHAR) as label"),
    )
    .await
    .expect("udf schema inference should use cast type");

    assert_eq!(
        argument_types(&signature),
        [("label", ManifestDataType::Utf8)]
    );
    let columns = column_types(&signature);
    let [("label", column_type)] = columns.as_slice() else {
        panic!("expected label result column");
    };
    assert!(matches!(column_type, DataType::Utf8 | DataType::Utf8View));
    assert!(signature.source_names.is_empty());
}

#[tokio::test]
async fn infer_udf_signature_accepts_timestamp_source_argument_and_cast() {
    let source = build_source(search_function_manifest(
        "mixed_timestamp_search",
        "https://example.test",
    ));

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        udf_sql(
            "mixed_timestamp",
            "select cast($since as TIMESTAMP) as since \
             from mixed_timestamp_search.search_issues(q => 'open', since => $since)",
        ),
    )
    .await
    .expect("timestamp source argument and timestamp cast should agree");

    assert_eq!(
        argument_types(&signature),
        [("since", ManifestDataType::Timestamp)]
    );
}

#[tokio::test]
async fn infer_udf_signature_uses_column_comparison_for_argument_type() {
    let (_temp, source) = events_source("schema_udf_events");
    let observer = Arc::new(RowCountObserver::default());

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        runtime_with_observer(observer.clone()),
        min_id_sql_udf("schema_udf_events"),
    )
    .await
    .expect("udf signature inference should plan without collection");

    assert_eq!(
        argument_types(&signature),
        [("min_id", ManifestDataType::Int64)]
    );
    assert!(matches!(
        column_types(&signature).as_slice(),
        [("id", DataType::Int64)]
    ));
    assert_eq!(signature.source_names, ["schema_udf_events"]);
    assert_eq!(observer.calls(), 0);
}

#[tokio::test]
async fn infer_udf_signature_maps_component_schema_to_canonical_source_name() {
    let (_temp, component_source) = events_source("github_rest");
    let source = QuerySource::from_runtime_components(
        RuntimeSourcePackage {
            source_name: "github".to_string(),
            authored_version: None,
            description: String::new(),
            declared_inputs: Vec::new(),
            test_queries: Vec::new(),
            identity_requirements: None,
            catalog_target: RuntimeCatalogTarget::Default,
            components: component_source.components().to_vec(),
        },
        BTreeMap::new(),
        BTreeMap::new(),
    )
    .expect("composite runtime source");

    let signature = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        udf_sql("github_events", "select id from github_rest.events"),
    )
    .await
    .expect("udf signature inference should resolve the installed source name");

    assert_eq!(signature.source_names, ["github"]);
}

#[tokio::test]
async fn infer_udf_signature_sorts_and_deduplicates_source_names() {
    let (_zeta_temp, zeta_source) = events_source("zeta_events");
    let (_alpha_temp, alpha_source) = events_source("alpha_events");

    let signature = CoralQuery::infer_udf_signature(
        &[zeta_source, alpha_source],
        test_runtime(),
        udf_sql(
            "combined_events",
            "select id from zeta_events.events \
             union all select id from alpha_events.events \
             union all select id from zeta_events.events",
        ),
    )
    .await
    .expect("udf signature inference should collect every source once");

    assert_eq!(signature.source_names, ["alpha_events", "zeta_events"]);
}

#[tokio::test]
async fn infer_udf_signature_excludes_system_catalog_tables() {
    let signature = CoralQuery::infer_udf_signature(
        &[],
        test_runtime(),
        udf_sql(
            "catalog_tables",
            "select schema_name, table_name from coral.tables",
        ),
    )
    .await
    .expect("system catalog query should plan");

    assert!(signature.source_names.is_empty());
}

#[tokio::test]
async fn infer_udf_signature_rejects_ambiguous_argument_type() {
    let error = CoralQuery::infer_udf_signature(
        &[],
        test_runtime(),
        udf_sql("ambiguous_value", "select $value as value"),
    )
    .await
    .expect_err("ambiguous udf argument should fail");

    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("SQL parameter '$value' has no inferred type"),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("CAST($value AS VARCHAR)"),
        "unexpected error: {error}"
    );
    assert!(
        !error.to_string().contains("Error during planning"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn infer_udf_signature_rejects_conflicting_argument_types() {
    let source = build_source(search_function_manifest(
        "conflicting_param_search",
        "https://example.test",
    ));
    let error = CoralQuery::infer_udf_signature(
        &[source],
        test_runtime(),
        udf_sql(
            "conflicting_param",
            "select title from conflicting_param_search.search_issues(q => $value, min_score => $value)",
        ),
    )
    .await
    .expect_err("conflicting udf argument types should fail");

    assert_eq!(error.status_code(), StatusCode::InvalidArgument);
    assert!(
        error
            .to_string()
            .contains("conflicting types for parameter"),
        "unexpected error: {error}"
    );
    assert!(
        error.to_string().contains("use explicit casts"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn infer_udf_signatures_returns_per_function_results() {
    let results = CoralQuery::infer_udf_signatures(
        &[],
        test_runtime(),
        vec![
            udf_sql("typed_value", "select cast($value as VARCHAR) as value"),
            udf_sql("ambiguous_value", "select $value as value"),
        ],
    )
    .await
    .expect("shared runtime should build");
    let mut results = results.into_iter();

    let signature = results
        .next()
        .expect("typed result")
        .expect("typed udf should validate");
    assert_eq!(
        argument_types(&signature),
        [("value", ManifestDataType::Utf8)]
    );
    let error = results
        .next()
        .expect("ambiguous result")
        .expect_err("ambiguous udf should fail independently");
    assert!(error.to_string().contains("has no inferred type"));
    assert!(results.next().is_none());
}

#[tokio::test]
async fn published_udf_table_function_executes_udf_sql() {
    let (_temp, source) = events_source("published_udf_events");
    let runtime = test_runtime().with_udfs(vec![min_id_udf("published_udf_events")]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select id from udfs.min_id_events(min_id => 2)",
    )
    .await
    .expect("published udf table function should execute");

    assert_eq!(execution_to_rows(&execution), vec![json!({"id": 2})]);
    assert_eq!(execution.provenance().sources(), ["published_udf_events"]);
    let table = execution
        .provenance()
        .tables()
        .first()
        .expect("UDF body table provenance");
    assert_eq!(table.source_name(), "published_udf_events");
    assert_eq!(table.schema_name(), "published_udf_events");
    assert_eq!(table.table_name(), "events");
    let function = execution
        .provenance()
        .table_functions()
        .first()
        .expect("invoked UDF provenance");
    assert_eq!(function.source_name(), "udfs");
    assert_eq!(function.schema_name(), "udfs");
    assert_eq!(function.function_name(), "min_id_events");
}

#[tokio::test]
async fn published_udf_table_function_coerces_arguments_in_the_expanded_body() {
    let udf = CoralSqlFunctionDefinition {
        name: "format_id".to_string(),
        description: "Formats an id".to_string(),
        arguments: vec![udf_argument("id", ManifestDataType::Int64)],
        query: "select concat($id, '-x') as formatted_id".to_string(),
        publish: udf_publish("format_id"),
        result_columns: vec![udf_result_column("formatted_id", DataType::Utf8View)],
        source_names: Vec::new(),
    };
    let runtime = CoralQuery::prepare(&[], test_runtime())
        .await
        .expect("prepare source runtime")
        .with_udfs(vec![udf])
        .await
        .expect("install UDF after source preparation");

    let execution = runtime
        .execute_sql("select formatted_id from udfs.format_id(id => 42)")
        .await
        .expect("expanded UDF body should receive normal DataFusion type coercion");

    let batch = execution
        .batches()
        .first()
        .expect("format UDF should produce one batch");
    assert_eq!(execution.arrow_schema().as_ref(), batch.schema().as_ref());
    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"formatted_id": "42-x"})]
    );
}

#[tokio::test]
async fn published_udf_table_function_normalizes_publish_identifiers() {
    let (_temp, source) = events_source("mixed_case_published_udf_events");
    let runtime = test_runtime().with_udfs(vec![mixed_case_published_min_id_udf(
        "mixed_case_published_udf_events",
    )]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select id from udfs.min_id_events(min_id => 2)",
    )
    .await
    .expect("published udf table function should use normalized identifiers");

    assert_eq!(execution_to_rows(&execution), vec![json!({"id": 2})]);
}

#[tokio::test]
async fn published_udf_table_function_normalizes_argument_identifiers() {
    let (_temp, source) = events_source("mixed_case_arg_udf_events");
    let mut udf = min_id_udf("mixed_case_arg_udf_events");
    udf.arguments
        .first_mut()
        .expect("test UDF should have one argument")
        .name = "minId".to_string();
    udf.query = "select id from mixed_case_arg_udf_events.events where id >= $minId order by id"
        .to_string();
    let runtime = test_runtime().with_udfs(vec![udf]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select id from udfs.min_id_events(minId => 2)",
    )
    .await
    .expect("published UDF argument lookup should normalize SQL identifiers");

    assert_eq!(execution_to_rows(&execution), vec![json!({"id": 2})]);
}

#[tokio::test]
async fn published_udf_table_function_accepts_query_params() {
    let (_temp, source) = events_source("published_param_udf_events");
    let runtime = test_runtime().with_udfs(vec![min_id_udf("published_param_udf_events")]);

    let execution = CoralQuery::execute_sql_with_params(
        &[source],
        runtime,
        "select id from udfs.min_id_events(min_id => $min_id)",
        QueryParameters::from([("min_id".to_string(), QueryParameterValue::integer(2))]),
    )
    .await
    .expect("published udf table function should accept params");

    assert_eq!(execution_to_rows(&execution), vec![json!({"id": 2})]);
}

#[tokio::test]
async fn published_udf_table_function_requires_every_argument() {
    assert_udf_sql_error(
        "missing_arg_udf_events",
        vec![min_id_udf("missing_arg_udf_events")],
        "select id from udfs.min_id_events()",
        "udfs.min_id_events is missing argument 'min_id'",
    )
    .await;
}

#[tokio::test]
async fn published_udf_table_function_rejects_unbound_query_params() {
    assert_udf_sql_error(
        "unbound_param_udf_events",
        vec![min_id_udf("unbound_param_udf_events")],
        "select id from udfs.min_id_events(min_id => $min_id)",
        "udfs.min_id_events argument 'min_id' is bound to parameter $min_id, but no value was provided for it",
    )
    .await;
}

#[tokio::test]
async fn duplicate_udf_table_function_publish_fails() {
    let mut duplicate = min_id_udf("duplicate_udf_events");
    duplicate.name = "duplicate_min_id_events".to_string();

    assert_udf_sql_error(
        "duplicate_udf_events",
        vec![min_id_udf("duplicate_udf_events"), duplicate],
        EVENTS_CALL,
        "duplicate udf table function udfs.min_id_events",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_duplicate_argument_definition() {
    let mut udf = min_id_udf("duplicate_arg_udf_events");
    udf.arguments
        .push(udf_argument("MIN_ID", ManifestDataType::Int64));

    assert_udf_sql_error(
        "duplicate_arg_udf_events",
        vec![udf],
        EVENTS_CALL,
        "udf 'min_id_events' argument 'MIN_ID' is declared more than once",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_requires_result_columns() {
    assert_udf_sql_error(
        "missing_columns_udf_events",
        vec![min_id_udf_without_columns("missing_columns_udf_events")],
        EVENTS_CALL,
        "published udf 'min_id_events' requires declared result columns",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_mismatched_declared_result_name() {
    assert_udf_sql_error(
        "mismatched_column_name_udf_events",
        vec![min_id_udf_with_result_column(
            "mismatched_column_name_udf_events",
            "event_id",
            DataType::Int64,
        )],
        EVENTS_CALL,
        "udf 'min_id_events' declared column 1 as 'event_id' but its SQL body produces 'id'",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_mismatched_declared_result_type() {
    assert_udf_sql_error(
        "mismatched_column_type_udf_events",
        vec![min_id_udf_with_result_column(
            "mismatched_column_type_udf_events",
            "id",
            DataType::Utf8,
        )],
        EVENTS_CALL,
        "udf 'min_id_events' declared column 'id' as Utf8 but its SQL body produces Int64",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_overstated_non_nullable_result() {
    assert_udf_sql_error(
        "mismatched_column_nullability_udf_events",
        vec![min_id_udf_with_non_nullable_result(
            "mismatched_column_nullability_udf_events",
            "select cast(null as bigint) as id",
        )],
        EVENTS_CALL,
        "udf 'min_id_events' declared column 'id' as non-nullable but its SQL body produces nullable values",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_undeclared_body_parameter() {
    assert_udf_sql_error(
        "undeclared_body_param_udf_events",
        vec![min_id_udf_with_body(
            "undeclared_body_param_udf_events",
            "select id from undeclared_body_param_udf_events.events where id >= $min_id and $status is not null order by id",
        )],
        EVENTS_CALL,
        "udf 'udfs.min_id_events' body references parameter '$status' not declared as an argument",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_unknown_function_in_udf_schema() {
    assert_udf_sql_error(
        "unknown_udf_events",
        vec![min_id_udf("unknown_udf_events")],
        "select * from udfs.nope()",
        "unknown udf table function udfs.nope; available functions: udfs.min_id_events",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_rejects_unsupported_modifiers_with_neutral_error() {
    assert_udf_sql_error(
        "modifier_udf_events",
        vec![min_id_udf("modifier_udf_events")],
        "select * from udfs.min_id_events(min_id => 1) WITH ORDINALITY",
        "table function udfs.min_id_events does not support WITH ORDINALITY",
    )
    .await;
}

#[tokio::test]
async fn published_udf_table_function_preserves_inner_limit() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(temp.path(), "first/events.jsonl", &[json!({"id": 1})]);
    write_jsonl_file(temp.path(), "second/events.jsonl", &[json!({"id": 2})]);

    let source = build_source(events_manifest("limited_udf_events", temp.path()));
    let runtime =
        test_runtime().with_udfs(vec![published_limited_events_udf("limited_udf_events")]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select count(*) as count from udfs.limited_events()",
    )
    .await
    .expect("published udf table function should preserve inner limit");

    assert_eq!(execution_to_rows(&execution), vec![json!({"count": 1})]);
}

#[tokio::test]
async fn udf_can_share_source_schema_and_call_source_function_with_params() {
    let server = MockServer::start().await;
    let source = search_source_with_response(
        &server,
        "shared_udf_schema",
        "semantic",
        "Schema-shared udf",
        3.0,
    )
    .await;
    let runtime = CoralQuery::prepare(&[source], test_runtime())
        .await
        .expect("prepare source runtime")
        .with_udfs(vec![review_queue_udf_published_as(
            "shared_udf_schema",
            "shared_udf_schema",
        )])
        .await
        .expect("install shared-schema UDF");

    let execution = runtime
        .execute_sql_with_params(
            "select title, score from shared_udf_schema.review_queue(\
             query => $query, \
             mode => $mode, \
             min_score => 1, \
             payload => NULL, \
             since => TIMESTAMP '2024-01-01T00:00:00Z'\
         )",
            QueryParameters::from([
                (
                    "query".to_string(),
                    QueryParameterValue::string(REVIEW_QUERY),
                ),
                ("mode".to_string(), QueryParameterValue::string("semantic")),
            ]),
        )
        .await
        .expect("schema-shared udf should call source function with params");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"title": "Schema-shared udf", "score": 3.0})]
    );
    assert_eq!(execution.provenance().sources(), ["shared_udf_schema"]);
    let published_function = execution
        .provenance()
        .table_functions()
        .iter()
        .find(|function| function.function_name() == "review_queue")
        .expect("invoked UDF provenance");
    assert_eq!(published_function.source_name(), "shared_udf_schema");
    let body_function = execution
        .provenance()
        .table_functions()
        .iter()
        .find(|function| function.function_name() == "search_issues")
        .expect("UDF body table-function provenance");
    assert_eq!(body_function.source_name(), "shared_udf_schema");
}

#[tokio::test]
async fn unknown_function_in_shared_source_schema_keeps_source_diagnostic() {
    assert_source_udf_sql_error(
        "shared_udf_schema",
        vec![review_queue_udf_published_as(
            "shared_udf_schema",
            "shared_udf_schema",
        )],
        "select * from shared_udf_schema.nope()",
        "unknown source table function shared_udf_schema.nope; available functions: shared_udf_schema.search_issues",
    )
    .await;
}

#[tokio::test]
async fn udf_table_function_cannot_replace_source_table_function() {
    assert_source_udf_sql_error(
        "source_function_collision_search",
        vec![review_queue_udf_published_at(
            "source_function_collision_search",
            "source_function_collision_search",
            "search_issues",
        )],
        "select * from source_function_collision_search.search_issues(query => 'repo:withcoral/coral review', mode => 'hybrid')",
        "udf table function source_function_collision_search.search_issues conflicts with existing table function",
    )
    .await;
}
#[tokio::test]
async fn published_udf_table_function_is_cataloged() {
    let server = MockServer::start().await;
    let source = search_source(&server, "catalog_udf_search");
    let mut udf = published_review_queue_udf("catalog_udf_search");
    udf.publish.guide = "Use this function for review queue lookups.".to_string();
    let runtime = test_runtime().with_udfs(vec![udf]);

    let catalog = CoralQuery::list_catalog(&[source], runtime, None, Some("udfs"))
        .await
        .expect("catalog should include udf function");

    assert!(catalog.tables.is_empty());
    assert_eq!(catalog.table_functions.len(), 1);
    let function = catalog.table_functions.first().expect("udf table function");
    assert_eq!(function.schema_name, "udfs");
    assert_eq!(function.function_name, "review_queue");
    assert_eq!(
        function.guide,
        "Use this function for review queue lookups."
    );
    let [_min_score, _mode, payload, _query, _since] = function.arguments.as_slice() else {
        panic!("expected five review queue arguments");
    };
    assert_eq!(payload.name, "payload");
    assert!(payload.required);
    assert_eq!(function.result_columns.len(), 2);
    assert_eq!(
        function
            .result_columns
            .first()
            .expect("title result column")
            .name,
        "title"
    );
}

#[tokio::test]
async fn published_udf_table_function_catalog_uses_normalized_publish_identifiers() {
    let (_temp, source) = events_source("catalog_mixed_case_udf_events");
    let runtime = test_runtime().with_udfs(vec![mixed_case_published_min_id_udf(
        "catalog_mixed_case_udf_events",
    )]);

    let catalog = CoralQuery::list_catalog(&[source], runtime, None, Some("udfs"))
        .await
        .expect("catalog should include normalized udf function");

    assert!(catalog.tables.is_empty());
    assert_eq!(catalog.table_functions.len(), 1);
    let function = catalog.table_functions.first().expect("udf table function");
    assert_eq!(function.schema_name, "udfs");
    assert_eq!(function.function_name, "min_id_events");
}
