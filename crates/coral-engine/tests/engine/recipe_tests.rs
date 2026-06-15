use std::collections::BTreeMap;
use std::path::Path;

use arrow::datatypes::DataType;
use coral_engine::{
    CoralQuery, CoreError, QueryParameterValue, QueryParameters, RecipeRuntimeArgument,
    RecipeRuntimeArgumentType, RecipeRuntimeArgumentValue, RecipeRuntimeDefinition,
    RecipeRuntimeImplementation, RecipeRuntimePublish, RecipeRuntimeResultColumn,
    RecipeRuntimeTableFunctionPublish,
};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

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

fn recipe_publish(name: &str) -> RecipeRuntimePublish {
    RecipeRuntimePublish {
        table_function: RecipeRuntimeTableFunctionPublish {
            schema: "recipes".to_string(),
            name: name.to_string(),
            description: String::new(),
        },
    }
}

fn review_queue_recipe(source_name: &str) -> RecipeRuntimeDefinition {
    RecipeRuntimeDefinition {
        name: "review_queue".to_string(),
        description: "Review queue".to_string(),
        arguments: vec![
            RecipeRuntimeArgument {
                name: "query".to_string(),
                data_type: RecipeRuntimeArgumentType::String,
                required: true,
                description: String::new(),
            },
            RecipeRuntimeArgument {
                name: "mode".to_string(),
                data_type: RecipeRuntimeArgumentType::String,
                required: false,
                description: String::new(),
            },
        ],
        implementation: RecipeRuntimeImplementation::CoralSql {
            query: format!(
                "select title, score from {source_name}.search_issues(q => $query, mode => $mode)"
            ),
        },
        publish: recipe_publish("review_queue"),
        result_columns: Vec::new(),
    }
}

fn published_review_queue_recipe(source_name: &str) -> RecipeRuntimeDefinition {
    let mut recipe = review_queue_recipe(source_name);
    recipe.result_columns = vec![
        RecipeRuntimeResultColumn {
            name: "title".to_string(),
            data_type: "Utf8".to_string(),
            nullable: true,
            description: String::new(),
        },
        RecipeRuntimeResultColumn {
            name: "score".to_string(),
            data_type: "Float64".to_string(),
            nullable: true,
            description: String::new(),
        },
    ];
    recipe
}

fn review_queue_recipe_published_as(source_name: &str, schema: &str) -> RecipeRuntimeDefinition {
    review_queue_recipe_published_at(source_name, schema, "review_queue")
}

fn review_queue_recipe_published_at(
    source_name: &str,
    schema: &str,
    name: &str,
) -> RecipeRuntimeDefinition {
    let mut recipe = published_review_queue_recipe(source_name);
    recipe.publish = RecipeRuntimePublish {
        table_function: RecipeRuntimeTableFunctionPublish {
            schema: schema.to_string(),
            name: name.to_string(),
            description: String::new(),
        },
    };
    recipe
}

fn published_limited_events_recipe(source_name: &str) -> RecipeRuntimeDefinition {
    RecipeRuntimeDefinition {
        name: "limited_events".to_string(),
        description: "Limited events".to_string(),
        arguments: Vec::new(),
        implementation: RecipeRuntimeImplementation::CoralSql {
            query: format!("select id from {source_name}.events limit 1"),
        },
        publish: recipe_publish("limited_events"),
        result_columns: vec![RecipeRuntimeResultColumn {
            name: "id".to_string(),
            data_type: "Int64".to_string(),
            nullable: true,
            description: String::new(),
        }],
    }
}

fn review_queue_args(mode: &str) -> BTreeMap<String, RecipeRuntimeArgumentValue> {
    BTreeMap::from([
        (
            "query".to_string(),
            RecipeRuntimeArgumentValue::String("repo:withcoral/coral review".to_string()),
        ),
        (
            "mode".to_string(),
            RecipeRuntimeArgumentValue::String(mode.to_string()),
        ),
    ])
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

#[tokio::test]
async fn infer_recipe_schema_uses_explicit_validation_args() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "schema_recipe_search",
        &server.uri(),
    ));

    let schema = CoralQuery::infer_recipe_schema(
        &[source],
        test_runtime(),
        review_queue_recipe("schema_recipe_search"),
        review_queue_args("lexical"),
    )
    .await
    .expect("recipe schema should infer");

    let fields = schema.fields();
    assert_eq!(fields.len(), 2);
    let title = fields.first().expect("title field");
    let score = fields.get(1).expect("score field");
    assert_eq!(title.name(), "title");
    assert_eq!(title.data_type(), &DataType::Utf8);
    assert_eq!(score.name(), "score");
    assert_eq!(score.data_type(), &DataType::Float64);
}

#[tokio::test]
async fn infer_recipe_schema_rejects_missing_validation_args() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "missing_arg_schema_recipe_search",
        &server.uri(),
    ));

    let error = CoralQuery::infer_recipe_schema(
        &[source],
        test_runtime(),
        review_queue_recipe("missing_arg_schema_recipe_search"),
        BTreeMap::new(),
    )
    .await
    .expect_err("missing validation args should fail");

    assert!(
        error
            .to_string()
            .contains("recipe 'review_queue' is missing required argument 'query'"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn validate_recipe_executes_explicit_validation_call() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "repo:withcoral/coral review"))
        .and(query_param("search_type", "lexical"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Review needed",
                "score": 7.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest(
        "validate_recipe_search",
        &server.uri(),
    ));

    let schema = CoralQuery::validate_recipe(
        &[source],
        test_runtime(),
        review_queue_recipe("validate_recipe_search"),
        review_queue_args("lexical"),
    )
    .await
    .expect("recipe validation should infer schema and execute");

    assert_eq!(schema.fields().len(), 2);
}

#[tokio::test]
async fn published_recipe_table_function_executes_recipe_sql() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "repo:withcoral/coral review"))
        .and(query_param("search_type", "hybrid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Review needed",
                "score": 7.5
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest(
        "published_recipe_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![published_review_queue_recipe(
        "published_recipe_search",
    )]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select title, score from recipes.review_queue(query => 'repo:withcoral/coral review', mode => 'hybrid')",
    )
    .await
    .expect("published recipe table function should execute");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "title": "Review needed",
            "score": 7.5
        })]
    );
}

#[tokio::test]
async fn published_recipe_table_function_accepts_query_params() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "repo:withcoral/coral review"))
        .and(query_param("search_type", "semantic"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{
                "title": "Param review",
                "score": 8.25
            }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest(
        "published_param_recipe_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![published_review_queue_recipe(
        "published_param_recipe_search",
    )]);

    let execution = CoralQuery::execute_sql_with_params(
        &[source],
        runtime,
        "select title, score from recipes.review_queue(query => $query, mode => $mode)",
        QueryParameters::from([
            (
                "query".to_string(),
                QueryParameterValue::String("repo:withcoral/coral review".to_string()),
            ),
            (
                "mode".to_string(),
                QueryParameterValue::String("semantic".to_string()),
            ),
        ]),
    )
    .await
    .expect("published recipe table function should accept params");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "title": "Param review",
            "score": 8.25
        })]
    );
}

#[tokio::test]
async fn recipe_function_can_share_source_schema_with_source_functions() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "repo:withcoral/coral review"))
        .and(query_param("search_type", "hybrid"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "items": [{ "title": "Schema-shared recipe", "score": 3.0 }]
        })))
        .expect(1)
        .mount(&server)
        .await;

    let source = build_source(search_function_manifest(
        "shared_recipe_schema",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![review_queue_recipe_published_as(
        "shared_recipe_schema",
        "shared_recipe_schema",
    )]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select title, score from shared_recipe_schema.review_queue(query => 'repo:withcoral/coral review', mode => 'hybrid')",
    )
    .await
    .expect("recipe should plan before source-schema unknown-function handling");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"title": "Schema-shared recipe", "score": 3.0})]
    );
}

#[tokio::test]
async fn recipe_table_function_rejects_unknown_function_in_recipe_schema() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "unknown_recipe_function_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![published_review_queue_recipe(
        "unknown_recipe_function_search",
    )]);

    let error = CoralQuery::execute_sql(&[source], runtime, "select * from recipes.nope()")
        .await
        .expect_err("unknown recipe table function should fail");

    assert_invalid_input_contains(
        error,
        "unknown recipe table function recipes.nope; available functions: recipes.review_queue",
    );
}

#[tokio::test]
async fn unknown_function_in_shared_source_schema_keeps_source_diagnostic() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "shared_recipe_schema",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![review_queue_recipe_published_as(
        "shared_recipe_schema",
        "shared_recipe_schema",
    )]);

    let error = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select * from shared_recipe_schema.nope()",
    )
    .await
    .expect_err("unknown source table function should fail");

    assert_invalid_input_contains(
        error,
        "unknown source table function shared_recipe_schema.nope; available functions: shared_recipe_schema.search_issues",
    );
}

#[tokio::test]
async fn duplicate_recipe_table_function_publish_fails() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "duplicate_recipe_publish_search",
        &server.uri(),
    ));
    let mut duplicate = published_review_queue_recipe("duplicate_recipe_publish_search");
    duplicate.name = "duplicate_review_queue".to_string();
    let runtime = test_runtime().with_recipes(vec![
        published_review_queue_recipe("duplicate_recipe_publish_search"),
        duplicate,
    ]);

    let error = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select * from recipes.review_queue(query => 'repo:withcoral/coral review', mode => 'hybrid')",
    )
    .await
    .expect_err("duplicate recipe publish targets should fail");

    assert_invalid_input_contains(
        error,
        "duplicate recipe table function recipes.review_queue",
    );
}

#[tokio::test]
async fn recipe_table_function_cannot_replace_source_table_function() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "source_function_collision_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![review_queue_recipe_published_at(
        "source_function_collision_search",
        "source_function_collision_search",
        "search_issues",
    )]);

    let error = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select * from source_function_collision_search.search_issues(query => 'repo:withcoral/coral review', mode => 'hybrid')",
    )
    .await
    .expect_err("recipe publish target should not replace a source table function");

    assert_invalid_input_contains(
        error,
        "recipe table function source_function_collision_search.search_issues conflicts with existing table function",
    );
}

#[tokio::test]
async fn recipe_table_function_rejects_unsupported_modifiers_with_neutral_error() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "modifier_recipe_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![published_review_queue_recipe(
        "modifier_recipe_search",
    )]);

    let error = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select * from recipes.review_queue(query => 'repo:withcoral/coral review', mode => 'hybrid') WITH ORDINALITY",
    )
    .await
    .expect_err("unsupported recipe table-function modifiers should fail");

    assert_invalid_input_contains(
        error,
        "table function recipes.review_queue does not support WITH ORDINALITY",
    );
}

#[tokio::test]
async fn published_recipe_table_function_preserves_inner_limit() {
    let temp = tempfile::tempdir().expect("temp dir");
    write_jsonl_file(temp.path(), "first/events.jsonl", &[json!({"id": 1})]);
    write_jsonl_file(temp.path(), "second/events.jsonl", &[json!({"id": 2})]);

    let source = build_source(events_manifest("limited_recipe_events", temp.path()));
    let runtime = test_runtime().with_recipes(vec![published_limited_events_recipe(
        "limited_recipe_events",
    )]);

    let execution = CoralQuery::execute_sql(
        &[source],
        runtime,
        "select count(*) as count from recipes.limited_events()",
    )
    .await
    .expect("published recipe table function should preserve inner limit");

    assert_eq!(execution_to_rows(&execution), vec![json!({"count": 1})]);
}

#[tokio::test]
async fn published_recipe_table_function_is_cataloged() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "catalog_recipe_search",
        &server.uri(),
    ));
    let runtime =
        test_runtime().with_recipes(vec![published_review_queue_recipe("catalog_recipe_search")]);

    let catalog = CoralQuery::list_catalog(&[source], runtime, Some("recipes"))
        .await
        .expect("catalog should include recipe function");

    assert!(catalog.tables.is_empty());
    assert_eq!(catalog.table_functions.len(), 1);
    let function = catalog
        .table_functions
        .first()
        .expect("recipe table function");
    assert_eq!(function.schema_name, "recipes");
    assert_eq!(function.function_name, "review_queue");
    assert_eq!(function.arguments.len(), 2);
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
