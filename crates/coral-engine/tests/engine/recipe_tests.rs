use std::collections::BTreeMap;

use arrow::datatypes::DataType;
use coral_engine::{
    CoralQuery, QueryParameterValue, QueryParameters, RecipeRuntimeArgument,
    RecipeRuntimeArgumentType, RecipeRuntimeArgumentValue, RecipeRuntimeCall,
    RecipeRuntimeDefinition, RecipeRuntimeImplementation, RecipeRuntimePublish,
    RecipeRuntimeResultColumn,
};
use serde_json::{Value, json};
use wiremock::matchers::{method, path, query_param, query_param_is_missing};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::harness::{build_source, execution_to_rows, test_runtime};

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
        publish: Vec::new(),
        result_columns: Vec::new(),
    }
}

fn published_review_queue_recipe(source_name: &str) -> RecipeRuntimeDefinition {
    let mut recipe = review_queue_recipe(source_name);
    recipe.publish = vec![RecipeRuntimePublish::TableFunction {
        schema: "recipes".to_string(),
        name: "review_queue".to_string(),
        description: String::new(),
    }];
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

#[tokio::test]
async fn execute_recipe_runs_param_bound_coral_sql() {
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

    let source = build_source(search_function_manifest("recipe_search", &server.uri()));
    let runtime = test_runtime().with_recipes(vec![review_queue_recipe("recipe_search")]);
    let call = RecipeRuntimeCall {
        recipe_name: "review_queue".to_string(),
        arguments: BTreeMap::from([
            (
                "query".to_string(),
                RecipeRuntimeArgumentValue::String("repo:withcoral/coral review".to_string()),
            ),
            (
                "mode".to_string(),
                RecipeRuntimeArgumentValue::String("hybrid".to_string()),
            ),
        ]),
    };

    let execution = CoralQuery::execute_recipe(&[source], runtime, call)
        .await
        .expect("recipe should execute");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "title": "Review needed",
            "score": 7.5
        })]
    );
}

#[tokio::test]
async fn execute_recipe_binds_missing_optional_argument_as_null() {
    let server = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/search/issues"))
        .and(query_param("q", "repo:withcoral/coral review"))
        .and(query_param_is_missing("search_type"))
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
        "optional_recipe_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![review_queue_recipe("optional_recipe_search")]);
    let call = RecipeRuntimeCall {
        recipe_name: "review_queue".to_string(),
        arguments: BTreeMap::from([(
            "query".to_string(),
            RecipeRuntimeArgumentValue::String("repo:withcoral/coral review".to_string()),
        )]),
    };

    let execution = CoralQuery::execute_recipe(&[source], runtime, call)
        .await
        .expect("recipe should execute");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({
            "title": "Review needed",
            "score": 7.5
        })]
    );
}

#[tokio::test]
async fn execute_recipe_rejects_unknown_recipe() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "unknown_recipe_search",
        &server.uri(),
    ));
    let runtime = test_runtime().with_recipes(vec![review_queue_recipe("unknown_recipe_search")]);
    let call = RecipeRuntimeCall {
        recipe_name: "missing_recipe".to_string(),
        arguments: BTreeMap::new(),
    };

    let error = CoralQuery::execute_recipe(&[source], runtime, call)
        .await
        .expect_err("unknown recipe should fail");

    assert_eq!(
        error.to_string(),
        "invalid input: unknown recipe 'missing_recipe'"
    );
}

#[tokio::test]
async fn execute_recipe_rejects_invalid_arguments() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "invalid_arg_recipe_search",
        &server.uri(),
    ));
    let runtime =
        test_runtime().with_recipes(vec![review_queue_recipe("invalid_arg_recipe_search")]);
    let call = RecipeRuntimeCall {
        recipe_name: "review_queue".to_string(),
        arguments: BTreeMap::from([("query".to_string(), RecipeRuntimeArgumentValue::Integer(42))]),
    };

    let error = CoralQuery::execute_recipe(&[source], runtime, call)
        .await
        .expect_err("invalid recipe args should fail");

    assert!(
        error
            .to_string()
            .contains("recipe 'review_queue' argument 'query' expected string, got integer"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn infer_recipe_schema_uses_param_bound_coral_sql() {
    let server = MockServer::start().await;
    let source = build_source(search_function_manifest(
        "schema_recipe_search",
        &server.uri(),
    ));

    let schema = CoralQuery::infer_recipe_schema(
        &[source],
        test_runtime(),
        review_queue_recipe("schema_recipe_search"),
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
