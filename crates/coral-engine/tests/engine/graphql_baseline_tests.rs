use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use coral_engine::{CoralQuery, GraphDeclaration};
use serde::Deserialize;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

#[derive(Debug, Deserialize)]
struct BaselineSuite {
    suite: String,
    minimum_feature_counts: BTreeMap<String, usize>,
    scenarios: Vec<BaselineScenario>,
}

#[derive(Debug, Deserialize)]
struct BaselineScenario {
    id: String,
    feature: String,
    query: String,
    expected: BaselineExpectation,
    #[serde(default)]
    fixture: BaselineFixture,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BaselineFixture {
    #[default]
    Baseline,
    Rich,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BaselineExpectation {
    Rows { ordered: bool, rows: Vec<Value> },
    Error { contains: String },
}

#[tokio::test]
async fn graphql_read_baseline_gate() {
    let suite: BaselineSuite = serde_json::from_str(include_str!(
        "../fixtures/virtual_graph/graphql_read_baseline.json"
    ))
    .expect("GraphQL read baseline should parse");
    assert_eq!(suite.suite, "coral-graphql-read-baseline");
    assert!(
        suite.scenarios.len() >= 12,
        "baseline should not shrink without an explicit compatibility decision"
    );
    assert_baseline_coverage_contract(&suite);

    let temp = TempDir::new().expect("temp dir");
    write_graphql_fixture(temp.path());
    crate::harness::write_rich_fixture(temp.path());
    let graph = GraphDeclaration::from_yaml(GRAPHQL_BASELINE_GRAPH)
        .expect("GraphQL baseline graph should parse");
    let rich_graph = GraphDeclaration::from_yaml(crate::harness::RICH_GRAPH)
        .expect("rich fixture graph should parse");

    for scenario in suite.scenarios {
        let (source, graph) = match scenario.fixture {
            BaselineFixture::Baseline => (build_source(graphql_manifest(temp.path())), &graph),
            BaselineFixture::Rich => (
                build_source(crate::harness::rich_manifest(temp.path())),
                &rich_graph,
            ),
        };
        let result =
            CoralQuery::execute_graphql(&[source], test_runtime(), graph, &scenario.query).await;
        match scenario.expected {
            BaselineExpectation::Rows { ordered, mut rows } => {
                let execution = result.unwrap_or_else(|error| {
                    panic!(
                        "scenario {} ({}) should execute: {error}",
                        scenario.id, scenario.feature
                    )
                });
                let mut actual = execution_to_rows(execution.execution());
                if !ordered {
                    sort_rows(&mut actual);
                    sort_rows(&mut rows);
                }
                assert_eq!(
                    actual,
                    rows,
                    "scenario {} ({}) produced unexpected rows\ntranslated SQL:\n{}",
                    scenario.id,
                    scenario.feature,
                    execution.translated_sql()
                );
            }
            BaselineExpectation::Error { contains } => {
                let error = result.expect_err(&format!(
                    "scenario {} ({}) should be rejected",
                    scenario.id, scenario.feature
                ));
                let message = error.to_string();
                assert!(
                    message.contains(&contains),
                    "scenario {} ({}) expected error containing {contains:?}, got {message:?}",
                    scenario.id,
                    scenario.feature
                );
            }
        }
    }
}

fn assert_baseline_coverage_contract(suite: &BaselineSuite) {
    let mut ids = BTreeSet::new();
    let mut feature_counts = BTreeMap::<String, usize>::new();
    for scenario in &suite.scenarios {
        assert!(
            ids.insert(scenario.id.as_str()),
            "duplicate GraphQL baseline scenario id: {}",
            scenario.id
        );
        *feature_counts.entry(scenario.feature.clone()).or_default() += 1;
    }

    for (feature, minimum) in &suite.minimum_feature_counts {
        let actual = feature_counts.get(feature).copied().unwrap_or_default();
        assert!(
            actual >= *minimum,
            "GraphQL baseline feature {feature} shrank below its declared floor: expected at least {minimum}, found {actual}"
        );
    }
    for feature in feature_counts.keys() {
        assert!(
            suite.minimum_feature_counts.contains_key(feature),
            "GraphQL baseline feature {feature} is missing from minimum_feature_counts"
        );
    }
}

fn sort_rows(rows: &mut [Value]) {
    rows.sort_by_key(std::string::ToString::to_string);
}

fn write_graphql_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "people.jsonl",
        &[
            json!({"id": 1, "name": "Alice", "age": 34, "city": "London", "active": true}),
            json!({"id": 2, "name": "Bob", "age": 29, "city": "London", "active": false}),
            json!({"id": 3, "name": "Carol", "age": 41, "city": "Paris", "active": true}),
        ],
    );
    write_jsonl_file(
        dir,
        "knows.jsonl",
        &[
            json!({"id": 100, "person_id": 1, "friend_id": 2, "since": 2020, "strength": 0.9}),
            json!({"id": 101, "person_id": 2, "friend_id": 3, "since": 2021, "strength": 0.7}),
            json!({"id": 102, "person_id": 1, "friend_id": 3, "since": 2022, "strength": 0.3}),
        ],
    );
}

fn graphql_manifest(dir: &Path) -> Value {
    json!({
        "name": "graphql_baseline",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "people",
                "description": "Synthetic GraphQL baseline people fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "people.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "name", "type": "Utf8" },
                    { "name": "age", "type": "Int64" },
                    { "name": "city", "type": "Utf8" },
                    { "name": "active", "type": "Boolean" }
                ]
            },
            {
                "name": "knows",
                "description": "Synthetic GraphQL baseline KNOWS edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "knows.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "person_id", "type": "Int64" },
                    { "name": "friend_id", "type": "Int64" },
                    { "name": "since", "type": "Int64" },
                    { "name": "strength", "type": "Float64" }
                ]
            }
        ]
    })
}

const GRAPHQL_BASELINE_GRAPH: &str = r"
version: 1
name: graphql-read-baseline
description: Synthetic graph used by the GraphQL read baseline gate
nodes:
  - label: Person
    table: { schema: graphql_baseline, name: people }
    key: id
    properties:
      name: name
      age: age
      city: city
      active: active
relationships:
  - type: KNOWS
    table: { schema: graphql_baseline, name: knows }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
    properties:
      since: since
      strength: strength
";
