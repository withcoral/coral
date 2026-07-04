use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use coral_engine::{CoralQuery, GraphCypherParameterValue, GraphDeclaration, GraphLiteral};
use serde::Deserialize;
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

#[derive(Debug, Deserialize)]
struct TckSuite {
    suite: String,
    minimum_feature_counts: BTreeMap<String, usize>,
    scenarios: Vec<TckScenario>,
}

#[derive(Debug, Deserialize)]
struct TckScenario {
    id: String,
    feature: String,
    query: String,
    #[serde(default)]
    parameters: BTreeMap<String, Value>,
    #[serde(default)]
    fixture: TckFixture,
    expected: TckExpectation,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TckFixture {
    #[default]
    Baseline,
    Rich,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum TckExpectation {
    Rows { ordered: bool, rows: Vec<Value> },
    Error { contains: String },
}

#[tokio::test]
async fn opencypher_tck_read_baseline_gate() {
    let suite: TckSuite = serde_json::from_str(include_str!(
        "../fixtures/virtual_graph/opencypher_read_baseline.json"
    ))
    .expect("openCypher read baseline should parse");
    assert_eq!(suite.suite, "coral-opencypher-read-baseline");
    assert!(
        suite.scenarios.len() >= 41,
        "baseline should not shrink without an explicit compatibility decision"
    );
    assert_tck_coverage_contract(&suite);

    let temp = TempDir::new().expect("temp dir");
    write_tck_fixture(temp.path());
    crate::harness::write_rich_fixture(temp.path());
    let graph = GraphDeclaration::from_yaml(TCK_GRAPH).expect("TCK graph should parse");
    let rich_graph = GraphDeclaration::from_yaml(crate::harness::RICH_GRAPH)
        .expect("rich fixture graph should parse");

    for scenario in suite.scenarios {
        let (source, graph) = match scenario.fixture {
            TckFixture::Baseline => (build_source(tck_manifest(temp.path())), &graph),
            TckFixture::Rich => (
                build_source(crate::harness::rich_manifest(temp.path())),
                &rich_graph,
            ),
        };
        let result = if scenario.parameters.is_empty() {
            CoralQuery::execute_cypher(&[source], test_runtime(), graph, &scenario.query).await
        } else {
            let parameters = scenario_parameters(&scenario);
            CoralQuery::execute_cypher_with_parameters(
                &[source],
                test_runtime(),
                graph,
                &scenario.query,
                &parameters,
            )
            .await
        };
        match scenario.expected {
            TckExpectation::Rows { ordered, mut rows } => {
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
            TckExpectation::Error { contains } => {
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

fn assert_tck_coverage_contract(suite: &TckSuite) {
    let mut ids = BTreeSet::new();
    let mut feature_counts = BTreeMap::<String, usize>::new();
    for scenario in &suite.scenarios {
        assert!(
            ids.insert(scenario.id.as_str()),
            "duplicate openCypher baseline scenario id: {}",
            scenario.id
        );
        *feature_counts.entry(scenario.feature.clone()).or_default() += 1;
    }

    for (feature, minimum) in &suite.minimum_feature_counts {
        let actual = feature_counts.get(feature).copied().unwrap_or_default();
        assert!(
            actual >= *minimum,
            "openCypher baseline feature {feature} shrank below its declared floor: expected at least {minimum}, found {actual}"
        );
    }
    for feature in feature_counts.keys() {
        assert!(
            suite.minimum_feature_counts.contains_key(feature),
            "openCypher baseline feature {feature} is missing from minimum_feature_counts"
        );
    }
}

fn sort_rows(rows: &mut [Value]) {
    rows.sort_by_key(std::string::ToString::to_string);
}

fn scenario_parameters(scenario: &TckScenario) -> BTreeMap<String, GraphCypherParameterValue> {
    scenario
        .parameters
        .iter()
        .map(|(name, value)| {
            let parameter = if let Some(values) = value.as_array() {
                GraphCypherParameterValue::List(
                    values
                        .iter()
                        .map(|value| scenario_literal(scenario, name, value))
                        .collect(),
                )
            } else {
                GraphCypherParameterValue::Literal(scenario_literal(scenario, name, value))
            };
            (name.clone(), parameter)
        })
        .collect()
}

fn scenario_literal(scenario: &TckScenario, name: &str, value: &Value) -> GraphLiteral {
    match value {
        Value::Null => GraphLiteral::Null,
        Value::Bool(value) => GraphLiteral::Boolean(*value),
        Value::String(value) => GraphLiteral::String(value.clone()),
        Value::Number(value) => {
            if let Some(value) = value.as_i64() {
                return GraphLiteral::Integer(value);
            }
            if let Some(value) = value.as_u64() {
                return GraphLiteral::Integer(i64::try_from(value).unwrap_or_else(|_| {
                    panic!(
                        "scenario {} parameter '{}' integer is outside i64 range: {}",
                        scenario.id, name, value
                    )
                }));
            }
            GraphLiteral::Float(ordered_float::OrderedFloat(value.as_f64().unwrap_or_else(
                || {
                    panic!(
                        "scenario {} parameter '{}' number cannot be represented as f64: {}",
                        scenario.id, name, value
                    )
                },
            )))
        }
        Value::Array(_) | Value::Object(_) => panic!(
            "scenario {} parameter '{}' must be a scalar or scalar list",
            scenario.id, name
        ),
    }
}

fn write_tck_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "people.jsonl",
        &[
            json!({"id": 1, "name": "Alice", "age": 34, "city": "London", "active": true}),
            json!({"id": 2, "name": "Bob", "age": 29, "city": "", "active": false}),
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
    write_jsonl_file(dir, "likes.jsonl", &[]);
}

fn tck_manifest(dir: &Path) -> Value {
    json!({
        "name": "tck",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "people",
                "description": "Synthetic openCypher TCK-style people fixture",
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
                "description": "Synthetic openCypher TCK-style KNOWS edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "knows.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "person_id", "type": "Int64" },
                    { "name": "friend_id", "type": "Int64" },
                    { "name": "since", "type": "Int64" },
                    { "name": "strength", "type": "Float64" }
                ]
            },
            {
                "name": "likes",
                "description": "Synthetic openCypher TCK-style empty LIKES edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "likes.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "person_id", "type": "Int64" },
                    { "name": "liked_person_id", "type": "Int64" }
                ]
            }
        ]
    })
}

const TCK_GRAPH: &str = r"
version: 1
name: opencypher-read-baseline
description: Synthetic graph used by the openCypher TCK-style read baseline gate
nodes:
  - label: Person
    table: { schema: tck, name: people }
    key: id
    properties:
      name: name
      age: age
      city: city
      active: active
relationships:
  - type: KNOWS
    table: { schema: tck, name: knows }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
    properties:
      since: since
      strength: strength
  - type: LIKES
    table: { schema: tck, name: likes }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: liked_person_id }
";
