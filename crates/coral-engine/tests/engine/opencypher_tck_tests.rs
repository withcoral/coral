use std::{
    collections::{BTreeMap, BTreeSet},
    fmt::Write as _,
    path::Path,
};

use coral_engine::{
    CoralQuery, GraphCypherParameterValue, GraphDeclaration, GraphLiteral, QueryExecution,
    QuerySource,
};
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

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum TckFixture {
    #[default]
    Baseline,
    ConsecutiveOptional,
    Rich,
    WideProperties,
    Match6SingleNode,
    Match7OptionalPath,
    PathThroughWith,
    StagedRelationshipCarry,
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
    write_all_tck_fixtures(temp.path());
    let graphs = TckGraphs::load();

    for scenario in suite.scenarios {
        let source = tck_source(scenario.fixture, temp.path());
        let graph = graphs.graph(scenario.fixture);
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
                let mut actual = execution_to_tck_rows(execution.execution(), &rows);
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

struct TckGraphs {
    baseline: GraphDeclaration,
    consecutive_optional: GraphDeclaration,
    match6_single_node: GraphDeclaration,
    match7_optional_path: GraphDeclaration,
    path_through_with: GraphDeclaration,
    staged_relationship_carry: GraphDeclaration,
    rich: GraphDeclaration,
    wide_properties: GraphDeclaration,
}

impl TckGraphs {
    fn load() -> Self {
        Self {
            baseline: GraphDeclaration::from_yaml(TCK_GRAPH).expect("TCK graph should parse"),
            consecutive_optional: GraphDeclaration::from_yaml(CONSECUTIVE_OPTIONAL_GRAPH)
                .expect("consecutive optional graph should parse"),
            match6_single_node: GraphDeclaration::from_yaml(MATCH6_SINGLE_NODE_GRAPH)
                .expect("Match6 graph should parse"),
            match7_optional_path: GraphDeclaration::from_yaml(MATCH7_OPTIONAL_PATH_GRAPH)
                .expect("Match7 graph should parse"),
            path_through_with: GraphDeclaration::from_yaml(PATH_THROUGH_WITH_GRAPH)
                .expect("path carry graph should parse"),
            staged_relationship_carry: GraphDeclaration::from_yaml(STAGED_RELATIONSHIP_CARRY_GRAPH)
                .expect("staged relationship carry graph should parse"),
            rich: GraphDeclaration::from_yaml(crate::harness::RICH_GRAPH)
                .expect("rich fixture graph should parse"),
            wide_properties: wide_property_test_graph(65),
        }
    }

    fn graph(&self, fixture: TckFixture) -> &GraphDeclaration {
        match fixture {
            TckFixture::Baseline => &self.baseline,
            TckFixture::ConsecutiveOptional => &self.consecutive_optional,
            TckFixture::Match6SingleNode => &self.match6_single_node,
            TckFixture::Match7OptionalPath => &self.match7_optional_path,
            TckFixture::PathThroughWith => &self.path_through_with,
            TckFixture::StagedRelationshipCarry => &self.staged_relationship_carry,
            TckFixture::Rich => &self.rich,
            TckFixture::WideProperties => &self.wide_properties,
        }
    }
}

fn write_all_tck_fixtures(dir: &Path) {
    write_tck_fixture(dir);
    write_consecutive_optional_fixture(dir);
    write_match6_single_node_fixture(dir);
    write_match7_optional_path_fixture(dir);
    write_path_through_with_fixture(dir);
    write_staged_relationship_carry_fixture(dir);
    write_wide_property_fixture(dir);
    crate::harness::write_rich_fixture(dir);
}

fn tck_source(fixture: TckFixture, dir: &Path) -> QuerySource {
    match fixture {
        TckFixture::Baseline => build_source(tck_manifest(dir)),
        TckFixture::ConsecutiveOptional => build_source(consecutive_optional_manifest(dir)),
        TckFixture::Match6SingleNode => build_source(match6_single_node_manifest(dir)),
        TckFixture::Match7OptionalPath => build_source(match7_optional_path_manifest(dir)),
        TckFixture::PathThroughWith => build_source(path_through_with_manifest(dir)),
        TckFixture::StagedRelationshipCarry => {
            build_source(staged_relationship_carry_manifest(dir))
        }
        TckFixture::Rich => build_source(crate::harness::rich_manifest(dir)),
        TckFixture::WideProperties => build_source(wide_property_manifest(dir)),
    }
}

fn execution_to_tck_rows(execution: &QueryExecution, expected_rows: &[Value]) -> Vec<Value> {
    let mut actual_rows = execution_to_rows(execution);
    for row in &mut actual_rows {
        normalize_tck_path_values(row);
    }
    let projected_columns = execution
        .schema()
        .iter()
        .map(|column| column.name.as_str())
        .collect::<BTreeSet<_>>();

    for (actual_row, expected_row) in actual_rows.iter_mut().zip(expected_rows) {
        let (Value::Object(actual), Value::Object(expected)) = (actual_row, expected_row) else {
            continue;
        };
        for (key, expected_value) in expected {
            if expected_value.is_null()
                && projected_columns.contains(key.as_str())
                && !actual.contains_key(key)
            {
                actual.insert(key.clone(), Value::Null);
            }
        }
    }

    actual_rows
}

fn normalize_tck_path_values(value: &mut Value) {
    match value {
        Value::Object(object) => {
            if let Some(path_value) = path_value_from_struct_object(object) {
                *value = path_value;
                return;
            }
            for child in object.values_mut() {
                normalize_tck_path_values(child);
            }
        }
        Value::Array(values) => {
            for child in values {
                normalize_tck_path_values(child);
            }
        }
        Value::Null | Value::Bool(_) | Value::Number(_) | Value::String(_) => {}
    }
}

fn path_value_from_struct_object(object: &serde_json::Map<String, Value>) -> Option<Value> {
    let node_ids = object.get("node_ids")?.as_array()?;
    let relationship_ids = object.get("relationship_ids")?.as_array()?;
    if node_ids.len() != relationship_ids.len().saturating_add(1) {
        return None;
    }

    let mut elements = Vec::with_capacity(node_ids.len() + relationship_ids.len());
    for (index, node_id) in node_ids.iter().enumerate() {
        elements.push(json!({ "node_id": node_id.clone() }));
        if let Some(relationship_id) = relationship_ids.get(index) {
            elements.push(json!({ "relationship_id": relationship_id.clone() }));
        }
    }
    Some(Value::Array(elements))
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

fn write_consecutive_optional_fixture(dir: &Path) {
    write_jsonl_file(dir, "multiopt_exists.jsonl", &[json!({"id": 1, "num": 7})]);
    write_jsonl_file(dir, "multiopt_does_not_exist.jsonl", &[]);
    write_jsonl_file(dir, "multiopt_not_there.jsonl", &[]);
    write_jsonl_file(dir, "multiopt_empty_a.jsonl", &[]);
    write_jsonl_file(dir, "multiopt_empty_b.jsonl", &[]);
    write_jsonl_file(dir, "multiopt_nor_this.jsonl", &[]);
    write_jsonl_file(
        dir,
        "multiopt_owners.jsonl",
        &[json!({"id": 1, "name": "Ada Lovelace"})],
    );
    write_jsonl_file(
        dir,
        "multiopt_alt_owners.jsonl",
        &[json!({"id": 2, "name": "Bea Admin"})],
    );
    write_jsonl_file(
        dir,
        "multiopt_services.jsonl",
        &[json!({"id": 10, "name": "billing-api"})],
    );
    write_jsonl_file(
        dir,
        "multiopt_ownerships.jsonl",
        &[
            json!({"id": 100, "owner_id": 1, "service_id": 10}),
            json!({"id": 101, "owner_id": 1, "service_id": 10}),
        ],
    );
    write_jsonl_file(
        dir,
        "multiopt_alt_ownerships.jsonl",
        &[json!({"id": 200, "owner_id": 2, "service_id": 10})],
    );
}

fn write_match6_single_node_fixture(dir: &Path) {
    write_jsonl_file(dir, "match6_nodes.jsonl", &[json!({"id": 1})]);
}

fn write_match7_optional_path_fixture(dir: &Path) {
    write_jsonl_file(dir, "match7_a.jsonl", &[json!({"id": 1, "num": 42})]);
    write_jsonl_file(dir, "match7_b.jsonl", &[json!({"id": 2, "num": 46})]);
    write_jsonl_file(dir, "match7_x.jsonl", &[]);
}

fn write_path_through_with_fixture(dir: &Path) {
    write_jsonl_file(dir, "path_with_a.jsonl", &[json!({"id": 1})]);
    write_jsonl_file(dir, "path_with_b.jsonl", &[json!({"id": 2})]);
    write_jsonl_file(
        dir,
        "path_with_x.jsonl",
        &[json!({"id": 100, "a_id": 1, "b_id": 2})],
    );
}

fn write_staged_relationship_carry_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "staged_relcarry_nodes.jsonl",
        &[
            json!({"id": 1, "name": "Alpha"}),
            json!({"id": 2, "name": "Beta"}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_relcarry_edges.jsonl",
        &[json!({"id": 10, "from_id": 1, "to_id": 2})],
    );
}

fn write_wide_property_fixture(dir: &Path) {
    let mut row = serde_json::Map::new();
    row.insert("id".to_string(), json!(1));
    for index in 0..65 {
        row.insert(format!("p{index:02}"), json!(format!("v{index:02}")));
    }
    write_jsonl_file(dir, "wide_nodes.jsonl", &[Value::Object(row)]);
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

fn wide_property_manifest(dir: &Path) -> Value {
    let columns = std::iter::once(json!({ "name": "id", "type": "Int64" }))
        .chain((0..65).map(|index| {
            json!({
                "name": format!("p{index:02}"),
                "type": "Utf8"
            })
        }))
        .collect::<Vec<_>>();
    json!({
        "name": "wide",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "wide_nodes",
                "description": "Synthetic wide property fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "wide_nodes.jsonl" },
                "columns": columns
            }
        ]
    })
}

fn consecutive_optional_manifest(dir: &Path) -> Value {
    let mut tables = consecutive_optional_base_tables(dir);
    tables.extend(consecutive_optional_keyed_relationship_tables(dir));
    json!({
        "name": "multiopt",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": tables
    })
}

fn consecutive_optional_base_tables(dir: &Path) -> Vec<Value> {
    vec![
        json!({
            "name": "does_exist",
            "description": "Matched side for consecutive leading OPTIONAL MATCH TCK scenarios",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_exists.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "num", "type": "Int64" }
            ]
        }),
        json!({
            "name": "does_not_exist",
            "description": "Empty side for consecutive leading OPTIONAL MATCH TCK scenarios",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_does_not_exist.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "num", "type": "Int64" }
            ]
        }),
        json!({
            "name": "not_there",
            "description": "Empty NotThere node table for consecutive OPTIONAL repros",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_not_there.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "num", "type": "Int64" }
            ]
        }),
        json!({
            "name": "empty_a",
            "description": "First empty label-alternative table",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_empty_a.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "num", "type": "Int64" }
            ]
        }),
        json!({
            "name": "empty_b",
            "description": "Second empty label-alternative table",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_empty_b.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "num", "type": "Int64" }
            ]
        }),
        json!({
            "name": "nor_this",
            "description": "Empty NOR_THIS relationship table",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_nor_this.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "source_id", "type": "Int64" },
                { "name": "target_id", "type": "Int64" }
            ]
        }),
    ]
}

fn consecutive_optional_keyed_relationship_tables(dir: &Path) -> Vec<Value> {
    vec![
        json!({
            "name": "owners",
            "description": "Owner node table for consecutive OPTIONAL keyed relationship repros",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_owners.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" }
            ]
        }),
        json!({
            "name": "alt_owners",
            "description": "Alternate owner node table for consecutive OPTIONAL keyed relationship repros",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_alt_owners.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" }
            ]
        }),
        json!({
            "name": "services",
            "description": "Service node table for consecutive OPTIONAL keyed relationship repros",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_services.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "name", "type": "Utf8" }
            ]
        }),
        json!({
            "name": "ownerships",
            "description": "Keyed OWNS relationship table for consecutive OPTIONAL repros",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_ownerships.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "owner_id", "type": "Int64" },
                { "name": "service_id", "type": "Int64" }
            ]
        }),
        json!({
            "name": "alt_ownerships",
            "description": "Alternate keyed OWNS relationship table for consecutive OPTIONAL repros",
            "format": "jsonl",
            "source": { "location": dir_url(dir), "glob": "multiopt_alt_ownerships.jsonl" },
            "columns": [
                { "name": "id", "type": "Int64" },
                { "name": "owner_id", "type": "Int64" },
                { "name": "service_id", "type": "Int64" }
            ]
        }),
    ]
}

fn match6_single_node_manifest(dir: &Path) -> Value {
    json!({
        "name": "match6",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "nodes",
                "description": "Single-node Match6 path-value fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "match6_nodes.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" }
                ]
            }
        ]
    })
}

fn match7_optional_path_manifest(dir: &Path) -> Value {
    json!({
        "name": "match7",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "a_nodes",
                "description": "Match7 A nodes",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "match7_a.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "num", "type": "Int64" }
                ]
            },
            {
                "name": "b_nodes",
                "description": "Match7 B nodes",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "match7_b.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "num", "type": "Int64" }
                ]
            },
            {
                "name": "x_edges",
                "description": "Empty Match7 X relationship table",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "match7_x.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "a_id", "type": "Int64" },
                    { "name": "b_id", "type": "Int64" }
                ]
            }
        ]
    })
}

fn path_through_with_manifest(dir: &Path) -> Value {
    json!({
        "name": "pathwith",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "a_nodes",
                "description": "Path-through-WITH A nodes",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "path_with_a.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" }
                ]
            },
            {
                "name": "b_nodes",
                "description": "Path-through-WITH B nodes",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "path_with_b.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" }
                ]
            },
            {
                "name": "x_edges",
                "description": "Path-through-WITH X relationship table",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "path_with_x.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "a_id", "type": "Int64" },
                    { "name": "b_id", "type": "Int64" }
                ]
            }
        ]
    })
}

fn staged_relationship_carry_manifest(dir: &Path) -> Value {
    json!({
        "name": "staged_relcarry",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "nodes",
                "description": "Staged relationship carry node fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_relcarry_nodes.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "name", "type": "Utf8" }
                ]
            },
            {
                "name": "edges",
                "description": "Staged relationship carry keyed edge fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_relcarry_edges.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "from_id", "type": "Int64" },
                    { "name": "to_id", "type": "Int64" }
                ]
            }
        ]
    })
}

fn wide_property_test_graph(property_count: usize) -> GraphDeclaration {
    let mut properties = String::new();
    for index in 0..property_count {
        writeln!(&mut properties, "      p{index:02}: p{index:02}")
            .expect("writing property YAML should not fail");
    }
    GraphDeclaration::from_yaml(&format!(
        r"
version: 1
name: wide-property-tck
nodes:
  - label: Wide
    table: {{ schema: wide, name: wide_nodes }}
    key: id
    properties:
{properties}
"
    ))
    .expect("wide property TCK graph should parse")
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

const CONSECUTIVE_OPTIONAL_GRAPH: &str = r"
version: 1
name: consecutive-optional
description: Synthetic graph for consecutive leading OPTIONAL MATCH TCK scenarios
nodes:
  - label: DoesExist
    table: { schema: multiopt, name: does_exist }
    key: id
    properties:
      num: num
  - label: DoesNotExist
    table: { schema: multiopt, name: does_not_exist }
    key: id
    properties:
      num: num
  - label: NotThere
    table: { schema: multiopt, name: not_there }
    key: id
    properties:
      num: num
  - label: EmptyA
    table: { schema: multiopt, name: empty_a }
    key: id
    properties:
      num: num
  - label: EmptyB
    table: { schema: multiopt, name: empty_b }
    key: id
    properties:
      num: num
  - label: Owner
    table: { schema: multiopt, name: owners }
    key: id
    properties:
      name: name
  - label: AltOwner
    table: { schema: multiopt, name: alt_owners }
    key: id
    properties:
      name: name
  - label: Service
    table: { schema: multiopt, name: services }
    key: id
    properties:
      name: name
relationships:
  - type: NOR_THIS
    table: { schema: multiopt, name: nor_this }
    key: id
    from: { label: NotThere, key: source_id }
    to: { label: NotThere, key: target_id }
  - type: OWNS
    table: { schema: multiopt, name: ownerships }
    key: id
    from: { label: Owner, key: owner_id }
    to: { label: Service, key: service_id }
  - type: OWNS
    table: { schema: multiopt, name: alt_ownerships }
    key: id
    from: { label: AltOwner, key: owner_id }
    to: { label: Service, key: service_id }
";

const MATCH6_SINGLE_NODE_GRAPH: &str = r"
version: 1
name: match6-single-node
nodes:
  - label: Node
    table: { schema: match6, name: nodes }
    key: id
";

const MATCH7_OPTIONAL_PATH_GRAPH: &str = r"
version: 1
name: match7-optional-path
nodes:
  - label: A
    table: { schema: match7, name: a_nodes }
    key: id
    properties:
      num: num
  - label: B
    table: { schema: match7, name: b_nodes }
    key: id
    properties:
      num: num
relationships:
  - type: X
    table: { schema: match7, name: x_edges }
    key: id
    from: { label: A, key: a_id }
    to: { label: B, key: b_id }
";

const PATH_THROUGH_WITH_GRAPH: &str = r"
version: 1
name: path-through-with
nodes:
  - label: A
    table: { schema: pathwith, name: a_nodes }
    key: id
  - label: B
    table: { schema: pathwith, name: b_nodes }
    key: id
relationships:
  - type: X
    table: { schema: pathwith, name: x_edges }
    key: id
    from: { label: A, key: a_id }
    to: { label: B, key: b_id }
";

const STAGED_RELATIONSHIP_CARRY_GRAPH: &str = r"
version: 1
name: staged-relationship-carry
nodes:
  - label: X
    table: { schema: staged_relcarry, name: nodes }
    key: id
    properties:
      name: name
relationships:
  - type: REL
    table: { schema: staged_relcarry, name: edges }
    key: id
    from: { label: X, key: from_id }
    to: { label: X, key: to_id }
";
