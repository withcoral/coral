use std::collections::BTreeMap;
use std::path::Path;

use arrow::array::Array;
use coral_engine::{
    ComparisonOperator, CoralQuery, GraphCypherParameterValue, GraphDeclaration, GraphDirection,
    GraphGraphqlVariableValue, GraphLiteral, GraphOrderDirection, GraphOrderExpression,
    GraphOrderKey, GraphPlan, GraphPredicateRhs, GraphProjection, GraphPropertyPredicate,
    GraphPropertyRef, NodePattern, RelationshipPattern,
};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{
    RICH_GRAPH, build_source, dir_url, execution_to_rows, rich_manifest, test_runtime,
    write_jsonl_file, write_rich_fixture,
};

fn assert_close(actual: f64, expected: f64) {
    let delta = (actual - expected).abs();
    assert!(
        delta <= 1e-12,
        "expected {actual} to be within 1e-12 of {expected}, delta {delta}"
    );
}

fn sort_string_array_field(row: &mut Value, field: &str) {
    let Some(values) = row.get_mut(field).and_then(Value::as_array_mut) else {
        panic!("row should contain array field '{field}': {row}");
    };
    values.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
}

fn sort_i64_array_field(row: &mut Value, field: &str) {
    let Some(values) = row.get_mut(field).and_then(Value::as_array_mut) else {
        panic!("row should contain array field '{field}': {row}");
    };
    values.sort_by_key(Value::as_i64);
}

fn sort_bool_array_field(row: &mut Value, field: &str) {
    let Some(values) = row.get_mut(field).and_then(Value::as_array_mut) else {
        panic!("row should contain array field '{field}': {row}");
    };
    values.sort_by_key(Value::as_bool);
}

#[tokio::test]
async fn virtual_graph_translation_executes_against_synthetic_file_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let plan = owner_service_plan();

    let graph_rows = execution_to_rows(
        CoralQuery::execute_graph_plan(
            std::slice::from_ref(&source),
            test_runtime(),
            &graph,
            &plan,
        )
        .await
        .expect("graph plan should execute")
        .execution(),
    );
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT people.full_name AS owner, services.service_name AS service \
             FROM ops.people \
             JOIN ops.ownerships ON ownerships.person_id = people.id \
             JOIN ops.services ON ownerships.service_id = services.id \
             WHERE services.tier = 'prod' \
             ORDER BY people.full_name ASC \
             LIMIT 25",
        )
        .await
        .expect("equivalent SQL should execute"),
    );

    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_translation_executes_against_synthetic_file_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY person.name ASC \
         LIMIT 25",
    )
    .await
    .expect("Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_with_order_limit_executes_with_second_relationship_type() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:LIKES]->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .await
    .expect("staged Cypher query with non-KNOWS relationship type should execute");

    assert!(
        execution.translated_sql().contains("WITH \"stage0\" AS"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("\"staged\".\"likes\""),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"a": "Alice", "b": "Bob"}),
            json!({"a": "Bob", "b": "Carol"}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_optional_match_preserves_empty_optional_row() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_keyed_fixture(temp.path());
    let source = build_source(staged_planning_keyed_manifest(temp.path()));
    let graph = staged_planning_keyed_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 1 \
         OPTIONAL MATCH (a)-[:LIKES]->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .await
    .expect("staged optional query with empty optional side should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"a": "Bob"})]
    );
    assert_eq!(execution.execution().row_count(), 1);
    let batch = execution
        .execution()
        .batches()
        .iter()
        .find(|batch| batch.num_rows() > 0)
        .expect("empty optional query should return one row");
    let b = batch
        .column_by_name("b")
        .expect("empty optional query should project b");
    assert!(b.is_null(0), "optional-only b should be NULL");
}

#[tokio::test]
async fn cypher_staged_optional_match_preserves_matched_optional_row() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_keyed_fixture(temp.path());
    let source = build_source(staged_planning_keyed_manifest(temp.path()));
    let graph = staged_planning_keyed_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 1 \
         OPTIONAL MATCH (a)-[:KNOWS]->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .await
    .expect("staged optional query with matched optional side should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"a": "Bob", "b": "Carol"})]
    );
}

#[tokio::test]
async fn cypher_staged_relationship_carry_optional_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_keyed_fixture(temp.path());
    let source = build_source(staged_planning_keyed_manifest(temp.path()));
    let graph = staged_planning_keyed_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[r:KNOWS]->(b:Person) \
         WITH r ORDER BY id(r) LIMIT 1 \
         OPTIONAL MATCH (a2:Person)-[r:KNOWS]->(b2:Person) \
         RETURN a2.name AS a, id(r) AS r, b2.name AS b",
    )
    .await
    .expect("staged relationship carry optional query should execute");

    assert!(
        execution.translated_sql().contains("\"stage0\".\"r_id\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"a": "Alice", "r": 100, "b": "Bob"})]
    );
}

#[tokio::test]
async fn cypher_staged_node_and_relationship_carry_optional_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_keyed_fixture(temp.path());
    let source = build_source(staged_planning_keyed_manifest(temp.path()));
    let graph = staged_planning_keyed_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a1:Person)-[r:KNOWS]->(:Person) \
         WITH r, a1 ORDER BY id(r) LIMIT 1 \
         OPTIONAL MATCH (a1)-[r:KNOWS]->(b2:Person) \
         RETURN a1.name AS a, id(r) AS r, b2.name AS b",
    )
    .await
    .expect("staged node and relationship carry optional query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"a": "Alice", "r": 100, "b": "Bob"})]
    );
}

#[tokio::test]
async fn cypher_staged_with_order_limit_incoming_final_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (b:Person)-[:KNOWS]->(a) \
         RETURN a.name AS a, b.name AS b",
    )
    .await
    .expect("staged incoming final match should execute");

    assert!(
        execution.translated_sql().contains("\"friend_id\""),
        "{}",
        execution.translated_sql()
    );
    let rows = execution_to_rows(execution.execution());
    assert_eq!(rows, vec![json!({"a": "Bob", "b": "Alice"})]);
}

#[tokio::test]
async fn cypher_staged_with_order_limit_undirected_final_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:KNOWS]-(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .await
    .expect("staged undirected final match should execute");

    assert!(
        execution.translated_sql().contains(" OR "),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"a": "Alice", "b": "Bob"}),
            json!({"a": "Alice", "b": "Carol"}),
            json!({"a": "Bob", "b": "Alice"}),
            json!({"a": "Bob", "b": "Carol"}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_with_order_limit_multihop_final_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:KNOWS]->(x:Person)-[:KNOWS]->(b:Person) \
         RETURN a.name AS a, b.name AS b",
    )
    .await
    .expect("staged multi-hop final match should execute");

    assert!(
        execution.translated_sql().contains("\"stage0\".\"a_id\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("\"r1\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"a": "Alice", "b": "Carol"})]
    );
}

#[tokio::test]
async fn cypher_staged_with_order_limit_rehydrates_multiple_carried_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person) \
         WITH a ORDER BY a.age LIMIT 2 \
         MATCH (a)-[:OWNS]->(b:Service) \
         RETURN a.name AS a, a.age AS age, b.name AS b",
    )
    .await
    .expect("staged Cypher query should rehydrate multiple carried properties");

    assert!(
        execution.translated_sql().contains("WITH \"stage0\" AS"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"staged\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"a": "Alice", "age": 30, "b": "billing-api"}),
            json!({"a": "Bob", "age": 25, "b": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_count_aggregation_before_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, deg",
    )
    .await
    .expect("staged count aggregate Cypher query should execute");

    assert!(
        execution.translated_sql().contains("WITH \"stage0\" AS"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("GROUP BY"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"name": "Alice", "deg": 2}),
            json!({"name": "Alice", "deg": 2}),
            json!({"name": "Bob", "deg": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_count_aggregation_before_incoming_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (c:Person)-[:KNOWS]->(a) \
         RETURN a.name AS name, c.name AS c, deg",
    )
    .await
    .expect("staged aggregate query with incoming final match should execute");

    let rows = execution_to_rows(execution.execution());
    assert_eq!(rows, vec![json!({"name": "Bob", "c": "Alice", "deg": 1})]);
}

#[tokio::test]
async fn cypher_staged_sum_aggregation_before_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, sum(b.age) AS total_age \
         MATCH (a)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, total_age",
    )
    .await
    .expect("staged sum aggregate Cypher query should execute");

    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"name": "Alice", "total_age": 60}),
            json!({"name": "Alice", "total_age": 60}),
            json!({"name": "Bob", "total_age": 35}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_two_group_key_aggregation_before_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, b, count(*) AS c \
         MATCH (a)-[:KNOWS]->(b) \
         RETURN a.name AS a, b.name AS b, c",
    )
    .await
    .expect("staged two-key aggregate Cypher query should execute");

    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"a": "Alice", "b": "Bob", "c": 1}),
            json!({"a": "Alice", "b": "Carol", "c": 1}),
            json!({"a": "Bob", "b": "Carol", "c": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_aggregate_alias_filters_in_final_match() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(c:Person) WHERE deg > 1 \
         RETURN a.name AS name, deg",
    )
    .await
    .expect("staged aggregate alias final WHERE should execute");

    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(std::string::ToString::to_string);
    assert_eq!(
        rows,
        vec![
            json!({"name": "Alice", "deg": 2}),
            json!({"name": "Alice", "deg": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_staged_aggregate_multihop_final_match_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_staged_planning_fixture(temp.path());
    let source = build_source(staged_planning_manifest(temp.path()));
    let graph = staged_planning_test_graph();

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (a:Person)-[:KNOWS]->(b:Person) \
         WITH a, count(b) AS deg \
         MATCH (a)-[:KNOWS]->(x:Person)-[:KNOWS]->(c:Person) \
         RETURN a.name AS name, c.name AS c, deg",
    )
    .await
    .expect("staged aggregate multi-hop final match should execute");

    let rows = execution_to_rows(execution.execution());
    assert_eq!(rows, vec![json!({"name": "Alice", "c": "Carol", "deg": 2})]);
}

#[tokio::test]
async fn cypher_date_map_constructor_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN date({year: 1984, month: 10, day: 11}) AS d, \
                toString(date({year: 1984, month: 10, day: 11})) AS text",
    )
    .await
    .expect("Cypher DATE map constructor should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT make_date(1984, 10, 11) AS d, \
                    CAST(make_date(1984, 10, 11) AS VARCHAR) AS text \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent DATE SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains("make_date(1984, 10, 11)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({"d": "1984-10-11", "text": "1984-10-11"})]
    );
}

#[tokio::test]
async fn cypher_date_string_constructor_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN date('2015-07-21') AS d, \
                toString(date('2015-07-21')) AS text",
    )
    .await
    .expect("Cypher DATE string constructor should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT CAST('2015-07-21' AS DATE) AS d, \
                    CAST(CAST('2015-07-21' AS DATE) AS VARCHAR) AS text \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent DATE string SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains("CAST('2015-07-21' AS DATE)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({"d": "2015-07-21", "text": "2015-07-21"})]
    );
}

#[tokio::test]
async fn cypher_localdatetime_constructors_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN localdatetime('2020-01-15T12:34:56') AS from_string, \
                localdatetime({year: 2020, month: 1, day: 15, hour: 12, minute: 34, second: 56}) AS from_map, \
                localdatetime({year: 2020, month: 1, day: 15}) AS default_time, \
                localdatetime('2020-01-15T12:00:00') < localdatetime('2020-01-16T00:00:00') AS ordered, \
                toString(localdatetime('2020-01-15T12:34:56')) AS text",
    )
    .await
    .expect("Cypher LOCALDATETIME constructors should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS from_string, \
                    CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS from_map, \
                    CAST('2020-01-15T00:00:00' AS TIMESTAMP) AS default_time, \
                    CAST('2020-01-15T12:00:00' AS TIMESTAMP) < CAST('2020-01-16T00:00:00' AS TIMESTAMP) AS ordered, \
                    CAST(CAST('2020-01-15T12:34:56' AS TIMESTAMP) AS VARCHAR) AS text \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent LOCALDATETIME SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains("CAST('2020-01-15T12:34:56' AS TIMESTAMP)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "from_string": "2020-01-15T12:34:56",
            "from_map": "2020-01-15T12:34:56",
            "default_time": "2020-01-15T00:00:00",
            "ordered": true,
            "text": "2020-01-15T12:34:56"
        })]
    );
}

#[tokio::test]
async fn cypher_localtime_constructors_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN localtime('12:34:56') AS from_string, \
                localtime({hour: 12, minute: 34, second: 56}) AS from_map, \
                localtime({hour: 12}) AS default_time, \
                localtime('12:00:00') < localtime('13:00:00') AS ordered, \
                toString(localtime('12:34:56')) AS text",
    )
    .await
    .expect("Cypher LOCALTIME constructors should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT CAST('12:34:56' AS TIME) AS from_string, \
                    CAST('12:34:56' AS TIME) AS from_map, \
                    CAST('12:00:00' AS TIME) AS default_time, \
                    CAST('12:00:00' AS TIME) < CAST('13:00:00' AS TIME) AS ordered, \
                    CAST(CAST('12:34:56' AS TIME) AS VARCHAR) AS text \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent LOCALTIME SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains("CAST('12:34:56' AS TIME)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "from_string": "12:34:56",
            "from_map": "12:34:56",
            "default_time": "12:00:00",
            "ordered": true,
            "text": "12:34:56"
        })]
    );
}

#[tokio::test]
async fn cypher_temporal_components_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN date('2020-01-15').year AS date_year, \
                date('2020-01-15').quarter AS date_quarter, \
                date('2020-01-15').month AS date_month, \
                date('2020-01-15').week AS date_week, \
                date('2020-01-15').day AS date_day, \
                localdatetime('2020-01-15T12:34:56.789123456').hour AS datetime_hour, \
                localdatetime('2020-01-15T12:34:56.789123456').minute AS datetime_minute, \
                localdatetime('2020-01-15T12:34:56.789123456').second AS datetime_second, \
                localdatetime('2020-01-15T12:34:56.789123456').millisecond AS datetime_millisecond, \
                localdatetime('2020-01-15T12:34:56.789123456').microsecond AS datetime_microsecond, \
                localtime('12:34:56.789123456').hour AS time_hour, \
                localtime('12:34:56.789123456').minute AS time_minute, \
                localtime('12:34:56.789123456').second AS time_second, \
                localtime('12:34:56.789123456').millisecond AS time_millisecond, \
                localtime('12:34:56.789123456').microsecond AS time_microsecond",
    )
    .await
    .expect("Cypher temporal component access should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT CAST(date_part('year', CAST('2020-01-15' AS DATE)) AS BIGINT) AS date_year, \
                    CAST(date_part('quarter', CAST('2020-01-15' AS DATE)) AS BIGINT) AS date_quarter, \
                    CAST(date_part('month', CAST('2020-01-15' AS DATE)) AS BIGINT) AS date_month, \
                    CAST(date_part('week', CAST('2020-01-15' AS DATE)) AS BIGINT) AS date_week, \
                    CAST(date_part('day', CAST('2020-01-15' AS DATE)) AS BIGINT) AS date_day, \
                    CAST(date_part('hour', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) AS datetime_hour, \
                    CAST(date_part('minute', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) AS datetime_minute, \
                    CAST(date_part('second', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) AS datetime_second, \
                    (CAST(date_part('millisecond', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) % 1000) AS datetime_millisecond, \
                    (CAST(date_part('microsecond', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) % 1000000) AS datetime_microsecond, \
                    CAST(date_part('hour', CAST('12:34:56.789123456' AS TIME)) AS BIGINT) AS time_hour, \
                    CAST(date_part('minute', CAST('12:34:56.789123456' AS TIME)) AS BIGINT) AS time_minute, \
                    CAST(date_part('second', CAST('12:34:56.789123456' AS TIME)) AS BIGINT) AS time_second, \
                    (CAST(date_part('millisecond', CAST('12:34:56.789123456' AS TIME)) AS BIGINT) % 1000) AS time_millisecond, \
                    (CAST(date_part('microsecond', CAST('12:34:56.789123456' AS TIME)) AS BIGINT) % 1000000) AS time_microsecond \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent temporal component SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains(
                "(CAST(date_part('millisecond', CAST('2020-01-15T12:34:56.789123456' AS TIMESTAMP)) AS BIGINT) % 1000)"
            ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "date_year": 2020,
            "date_quarter": 1,
            "date_month": 1,
            "date_week": 3,
            "date_day": 15,
            "datetime_hour": 12,
            "datetime_minute": 34,
            "datetime_second": 56,
            "datetime_millisecond": 789,
            "datetime_microsecond": 789_123,
            "time_hour": 12,
            "time_minute": 34,
            "time_second": 56,
            "time_millisecond": 789,
            "time_microsecond": 789_123
        })]
    );
}

#[tokio::test]
async fn cypher_temporal_duration_arithmetic_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN date('2020-01-31') + duration('P1M') AS month_clamp, \
                date('2020-01-15') + duration('P10D') AS plus_days, \
                date('2020-03-15') - duration('P1M') AS minus_month, \
                localdatetime('2020-01-01T00:00:00') + duration('PT1H30M') AS shifted_datetime, \
                localtime('12:00:00') + duration('PT90M') AS shifted_time",
    )
    .await
    .expect("Cypher temporal duration arithmetic should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT CAST('2020-01-31' AS DATE) + CAST('1 months 0 days 0 seconds' AS INTERVAL) AS month_clamp, \
                    CAST('2020-01-15' AS DATE) + CAST('0 months 10 days 0 seconds' AS INTERVAL) AS plus_days, \
                    CAST('2020-03-15' AS DATE) - CAST('1 months 0 days 0 seconds' AS INTERVAL) AS minus_month, \
                    CAST('2020-01-01T00:00:00' AS TIMESTAMP) + CAST('0 months 0 days 5400 seconds' AS INTERVAL) AS shifted_datetime, \
                    CAST(CAST(concat('1970-01-01T', CAST(CAST('12:00:00' AS TIME) AS VARCHAR)) AS TIMESTAMP) + CAST('0 months 0 days 5400 seconds' AS INTERVAL) AS TIME) AS shifted_time \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent temporal duration SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains("CAST('1 months 0 days 0 seconds' AS INTERVAL)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "month_clamp": "2020-02-29",
            "plus_days": "2020-01-25",
            "minus_month": "2020-02-15",
            "shifted_datetime": "2020-01-01T01:30:00",
            "shifted_time": "13:30:00"
        })]
    );
}

#[tokio::test]
async fn cypher_duration_results_render_as_iso_strings_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1}) AS compound, \
                toString(duration({years: 12, months: 5, days: 14, hours: 16, minutes: 12, seconds: 70, nanoseconds: 1})) AS compound_text, \
                duration({years: 12, months: 5, days: -14, hours: 16}) AS signed_days, \
                toString(duration({minutes: 12, seconds: -60})) AS seconds_underflow, \
                toString(duration({seconds: 2, milliseconds: -1})) AS subsecond, \
                toString(duration({seconds: -60, milliseconds: -1})) AS negative_subsecond, \
                duration({days: 1, milliseconds: 1}) AS day_subsecond, \
                duration({}) AS zero_duration",
    )
    .await
    .expect("Cypher duration rendering should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT 'P12Y5M14DT16H13M10.000000001S' AS compound, \
                    'P12Y5M14DT16H13M10.000000001S' AS compound_text, \
                    'P12Y5M-14DT16H' AS signed_days, \
                    'PT11M' AS seconds_underflow, \
                    'PT1.999S' AS subsecond, \
                    'PT-1M-0.001S' AS negative_subsecond, \
                    'P1DT0.001S' AS day_subsecond, \
                    'PT0S' AS zero_duration \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent duration rendering SQL should execute"),
    );

    assert!(
        execution
            .translated_sql()
            .contains("coral_duration_to_iso("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "compound": "P12Y5M14DT16H13M10.000000001S",
            "compound_text": "P12Y5M14DT16H13M10.000000001S",
            "signed_days": "P12Y5M-14DT16H",
            "seconds_underflow": "PT11M",
            "subsecond": "PT1.999S",
            "negative_subsecond": "PT-1M-0.001S",
            "day_subsecond": "P1DT0.001S",
            "zero_duration": "PT0S"
        })]
    );
}

#[tokio::test]
async fn cypher_duration_components_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         WITH duration({years: 1, months: 4, days: 10, hours: 1, minutes: 1, seconds: 1, nanoseconds: 111111111}) AS d, \
              duration({months: 14}) AS normalized, \
              duration({months: -14, days: -10, hours: -1, minutes: -1, seconds: -1, nanoseconds: -111111111}) AS negative, \
              duration({}) AS zero \
         RETURN d.years AS years, d.quarters AS quarters, d.months AS months, d.weeks AS weeks, d.days AS days, \
                d.hours AS hours, d.minutes AS minutes, d.seconds AS seconds, d.milliseconds AS milliseconds, d.microseconds AS microseconds, d.nanoseconds AS nanoseconds, \
                d.quartersOfYear AS quartersOfYear, d.monthsOfQuarter AS monthsOfQuarter, d.monthsOfYear AS monthsOfYear, d.daysOfWeek AS daysOfWeek, \
                d.minutesOfHour AS minutesOfHour, d.secondsOfMinute AS secondsOfMinute, d.millisecondsOfSecond AS millisecondsOfSecond, \
                d.microsecondsOfSecond AS microsecondsOfSecond, d.nanosecondsOfSecond AS nanosecondsOfSecond, \
                normalized.years AS normalized_years, normalized.months AS normalized_months, normalized.monthsOfYear AS normalized_monthsOfYear, \
                negative.monthsOfYear AS negative_monthsOfYear, \
                negative.secondsOfMinute AS negative_secondsOfMinute, \
                zero.nanoseconds AS zero_nanoseconds",
    )
    .await
    .expect("Cypher duration component access should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT 1 AS years, \
                    5 AS quarters, \
                    16 AS months, \
                    1 AS weeks, \
                    10 AS days, \
                    1 AS hours, \
                    61 AS minutes, \
                    3661 AS seconds, \
                    3661111 AS milliseconds, \
                    3661111111 AS microseconds, \
                    3661111111111 AS nanoseconds, \
                    1 AS \"quartersOfYear\", \
                    1 AS \"monthsOfQuarter\", \
                    4 AS \"monthsOfYear\", \
                    3 AS \"daysOfWeek\", \
                    1 AS \"minutesOfHour\", \
                    1 AS \"secondsOfMinute\", \
                    111 AS \"millisecondsOfSecond\", \
                    111111 AS \"microsecondsOfSecond\", \
                    111111111 AS \"nanosecondsOfSecond\", \
                    1 AS normalized_years, \
                    14 AS normalized_months, \
                    2 AS \"normalized_monthsOfYear\", \
                    -2 AS \"negative_monthsOfYear\", \
                    -1 AS \"negative_secondsOfMinute\", \
                    0 AS zero_nanoseconds \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent duration component SQL should execute"),
    );

    assert!(
        execution.translated_sql().contains("coral_duration_part("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "years": 1,
            "quarters": 5,
            "months": 16,
            "weeks": 1,
            "days": 10,
            "hours": 1,
            "minutes": 61,
            "seconds": 3661,
            "milliseconds": 3_661_111,
            "microseconds": 3_661_111_111i64,
            "nanoseconds": 3_661_111_111_111i64,
            "quartersOfYear": 1,
            "monthsOfQuarter": 1,
            "monthsOfYear": 4,
            "daysOfWeek": 3,
            "minutesOfHour": 1,
            "secondsOfMinute": 1,
            "millisecondsOfSecond": 111,
            "microsecondsOfSecond": 111_111,
            "nanosecondsOfSecond": 111_111_111,
            "normalized_years": 1,
            "normalized_months": 14,
            "normalized_monthsOfYear": 2,
            "negative_monthsOfYear": -2,
            "negative_secondsOfMinute": -1,
            "zero_nanoseconds": 0
        })]
    );
}

#[tokio::test]
async fn cypher_temporal_duration_unit_totals_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN duration.between(date('1984-10-11'), date('2015-06-24')) AS between_duration, \
                duration.inMonths(date('1984-10-11'), date('2015-06-24')) AS months_duration, \
                duration.between(date('1984-10-11'), localdatetime('2016-07-21T21:45:22.142')) AS mixed_between_duration, \
                duration.between(date('2020-01-31'), date('2020-02-29')) AS leap_month_end, \
                duration.between(date('2020-01-31'), date('2020-03-30')) AS march_month_boundary, \
                duration.between(date('2020-01-31'), date('2020-04-30')) AS april_month_end, \
                duration.between(date('2015-06-24'), date('1984-10-11')) AS negative_between_duration, \
                duration.inSeconds(localdatetime('2020-01-01T00:00:00'), localdatetime('2020-03-01T12:00:00')) AS seconds_duration, \
                duration.inDays(date('1984-10-11'), date('2015-06-24')) AS days_duration, \
                duration.inDays(localdatetime('2015-07-21T21:40:32.142'), date('2015-06-24')) AS negative_partial_days, \
                duration.inSeconds(localdatetime('2014-07-21T21:40:36.143'), localdatetime('2014-07-21T21:40:36.142')) AS negative_subsecond, \
                duration.inSeconds(localdatetime('2020-01-01T00:00:00'), localdatetime('2020-01-01T00:00:00')) AS zero_duration, \
                toString(duration.inSeconds(null, null)) AS null_duration",
    )
    .await
    .expect("Cypher temporal duration unit totals should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT coral_duration_to_iso(coral_duration_between(CAST('1984-10-11' AS DATE), CAST('2015-06-24' AS DATE))) AS between_duration, \
                    coral_duration_to_iso(coral_duration_in_months(CAST('1984-10-11' AS DATE), CAST('2015-06-24' AS DATE))) AS months_duration, \
                    coral_duration_to_iso(coral_duration_between(CAST('1984-10-11' AS DATE), CAST('2016-07-21T21:45:22.142' AS TIMESTAMP))) AS mixed_between_duration, \
                    coral_duration_to_iso(coral_duration_between(CAST('2020-01-31' AS DATE), CAST('2020-02-29' AS DATE))) AS leap_month_end, \
                    coral_duration_to_iso(coral_duration_between(CAST('2020-01-31' AS DATE), CAST('2020-03-30' AS DATE))) AS march_month_boundary, \
                    coral_duration_to_iso(coral_duration_between(CAST('2020-01-31' AS DATE), CAST('2020-04-30' AS DATE))) AS april_month_end, \
                    coral_duration_to_iso(coral_duration_between(CAST('2015-06-24' AS DATE), CAST('1984-10-11' AS DATE))) AS negative_between_duration, \
                    coral_duration_to_iso(CAST(concat('0 months 0 days ', coalesce(CAST(date_part('epoch', (CAST('2020-03-01T12:00:00' AS TIMESTAMP) - CAST('2020-01-01T00:00:00' AS TIMESTAMP))) AS VARCHAR), '0'), ' seconds') AS INTERVAL)) AS seconds_duration, \
                    coral_duration_to_iso(CAST(concat('0 months ', coalesce(CAST(trunc(date_part('epoch', (CAST(CAST('2015-06-24' AS DATE) AS TIMESTAMP) - CAST(CAST('1984-10-11' AS DATE) AS TIMESTAMP))) / 86400) AS VARCHAR), '0'), ' days 0 seconds') AS INTERVAL)) AS days_duration, \
                    coral_duration_to_iso(CAST(concat('0 months ', coalesce(CAST(trunc(date_part('epoch', (CAST(CAST('2015-06-24' AS DATE) AS TIMESTAMP) - CAST('2015-07-21T21:40:32.142' AS TIMESTAMP))) / 86400) AS VARCHAR), '0'), ' days 0 seconds') AS INTERVAL)) AS negative_partial_days, \
                    coral_duration_to_iso(CAST(concat('0 months 0 days ', coalesce(CAST(date_part('epoch', (CAST('2014-07-21T21:40:36.142' AS TIMESTAMP) - CAST('2014-07-21T21:40:36.143' AS TIMESTAMP))) AS VARCHAR), '0'), ' seconds') AS INTERVAL)) AS negative_subsecond, \
                    coral_duration_to_iso(CAST(concat('0 months 0 days ', coalesce(CAST(date_part('epoch', (CAST('2020-01-01T00:00:00' AS TIMESTAMP) - CAST('2020-01-01T00:00:00' AS TIMESTAMP))) AS VARCHAR), '0'), ' seconds') AS INTERVAL)) AS zero_duration, \
                    coral_duration_to_iso(CAST(NULL AS INTERVAL)) AS null_duration \
             FROM ops.people \
             WHERE people.full_name = 'Ada Lovelace'",
        )
        .await
        .expect("equivalent temporal duration unit SQL should execute"),
    );

    assert!(
        execution.translated_sql().contains("date_part('epoch'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("coral_duration_between"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "between_duration": "P30Y8M13D",
            "months_duration": "P30Y8M",
            "mixed_between_duration": "P31Y9M10DT21H45M22.142S",
            "leap_month_end": "P29D",
            "march_month_boundary": "P1M30D",
            "april_month_end": "P2M30D",
            "negative_between_duration": "P-30Y-8M-13D",
            "seconds_duration": "PT1452H",
            "days_duration": "P11213D",
            "negative_partial_days": "P-27D",
            "negative_subsecond": "PT-0.001S",
            "zero_duration": "PT0S"
        })]
    );
}

#[tokio::test]
async fn cypher_stored_temporal_components_execute_against_rich_fixture() {
    let temp = TempDir::new().expect("temp dir");
    write_rich_fixture(temp.path());
    let source = build_source(rich_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(RICH_GRAPH).expect("rich graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person) \
         WHERE person.name = 'Ada' \
         RETURN person.joined.year AS joined_year, \
                person.joined.month AS joined_month, \
                person.joined.day AS joined_day, \
                person.joined.hour AS joined_hour, \
                person.birthday.year AS birthday_year, \
                person.birthday.month AS birthday_month, \
                person.birthday.day AS birthday_day",
    )
    .await
    .expect("Cypher stored temporal component access should execute");
    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT CAST(date_part('year', people.joined) AS BIGINT) AS joined_year, \
                    CAST(date_part('month', people.joined) AS BIGINT) AS joined_month, \
                    CAST(date_part('day', people.joined) AS BIGINT) AS joined_day, \
                    CAST(date_part('hour', people.joined) AS BIGINT) AS joined_hour, \
                    CAST(date_part('year', people.birthday) AS BIGINT) AS birthday_year, \
                    CAST(date_part('month', people.birthday) AS BIGINT) AS birthday_month, \
                    CAST(date_part('day', people.birthday) AS BIGINT) AS birthday_day \
             FROM rich.people \
             WHERE people.name = 'Ada'",
        )
        .await
        .expect("equivalent stored temporal component SQL should execute"),
    );

    assert!(
        execution.translated_sql().contains("date_part('year'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "joined_year": 2020,
            "joined_month": 6,
            "joined_day": 1,
            "joined_hour": 9,
            "birthday_year": 1990,
            "birthday_month": 5,
            "birthday_day": 20
        })]
    );
}

#[tokio::test]
async fn cypher_stored_date_rejects_time_component_against_rich_fixture() {
    let temp = TempDir::new().expect("temp dir");
    write_rich_fixture(temp.path());
    let source = build_source(rich_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(RICH_GRAPH).expect("rich graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person) WHERE person.name = 'Ada' RETURN person.birthday.hour AS hour",
    )
    .await
    .expect_err("stored DATE hour component should reject");

    assert!(
        error
            .to_string()
            .contains("hour is not supported for date values"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn cypher_parenthesized_path_patterns_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH ownership_path = ((person:Person)-[:OWNS]->(service:Service)) \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service, length(ownership_path) AS hops \
         ORDER BY owner ASC",
    )
    .await
    .expect("parenthesized Cypher path query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "hops": 1}),
            json!({"owner": "Grace Hopper", "service": "deployments", "hops": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_exact_one_quantified_parenthesized_path_patterns_execute_against_synthetic_sources()
{
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH ownership_path = ((person:Person)-[:OWNS]->(service:Service)){1} \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service, length(ownership_path) AS hops \
         ORDER BY owner ASC",
    )
    .await
    .expect("exact-one quantified parenthesized Cypher path query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "hops": 1}),
            json!({"owner": "Grace Hopper", "service": "deployments", "hops": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_order_by_null_placement_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, service.tier AS tier \
         ORDER BY service.tier ASC NULLS FIRST, service.name DESC NULLS LAST",
    )
    .await
    .expect("Cypher ORDER BY NULLS FIRST/LAST query should execute");

    assert!(
        execution.translated_sql().contains(
            "ORDER BY \"n0\".\"tier\" ASC NULLS FIRST, \"n0\".\"service_name\" DESC NULLS LAST"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "legacy-sync"}),
            json!({"service": "experiments", "tier": "dev"}),
            json!({"service": "deployments", "tier": "prod"}),
            json!({"service": "billing-api", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_label_inference_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service) \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY person.name ASC",
    )
    .await
    .expect("graph-aware Cypher should infer the unlabeled service endpoint");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_anonymous_label_inference_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH ()-[:ROUTES]->(service:Service) \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("graph-aware Cypher should infer the anonymous Person endpoint");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"person_service_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_relationship_type_inference_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-->(service:Service) \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY owner",
    )
    .await
    .expect("graph-aware Cypher should infer the OWNS relationship type");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_untyped_endpoint_inference_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-->(service) \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY owner",
    )
    .await
    .expect("graph-aware Cypher should infer the endpoint label and relationship type");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_label_inference_preserves_unmatched_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service) \
         OPTIONAL MATCH (source)-[:DEPENDS_ON]->(target) \
         RETURN source.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("optional Cypher should infer the unlabeled dependency endpoint");

    assert!(
        execution
            .translated_sql()
            .contains("LEFT JOIN \"ops\".\"service_dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
            json!({"source": "experiments"}),
            json!({"source": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_return_star_expands_graph_declaration_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
         WHERE service.name = 'billing-api' \
         RETURN *, service.tier AS tier_copy \
         ORDER BY tier_copy, person.name \
         LIMIT 1",
    )
    .await
    .expect("RETURN * Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"id\" AS \"person.__id\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "person.__id": 1,
            "person.__labels": ["Person"],
            "person.name": "Ada Lovelace",
            "person.team": "platform",
            "service.__id": 10,
            "service.__labels": ["Service"],
            "service.active": true,
            "service.id": 10,
            "service.name": "billing-api",
            "service.risk": 0.9,
            "service.team": "platform",
            "service.tier": "prod",
            "ownership.__id": 100,
            "ownership.__type": "OWNS",
            "ownership.since": "2024-01-10",
            "tier_copy": "prod"
        })]
    );
}

#[tokio::test]
async fn cypher_return_graph_variable_expands_declaration_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name = 'billing-api' \
         RETURN service AS svc",
    )
    .await
    .expect("graph variable return should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "svc.__id": 10,
            "svc.__labels": ["Service"],
            "svc.active": true,
            "svc.id": 10,
            "svc.name": "billing-api",
            "svc.risk": 0.9,
            "svc.team": "platform",
            "svc.tier": "prod"
        })]
    );
}

#[tokio::test]
async fn cypher_return_relationship_endpoint_graph_values_expand_declaration_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[ownership:OWNS]->(service:Service) \
         WHERE service.name = 'billing-api' \
         RETURN startNode(ownership) AS owner, endNode(ownership) AS owned",
    )
    .await
    .expect("endpoint graph value return should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "owner.__id": 1,
            "owner.__labels": ["Person"],
            "owner.name": "Ada Lovelace",
            "owner.team": "platform",
            "owned.__id": 10,
            "owned.__labels": ["Service"],
            "owned.active": true,
            "owned.id": 10,
            "owned.name": "billing-api",
            "owned.risk": 0.9,
            "owned.team": "platform",
            "owned.tier": "prod"
        })]
    );

    let optional_execution = CoralQuery::execute_cypher(
        &[build_source(ops_manifest(temp.path()))],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name = 'legacy-sync' \
         OPTIONAL MATCH (person:Person)-[ownership:OWNS]->(service) \
         RETURN startNode(ownership) AS owner, endNode(ownership) AS owned",
    )
    .await
    .expect("optional endpoint graph value return should execute");

    assert!(
        optional_execution
            .translated_sql()
            .contains("CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL"),
        "{}",
        optional_execution.translated_sql()
    );
    assert_eq!(optional_execution.execution().row_count(), 1);
    assert_eq!(
        execution_to_rows(optional_execution.execution()),
        vec![json!({})]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, service.name AS service",
    )
    .await
    .expect("static label alternative Cypher query should execute");

    assert!(
        execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by(|left, right| {
        let left_key = format!("{}:{}", left["owner"], left["service"]);
        let right_key = format!("{}:{}", right["owner"], right["service"]);
        left_key.cmp(&right_key)
    });
    assert_eq!(
        rows,
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
            json!({"owner": "analytics", "service": "experiments"}),
            json!({"owner": "infra", "service": "deployments"}),
            json!({"owner": "platform", "service": "billing-api"}),
            json!({"owner": "platform", "service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_executes_across_declared_labels() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY label, name",
    )
    .await
    .expect("graph-aware unlabeled node scan should execute");

    assert!(
        execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Person", "name": "Ada Lovelace"}),
            json!({"label": "Person", "name": "Grace Hopper"}),
            json!({"label": "Person", "name": "Katherine Johnson"}),
            json!({"label": "Service", "name": "billing-api"}),
            json!({"label": "Service", "name": "deployments"}),
            json!({"label": "Service", "name": "experiments"}),
            json!({"label": "Service", "name": "legacy-sync"}),
            json!({"label": "Team", "name": "analytics"}),
            json!({"label": "Team", "name": "infra"}),
            json!({"label": "Team", "name": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_aggregates_across_declared_labels() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let named = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (entity) RETURN count(entity) AS entities",
    )
    .await
    .expect("graph-aware unlabeled node count should execute");
    assert_eq!(
        execution_to_rows(named.execution()),
        vec![json!({"entities": 10})]
    );

    let anonymous = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH () RETURN count(*) AS entities",
    )
    .await
    .expect("graph-aware anonymous unlabeled node count should execute");
    assert_eq!(
        execution_to_rows(anonymous.execution()),
        vec![json!({"entities": 10})]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_projects_missing_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         RETURN labels(entity)[0] AS label, entity.name AS name, entity.tier AS tier \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous branch property projection should execute");

    let schema = execution
        .execution()
        .batches()
        .first()
        .expect("execution should produce a batch")
        .schema();
    let field_names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(field_names, vec!["label", "name", "tier"]);
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Person", "name": "Ada Lovelace"}),
            json!({"label": "Person", "name": "Grace Hopper"}),
            json!({"label": "Person", "name": "Katherine Johnson"}),
            json!({"label": "Service", "name": "billing-api", "tier": "prod"}),
            json!({"label": "Service", "name": "deployments", "tier": "prod"}),
            json!({"label": "Service", "name": "experiments", "tier": "dev"}),
            json!({"label": "Service", "name": "legacy-sync"}),
            json!({"label": "Team", "name": "analytics"}),
            json!({"label": "Team", "name": "infra"}),
            json!({"label": "Team", "name": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_orders_by_missing_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY entity.tier ASC NULLS LAST, label, name",
    )
    .await
    .expect("heterogeneous hidden ORDER BY property should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Service", "name": "experiments"}),
            json!({"label": "Service", "name": "billing-api"}),
            json!({"label": "Service", "name": "deployments"}),
            json!({"label": "Person", "name": "Ada Lovelace"}),
            json!({"label": "Person", "name": "Grace Hopper"}),
            json!({"label": "Person", "name": "Katherine Johnson"}),
            json!({"label": "Service", "name": "legacy-sync"}),
            json!({"label": "Team", "name": "analytics"}),
            json!({"label": "Team", "name": "infra"}),
            json!({"label": "Team", "name": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_counts_missing_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) RETURN count(entity.tier) AS tiered_entities",
    )
    .await
    .expect("heterogeneous branch property count should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tiered_entities": 3})]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_filters_missing_property_comparisons_as_unknown() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         WHERE entity.tier = 'prod' \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous missing property comparison should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Service", "name": "billing-api"}),
            json!({"label": "Service", "name": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_matches_missing_property_is_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         WHERE entity.tier IS NULL \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous missing property null predicate should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Person", "name": "Ada Lovelace"}),
            json!({"label": "Person", "name": "Grace Hopper"}),
            json!({"label": "Person", "name": "Katherine Johnson"}),
            json!({"label": "Service", "name": "legacy-sync"}),
            json!({"label": "Team", "name": "analytics"}),
            json!({"label": "Team", "name": "infra"}),
            json!({"label": "Team", "name": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_filters_missing_property_is_not_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         WHERE entity.tier IS NOT NULL \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous missing property non-null predicate should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Service", "name": "billing-api"}),
            json!({"label": "Service", "name": "deployments"}),
            json!({"label": "Service", "name": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_preserves_missing_property_null_under_not() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         WHERE NOT entity.tier = 'prod' \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous missing property negated comparison should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"label": "Service", "name": "experiments"})]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_treats_missing_rhs_property_as_unknown() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service), (entity) \
         WHERE service.tier = entity.tier \
         RETURN service.name AS service, labels(entity)[0] AS entity_label, entity.name AS entity \
         ORDER BY service, entity_label, entity",
    )
    .await
    .expect("heterogeneous missing RHS property comparison should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "entity_label": "Service", "entity": "billing-api"}),
            json!({"service": "billing-api", "entity_label": "Service", "entity": "deployments"}),
            json!({"service": "deployments", "entity_label": "Service", "entity": "billing-api"}),
            json!({"service": "deployments", "entity_label": "Service", "entity": "deployments"}),
            json!({"service": "experiments", "entity_label": "Service", "entity": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_projects_missing_scalar_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         RETURN labels(entity)[0] AS label, entity.name AS name, coalesce(entity.tier, 'unknown') AS tier_bucket \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous missing scalar projection property should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Person", "name": "Ada Lovelace", "tier_bucket": "unknown"}),
            json!({"label": "Person", "name": "Grace Hopper", "tier_bucket": "unknown"}),
            json!({"label": "Person", "name": "Katherine Johnson", "tier_bucket": "unknown"}),
            json!({"label": "Service", "name": "billing-api", "tier_bucket": "prod"}),
            json!({"label": "Service", "name": "deployments", "tier_bucket": "prod"}),
            json!({"label": "Service", "name": "experiments", "tier_bucket": "dev"}),
            json!({"label": "Service", "name": "legacy-sync", "tier_bucket": "unknown"}),
            json!({"label": "Team", "name": "analytics", "tier_bucket": "unknown"}),
            json!({"label": "Team", "name": "infra", "tier_bucket": "unknown"}),
            json!({"label": "Team", "name": "platform", "tier_bucket": "unknown"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_filters_missing_scalar_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) \
         WHERE coalesce(entity.tier, 'unknown') = 'unknown' \
         RETURN labels(entity)[0] AS label, entity.name AS name \
         ORDER BY label, name",
    )
    .await
    .expect("heterogeneous missing scalar predicate property should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"label": "Person", "name": "Ada Lovelace"}),
            json!({"label": "Person", "name": "Grace Hopper"}),
            json!({"label": "Person", "name": "Katherine Johnson"}),
            json!({"label": "Service", "name": "legacy-sync"}),
            json!({"label": "Team", "name": "analytics"}),
            json!({"label": "Team", "name": "infra"}),
            json!({"label": "Team", "name": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_graph_aware_unlabeled_node_scan_aggregates_missing_scalar_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (entity) RETURN count(coalesce(entity.tier, 'unknown')) AS tier_buckets",
    )
    .await
    .expect("heterogeneous missing scalar aggregate property should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tier_buckets": 10})]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_project_missing_relationship_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[ownership:OWNS]->(service:Service) \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner, service.name AS service, ownership.since AS since \
         ORDER BY owner_label, owner, service",
    )
    .await
    .expect("heterogeneous relationship property projection should execute");

    let schema = execution
        .execution()
        .batches()
        .first()
        .expect("execution should produce a batch")
        .schema();
    let field_names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        field_names,
        vec!["owner_label", "owner", "service", "since"]
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Person", "owner": "Ada Lovelace", "service": "billing-api", "since": "2024-01-10"}),
            json!({"owner_label": "Person", "owner": "Grace Hopper", "service": "deployments", "since": "2024-02-20"}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson", "service": "experiments", "since": "2024-03-15"}),
            json!({"owner_label": "Team", "owner": "analytics", "service": "experiments"}),
            json!({"owner_label": "Team", "owner": "infra", "service": "deployments"}),
            json!({"owner_label": "Team", "owner": "platform", "service": "billing-api"}),
            json!({"owner_label": "Team", "owner": "platform", "service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_filter_missing_relationship_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[ownership:OWNS]->(service:Service) \
         WHERE ownership.since IS NULL \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner, service.name AS service \
         ORDER BY owner, service",
    )
    .await
    .expect("heterogeneous relationship property null predicate should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Team", "owner": "analytics", "service": "experiments"}),
            json!({"owner_label": "Team", "owner": "infra", "service": "deployments"}),
            json!({"owner_label": "Team", "owner": "platform", "service": "billing-api"}),
            json!({"owner_label": "Team", "owner": "platform", "service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_relationship_scalar_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[ownership:OWNS]->(service:Service) \
         RETURN owner.name AS owner, service.name AS service, coalesce(ownership.since, 'unspecified') AS since_bucket \
         ORDER BY owner, service",
    )
    .await
    .expect("heterogeneous relationship scalar property should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "since_bucket": "2024-01-10"}),
            json!({"owner": "Grace Hopper", "service": "deployments", "since_bucket": "2024-02-20"}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "since_bucket": "2024-03-15"}),
            json!({"owner": "analytics", "service": "experiments", "since_bucket": "unspecified"}),
            json!({"owner": "infra", "service": "deployments", "since_bucket": "unspecified"}),
            json!({"owner": "platform", "service": "billing-api", "since_bucket": "unspecified"}),
            json!({"owner": "platform", "service": "legacy-sync", "since_bucket": "unspecified"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_properties_in_exists_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         WHERE EXISTS { MATCH (owner)-[:OWNS]->(:Service) WHERE owner.cost_center IS NULL } \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner \
         ORDER BY owner_label, owner",
    )
    .await
    .expect("missing outer branch properties inside EXISTS subqueries should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Person", "owner": "Ada Lovelace"}),
            json!({"owner_label": "Person", "owner": "Grace Hopper"}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_relationships_in_exists() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         WHERE EXISTS { MATCH (owner)-[ownership:OWNS]->(:Service) WHERE ownership.since IS NULL } \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner \
         ORDER BY owner",
    )
    .await
    .expect("missing scoped relationship properties inside EXISTS subqueries should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Team", "owner": "analytics"}),
            json!({"owner_label": "Team", "owner": "infra"}),
            json!({"owner_label": "Team", "owner": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_properties_in_count_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         WHERE COUNT { MATCH (owner)-[:OWNS]->(:Service) WHERE owner.cost_center IS NULL } > 0 \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner \
         ORDER BY owner_label, owner",
    )
    .await
    .expect("missing outer branch properties inside COUNT subqueries should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Person", "owner": "Ada Lovelace"}),
            json!({"owner_label": "Person", "owner": "Grace Hopper"}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_relationships_in_count() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         WHERE COUNT { \
           MATCH (owner)-[ownership:OWNS]->(:Service) \
           WHERE coalesce(ownership.since, 'unclassified') = 'unclassified' \
         } > 0 \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner \
         ORDER BY owner",
    )
    .await
    .expect("missing scoped relationship properties inside COUNT subqueries should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Team", "owner": "analytics"}),
            json!({"owner_label": "Team", "owner": "infra"}),
            json!({"owner_label": "Team", "owner": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_project_count_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner, \
                COUNT { \
                  MATCH (owner)-[ownership:OWNS]->(:Service) \
                  WHERE coalesce(ownership.since, 'unclassified') = 'unclassified' \
                } AS unclassified_ownerships \
         ORDER BY owner_label, owner",
    )
    .await
    .expect("COUNT subquery projections should execute after branch expansion");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Person", "owner": "Ada Lovelace", "unclassified_ownerships": 0}),
            json!({"owner_label": "Person", "owner": "Grace Hopper", "unclassified_ownerships": 0}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson", "unclassified_ownerships": 0}),
            json!({"owner_label": "Team", "owner": "analytics", "unclassified_ownerships": 1}),
            json!({"owner_label": "Team", "owner": "infra", "unclassified_ownerships": 1}),
            json!({"owner_label": "Team", "owner": "platform", "unclassified_ownerships": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_order_by_count_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner \
         ORDER BY COUNT { MATCH (owner)-[:OWNS]->(:Service) } DESC, owner",
    )
    .await
    .expect("COUNT subquery order keys should execute after branch expansion");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Team", "owner": "platform"}),
            json!({"owner_label": "Person", "owner": "Ada Lovelace"}),
            json!({"owner_label": "Person", "owner": "Grace Hopper"}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson"}),
            json!({"owner_label": "Team", "owner": "analytics"}),
            json!({"owner_label": "Team", "owner": "infra"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_properties_in_optional_match() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         OPTIONAL MATCH (owner)-[:OWNS]->(service:Service) \
         WHERE owner.cost_center IS NULL \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner, service.name AS service \
         ORDER BY owner_label, owner, service",
    )
    .await
    .expect("missing outer properties inside OPTIONAL MATCH predicates should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Person", "owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner_label": "Person", "owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson", "service": "experiments"}),
            json!({"owner_label": "Team", "owner": "analytics"}),
            json!({"owner_label": "Team", "owner": "infra"}),
            json!({"owner_label": "Team", "owner": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_rewrite_missing_relationships_in_optional_match() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
         WHERE ownership.since IS NULL \
         RETURN labels(owner)[0] AS owner_label, owner.name AS owner, service.name AS service \
         ORDER BY owner_label, owner, service",
    )
    .await
    .expect("missing relationship properties inside OPTIONAL MATCH predicates should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner_label": "Person", "owner": "Ada Lovelace"}),
            json!({"owner_label": "Person", "owner": "Grace Hopper"}),
            json!({"owner_label": "Person", "owner": "Katherine Johnson"}),
            json!({"owner_label": "Team", "owner": "analytics", "service": "experiments"}),
            json!({"owner_label": "Team", "owner": "infra", "service": "deployments"}),
            json!({"owner_label": "Team", "owner": "platform", "service": "billing-api"}),
            json!({"owner_label": "Team", "owner": "platform", "service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_apply_global_row_modifiers_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN lower(owner.name) AS owner, service.name AS service \
         ORDER BY lower(owner.name), service.name \
         SKIP 1 \
         LIMIT 3",
    )
    .await
    .expect("static label alternatives with global row modifiers should execute");

    assert!(
        execution.translated_sql().contains("__coral_union_outer"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "analytics", "service": "experiments"}),
            json!({"owner": "grace hopper", "service": "deployments"}),
            json!({"owner": "infra", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_apply_hidden_global_ordering_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner \
         ORDER BY service.risk, lower(owner.name) \
         LIMIT 4",
    )
    .await
    .expect("static label alternatives with hidden global ordering should execute");

    assert!(
        execution.translated_sql().contains("__coral_order_0"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "analytics"}),
            json!({"owner": "Katherine Johnson"}),
            json!({"owner": "Grace Hopper"}),
            json!({"owner": "infra"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_terminal_with_projection_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         WITH owner.name AS owner, service.name AS service \
         WHERE service = 'billing-api' \
         RETURN owner, service \
         ORDER BY owner",
    )
    .await
    .expect("static alternatives with terminal WITH projection should execute");

    assert!(
        execution
            .translated_sql()
            .contains("WHERE \"n1\".\"service_name\" = 'billing-api'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "platform", "service": "billing-api"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_apply_distinct_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN DISTINCT service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("static label alternatives with global distinct should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SELECT DISTINCT * FROM"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_count_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN count(*) AS count",
    )
    .await
    .expect("static label alternatives with outer count should execute");

    assert!(
        execution.translated_sql().contains("COUNT(*) AS \"count\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"count": 7})]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_grouped_count_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.tier AS tier, count(*) AS owner_links \
         ORDER BY count(*) DESC, tier",
    )
    .await
    .expect("static label alternatives with grouped outer count should execute");

    assert!(
        execution.translated_sql().contains(" GROUP BY \"tier\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod", "owner_links": 4}),
            json!({"tier": "dev", "owner_links": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_grouped_count_property_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, count(service.tier) AS tiered_services \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with grouped outer count(property) should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"__coral_agg_1\") AS \"tiered_services\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "tiered_services": 1}),
            json!({"owner": "Grace Hopper", "tiered_services": 1}),
            json!({"owner": "Katherine Johnson", "tiered_services": 1}),
            json!({"owner": "analytics", "tiered_services": 1}),
            json!({"owner": "infra", "tiered_services": 1}),
            json!({"owner": "platform", "tiered_services": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_count_node_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, count(service) AS services \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with grouped outer count(node) should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"__coral_agg_1\") AS \"services\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n1\".\"id\" AS VARCHAR) AS \"__coral_agg_1\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "services": 1}),
            json!({"owner": "Grace Hopper", "services": 1}),
            json!({"owner": "Katherine Johnson", "services": 1}),
            json!({"owner": "analytics", "services": 1}),
            json!({"owner": "infra", "services": 1}),
            json!({"owner": "platform", "services": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_count_relationship_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[owns:OWNS]->(service:Service) \
         RETURN owner.name AS owner, count(owns) AS ownerships \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with grouped outer count(relationship) should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"__coral_agg_1\") AS \"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"r0\".\"ownership_id\" AS VARCHAR) AS \"__coral_agg_1\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"r0\".\"team_id\" AS VARCHAR) AS \"__coral_agg_1\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "ownerships": 1}),
            json!({"owner": "Grace Hopper", "ownerships": 1}),
            json!({"owner": "Katherine Johnson", "ownerships": 1}),
            json!({"owner": "analytics", "ownerships": 1}),
            json!({"owner": "infra", "ownerships": 1}),
            json!({"owner": "platform", "ownerships": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_count_distinct_node_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN count(DISTINCT owner) AS owners",
    )
    .await
    .expect("static label alternatives with outer count(DISTINCT node) should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(DISTINCT \"__coral_agg_0\") AS \"owners\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("'node:Person:'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("'node:Team:'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owners": 6})]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_collect_property_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, collect(DISTINCT service.name) AS services \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with outer collect(property) should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(DISTINCT \"__coral_agg_1\") FILTER (WHERE (\"__coral_agg_1\") IS NOT NULL), make_array()) AS \"services\""),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        let services = row
            .get_mut("services")
            .and_then(Value::as_array_mut)
            .expect("services should be a JSON array");
        services.sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
    assert_eq!(
        rows,
        vec![
            json!({"owner": "Ada Lovelace", "services": ["billing-api"]}),
            json!({"owner": "Grace Hopper", "services": ["deployments"]}),
            json!({"owner": "Katherine Johnson", "services": ["experiments"]}),
            json!({"owner": "analytics", "services": ["experiments"]}),
            json!({"owner": "infra", "services": ["deployments"]}),
            json!({"owner": "platform", "services": ["billing-api", "legacy-sync"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_collect_graph_variables_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         RETURN collect(owner) AS owners, collect(DISTINCT owner) AS distinct_owners",
    )
    .await
    .expect("static label alternatives with collect(node) should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(\"__coral_agg_0\") FILTER (WHERE (\"__coral_agg_0\") IS NOT NULL), make_array()) AS \"owners\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("'node:Person:'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("'node:Team:'"),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("collect graph variable query should return one row");
    sort_string_array_field(row, "owners");
    sort_string_array_field(row, "distinct_owners");
    assert_eq!(
        rows,
        vec![json!({
            "owners": [
                "node:Person:1",
                "node:Person:2",
                "node:Person:3",
                "node:Team:1000",
                "node:Team:2000",
                "node:Team:3000"
            ],
            "distinct_owners": [
                "node:Person:1",
                "node:Person:2",
                "node:Person:3",
                "node:Team:1000",
                "node:Team:2000",
                "node:Team:3000"
            ]
        })]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_optional_endpoint_identity_aggregates_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
         WHERE service.tier = 'prod' \
         RETURN owner.name AS owner, \
                count(endNode(ownership)) AS services, \
                count(DISTINCT endNode(ownership)) AS distinct_services, \
                collect(endNode(ownership)) AS service_ids \
         ORDER BY owner",
    )
    .await
    .expect("static alternatives with optional endpoint identity aggregates should execute");

    assert!(
        execution.translated_sql().contains(
            "COALESCE(ARRAY_AGG(\"__coral_agg_3\") FILTER (WHERE (\"__coral_agg_3\") IS NOT NULL), make_array()) AS \"service_ids\""
        ),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_i64_array_field(row, "service_ids");
    }
    assert_eq!(
        rows,
        vec![
            json!({"owner": "Ada Lovelace", "services": 1, "distinct_services": 1, "service_ids": [10]}),
            json!({"owner": "Grace Hopper", "services": 1, "distinct_services": 1, "service_ids": [20]}),
            json!({"owner": "Katherine Johnson", "services": 0, "distinct_services": 0, "service_ids": []}),
            json!({"owner": "analytics", "services": 0, "distinct_services": 0, "service_ids": []}),
            json!({"owner": "infra", "services": 1, "distinct_services": 1, "service_ids": [20]}),
            json!({"owner": "platform", "services": 1, "distinct_services": 1, "service_ids": [10]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_optional_endpoint_property_aggregates_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team) \
         OPTIONAL MATCH (owner)-[ownership:OWNS]->(service:Service) \
         WHERE service.tier = 'prod' \
         RETURN owner.name AS owner, \
                count(endNode(ownership).name) AS named_services, \
                sum(endNode(ownership).risk) AS total_risk, \
                collect(endNode(ownership).name) AS service_names \
         ORDER BY owner",
    )
    .await
    .expect("static alternatives with optional endpoint property aggregates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SUM(\"__coral_agg_2\") AS \"total_risk\""),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_string_array_field(row, "service_names");
    }
    assert_eq!(
        rows,
        vec![
            json!({"owner": "Ada Lovelace", "named_services": 1, "total_risk": 0.9, "service_names": ["billing-api"]}),
            json!({"owner": "Grace Hopper", "named_services": 1, "total_risk": 0.5, "service_names": ["deployments"]}),
            json!({"owner": "Katherine Johnson", "named_services": 0, "service_names": []}),
            json!({"owner": "analytics", "named_services": 0, "service_names": []}),
            json!({"owner": "infra", "named_services": 1, "total_risk": 0.5, "service_names": ["deployments"]}),
            json!({"owner": "platform", "named_services": 1, "total_risk": 0.9, "service_names": ["billing-api"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_numeric_aggregates_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, \
                sum(service.risk) AS total_risk, \
                avg(service.risk) AS average_risk, \
                min(service.risk) AS lowest_risk, \
                max(service.risk) AS highest_risk \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with outer numeric aggregates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SUM(\"__coral_agg_1\") AS \"total_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "total_risk": 0.9, "average_risk": 0.9, "lowest_risk": 0.9, "highest_risk": 0.9}),
            json!({"owner": "Grace Hopper", "total_risk": 0.5, "average_risk": 0.5, "lowest_risk": 0.5, "highest_risk": 0.5}),
            json!({"owner": "Katherine Johnson", "total_risk": 0.25, "average_risk": 0.25, "lowest_risk": 0.25, "highest_risk": 0.25}),
            json!({"owner": "analytics", "total_risk": 0.25, "average_risk": 0.25, "lowest_risk": 0.25, "highest_risk": 0.25}),
            json!({"owner": "infra", "total_risk": 0.5, "average_risk": 0.5, "lowest_risk": 0.5, "highest_risk": 0.5}),
            json!({"owner": "platform", "total_risk": 1.85, "average_risk": 0.925, "lowest_risk": 0.9, "highest_risk": 0.95}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_distinct_stddev_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         WHERE owner.name = 'platform' \
         RETURN owner.name AS owner, \
                stDev(DISTINCT service.risk) AS sample_risk, \
                stDevP(DISTINCT service.risk) AS population_risk",
    )
    .await
    .expect("static label alternatives with distinct stddev aggregates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SQRT(VAR_SAMP(DISTINCT \"__coral_agg_1\")) AS \"sample_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("SQRT(VAR_POP(DISTINCT \"__coral_agg_2\")) AS \"population_risk\""),
        "{}",
        execution.translated_sql()
    );

    let rows = execution_to_rows(execution.execution());
    assert_eq!(rows.len(), 1);
    let row = rows.first().expect("platform aggregate row should exist");
    assert_eq!(row.get("owner"), Some(&json!("platform")));
    assert_close(
        row["sample_risk"].as_f64().unwrap(),
        0.035_355_339_059_327_41,
    );
    assert_close(
        row["population_risk"].as_f64().unwrap(),
        0.024_999_999_999_999_967,
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_aggregate_expressions_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, \
                collect(DISTINCT coalesce(service.tier, 'unknown')) AS tiers, \
                count(coalesce(service.tier, 'unknown')) AS tier_count, \
                sum(service.risk + 1) AS adjusted_risk \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with outer aggregate expressions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"n1\".\"tier\", 'unknown') AS \"__coral_agg_1\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("SUM(\"__coral_agg_3\") AS \"adjusted_risk\""),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_string_array_field(row, "tiers");
    }
    assert_eq!(
        rows,
        vec![
            json!({"owner": "Ada Lovelace", "tiers": ["prod"], "tier_count": 1, "adjusted_risk": 1.9}),
            json!({"owner": "Grace Hopper", "tiers": ["prod"], "tier_count": 1, "adjusted_risk": 1.5}),
            json!({"owner": "Katherine Johnson", "tiers": ["dev"], "tier_count": 1, "adjusted_risk": 1.25}),
            json!({"owner": "analytics", "tiers": ["dev"], "tier_count": 1, "adjusted_risk": 1.25}),
            json!({"owner": "infra", "tiers": ["prod"], "tier_count": 1, "adjusted_risk": 1.5}),
            json!({"owner": "platform", "tiers": ["prod", "unknown"], "tier_count": 2, "adjusted_risk": 3.85}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_predicate_aggregates_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, collect(service.risk > 0.8) AS high_risk_flags \
         ORDER BY owner",
    )
    .await
    .expect("static label alternatives with outer predicate aggregates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n1\".\"risk_score\" > 0.8 AS \"__coral_agg_1\""),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_bool_array_field(row, "high_risk_flags");
    }
    assert_eq!(
        rows,
        vec![
            json!({"owner": "Ada Lovelace", "high_risk_flags": [true]}),
            json!({"owner": "Grace Hopper", "high_risk_flags": [false]}),
            json!({"owner": "Katherine Johnson", "high_risk_flags": [false]}),
            json!({"owner": "analytics", "high_risk_flags": [false]}),
            json!({"owner": "infra", "high_risk_flags": [false]}),
            json!({"owner": "platform", "high_risk_flags": [true, true]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_numeric_aggregate_type_errors_reject_before_sql_execution()
 {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, sum(service.name) AS bad_sum",
    )
    .await
    .expect_err("sum(string) should fail during graph query validation");

    assert!(
        error.to_string().contains("requires a numeric property"),
        "{error:?}"
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_aggregate_expression_type_errors_reject_before_sql_execution()
 {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS owner, sum(toString(service.risk)) AS bad_sum",
    )
    .await
    .expect_err("sum(toString(...)) should fail during graph query validation");

    assert!(
        error.to_string().contains("requires a numeric property"),
        "{error:?}"
    );
}

#[tokio::test]
async fn cypher_union_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'prod' \
         RETURN service.name AS item \
         UNION \
         MATCH (person:Person) \
         WHERE person.team = 'platform' \
         RETURN person.name AS item",
    )
    .await
    .expect("UNION Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" UNION "),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(|row| row["item"].as_str().unwrap_or_default().to_string());
    assert_eq!(
        rows,
        vec![
            json!({"item": "Ada Lovelace"}),
            json!({"item": "billing-api"}),
            json!({"item": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_flatten_inside_union_distinct() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN service.name AS item \
         UNION \
         MATCH (service:Service) \
         RETURN service.name AS item",
    )
    .await
    .expect("static alternatives should flatten into uniform UNION distinct");

    assert!(
        execution
            .translated_sql()
            .contains("SELECT DISTINCT * FROM"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(|row| row["item"].as_str().unwrap_or_default().to_string());
    assert_eq!(
        rows,
        vec![
            json!({"item": "billing-api"}),
            json!({"item": "deployments"}),
            json!({"item": "experiments"}),
            json!({"item": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_union_all_preserves_duplicate_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'prod' \
         RETURN service.tier AS tier \
         UNION ALL \
         MATCH (service:Service) \
         WHERE service.name = 'billing-api' \
         RETURN service.tier AS tier",
    )
    .await
    .expect("UNION ALL Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" UNION ALL "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod"}),
            json!({"tier": "prod"}),
            json!({"tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND ['prod', 'dev'] AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN tier AS tier, service.name AS service \
         ORDER BY tier, service",
    )
    .await
    .expect("static UNWIND Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" UNION ALL "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "dev", "service": "experiments"}),
            json!({"tier": "prod", "service": "billing-api"}),
            json!({"tier": "prod", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_after_with_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (p:Person) \
         WHERE p.name IN ['Ada Lovelace', 'Grace Hopper'] \
         WITH p \
         UNWIND [1, 2] AS n \
         RETURN p.name AS name, n \
         ORDER BY name, n",
    )
    .await
    .expect("WITH-separated static UNWIND Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" UNION ALL "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"name": "Ada Lovelace", "n": 1}),
            json!({"name": "Ada Lovelace", "n": 2}),
            json!({"name": "Grace Hopper", "n": 1}),
            json!({"name": "Grace Hopper", "n": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_case_lists_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND (CASE WHEN true THEN ['prod', 'dev', 'stage'] ELSE ['legacy'] END)[0..2] AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN tier AS tier, service.name AS service \
         ORDER BY tier, service",
    )
    .await
    .expect("static UNWIND over list-valued CASE should execute");

    assert!(
        execution.translated_sql().contains(" UNION ALL "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "dev", "service": "experiments"}),
            json!({"tier": "prod", "service": "billing-api"}),
            json!({"tier": "prod", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_range_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND range(1, 3) AS ordinal \
         MATCH (service:Service) \
         WHERE service.id = ordinal * 10 \
         RETURN ordinal AS ordinal, service.name AS service \
         ORDER BY ordinal",
    )
    .await
    .expect("static range UNWIND query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"ordinal": 1, "service": "billing-api"}),
            json!({"ordinal": 2, "service": "deployments"}),
            json!({"ordinal": 3, "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_parameterized_range_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "start".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(1)),
        ),
        (
            "end".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(4)),
        ),
        (
            "step".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(2)),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND range($start, $end, $step) AS ordinal \
         MATCH (service:Service) \
         WHERE service.id = ordinal * 10 \
         RETURN ordinal AS ordinal, service.name AS service \
         ORDER BY ordinal",
        &parameters,
    )
    .await
    .expect("parameterized static range UNWIND query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"ordinal": 1, "service": "billing-api"}),
            json!({"ordinal": 3, "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_parameterized_split_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "tiers".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("prod|dev".to_string())),
        ),
        (
            "delimiter".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("|".to_string())),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND split($tiers, $delimiter) AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN tier AS tier, service.name AS service \
         ORDER BY tier, service",
        &parameters,
    )
    .await
    .expect("parameterized static split UNWIND query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "dev", "service": "experiments"}),
            json!({"tier": "prod", "service": "billing-api"}),
            json!({"tier": "prod", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_duplicate_aggregates_execute_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND ['prod', 'prod', 'dev'] AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN tier AS tier, count(*) AS services \
         ORDER BY tier",
    )
    .await
    .expect("duplicate static UNWIND aggregate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) AS \"services\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "dev", "services": 1}),
            json!({"tier": "prod", "services": 4}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_empty_list_preserves_aggregate_zero_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND [] AS tier \
         MATCH (service:Service) \
         RETURN count(*) AS services",
    )
    .await
    .expect("empty static UNWIND aggregate query should execute");

    assert!(
        execution.translated_sql().contains("WHERE FALSE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"services": 0})]
    );
}

#[tokio::test]
async fn cypher_static_unwind_list_parameters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "tiers".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("dev".to_string()),
            GraphLiteral::String("prod".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND $tiers AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN tier AS tier, service.name AS service \
         ORDER BY tier, service",
        &parameters,
    )
    .await
    .expect("parameterized static UNWIND query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "dev", "service": "experiments"}),
            json!({"tier": "prod", "service": "billing-api"}),
            json!({"tier": "prod", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_unwind_applies_hidden_ordering_after_union() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND ['dev', 'prod'] AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN service.name AS service \
         ORDER BY CASE WHEN tier = 'prod' THEN 0 ELSE 1 END, service",
    )
    .await
    .expect("static UNWIND hidden ordering query should execute");

    assert!(
        execution.translated_sql().contains("__coral_order_0"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_node_label_alternatives_flatten_inside_union_all() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person|Team)-[:OWNS]->(service:Service) \
         RETURN owner.name AS item \
         UNION ALL \
         MATCH (service:Service) \
         WHERE service.name = 'billing-api' \
         RETURN service.name AS item",
    )
    .await
    .expect("static alternatives inside UNION ALL should execute");

    assert!(
        execution.translated_sql().contains(" UNION ALL "),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains("__coral_union_outer"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(|row| row["item"].as_str().unwrap_or_default().to_string());
    assert_eq!(
        rows,
        vec![
            json!({"item": "Ada Lovelace"}),
            json!({"item": "Grace Hopper"}),
            json!({"item": "Katherine Johnson"}),
            json!({"item": "analytics"}),
            json!({"item": "billing-api"}),
            json!({"item": "infra"}),
            json!({"item": "platform"}),
            json!({"item": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_union_branches_preserve_local_ordering_and_limits() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS item \
         ORDER BY service.name \
         LIMIT 1 \
         UNION ALL \
         MATCH (person:Person) \
         RETURN person.name AS item \
         ORDER BY person.name \
         LIMIT 1",
    )
    .await
    .expect("UNION branches with local modifiers should execute");

    assert!(
        execution.translated_sql().contains("SELECT * FROM (SELECT"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(|row| row["item"].as_str().unwrap_or_default().to_string());
    assert_eq!(
        rows,
        vec![
            json!({"item": "Ada Lovelace"}),
            json!({"item": "billing-api"})
        ]
    );
}

#[tokio::test]
async fn cypher_union_rejects_catalog_typed_projection_mismatches_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS value \
         UNION \
         MATCH (service:Service) \
         RETURN service.risk AS value",
    )
    .await
    .expect_err("UNION projection type mismatch should fail before SQL execution");

    assert!(
        error.to_string().contains("UNION branch projection types"),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_ignored_path_variables_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
         WHERE service.tier = 'prod' \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY person.name ASC \
         LIMIT 25",
    )
    .await
    .expect("path-bound Cypher query should execute without materializing path values");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_path_element_id_lists_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[:OWNS]->(service:Service) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN nodes(path) AS path_nodes, relationships(path) AS path_relationships",
    )
    .await
    .expect("fixed path element id lists should execute");

    assert!(
        execution.translated_sql().contains("make_array("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "path_nodes": [1, 10],
            "path_relationships": [100],
        })]
    );
}

#[tokio::test]
async fn cypher_path_element_list_indexes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE person.name = 'Ada Lovelace' AND nodes(path)[0] = id(person) \
         RETURN nodes(path)[0] AS first_node, \
                nodes(path)[-1] AS last_node, \
                nodes(path)[2] AS missing_node, \
                relationships(path)[0] AS first_relationship, \
                relationships(path)[-1] AS last_relationship, \
                relationships(path)[1] AS missing_relationship, \
                head(nodes(path)) AS head_node, \
                last(relationships(path)) AS last_relationship_endpoint \
         ORDER BY nodes(path)[0], relationships(path)[-1]",
    )
    .await
    .expect("fixed path element list indexes should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"id\" AS \"first_node\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("NULL AS \"missing_node\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("NULL AS \"missing_relationship\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "first_node": 1,
            "last_node": 10,
            "first_relationship": 100,
            "last_relationship": 100,
            "head_node": 1,
            "last_relationship_endpoint": 100,
        })]
    );
}

#[tokio::test]
async fn cypher_path_element_list_slices_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE person.name = 'Ada Lovelace' \
         RETURN nodes(path)[1..] AS node_tail_slice, \
                nodes(path)[..1] AS node_prefix_slice, \
                relationships(path)[..1] AS relationship_prefix_slice, \
                tail(nodes(path)) AS node_tail, \
                tail(relationships(path)) AS relationship_tail, \
                reverse(nodes(path)) AS reversed_nodes, \
                reverse(relationships(path)) AS reversed_relationships",
    )
    .await
    .expect("fixed path element list slices should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "node_tail_slice": [10],
            "node_prefix_slice": [1],
            "relationship_prefix_slice": [100],
            "node_tail": [10],
            "relationship_tail": [],
            "reversed_nodes": [10, 1],
            "reversed_relationships": [100],
        })]
    );
}

#[tokio::test]
async fn cypher_relationship_type_overloads_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team)-[:OWNS]->(service:Service) \
         WHERE service.tier = 'prod' \
         RETURN team.name AS team, team.cost_center AS cost_center, service.name AS service \
         ORDER BY team, service",
    )
    .await
    .expect("overloaded relationship type Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"team_ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "infra", "cost_center": "cc-infra", "service": "deployments"}),
            json!({"team": "platform", "cost_center": "cc-platform", "service": "billing-api"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exact_one_relationship_ranges_execute_as_single_hop() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON*1..1]->(target:Service) \
         RETURN source.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("exact-one relationship range query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exact_fixed_relationship_ranges_execute_as_repeated_hops() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON*2..2]->(target:Service) \
         RETURN source.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("exact fixed relationship range query should execute");

    assert!(
        execution
            .translated_sql()
            .matches("JOIN \"ops\".\"service_dependencies\"")
            .count()
            >= 2,
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments"})]
    );
}

#[tokio::test]
async fn cypher_cross_label_fixed_relationship_ranges_infer_intermediate_labels() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[:ROUTES*2]->(incident:Incident) \
         RETURN person.name AS person, incident.title AS incident, length(path) AS hops \
         ORDER BY person, incident",
    )
    .await
    .expect("cross-label fixed-hop path should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"person_service_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_incident_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"person": "Ada Lovelace", "incident": "billing latency", "hops": 2}),
            json!({"person": "Grace Hopper", "incident": "deploy failed", "hops": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_path_element_list_sizes_execute_as_folded_metadata() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[:ROUTES*2]->(incident:Incident) \
         WHERE size(nodes(path)) = 3 AND size(relationships(path)) = 2 \
         RETURN person.name AS person, \
                incident.title AS incident, \
                size(nodes(path)) AS path_nodes, \
                size(relationships(path)) AS path_relationships \
         ORDER BY person, incident",
    )
    .await
    .expect("path element-list sizes should execute as folded path metadata");

    assert!(
        execution.translated_sql().contains("3 AS \"path_nodes\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("2 AS \"path_relationships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"person": "Ada Lovelace", "incident": "billing latency", "path_nodes": 3, "path_relationships": 2}),
            json!({"person": "Grace Hopper", "incident": "deploy failed", "path_nodes": 3, "path_relationships": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_fixed_relationship_ranges_infer_unlabeled_endpoints() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:ROUTES*2]->(incident) \
         RETURN person.name AS person, incident.title AS incident \
         ORDER BY person, incident",
    )
    .await
    .expect("fixed-hop path should infer the unlabeled endpoint from declaration metadata");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_incident_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"person": "Ada Lovelace", "incident": "billing latency"}),
            json!({"person": "Grace Hopper", "incident": "deploy failed"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exact_gql_relationship_quantifiers_execute_as_repeated_hops() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON]->{2}(target:Service) \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target",
    )
    .await
    .expect("exact GQL relationship quantifier query should execute");

    assert!(
        execution
            .translated_sql()
            .matches("JOIN \"ops\".\"service_dependencies\"")
            .count()
            >= 2,
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("2 AS \"hops\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments", "hops": 2})]
    );
}

#[tokio::test]
async fn cypher_bounded_cross_label_ranges_prune_impossible_lengths() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[:ROUTES*0..2]->(incident:Incident) \
         RETURN person.name AS person, incident.title AS incident, length(path) AS hops \
         ORDER BY person, incident",
    )
    .await
    .expect("bounded cross-label path should execute after pruning impossible lengths");

    assert!(
        !execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"person_service_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_incident_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"person": "Ada Lovelace", "incident": "billing latency", "hops": 2}),
            json!({"person": "Grace Hopper", "incident": "deploy failed", "hops": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_parameterized_dynamic_bounded_cross_label_ranges_prune_impossible_lengths() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "from_label".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("Person".to_string())),
        ),
        (
            "relationship_type".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("ROUTES".to_string())),
        ),
        (
            "to_label".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("Incident".to_string())),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:$($from_label))-[:$($relationship_type)*0..2]->(incident:$($to_label)) \
         RETURN person.name AS person, incident.title AS incident, length(path) AS hops \
         ORDER BY person, incident",
        &parameters,
    )
    .await
    .expect("parameterized dynamic bounded cross-label path should execute after pruning impossible lengths");

    assert!(
        !execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"person_service_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_incident_routes\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"person": "Ada Lovelace", "incident": "billing latency", "hops": 2}),
            json!({"person": "Grace Hopper", "incident": "deploy failed", "hops": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_bounded_cross_label_ranges_with_no_feasible_lengths_return_empty_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_route_fixture(temp.path());
    let source = build_source(route_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(ROUTE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (person:Person)-[:ROUTES*0..1]->(incident:Incident) \
         RETURN person.name AS person, incident.title AS incident, length(path) AS hops \
         ORDER BY person, incident",
    )
    .await
    .expect("all-pruned bounded cross-label path should execute as an empty result");

    assert!(
        execution.translated_sql().contains("WHERE")
            && execution.translated_sql().contains("FALSE"),
        "{}",
        execution.translated_sql()
    );
    assert!(execution_to_rows(execution.execution()).is_empty());
}

#[tokio::test]
async fn cypher_path_length_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*2..2]->(target:Service) \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target",
    )
    .await
    .expect("path length query should execute");

    assert!(
        execution.translated_sql().contains("2 AS \"hops\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments", "hops": 2})]
    );
}

#[tokio::test]
async fn cypher_size_path_alias_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         WHERE size(path) = 2 \
         RETURN source.name AS source, target.name AS target, size(path) AS hops \
         ORDER BY size(path), source, target",
    )
    .await
    .expect("size(path) should execute as a path length alias");

    assert!(
        execution.translated_sql().contains("2 AS \"hops\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments", "hops": 2})]
    );
}

#[tokio::test]
async fn cypher_path_metadata_arithmetic_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         WHERE size(path) + 1 = 3 \
         RETURN source.name AS source, target.name AS target, length(path) + 1 AS depth \
         ORDER BY size(path) + 1, source, target",
    )
    .await
    .expect("path metadata arithmetic should execute");

    assert!(
        execution.translated_sql().contains("(2 + 1) AS \"depth\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments", "depth": 3})]
    );
}

#[tokio::test]
async fn cypher_path_metadata_scalar_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         WHERE coalesce(size(path), 0) = 2 \
         RETURN source.name AS source, target.name AS target, \
                coalesce(length(path), 0) AS hops, \
                toString(size(path)) AS hops_text, \
                CASE WHEN length(path) = 2 THEN size(path) ELSE 0 END AS case_hops \
         ORDER BY coalesce(size(path), 0), source, target",
    )
    .await
    .expect("path metadata scalar functions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(2, 0) AS \"hops\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "source": "billing-api",
            "target": "experiments",
            "hops": 2,
            "hops_text": "2",
            "case_hops": 2,
        })]
    );
}

#[tokio::test]
async fn cypher_anonymous_optional_path_length_preserves_unmatched_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service) \
         OPTIONAL MATCH path = (source)-[:DEPENDS_ON]->(target:Service) \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target",
    )
    .await
    .expect("anonymous optional path length query should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN")
            && execution
                .translated_sql()
                .contains("THEN NULL ELSE 1 END AS \"hops\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments", "hops": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 1}),
            json!({"source": "deployments", "target": "experiments", "hops": 1}),
            json!({"source": "experiments"}),
            json!({"source": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_zero_hop_path_length_executes_as_identity() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service) \
         OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(self:Service) \
         RETURN source.name AS source, self.name AS self, length(path) AS hops, size(path) AS path_size \
         ORDER BY source",
    )
    .await
    .expect("deterministic optional zero-hop path length query should execute");

    assert!(
        execution.translated_sql().contains("0 AS \"hops\"")
            && execution.translated_sql().contains("0 AS \"path_size\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains(" LEFT JOIN "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "self": "billing-api", "hops": 0, "path_size": 0}),
            json!({"source": "deployments", "self": "deployments", "hops": 0, "path_size": 0}),
            json!({"source": "experiments", "self": "experiments", "hops": 0, "path_size": 0}),
            json!({"source": "legacy-sync", "self": "legacy-sync", "hops": 0, "path_size": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_zero_hop_bound_path_length_uses_endpoint_equality() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let same_label = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (source:Service), (target:Service) \
         OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(target) \
         WITH * WHERE length(path) IS NOT NULL \
         RETURN count(*) AS self_pairs",
    )
    .await
    .expect("bound same-label zero-hop path length query should execute");

    assert!(
        same_label.translated_sql().contains("CASE WHEN")
            && same_label.translated_sql().contains("THEN 0 ELSE NULL END"),
        "{}",
        same_label.translated_sql()
    );
    assert_eq!(
        execution_to_rows(same_label.execution()),
        vec![json!({"self_pairs": 4})]
    );

    let cross_label = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (source:Service), (person:Person) \
         OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(person) \
         WITH * WHERE length(path) IS NULL \
         RETURN count(*) AS null_paths",
    )
    .await
    .expect("bound cross-label zero-hop path length query should execute");

    assert!(
        cross_label
            .translated_sql()
            .contains("CASE WHEN FALSE THEN 0 ELSE NULL END"),
        "{}",
        cross_label.translated_sql()
    );
    assert_eq!(
        execution_to_rows(cross_label.execution()),
        vec![json!({"null_paths": 12})]
    );
}

#[tokio::test]
async fn cypher_optional_zero_hop_local_predicate_gates_path_metadata() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service), (target:Service) \
         OPTIONAL MATCH path = (source)-[:DEPENDS_ON*0]->(target) \
         WHERE source.tier = 'prod' \
         WITH * WHERE length(path) IS NOT NULL \
         RETURN count(*) AS prod_self_pairs",
    )
    .await
    .expect("zero-hop optional local predicate should gate path metadata");

    assert!(
        execution.translated_sql().contains("CASE WHEN")
            && execution.translated_sql().contains("AND")
            && execution.translated_sql().contains("THEN 0 ELSE NULL END"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"prod_self_pairs": 2})]
    );
}

#[tokio::test]
async fn cypher_bounded_gql_relationship_quantifiers_expand_to_union_all() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON]->{1,2}(target:Service) \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target, hops",
    )
    .await
    .expect("bounded GQL relationship quantifier query should execute");

    assert!(
        execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments", "hops": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 2}),
            json!({"source": "deployments", "target": "experiments", "hops": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_bounded_variable_length_ranges_expand_to_union_all() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target, hops",
    )
    .await
    .expect("bounded variable-length range query should execute");

    assert!(
        execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments", "hops": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 2}),
            json!({"source": "deployments", "target": "experiments", "hops": 1}),
        ]
    );

    let ordered_execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         RETURN source.name AS source, target.name AS target \
         ORDER BY length(path), source, target",
    )
    .await
    .expect("bounded range path length ordering should execute");

    assert_eq!(
        execution_to_rows(ordered_execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
            json!({"source": "billing-api", "target": "experiments"}),
        ]
    );

    let filtered_execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         WHERE length(path) = 2 \
         RETURN source.name AS source, target.name AS target",
    )
    .await
    .expect("bounded range path length predicate should execute");

    assert_eq!(
        execution_to_rows(filtered_execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments"})]
    );

    let count_execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         RETURN count(*) AS paths",
    )
    .await
    .expect("bounded variable-length aggregate query should execute");

    assert_eq!(
        execution_to_rows(count_execution.execution()),
        vec![json!({"paths": 4})]
    );
}

#[tokio::test]
async fn cypher_zero_hop_bounded_variable_length_ranges_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*0..1]->(target:Service) \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target, hops",
    )
    .await
    .expect("zero-hop bounded variable-length range query should execute");

    assert!(
        execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "billing-api", "hops": 0}),
            json!({"source": "billing-api", "target": "deployments", "hops": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 1}),
            json!({"source": "deployments", "target": "deployments", "hops": 0}),
            json!({"source": "deployments", "target": "experiments", "hops": 1}),
            json!({"source": "experiments", "target": "experiments", "hops": 0}),
            json!({"source": "legacy-sync", "target": "legacy-sync", "hops": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_exact_fixed_relationship_range_property_maps_apply_per_hop() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON*2..2 {source: 'catalog'}]->(target:Service) \
         RETURN source.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("exact fixed relationship range property map query should execute");

    assert!(
        execution
            .translated_sql()
            .matches(".\"source\" = 'catalog'")
            .count()
            >= 2,
        "{}",
        execution.translated_sql()
    );
    assert!(execution_to_rows(execution.execution()).is_empty());
}

#[tokio::test]
async fn cypher_inline_node_property_maps_execute_as_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {tier: 'prod'}) \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_pattern_predicates_execute_as_semijoins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { (service)-[:DEPENDS_ON]->(:Service) } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS pattern predicate should execute");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_pattern_predicates_apply_inline_maps() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { (service)-[:DEPENDS_ON {source: 'catalog'}]->(:Service {tier: 'dev'}) } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS pattern predicate with property maps should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_r0\".\"source\" = 'catalog'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"tier\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_pattern_predicates_support_outer_right_endpoint() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { (:Service {name: 'billing-api'})-[:DEPENDS_ON]->(service) } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS pattern predicate with outer right endpoint should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_pattern_where_matches_explicit_match_subquery() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let compact = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { (service)-[:DEPENDS_ON]->(target:Service) WHERE target.tier = 'dev' } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("compact EXISTS pattern WHERE should execute");
    assert!(
        compact
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"tier\" = 'dev'"),
        "{}",
        compact.translated_sql()
    );

    let explicit = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { MATCH (service)-[:DEPENDS_ON]->(target:Service) WHERE target.tier = 'dev' } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("explicit EXISTS MATCH WHERE should execute");

    assert_eq!(
        execution_to_rows(compact.execution()),
        execution_to_rows(explicit.execution())
    );
    assert_eq!(
        execution_to_rows(compact.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_execute_as_semijoins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS MATCH subquery should execute");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_apply_inner_where_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[dependency:DEPENDS_ON]->(target:Service) \
           WHERE target.tier = 'dev' AND dependency.source = 'catalog' \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS MATCH subquery with inner WHERE should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"tier\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_r0\".\"source\" = 'catalog'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_subqueries_execute_as_precomputed_boolean_projections() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS has_dependency \
         ORDER BY has_dependency DESC, service",
    )
    .await
    .expect("Cypher EXISTS scalar projection should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) > 0 AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "has_dependency": true}),
            json!({"service": "deployments", "has_dependency": true}),
            json!({"service": "experiments", "has_dependency": false}),
            json!({"service": "legacy-sync", "has_dependency": false}),
        ]
    );
}

#[tokio::test]
async fn cypher_projected_correlated_subquery_order_expressions_use_aliases() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS has_dependency \
         ORDER BY EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC, service",
    )
    .await
    .expect("projected correlated subquery ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"has_dependency\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "has_dependency": true}),
            json!({"service": "deployments", "has_dependency": true}),
            json!({"service": "experiments", "has_dependency": false}),
            json!({"service": "legacy-sync", "has_dependency": false}),
        ]
    );
}

#[tokio::test]
async fn cypher_projected_count_subquery_order_expressions_use_aliases() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS dependency_count \
         ORDER BY COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC, service",
    )
    .await
    .expect("projected COUNT subquery ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"dependency_count\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_count": 2}),
            json!({"service": "deployments", "dependency_count": 1}),
            json!({"service": "experiments", "dependency_count": 0}),
            json!({"service": "legacy-sync", "dependency_count": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_count_subquery_order_expressions_use_precomputed_joins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC, service",
    )
    .await
    .expect("hidden COUNT subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains("LEFT JOIN (SELECT")
            && execution
                .translated_sql()
                .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_count_distinct_subquery_order_expressions_use_precomputed_joins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { \
           MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
           RETURN DISTINCT CASE WHEN dependency.criticality = 'optional' THEN null ELSE dependency.source END \
         } DESC, service",
    )
    .await
    .expect("hidden COUNT DISTINCT subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains("SELECT DISTINCT")
            && execution
                .translated_sql()
                .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_uncorrelated_count_distinct_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { \
           MATCH (:Service)-[dependency:DEPENDS_ON]->(:Service) \
           RETURN DISTINCT dependency.source \
         } DESC, service",
    )
    .await
    .expect("hidden uncorrelated COUNT DISTINCT subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains("CROSS JOIN (SELECT")
            && execution.translated_sql().contains("SELECT DISTINCT"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_uncorrelated_node_count_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { MATCH (other:Service) WHERE other.tier = 'prod' } + 1 DESC, service",
    )
    .await
    .expect("hidden uncorrelated node COUNT subquery ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CROSS JOIN (SELECT COUNT(*) AS \"__coral_value\" FROM \"ops\".\"services\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "ORDER BY (COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) + 1) DESC"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_uncorrelated_node_count_distinct_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { MATCH (other:Service) RETURN DISTINCT other.tier } DESC, service",
    )
    .await
    .expect("hidden uncorrelated node COUNT DISTINCT subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains("CROSS JOIN (SELECT")
            && execution.translated_sql().contains("SELECT DISTINCT")
            && execution
                .translated_sql()
                .contains("AS \"__coral_count_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_correlated_node_count_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { MATCH (other:Service) WHERE other.tier = service.tier } DESC, service",
    )
    .await
    .expect("hidden correlated node COUNT subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains(
            "LEFT JOIN (SELECT \"__coral_count_n0\".\"tier\" AS \"__coral_outer_key\", \
             COUNT(*) AS \"__coral_value\" FROM \"ops\".\"services\" AS \"__coral_count_n0\" \
             GROUP BY \"__coral_count_n0\".\"tier\") AS \"__coral_scalar_subquery_0\" \
             ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"tier\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_correlated_node_count_distinct_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { \
           MATCH (other:Service) \
           WHERE other.tier = service.tier \
           RETURN DISTINCT other.team \
         } DESC, service",
    )
    .await
    .expect("hidden correlated node COUNT DISTINCT subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains("LEFT JOIN (SELECT")
            && execution.translated_sql().contains("SELECT DISTINCT")
            && execution
                .translated_sql()
                .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_correlated_node_exists_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY EXISTS { \
           MATCH (other:Service) \
           WHERE other.tier = service.tier AND other.active = false \
         } DESC, service",
    )
    .await
    .expect("hidden correlated node EXISTS subquery ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) > 0 AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_uncorrelated_relationship_subquery_order_expressions_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { MATCH (:Service)-[:DEPENDS_ON]->(:Service) } \
           + CASE WHEN EXISTS { MATCH (:Service)-[:DEPENDS_ON]->(:Service {tier: 'dev'}) } \
                  THEN 1 ELSE 0 END DESC, service",
    )
    .await
    .expect("hidden uncorrelated relationship subquery ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .matches("CROSS JOIN (SELECT")
            .count()
            >= 2,
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"ops\".\"service_dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_exists_subquery_order_expressions_use_precomputed_joins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } DESC, service",
    )
    .await
    .expect("hidden EXISTS subquery ORDER BY expression should execute");

    assert!(
        execution.translated_sql().contains("LEFT JOIN (SELECT")
            && execution
                .translated_sql()
                .contains("COUNT(*) > 0 AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "ORDER BY COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", FALSE) DESC"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_compound_count_subquery_order_expressions_use_precomputed_joins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } \
                + COUNT { MATCH (:Service)-[:DEPENDS_ON]->(service) } DESC, service",
    )
    .await
    .expect("hidden compound COUNT subquery ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .matches("LEFT JOIN (SELECT")
            .count()
            >= 2
            && execution
                .translated_sql()
                .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY (COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) + COALESCE(\"__coral_scalar_subquery_1\".\"__coral_value\", 0)) DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_case_exists_order_expressions_use_precomputed_joins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY CASE \
           WHEN EXISTS { MATCH (service)-[:DEPENDS_ON {criticality: 'runtime'}]->(:Service) } THEN 0 \
           WHEN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } THEN 1 \
           ELSE 2 \
         END ASC, service",
    )
    .await
    .expect("hidden CASE EXISTS ORDER BY expression should execute");

    assert!(
        execution
            .translated_sql()
            .matches("LEFT JOIN (SELECT")
            .count()
            >= 2
            && execution
                .translated_sql()
                .contains("COUNT(*) > 0 AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("ORDER BY CASE WHEN COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", FALSE) THEN 0 WHEN COALESCE(\"__coral_scalar_subquery_1\".\"__coral_value\", FALSE) THEN 1 ELSE 2 END ASC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_subqueries_execute_inside_boolean_composition() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE NOT EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } \
            OR service.name = 'billing-api' \
            AND EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service {tier: 'dev'}) } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS boolean composition should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_subqueries_execute_inside_searched_case() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                CASE \
                  WHEN EXISTS { MATCH (service)-[:DEPENDS_ON {criticality: 'runtime'}]->(:Service) } THEN 'runtime-dependency' \
                  ELSE 'no-runtime-dependency' \
                END AS dependency_state \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS searched CASE should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_state": "runtime-dependency"}),
            json!({"service": "deployments", "dependency_state": "no-runtime-dependency"}),
            json!({"service": "experiments", "dependency_state": "no-runtime-dependency"}),
            json!({"service": "legacy-sync", "dependency_state": "no-runtime-dependency"}),
        ]
    );
}

#[tokio::test]
async fn cypher_multiple_exists_scalar_subqueries_execute_in_case_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN CASE \
                  WHEN EXISTS { MATCH (service)-[:DEPENDS_ON {criticality: 'runtime'}]->(:Service) } THEN 'runtime-dependency' \
                  WHEN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } THEN 'non-runtime-dependency' \
                  ELSE 'isolated' \
                END AS dependency_state, \
                service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("multiple correlated EXISTS scalar subqueries should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_state": "runtime-dependency"}),
            json!({"service": "deployments", "dependency_state": "non-runtime-dependency"}),
            json!({"service": "experiments", "dependency_state": "isolated"}),
            json!({"service": "legacy-sync", "dependency_state": "isolated"}),
        ]
    );
}

#[tokio::test]
async fn cypher_multiple_count_scalar_subqueries_execute_in_arithmetic_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } \
                  + COUNT { MATCH (:Service)-[:DEPENDS_ON]->(service) } AS dependency_degree \
         ORDER BY service",
    )
    .await
    .expect("multiple correlated COUNT scalar subqueries should execute");

    assert!(
        execution
            .translated_sql()
            .matches("LEFT JOIN (SELECT")
            .count()
            >= 2
            && execution.translated_sql().contains("GROUP BY"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_degree": 2}),
            json!({"service": "deployments", "dependency_degree": 2}),
            json!({"service": "experiments", "dependency_degree": 2}),
            json!({"service": "legacy-sync", "dependency_degree": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_multiple_count_scalar_subqueries_execute_as_separate_projections() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS outbound_dependencies, \
                COUNT { MATCH (:Service)-[:DEPENDS_ON]->(service) } AS inbound_dependencies \
         ORDER BY service",
    )
    .await
    .expect("multiple correlated COUNT scalar subquery projections should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "outbound_dependencies": 2, "inbound_dependencies": 0}),
            json!({"service": "deployments", "outbound_dependencies": 1, "inbound_dependencies": 1}),
            json!({"service": "experiments", "outbound_dependencies": 0, "inbound_dependencies": 2}),
            json!({"service": "legacy-sync", "outbound_dependencies": 0, "inbound_dependencies": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_multihop_patterns() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(:Service)-[:DEPENDS_ON]->(:Service {name: 'experiments'}) \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS MATCH subquery with a multi-hop pattern should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_dependencies\" AS \"__coral_exists_r1\" ON TRUE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_apply_later_hop_relationship_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(:Service)-[second:DEPENDS_ON]->(:Service {tier: 'dev'}) \
           WHERE second.source = 'deploy' \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS MATCH subquery should support predicates on later relationships");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_r1\".\"source\" = 'deploy'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_multiple_inner_match_clauses() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
           MATCH (owner:Person)-[:OWNS]->(dependency) \
           WHERE owner.team = 'analytics' \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS subqueries should support multiple inner MATCH clauses");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\" AS \"__coral_exists_r1\" ON TRUE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_apply_where_from_each_inner_match_clause() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
           WHERE dependency.tier = 'prod' \
           MATCH (owner:Person)-[:OWNS]->(dependency) \
           WHERE owner.team = 'infra' \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS subqueries should preserve WHERE predicates from each MATCH clause");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n1\".\"team\" = 'infra'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_node_only_patterns() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team) \
         WHERE EXISTS { \
           MATCH (service:Service) \
           WHERE service.team = team.name AND service.tier = 'prod' \
         } \
         RETURN team.name AS team \
         ORDER BY team",
    )
    .await
    .expect("Cypher EXISTS node-only subquery should execute");

    assert!(
        execution
            .translated_sql()
            .contains("EXISTS (SELECT 1 FROM \"ops\".\"services\" AS \"__coral_exists_n0\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"team": "infra"}), json!({"team": "platform"}),]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_uncorrelated_relationship_patterns() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team) \
         WHERE EXISTS { \
           MATCH (:Service)-[:DEPENDS_ON]->(dependency:Service) \
           WHERE dependency.tier = 'dev' \
         } \
         RETURN team.name AS team \
         ORDER BY team",
    )
    .await
    .expect("Cypher EXISTS uncorrelated relationship subquery should execute");

    assert!(
        execution
            .translated_sql()
            .contains("EXISTS (SELECT 1 FROM \"ops\".\"service_dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics"}),
            json!({"team": "infra"}),
            json!({"team": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_disconnected_inner_components() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(:Service), (:Service)-[:DEPENDS_ON]->(:Service) \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS MATCH disconnected inner components should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_dependencies\" AS \"__coral_exists_r1\" ON TRUE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_non_conjunctive_inner_where() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE target.tier = 'dev' OR target.tier = 'prod' \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("non-conjunctive EXISTS MATCH WHERE should execute");

    assert!(
        execution.translated_sql().contains(
            "(\"__coral_exists_n0\".\"tier\" = 'dev' OR \"__coral_exists_n0\".\"tier\" = 'prod')"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_pattern_where_supports_non_conjunctive_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { (service)-[:DEPENDS_ON]->(target:Service) WHERE target.tier = 'dev' OR target.tier = 'prod' } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("non-conjunctive compact EXISTS pattern WHERE should execute");

    assert!(
        execution.translated_sql().contains(
            "(\"__coral_exists_n0\".\"tier\" = 'dev' OR \"__coral_exists_n0\".\"tier\" = 'prod')"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_scoped_exists() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE EXISTS { MATCH (target)-[:DEPENDS_ON]->(:Service) } \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped EXISTS should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_exists_boolean_composition() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE target.tier = 'prod' OR EXISTS { MATCH (target)-[:DEPENDS_ON]->(:Service) } \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped EXISTS inside boolean composition should execute");

    assert!(
        execution
            .translated_sql()
            .contains("OR EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_exists_property_where() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE EXISTS { \
             MATCH (target)-[:DEPENDS_ON]->(leaf:Service) \
             WHERE leaf.tier = 'dev' \
           } \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped EXISTS property predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_nested_exists_n0\".\"tier\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_exists_complex_own_where() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE EXISTS { \
             MATCH (target)-[:DEPENDS_ON]->(leaf:Service) \
             WHERE leaf.tier = 'dev' OR leaf.tier = 'prod' \
           } \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped EXISTS complex predicates should execute");

    assert!(
        execution.translated_sql().contains(
            "(\"__coral_nested_exists_n0\".\"tier\" = 'dev' OR \"__coral_nested_exists_n0\".\"tier\" = 'prod')"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_exists_parent_scoped_property_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE EXISTS { \
             MATCH (target)-[:DEPENDS_ON]->(leaf:Service) \
             WHERE leaf.team <> target.team \
           } \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped EXISTS predicates should resolve parent scoped properties");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"owning_team\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_count_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE COUNT { MATCH (target)-[:DEPENDS_ON]->(:Service) } > 0 \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped COUNT relationship predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_nested_count_r0\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_count_parent_scoped_property_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE COUNT { \
             MATCH (target)-[:DEPENDS_ON]->(leaf:Service) \
             WHERE leaf.team <> target.team \
           } > 0 \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("nested scoped COUNT existence thresholds should resolve parent scoped properties");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"owning_team\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_count_zero_thresholds() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE COUNT { \
             MATCH (target)-[:DEPENDS_ON]->(leaf:Service) \
             WHERE leaf.team = target.team \
           } = 0 \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("nested scoped COUNT zero thresholds should lower to NOT EXISTS");

    assert!(
        execution
            .translated_sql()
            .contains("NOT EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_nested_node_count_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE COUNT { MATCH (leaf:Service) WHERE leaf.tier = 'dev' } > 0 \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("nested scoped COUNT node predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_nested_count_n0\".\"tier\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subqueries_execute_as_precomputed_correlated_counts() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } AS dependency_count \
         ORDER BY dependency_count DESC, service",
    )
    .await
    .expect("Cypher COUNT subquery projection should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) AS \"dependency_count\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_count": 2}),
            json!({"service": "deployments", "dependency_count": 1}),
            json!({"service": "experiments", "dependency_count": 0}),
            json!({"service": "legacy-sync", "dependency_count": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_distinct_subqueries_execute_as_precomputed_correlated_counts() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { \
                  MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
                  RETURN DISTINCT dependency.source \
                } AS distinct_dependency_sources, \
                COUNT { \
                  MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
                  RETURN DISTINCT CASE WHEN dependency.criticality = 'optional' THEN null ELSE dependency.source END \
                } AS distinct_sources_preserving_null, \
                COUNT { \
                  MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
                  RETURN DISTINCT dependency.team \
                } AS distinct_dependency_teams \
         ORDER BY service",
    )
    .await
    .expect("Cypher COUNT DISTINCT subquery projections should execute");

    assert!(
        execution.translated_sql().contains("SELECT DISTINCT"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "billing-api",
                "distinct_dependency_sources": 1,
                "distinct_sources_preserving_null": 2,
                "distinct_dependency_teams": 2
            }),
            json!({
                "service": "deployments",
                "distinct_dependency_sources": 1,
                "distinct_sources_preserving_null": 1,
                "distinct_dependency_teams": 1
            }),
            json!({
                "service": "experiments",
                "distinct_dependency_sources": 0,
                "distinct_sources_preserving_null": 0,
                "distinct_dependency_teams": 0
            }),
            json!({
                "service": "legacy-sync",
                "distinct_dependency_sources": 0,
                "distinct_sources_preserving_null": 0,
                "distinct_dependency_teams": 0
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_collect_subqueries_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COLLECT { \
                  MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
                  RETURN dependency.name \
                } AS dependency_names, \
                COLLECT { \
                  MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
                  RETURN CASE WHEN dependency.criticality = 'optional' THEN null ELSE dependency.criticality END \
                } AS non_optional_criticalities, \
                COLLECT { \
                  MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
                  RETURN dependency.source \
                } AS dependency_sources, \
                COLLECT { \
                  MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
                  RETURN DISTINCT dependency.source \
                } AS distinct_dependency_sources \
         ORDER BY service",
    )
    .await
    .expect("Cypher COLLECT subquery projections should execute");

    assert!(
        execution.translated_sql().contains("COALESCE(ARRAY_AGG("),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(DISTINCT "),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains("FILTER (WHERE"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        row["dependency_names"]
            .as_array_mut()
            .expect("dependency_names should be an array")
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        row["non_optional_criticalities"]
            .as_array_mut()
            .expect("non_optional_criticalities should be an array")
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        row["dependency_sources"]
            .as_array_mut()
            .expect("dependency_sources should be an array")
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
        row["distinct_dependency_sources"]
            .as_array_mut()
            .expect("distinct_dependency_sources should be an array")
            .sort_by(|left, right| left.as_str().cmp(&right.as_str()));
    }
    assert_eq!(
        rows,
        vec![
            json!({
                "service": "billing-api",
                "dependency_names": ["deployments", "experiments"],
                "non_optional_criticalities": [null, "runtime"],
                "dependency_sources": ["catalog", "catalog"],
                "distinct_dependency_sources": ["catalog"]
            }),
            json!({
                "service": "deployments",
                "dependency_names": ["experiments"],
                "non_optional_criticalities": ["dev"],
                "dependency_sources": ["deploy"],
                "distinct_dependency_sources": ["deploy"]
            }),
            json!({
                "service": "experiments",
                "dependency_names": [],
                "non_optional_criticalities": [],
                "dependency_sources": [],
                "distinct_dependency_sources": []
            }),
            json!({
                "service": "legacy-sync",
                "dependency_names": [],
                "non_optional_criticalities": [],
                "dependency_sources": [],
                "distinct_dependency_sources": []
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_collect_subquery_size_and_is_empty_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE isEmpty(COLLECT { \
           MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
           WHERE dependency.tier = 'prod' \
           RETURN dependency.name \
         }) \
         RETURN service.name AS service, \
                size(COLLECT { \
                  MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
                  RETURN dependency.name \
                }) AS dependency_count, \
                size(COLLECT { \
                  MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
                  RETURN DISTINCT dependency.tier \
                }) AS distinct_dependency_tiers \
         ORDER BY service",
    )
    .await
    .expect("Cypher COLLECT subquery size/isEmpty query should execute");

    assert!(
        execution.translated_sql().contains("COUNT(*)"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("NOT EXISTS"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("SELECT DISTINCT"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains("ARRAY_AGG"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "deployments",
                "dependency_count": 1,
                "distinct_dependency_tiers": 1
            }),
            json!({
                "service": "experiments",
                "dependency_count": 0,
                "distinct_dependency_tiers": 0
            }),
            json!({
                "service": "legacy-sync",
                "dependency_count": 0,
                "distinct_dependency_tiers": 0
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_pattern_comprehension_size_and_is_empty_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE isEmpty([(service)-[:DEPENDS_ON]->(target:Service) \
                        WHERE target.tier = 'prod' | target]) \
         RETURN service.name AS service, \
                size([(service)-[:DEPENDS_ON]->(target:Service) | target]) AS dependency_count \
         ORDER BY service",
    )
    .await
    .expect("Cypher pattern comprehension size and isEmpty should execute");

    assert!(
        execution.translated_sql().contains("COUNT(*)"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("NOT EXISTS"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "deployments",
                "dependency_count": 1
            }),
            json!({
                "service": "experiments",
                "dependency_count": 0
            }),
            json!({
                "service": "legacy-sync",
                "dependency_count": 0
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_noop_return_inside_scoped_subqueries_executes() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN DISTINCT target.name } \
         RETURN service.name AS service, \
                COUNT { MATCH (service)-[:DEPENDS_ON]->(target:Service) RETURN target.name, 1 } AS dependency_count \
         ORDER BY dependency_count DESC, service",
    )
    .await
    .expect("row-preserving scoped subquery RETURN clauses should execute");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_count": 2}),
            json!({"service": "deployments", "dependency_count": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subqueries_work_in_scalar_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } > 1 \
         RETURN service.name AS service",
    )
    .await
    .expect("Cypher COUNT subquery predicate should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_count_distinct_subqueries_work_in_scalar_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           RETURN DISTINCT target.tier \
         } > 1 \
         RETURN service.name AS service",
    )
    .await
    .expect("Cypher COUNT DISTINCT subquery predicate should execute");

    assert!(
        execution.translated_sql().contains("SELECT DISTINCT"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("LEFT JOIN (SELECT \"__coral_outer_key\", COUNT(*) AS \"__coral_value\"")
            && execution
                .translated_sql()
                .contains("GROUP BY \"__coral_outer_key\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) > 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_reversed_count_distinct_subqueries_work_in_scalar_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 1 < COUNT { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           RETURN DISTINCT target.tier \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect("Cypher reversed COUNT DISTINCT subquery predicate should execute");

    assert!(
        execution.translated_sql().contains("SELECT DISTINCT"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("LEFT JOIN (SELECT \"__coral_outer_key\", COUNT(*) AS \"__coral_value\"")
            && execution
                .translated_sql()
                .contains("GROUP BY \"__coral_outer_key\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) > 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_node_count_distinct_subqueries_work_in_scalar_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { \
           MATCH (other:Service) \
           WHERE other.tier = service.tier \
           RETURN DISTINCT other.team \
         } > 1 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher node COUNT DISTINCT subquery predicate should execute");

    assert!(
        execution.translated_sql().contains("SELECT DISTINCT")
            && execution
                .translated_sql()
                .contains("GROUP BY \"__coral_outer_key\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) > 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_reversed_node_count_distinct_subqueries_work_in_scalar_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 1 < COUNT { \
           MATCH (other:Service) \
           WHERE other.tier = service.tier \
           RETURN DISTINCT other.team \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher reversed node COUNT DISTINCT subquery predicate should execute");

    assert!(
        execution.translated_sql().contains("SELECT DISTINCT")
            && execution
                .translated_sql()
                .contains("GROUP BY \"__coral_outer_key\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"__coral_scalar_subquery_0\".\"__coral_value\", 0) > 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subquery_positive_threshold_predicates_lower_to_exists() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } > 0 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher COUNT positive threshold predicate should execute");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains("COUNT(*)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subquery_zero_threshold_predicates_lower_to_not_exists() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } = 0 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher COUNT zero threshold predicate should execute");

    assert!(
        execution
            .translated_sql()
            .contains("NOT EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains("COUNT(*)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subquery_reversed_threshold_predicates_lower_to_exists() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 0 < COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("reversed Cypher COUNT positive threshold predicate should execute");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains("COUNT(*)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subquery_constant_threshold_predicates_fold_without_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let always_true = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } >= 0 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("always-true Cypher COUNT threshold predicate should execute");

    assert!(
        always_true.translated_sql().contains(" WHERE TRUE "),
        "{}",
        always_true.translated_sql()
    );
    assert!(
        !always_true
            .translated_sql()
            .contains("EXISTS (SELECT 1 FROM")
            && !always_true.translated_sql().contains("COUNT(*)"),
        "{}",
        always_true.translated_sql()
    );
    assert_eq!(
        execution_to_rows(always_true.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );

    let always_false = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { MATCH (service)-[:DEPENDS_ON]->(:Service) } < 0 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("always-false Cypher COUNT threshold predicate should execute");

    assert!(
        always_false.translated_sql().contains(" WHERE FALSE "),
        "{}",
        always_false.translated_sql()
    );
    assert!(
        !always_false
            .translated_sql()
            .contains("EXISTS (SELECT 1 FROM")
            && !always_false.translated_sql().contains("COUNT(*)"),
        "{}",
        always_false.translated_sql()
    );
    assert!(execution_to_rows(always_false.execution()).is_empty());
}

#[tokio::test]
async fn cypher_count_subqueries_preserve_inner_where_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { \
                  MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
                  WHERE dependency.tier = 'dev' \
                } AS dev_dependency_count \
         ORDER BY service",
    )
    .await
    .expect("Cypher COUNT subquery with inner WHERE should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"tier\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dev_dependency_count": 1}),
            json!({"service": "deployments", "dev_dependency_count": 1}),
            json!({"service": "experiments", "dev_dependency_count": 0}),
            json!({"service": "legacy-sync", "dev_dependency_count": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_compact_count_patterns_preserve_property_maps_and_where_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { (service)-[:DEPENDS_ON {source: 'catalog'}]->(dependency:Service) WHERE dependency.tier = 'dev' } AS catalog_dev_dependencies \
         ORDER BY service",
    )
    .await
    .expect("compact Cypher COUNT pattern should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"source\" = 'catalog'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("\"tier\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "catalog_dev_dependencies": 1}),
            json!({"service": "deployments", "catalog_dev_dependencies": 0}),
            json!({"service": "experiments", "catalog_dev_dependencies": 0}),
            json!({"service": "legacy-sync", "catalog_dev_dependencies": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subqueries_support_node_only_matches() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team) \
         WHERE COUNT { MATCH (service:Service) WHERE service.tier = 'prod' OR service.tier = 'dev' } = 3 \
         RETURN team.name AS team, \
                COUNT { MATCH (service:Service) WHERE service.tier = 'dev' } AS dev_services, \
                COUNT { MATCH (service:Service) WHERE lower(service.name) CONTAINS 'api' } AS api_services \
         ORDER BY team",
    )
    .await
    .expect("Cypher COUNT node-only subquery should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SELECT COUNT(*) FROM \"ops\".\"services\" AS \"__coral_count_n0\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("contains(LOWER(\"__coral_count_n0\".\"service_name\"), 'api')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics", "dev_services": 1, "api_services": 1}),
            json!({"team": "infra", "dev_services": 1, "api_services": 1}),
            json!({"team": "platform", "dev_services": 1, "api_services": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subqueries_support_correlated_node_only_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team) \
         RETURN team.name AS team, \
                COUNT { \
                  MATCH (service:Service) \
                  WHERE service.team = team.name \
                } AS catalog_services \
         ORDER BY team",
    )
    .await
    .expect("Cypher COUNT node-only subquery with outer property reference should execute");

    assert!(
        execution.translated_sql().contains(
            "LEFT JOIN (SELECT \"__coral_count_n0\".\"owning_team\" AS \"__coral_outer_key\", \
             COUNT(*) AS \"__coral_value\""
        ) && execution.translated_sql().contains(
            "ON \"__coral_scalar_subquery_0\".\"__coral_outer_key\" = \"n0\".\"team_name\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics", "catalog_services": 1}),
            json!({"team": "infra", "catalog_services": 1}),
            json!({"team": "platform", "catalog_services": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subqueries_support_uncorrelated_relationship_matches() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team) \
         RETURN team.name AS team, \
                COUNT { MATCH (:Service)-[:DEPENDS_ON]->(:Service) } AS dependency_edges, \
                COUNT { \
                  MATCH (:Service)-[:DEPENDS_ON]->(dependency:Service) \
                  WHERE dependency.tier = 'dev' \
                } AS dev_dependency_edges \
         ORDER BY team",
    )
    .await
    .expect("Cypher COUNT relationship subquery without outer anchor should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SELECT COUNT(*) FROM \"ops\".\"service_dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics", "dependency_edges": 3, "dev_dependency_edges": 2}),
            json!({"team": "infra", "dependency_edges": 3, "dev_dependency_edges": 2}),
            json!({"team": "platform", "dependency_edges": 3, "dev_dependency_edges": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_count_subqueries_support_scoped_scalar_property_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team) \
         RETURN team.name AS team, \
                COUNT { MATCH (service:Service) WHERE service.active = false } AS inactive_services, \
                COUNT { MATCH (service:Service) WHERE service.risk > 0.8 } AS high_risk_services \
         ORDER BY team",
    )
    .await
    .expect("Cypher COUNT subquery scoped scalar predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_count_n0\".\"risk_score\" > 0.8"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics", "inactive_services": 2, "high_risk_services": 2}),
            json!({"team": "infra", "inactive_services": 2, "high_risk_services": 2}),
            json!({"team": "platform", "inactive_services": 2, "high_risk_services": 2}),
        ]
    );
}

#[tokio::test]
async fn cypher_exists_match_subqueries_support_scoped_scalar_property_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
           WHERE dependency.risk < 0.3 \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher EXISTS subquery scoped scalar predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_n0\".\"risk_score\" < 0.3"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn graphql_root_query_executes_against_synthetic_file_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: { tier: { eq: "prod" }, risk: { gte: 0.5 } }
            orderBy: [{ field: name, direction: ASC }]
            limit: 10
          ) {
            service: name
            tier
          }
        }
        "#,
    )
    .await
    .expect("GraphQL virtual graph query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"risk_score\" >= 0.5"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_execute_rejects_unknown_graph_declared_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query {
          Service {
            missingProperty
          }
        }
        ",
    )
    .await
    .expect_err("unknown graph-declared GraphQL property should fail before execution");

    assert!(
        error.to_string().contains("UNKNOWN_PROPERTY"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn graphql_declaration_root_field_alias_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          services(
            where: { tier: { eq: "prod" } }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            service: name
            tier
          }
        }
        "#,
    )
    .await
    .expect("GraphQL declaration-aware root field alias should execute");

    assert!(
        execution
            .translated_sql()
            .contains("FROM \"ops\".\"services\" AS \"n0\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_shorthand_where_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "tier".to_string(),
        GraphGraphqlVariableValue::Literal(GraphLiteral::String("prod".to_string())),
    )]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query Services($tier: String!) {
          Service(
            where: {
              tier: $tier
              name: "billing-api"
            }
          ) {
            service: name
            tier
          }
        }
        "#,
        &variables,
    )
    .await
    .expect("GraphQL shorthand where filters should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" = 'billing-api'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api", "tier": "prod"})]
    );
}

#[tokio::test]
async fn graphql_first_argument_executes_as_limit_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query {
          Service(orderBy: [{ field: name, direction: ASCENDING }], first: 2, skip: 1) {
            service: name
          }
        }
        ",
    )
    .await
    .expect("GraphQL first argument should execute as a row limit");

    assert!(
        execution.translated_sql().contains("LIMIT 2 OFFSET 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn graphql_conflicting_root_row_arguments_are_rejected() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query {
          Service(limit: 1, first: 2) {
            service: name
          }
        }
        ",
    )
    .await
    .expect_err("conflicting GraphQL root row arguments should be rejected");

    assert!(
        error
            .to_string()
            .contains("GraphQL root argument 'first' conflicts with earlier 'limit' argument"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn graphql_variables_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([
        (
            "tier".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::String("prod".to_string())),
        ),
        (
            "minRisk".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Float(ordered_float::OrderedFloat(
                0.5,
            ))),
        ),
        (
            "names".to_string(),
            GraphGraphqlVariableValue::List(vec![
                GraphLiteral::String("billing-api".to_string()),
                GraphLiteral::String("deployments".to_string()),
            ]),
        ),
        (
            "limit".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Integer(10)),
        ),
    ]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($tier: String!, $minRisk: Float!, $names: [String!], $limit: Int!) {
          Service(
            where: {
              tier: { eq: $tier }
              risk: { gte: $minRisk }
              name: { in: $names }
            }
            orderBy: [{ field: name, direction: ASC }]
            limit: $limit
          ) {
            service: name
            tier
          }
        }
        ",
        &variables,
    )
    .await
    .expect("GraphQL variable query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" IN ('billing-api', 'deployments')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_object_variables_execute_against_synthetic_sources() {
    fn object(
        entries: impl IntoIterator<Item = (&'static str, GraphGraphqlVariableValue)>,
    ) -> GraphGraphqlVariableValue {
        GraphGraphqlVariableValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        )
    }

    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "filter".to_string(),
        object([
            (
                "tier",
                object([(
                    "eq",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("prod".to_string())),
                )]),
            ),
            (
                "risk",
                object([(
                    "gte",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::Float(
                        ordered_float::OrderedFloat(0.5),
                    )),
                )]),
            ),
            (
                "name",
                object([(
                    "in",
                    GraphGraphqlVariableValue::List(vec![
                        GraphLiteral::String("billing-api".to_string()),
                        GraphLiteral::String("deployments".to_string()),
                    ]),
                )]),
            ),
        ]),
    )]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($filter: ServiceWhere!) {
          Service(
            where: $filter
            orderBy: [{ field: name, direction: ASC }]
          ) {
            service: name
            tier
          }
        }
        ",
        &variables,
    )
    .await
    .expect("GraphQL object variable query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" IN ('billing-api', 'deployments')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_order_by_object_variable_executes_against_synthetic_sources() {
    fn object(
        entries: impl IntoIterator<Item = (&'static str, GraphGraphqlVariableValue)>,
    ) -> GraphGraphqlVariableValue {
        GraphGraphqlVariableValue::Object(
            entries
                .into_iter()
                .map(|(key, value)| (key.to_string(), value))
                .collect(),
        )
    }

    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([
        (
            "tier".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::String("prod".to_string())),
        ),
        (
            "order".to_string(),
            object([
                (
                    "field",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("name".to_string())),
                ),
                (
                    "direction",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("DESC".to_string())),
                ),
            ]),
        ),
    ]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($tier: String!, $order: ServiceOrder!) {
          Service(
            where: { tier: { eq: $tier } }
            orderBy: $order
          ) {
            service: name
          }
        }
        ",
        &variables,
    )
    .await
    .expect("GraphQL orderBy object variable query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"n0\".\"service_name\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "deployments"}),
            json!({"service": "billing-api"}),
        ]
    );
}

#[tokio::test]
async fn graphql_order_by_null_placement_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query {
          Service(
            orderBy: [
              { field: tier, direction: ASC, nulls: LAST }
              { field: name, direction: ASC }
            ]
          ) {
            service: name
          }
        }
        ",
    )
    .await
    .expect("GraphQL orderBy null placement query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"n0\".\"tier\" ASC NULLS LAST, \"n0\".\"service_name\" ASC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn graphql_order_by_null_placement_variable_executes_against_synthetic_sources() {
    fn object_map(
        entries: impl IntoIterator<Item = (&'static str, GraphGraphqlVariableValue)>,
    ) -> BTreeMap<String, GraphGraphqlVariableValue> {
        entries
            .into_iter()
            .map(|(key, value)| (key.to_string(), value))
            .collect()
    }

    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "order".to_string(),
        GraphGraphqlVariableValue::ObjectList(vec![
            object_map([
                (
                    "field",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("tier".to_string())),
                ),
                (
                    "direction",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("ASC".to_string())),
                ),
                (
                    "nulls",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("LAST".to_string())),
                ),
            ]),
            object_map([
                (
                    "field",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("name".to_string())),
                ),
                (
                    "direction",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String("DESC".to_string())),
                ),
                (
                    "nulls",
                    GraphGraphqlVariableValue::Literal(GraphLiteral::String(
                        "NULLS_FIRST".to_string(),
                    )),
                ),
            ]),
        ]),
    )]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($order: [ServiceOrder!]!) {
          Service(orderBy: $order) {
            service: name
          }
        }
        ",
        &variables,
    )
    .await
    .expect("GraphQL orderBy null placement variable query should execute");

    assert!(
        execution.translated_sql().contains(
            "ORDER BY \"n0\".\"tier\" ASC NULLS LAST, \"n0\".\"service_name\" DESC NULLS FIRST"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "deployments"}),
            json!({"service": "billing-api"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn graphql_shorthand_order_by_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query {
          Service(
            orderBy: [
              { risk: DESC }
              { name: ASC }
            ]
            limit: 3
          ) {
            service: name
            risk
          }
        }
        ",
    )
    .await
    .expect("GraphQL shorthand orderBy query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"n0\".\"risk_score\" DESC, \"n0\".\"service_name\" ASC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "legacy-sync", "risk": 0.95}),
            json!({"service": "billing-api", "risk": 0.9}),
            json!({"service": "deployments", "risk": 0.5}),
        ]
    );
}

#[tokio::test]
async fn graphql_flat_aggregate_fields_match_equivalent_sql() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let graph_execution = CoralQuery::execute_graphql(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        r"
        query {
          Service(
            where: { tier: { isNotNull: true } }
            orderBy: [{ field: tier, direction: ASC }]
          ) {
            tier
            services: _count
            namedServices: _count(field: name)
            tierKinds: _countDistinct(field: tier)
            totalRisk: _sum(field: risk)
            averageRisk: _avg(field: risk)
            minRisk: _min(field: risk)
            maxRisk: _max(field: risk)
          }
        }
        ",
    )
    .await
    .expect("GraphQL flat aggregate fields should execute");

    assert!(
        graph_execution
            .translated_sql()
            .contains("COUNT(*) AS \"services\""),
        "{}",
        graph_execution.translated_sql()
    );
    assert!(
        graph_execution
            .translated_sql()
            .contains("SUM(\"n0\".\"risk_score\") AS \"totalRisk\""),
        "{}",
        graph_execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT tier, \
                COUNT(*) AS \"services\", \
                COUNT(service_name) AS \"namedServices\", \
                COUNT(DISTINCT tier) AS \"tierKinds\", \
                SUM(risk_score) AS \"totalRisk\", \
                AVG(risk_score) AS \"averageRisk\", \
                MIN(risk_score) AS \"minRisk\", \
                MAX(risk_score) AS \"maxRisk\" \
         FROM ops.services \
         WHERE tier IS NOT NULL \
         GROUP BY tier \
         ORDER BY tier ASC",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(graph_execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![
            json!({
                "tier": "dev",
                "services": 1,
                "namedServices": 1,
                "tierKinds": 1,
                "totalRisk": 0.25,
                "averageRisk": 0.25,
                "minRisk": 0.25,
                "maxRisk": 0.25
            }),
            json!({
                "tier": "prod",
                "services": 2,
                "namedServices": 2,
                "tierKinds": 1,
                "totalRisk": 1.4,
                "averageRisk": 0.7,
                "minRisk": 0.5,
                "maxRisk": 0.9
            }),
        ]
    );
}

#[tokio::test]
async fn graphql_statistical_aggregate_fields_match_equivalent_sql() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let graph_execution = CoralQuery::execute_graphql(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        r#"
        query {
          Service(where: { tier: { eq: "prod" } }) {
            sampleRisk: _stDev(field: risk)
            populationRisk: _stDevP(field: risk)
            distinctTotalRisk: _sumDistinct(field: risk)
            distinctAverageRisk: _avgDistinct(field: risk)
            medianRisk: _median(field: risk)
            distinctMedianRisk: _medianDistinct(field: risk)
            p75Risk: _percentileCont(field: risk, percentile: 0.75)
            distinctMinRisk: _minDistinct(field: risk)
            distinctMaxRisk: _maxDistinct(field: risk)
          }
        }
        "#,
    )
    .await
    .expect("GraphQL statistical aggregate fields should execute");

    assert!(
        graph_execution
            .translated_sql()
            .contains("STDDEV_SAMP(\"n0\".\"risk_score\") AS \"sampleRisk\""),
        "{}",
        graph_execution.translated_sql()
    );
    assert!(
        graph_execution
            .translated_sql()
            .contains("SUM(DISTINCT \"n0\".\"risk_score\") AS \"distinctTotalRisk\""),
        "{}",
        graph_execution.translated_sql()
    );
    assert!(
        graph_execution
            .translated_sql()
            .contains("MEDIAN(CAST(\"n0\".\"risk_score\" AS DOUBLE)) AS \"medianRisk\""),
        "{}",
        graph_execution.translated_sql()
    );
    assert!(
        graph_execution
            .translated_sql()
            .contains("PERCENTILE_CONT(\"n0\".\"risk_score\", 0.75) AS \"p75Risk\""),
        "{}",
        graph_execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT STDDEV_SAMP(risk_score) AS \"sampleRisk\", \
                STDDEV_POP(risk_score) AS \"populationRisk\", \
                SUM(DISTINCT risk_score) AS \"distinctTotalRisk\", \
                AVG(DISTINCT risk_score) AS \"distinctAverageRisk\", \
                MEDIAN(risk_score) AS \"medianRisk\", \
                MEDIAN(DISTINCT risk_score) AS \"distinctMedianRisk\", \
                PERCENTILE_CONT(risk_score, 0.75) AS \"p75Risk\", \
                MIN(DISTINCT risk_score) AS \"distinctMinRisk\", \
                MAX(DISTINCT risk_score) AS \"distinctMaxRisk\" \
         FROM ops.services \
         WHERE tier = 'prod'",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(graph_execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows, sql_rows);

    let row = graph_rows
        .first()
        .expect("aggregate query should return one row");
    assert_close(row["sampleRisk"].as_f64().unwrap(), 0.282_842_712_474_619);
    assert_close(row["populationRisk"].as_f64().unwrap(), 0.2);
    assert_close(row["distinctTotalRisk"].as_f64().unwrap(), 1.4);
    assert_close(row["distinctAverageRisk"].as_f64().unwrap(), 0.7);
    assert_close(row["medianRisk"].as_f64().unwrap(), 0.7);
    assert_close(row["distinctMedianRisk"].as_f64().unwrap(), 0.7);
    assert_close(row["p75Risk"].as_f64().unwrap(), 0.8);
    assert_close(row["distinctMinRisk"].as_f64().unwrap(), 0.5);
    assert_close(row["distinctMaxRisk"].as_f64().unwrap(), 0.9);
}

#[tokio::test]
async fn graphql_collect_aggregate_fields_match_equivalent_sql() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let graph_execution = CoralQuery::execute_graphql(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        r"
        query {
          Service(
            where: { tier: { isNotNull: true } }
            orderBy: [{ field: tier, direction: ASC }]
          ) {
            tier
            serviceNames: _collect(field: name)
            uniqueTiers: _collectDistinct(field: tier)
          }
        }
        ",
    )
    .await
    .expect("GraphQL collect aggregate fields should execute");

    assert!(
        graph_execution.translated_sql().contains(
            "COALESCE(ARRAY_AGG(\"n0\".\"service_name\") FILTER (WHERE (\"n0\".\"service_name\") IS NOT NULL), make_array()) AS \"serviceNames\""
        ),
        "{}",
        graph_execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT tier, \
                COALESCE(ARRAY_AGG(service_name) FILTER (WHERE service_name IS NOT NULL), make_array()) AS \"serviceNames\", \
                COALESCE(ARRAY_AGG(DISTINCT tier) FILTER (WHERE tier IS NOT NULL), make_array()) AS \"uniqueTiers\" \
         FROM ops.services \
         WHERE tier IS NOT NULL \
         GROUP BY tier \
         ORDER BY tier ASC",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(graph_execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![
            json!({
                "tier": "dev",
                "serviceNames": ["experiments"],
                "uniqueTiers": ["dev"]
            }),
            json!({
                "tier": "prod",
                "serviceNames": ["billing-api", "deployments"],
                "uniqueTiers": ["prod"]
            }),
        ]
    );
}

#[tokio::test]
async fn graphql_variable_defaults_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query Services(
          $tier: String = "prod"
          $names: [String!] = ["billing-api", "deployments"]
          $sortField: ServiceOrderField = name
          $sortDirection: SortDirection = ASC
          $limit: Int = 10
        ) {
          Service(
            where: {
              tier: { eq: $tier }
              name: { in: $names }
            }
            orderBy: [{ field: $sortField, direction: $sortDirection }]
            limit: $limit
          ) {
            service: name
            tier
          }
        }
        "#,
    )
    .await
    .expect("GraphQL variable default query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" IN ('billing-api', 'deployments')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_object_variable_defaults_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query Services(
          $filter: ServiceWhere = {
            tier: { eq: "prod" }
            name: { in: ["billing-api", "deployments"] }
          }
          $order: ServiceOrder = { field: name, direction: DESC }
        ) {
          Service(
            where: $filter
            orderBy: $order
          ) {
            service: name
          }
        }
        "#,
    )
    .await
    .expect("GraphQL object variable default query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" IN ('billing-api', 'deployments')"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"n0\".\"service_name\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "deployments"}),
            json!({"service": "billing-api"}),
        ]
    );
}

#[tokio::test]
async fn graphql_empty_order_by_default_executes_as_no_ordering() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($order: [ServiceOrder!] = []) {
          Service(orderBy: $order) {
            service: name
          }
        }
        ",
    )
    .await
    .expect("GraphQL empty orderBy default query should execute");

    assert!(
        !execution.translated_sql().contains("ORDER BY"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(execution_to_rows(execution.execution()).len(), 4);
}

#[tokio::test]
async fn graphql_boolean_root_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: {
              or: [
                { tier: { eq: "dev" } }
                { tier: { isNull: true } }
              ]
              not: { name: { contains: "legacy" } }
            }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            service: name
          }
        }
        "#,
    )
    .await
    .expect("GraphQL boolean root filter query should execute");

    assert!(
        execution.translated_sql().contains(" OR ") && execution.translated_sql().contains("NOT ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "experiments"})]
    );
}

#[tokio::test]
async fn graphql_regex_and_xor_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: {
              xor: [
                { name: { matches: "^billing.*" } }
                { tier: { regex: "^dev$" } }
              ]
            }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            service: name
            tier
          }
        }
        "#,
    )
    .await
    .expect("GraphQL regex and xor filter query should execute");

    assert!(
        execution.translated_sql().contains("regexp_like(")
            && execution.translated_sql().contains("NOT ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "experiments", "tier": "dev"}),
        ]
    );
}

#[tokio::test]
async fn graphql_filter_operator_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: {
              tier: { equals: "prod" }
              name: { starts_with: "billing", notEqual: "legacy-sync" }
              risk: { ge: 0.5, lessThanOrEqual: 0.95 }
            }
          ) {
            service: name
            risk
          }
        }
        "#,
    )
    .await
    .expect("GraphQL filter operator alias query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" LIKE 'billing%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'")
            && execution
                .translated_sql()
                .contains("\"n0\".\"risk_score\" <= 0.95"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api", "risk": 0.9})]
    );
}

#[tokio::test]
async fn graphql_negated_filter_operators_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: {
              tier: { isNotNull: true }
              name: {
                notIn: ["legacy-sync", "experiments"]
                notContains: "deploy"
                notRegex: "^internal"
              }
            }
          ) {
            service: name
          }
        }
        "#,
    )
    .await
    .expect("GraphQL negated filter operator query should execute");

    assert!(
        execution.translated_sql().contains("NOT (")
            && execution
                .translated_sql()
                .contains("\"n0\".\"service_name\" IN ('legacy-sync', 'experiments')")
            && execution
                .translated_sql()
                .contains("\"n0\".\"service_name\" LIKE '%deploy%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn graphql_generated_client_shape_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([
        (
            "includeTier".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Boolean(true)),
        ),
        (
            "skipRisk".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Boolean(true)),
        ),
    ]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($includeTier: Boolean!, $skipRisk: Boolean!) {
          services: Service(
            orderBy: [{ field: name, direction: ASC }]
            limit: 2
          ) {
            __typename
            ...ServiceFields
            ... on Service {
              tier @include(if: $includeTier)
              risk @skip(if: $skipRisk)
            }
          }
        }

        fragment ServiceFields on Service {
          service: name
        }
        ",
        &variables,
    )
    .await
    .expect("generated-client-shaped GraphQL query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("'Service' AS \"__typename\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"__typename": "Service", "service": "billing-api", "tier": "prod"}),
            json!({"__typename": "Service", "service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_query_operation_directives_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "skipQuery".to_string(),
        GraphGraphqlVariableValue::Literal(GraphLiteral::Boolean(true)),
    )]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services($skipQuery: Boolean!) @skip(if: $skipQuery) {
          services: Service(
            orderBy: [{ field: name, direction: ASC }]
            limit: 2
          ) {
            service: name
            tier
          }
        }
        ",
        &variables,
    )
    .await
    .expect("GraphQL query operation directives should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" AS \"service\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("WHERE FALSE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        Vec::<Value>::new()
    );
}

#[tokio::test]
async fn graphql_root_typename_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r"
        query Services {
          queryType: __typename
          services(
            orderBy: [{ field: name, direction: ASC }]
            limit: 1
          ) {
            service: name
          }
        }
        ",
    )
    .await
    .expect("GraphQL root __typename should execute");

    assert!(
        execution
            .translated_sql()
            .contains("'Query' AS \"queryType\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api", "queryType": "Query"})]
    );
}

#[tokio::test]
async fn graphql_root_fragments_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query Services {
          ...RootServices
          ... on Query {
            skipped: Team @skip(if: true) {
              name
            }
          }
        }

        fragment RootServices on Query {
          services: Service(
            where: { tier: { eq: "prod" } }
            orderBy: [{ field: name, direction: ASC }]
            limit: 2
          ) {
            service: name
            tier
          }
        }
        "#,
    )
    .await
    .expect("GraphQL root fragment query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_duplicate_root_fields_merge_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: { tier: { eq: "prod" } }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            service: name
          }
          ...ServiceRootDetails
        }

        fragment ServiceRootDetails on Query {
          Service(
            where: { tier: { eq: "prod" } }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            tier
          }
        }
        "#,
    )
    .await
    .expect("duplicate GraphQL root fields should merge");

    assert_eq!(
        execution
            .translated_sql()
            .matches("FROM \"ops\".\"services\"")
            .count(),
        1,
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier": "prod"}),
            json!({"service": "deployments", "tier": "prod"}),
        ]
    );
}

#[tokio::test]
async fn graphql_nested_relationship_query_executes_against_synthetic_file_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(where: { team: { eq: "infra" } }) {
            owner: name
            out_OWNS(
              to: Service
              relationshipWhere: { source: { eq: "pagerduty" } }
              where: { tier: { eq: "prod" } }
            ) {
              service: name
              _edge {
                ownershipKind: __typename
                ownershipSource: source
              }
              out_DEPENDS_ON(
                to: Service
                relationshipWhere: { criticality: { eq: "dev" } }
              ) {
                dependency: name
                _edge {
                  dependencyKind: __typename
                  dependencyCriticality: criticality
                }
              }
            }
          }
        }
        "#,
    )
    .await
    .expect("nested GraphQL virtual graph query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"service_dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"r1\".\"criticality\" = 'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "owner": "Grace Hopper",
            "service": "deployments",
            "ownershipKind": "OWNS",
            "ownershipSource": "pagerduty",
            "dependency": "experiments",
            "dependencyKind": "DEPENDS_ON",
            "dependencyCriticality": "dev",
        })]
    );
}

#[tokio::test]
async fn graphql_duplicate_nested_relationship_fields_merge_without_row_multiplication() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(where: { name: { eq: "billing-api" } }) {
            service: name
            out_DEPENDS_ON(to: Service) {
              dependency: name
            }
            ...DependencyDetails
          }
        }

        fragment DependencyDetails on Service {
          out_DEPENDS_ON(to: Service) {
            tier
          }
        }
        "#,
    )
    .await
    .expect("duplicate GraphQL relationship fields should merge");

    assert_eq!(
        execution
            .translated_sql()
            .matches("JOIN \"ops\".\"service_dependencies\"")
            .count(),
        1,
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    rows.sort_by_key(|row| row["dependency"].as_str().unwrap_or_default().to_string());
    assert_eq!(
        rows,
        vec![
            json!({"service": "billing-api", "dependency": "deployments", "service1_tier": "prod"}),
            json!({"service": "billing-api", "dependency": "experiments", "service1_tier": "dev"}),
        ]
    );
}

#[tokio::test]
async fn graphql_duplicate_nested_relationship_arguments_are_rejected() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person {
            out_OWNS(
              where: { tier: { eq: "prod" } }
              where: { name: { eq: "billing-api" } }
            ) {
              service: name
            }
          }
        }
        "#,
    )
    .await
    .expect_err("duplicate nested GraphQL relationship arguments should be rejected");

    assert!(
        error
            .to_string()
            .contains("GraphQL relationship argument 'where' is specified more than once"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn graphql_relationship_existence_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(
            where: {
              out_OWNS: {
                where: { tier: { eq: "prod" } }
                relationshipWhere: { source: { eq: "pagerduty" } }
              }
            }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            owner: name
          }
        }
        "#,
    )
    .await
    .expect("GraphQL relationship existence filter should execute");

    assert!(
        execution
            .translated_sql()
            .contains("EXISTS (SELECT 1 FROM \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owner": "Grace Hopper"})]
    );
}

#[tokio::test]
async fn graphql_named_operation_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([(
        "tier".to_string(),
        GraphGraphqlVariableValue::Literal(GraphLiteral::String("prod".to_string())),
    )]);

    let execution = CoralQuery::execute_graphql_with_variables_and_operation_name(
        &[source],
        test_runtime(),
        &graph,
        r"
        query ProdServices($tier: String!) {
          Service(
            where: { tier: { eq: $tier } }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            service: name
          }
        }

        query RequiresMissingVariable($missing: String!) {
          Person(where: { team: { eq: $missing } }) {
            owner: name
          }
        }
        ",
        &variables,
        "ProdServices",
    )
    .await
    .expect("selected GraphQL operation should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn graphql_nested_relationship_infers_unambiguous_endpoint_labels() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(where: { team: { eq: "infra" } }) {
            owner: name
            out_OWNS(
              relationshipWhere: { source: { eq: "pagerduty" } }
              where: { tier: { eq: "prod" } }
            ) {
              service: name
            }
          }
        }
        "#,
    )
    .await
    .expect("GraphQL should infer unambiguous relationship endpoint labels");

    assert!(
        execution
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owner": "Grace Hopper", "service": "deployments"})]
    );
}

#[tokio::test]
async fn graphql_identity_fields_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(where: { team: { eq: "platform" } }) {
            person_id: _id
            person_element_id: _elementId
            person: name
            out_OWNS(to: Service) {
              service_id: _id
              service_element_id: _elementId
              service: name
              _edge {
                ownership_id: _id
                ownership_element_id: _elementId
                ownership_type: __typename
              }
            }
          }
        }
        "#,
    )
    .await
    .expect("GraphQL identity field query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n0\".\"id\" AS VARCHAR) AS \"person_element_id\"")
            && execution
                .translated_sql()
                .contains("CAST(\"r0\".\"ownership_id\" AS VARCHAR) AS \"ownership_element_id\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "person_id": 1,
            "person_element_id": "1",
            "person": "Ada Lovelace",
            "service_id": 10,
            "service_element_id": "10",
            "service": "billing-api",
            "ownership_id": 100,
            "ownership_element_id": "100",
            "ownership_type": "OWNS",
        })]
    );
}

#[tokio::test]
async fn graphql_identity_filters_and_ordering_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Service(
            where: {
              _id: { in: [10, 20, 40] }
              _elementId: { notIn: ["40"] }
            }
            orderBy: [{ field: _id, direction: DESC }]
          ) {
            service_id: _id
            service_element_id: _elementId
            service: name
          }
        }
        "#,
    )
    .await
    .expect("GraphQL identity filter and order query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"id\" IN (10, 20, 40)")
            && execution
                .translated_sql()
                .contains("NOT (CAST(\"n0\".\"id\" AS VARCHAR) IN ('40'))")
            && execution
                .translated_sql()
                .contains("ORDER BY \"n0\".\"id\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service_id": 20, "service_element_id": "20", "service": "deployments"}),
            json!({"service_id": 10, "service_element_id": "10", "service": "billing-api"}),
        ]
    );
}

#[tokio::test]
async fn graphql_relationship_identity_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(where: { team: { eq: "infra" } }) {
            owner: name
            out_OWNS(
              to: Service
              relationshipWhere: {
                _id: { eq: 200 }
                _elementId: { eq: "200" }
              }
            ) {
              service: name
              _edge {
                ownership_id: _id
                ownership_element_id: _elementId
              }
            }
          }
        }
        "#,
    )
    .await
    .expect("GraphQL relationship identity filter query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"ownership_id\" = 200")
            && execution
                .translated_sql()
                .contains("CAST(\"r0\".\"ownership_id\" AS VARCHAR) = '200'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "owner": "Grace Hopper",
            "service": "deployments",
            "ownership_id": 200,
            "ownership_element_id": "200",
        })]
    );
}

#[tokio::test]
async fn graphql_edge_fragments_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(where: { team: { eq: "infra" } }) {
            owner: name
            out_OWNS(
              to: Service
              relationshipWhere: { source: { eq: "pagerduty" } }
            ) {
              service: name
              _edge {
                ...OwnershipEdge
                ... on OWNS {
                  ownershipSince: since
                }
              }
            }
          }
        }

        fragment OwnershipEdge on OWNS {
          edgeKind: __typename
          ownershipSource: source
        }
        "#,
    )
    .await
    .expect("GraphQL edge fragments should execute");

    assert!(
        execution
            .translated_sql()
            .contains("'OWNS' AS \"edgeKind\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "owner": "Grace Hopper",
            "service": "deployments",
            "edgeKind": "OWNS",
            "ownershipSource": "pagerduty",
            "ownershipSince": "2024-02-20",
        })]
    );
}

#[tokio::test]
async fn graphql_fragment_definition_directives_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let variables = BTreeMap::from([
        (
            "includeRoot".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Boolean(true)),
        ),
        (
            "includeOwner".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Boolean(true)),
        ),
        (
            "includeEdge".to_string(),
            GraphGraphqlVariableValue::Literal(GraphLiteral::Boolean(true)),
        ),
    ]);

    let execution = CoralQuery::execute_graphql_with_variables(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query FragmentDirectives(
          $includeRoot: Boolean!
          $includeOwner: Boolean!
          $includeEdge: Boolean!
        ) {
          ...OwnerRoot
        }

        fragment OwnerRoot on Query @include(if: $includeRoot) {
          Person(where: { team: { eq: "infra" } }) {
            ...OwnerFields
            out_OWNS(
              to: Service
              relationshipWhere: { source: { eq: "pagerduty" } }
            ) {
              service: name
              _edge {
                ...OwnershipEdge
              }
            }
          }
        }

        fragment OwnerFields on Person @include(if: $includeOwner) {
          owner: name
        }

        fragment OwnershipEdge on OWNS @include(if: $includeEdge) {
          ownershipSource: source
        }
        "#,
        &variables,
    )
    .await
    .expect("GraphQL fragment definition directives should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"source\" AS \"ownershipSource\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "owner": "Grace Hopper",
            "service": "deployments",
            "ownershipSource": "pagerduty",
        })]
    );
}

#[tokio::test]
async fn graphql_boolean_nested_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_graphql(
        &[source],
        test_runtime(),
        &graph,
        r#"
        query {
          Person(
            where: { or: [{ team: { eq: "infra" } }, { team: { eq: "analytics" } }] }
            orderBy: [{ field: name, direction: ASC }]
          ) {
            owner: name
            out_OWNS(
              to: Service
              relationshipWhere: { not: { source: { isNull: true } } }
              where: { or: [{ tier: { eq: "prod" } }, { name: { contains: "experiments" } }] }
            ) {
              service: name
              _edge {
                source
              }
            }
          }
        }
        "#,
    )
    .await
    .expect("GraphQL boolean nested filter query should execute");

    assert!(
        execution.translated_sql().contains(" OR ") && execution.translated_sql().contains("NOT ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Grace Hopper", "service": "deployments", "relationship0_source": "pagerduty"}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "relationship0_source": "catalog"}),
        ]
    );
}

#[tokio::test]
async fn cypher_property_to_property_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         WHERE person.team = service.team \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY owner, service",
    )
    .await
    .expect("property comparison Cypher query should execute");
    let graph_rows = execution_to_rows(execution.execution());

    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT people.full_name AS owner, services.service_name AS service \
             FROM ops.people \
             JOIN ops.ownerships ON ownerships.person_id = people.id \
             JOIN ops.services ON ownerships.service_id = services.id \
             WHERE people.team = services.owning_team \
             ORDER BY people.full_name, services.service_name",
        )
        .await
        .expect("equivalent SQL should execute"),
    );

    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_inline_relationship_property_maps_execute_as_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS {source: 'pagerduty'}]->(service:Service) \
         RETURN person.name AS owner, service.name AS service",
    )
    .await
    .expect("Cypher query should execute");

    assert!(
        execution.translated_sql().contains("\"r0\".\"source\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owner": "Grace Hopper", "service": "deployments"})]
    );
}

#[tokio::test]
async fn cypher_parameterized_inline_property_maps_execute_as_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "tier".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("prod".to_string())),
        ),
        (
            "source".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("pagerduty".to_string())),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS {source: $source}]->(service:Service {tier: $tier}) \
         RETURN person.name AS owner, service.name AS service",
        &parameters,
    )
    .await
    .expect("parameterized inline property maps should execute");

    assert!(
        execution.translated_sql().contains("\"r0\".\"source\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owner": "Grace Hopper", "service": "deployments"})]
    );
}

#[tokio::test]
async fn cypher_inline_property_maps_accept_scalar_alias_values() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team)-[ownership:OWNS]->(service:Service {name: 'billing-api'}) \
         WITH service, ownership.source AS source_filter \
         MATCH (service)-[:DEPENDS_ON {source: source_filter}]->(target:Service) \
         RETURN service.name AS service, source_filter AS source, target.name AS target \
         ORDER BY target",
    )
    .await
    .expect("inline property maps should accept property-backed scalar aliases");

    assert!(
        execution.translated_sql().contains("\"r1\".\"source\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "source": "catalog", "target": "deployments"}),
            json!({"service": "billing-api", "source": "catalog", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_inline_property_maps_accept_property_expression_values() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (team:Team)-[ownership:OWNS]->(service:Service {name: 'billing-api'}) \
         MATCH (service)-[:DEPENDS_ON {source: ownership.source}]->(target:Service) \
         RETURN service.name AS service, ownership.source AS source, target.name AS target \
         ORDER BY target",
    )
    .await
    .expect("inline property maps should accept direct property expression values");

    assert!(
        execution.translated_sql().contains("\"r1\".\"source\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "source": "catalog", "target": "deployments"}),
            json!({"service": "billing-api", "source": "catalog", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_parameterized_dynamic_label_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "label".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("Service".to_string())),
        ),
        (
            "type".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("OWNS".to_string())),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE service:$($label) AND owns:$($type) \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY service",
        &parameters,
    )
    .await
    .expect("parameterized dynamic label/type predicates should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_parameterized_dynamic_label_patterns_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "owner_label".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("Person".to_string())),
        ),
        (
            "relationship_type".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("OWNS".to_string())),
        ),
        (
            "service_label".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("Service".to_string())),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:$($owner_label))-[owns:$($relationship_type)]->(service:$($service_label)) \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY service",
        &parameters,
    )
    .await
    .expect("parameterized dynamic label/type patterns should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_parameterized_dynamic_label_alternatives_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "label".to_string(),
        GraphCypherParameterValue::Literal(GraphLiteral::String("Service".to_string())),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (item:Team|$($label)) \
         RETURN item.name AS item \
         ORDER BY item",
        &parameters,
    )
    .await
    .expect("parameterized dynamic label alternatives should execute");

    assert!(
        execution.translated_sql().contains("UNION ALL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"item": "analytics"}),
            json!({"item": "billing-api"}),
            json!({"item": "deployments"}),
            json!({"item": "experiments"}),
            json!({"item": "infra"}),
            json!({"item": "legacy-sync"}),
            json!({"item": "platform"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_relationship_type_alternatives_project_missing_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph =
        GraphDeclaration::from_yaml(SERVICE_TYPE_ALTERNATIVE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[edge:DEPENDS_ON|ALERTS]->(target:Service) \
         RETURN type(edge) AS relationship_type, source.name AS source, target.name AS target, edge.criticality AS criticality \
         ORDER BY source, target, relationship_type",
    )
    .await
    .expect("relationship type alternatives with heterogeneous properties should execute");

    let schema = execution
        .execution()
        .batches()
        .first()
        .expect("execution should produce a batch")
        .schema();
    let field_names = schema
        .fields()
        .iter()
        .map(|field| field.name().as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        field_names,
        vec!["relationship_type", "source", "target", "criticality"]
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"relationship_type": "ALERTS", "source": "billing-api", "target": "deployments"}),
            json!({"relationship_type": "DEPENDS_ON", "source": "billing-api", "target": "deployments", "criticality": "runtime"}),
            json!({"relationship_type": "ALERTS", "source": "billing-api", "target": "experiments"}),
            json!({"relationship_type": "DEPENDS_ON", "source": "billing-api", "target": "experiments", "criticality": "optional"}),
            json!({"relationship_type": "ALERTS", "source": "deployments", "target": "experiments"}),
            json!({"relationship_type": "DEPENDS_ON", "source": "deployments", "target": "experiments", "criticality": "dev"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_relationship_type_alternatives_filter_missing_properties_as_null() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph =
        GraphDeclaration::from_yaml(SERVICE_TYPE_ALTERNATIVE_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[edge:DEPENDS_ON|ALERTS]->(target:Service) \
         WHERE edge.criticality IS NULL \
         RETURN type(edge) AS relationship_type, source.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("relationship type alternatives should filter missing properties as null");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"relationship_type": "ALERTS", "source": "billing-api", "target": "deployments"}),
            json!({"relationship_type": "ALERTS", "source": "billing-api", "target": "experiments"}),
            json!({"relationship_type": "ALERTS", "source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_dynamic_relationship_type_alternatives_rewrite_missing_scalar_properties() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph =
        GraphDeclaration::from_yaml(SERVICE_TYPE_ALTERNATIVE_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "relationship_type".to_string(),
        GraphCypherParameterValue::Literal(GraphLiteral::String("ALERTS".to_string())),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[edge:DEPENDS_ON|$($relationship_type)]->(target:Service) \
         RETURN type(edge) AS relationship_type, source.name AS source, target.name AS target, coalesce(edge.criticality, 'unclassified') AS criticality \
         ORDER BY source, target, relationship_type",
        &parameters,
    )
    .await
    .expect("dynamic relationship type alternatives should rewrite missing scalar properties");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"relationship_type": "ALERTS", "source": "billing-api", "target": "deployments", "criticality": "unclassified"}),
            json!({"relationship_type": "DEPENDS_ON", "source": "billing-api", "target": "deployments", "criticality": "runtime"}),
            json!({"relationship_type": "ALERTS", "source": "billing-api", "target": "experiments", "criticality": "unclassified"}),
            json!({"relationship_type": "DEPENDS_ON", "source": "billing-api", "target": "experiments", "criticality": "optional"}),
            json!({"relationship_type": "ALERTS", "source": "deployments", "target": "experiments", "criticality": "unclassified"}),
            json!({"relationship_type": "DEPENDS_ON", "source": "deployments", "target": "experiments", "criticality": "dev"}),
        ]
    );
}

#[tokio::test]
async fn cypher_multihop_paths_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (owner:Person)-[:OWNS]->(service:Service)-[:DEPENDS_ON {criticality: 'runtime'}]->(dependency:Service) \
         RETURN owner.name AS owner, service.name AS service, dependency.name AS dependency \
         ORDER BY owner, service, dependency",
    )
    .await
    .expect("multi-hop Cypher query should execute");
    let graph_rows = execution_to_rows(execution.execution());

    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT people.full_name AS owner, service.service_name AS service, dependency.service_name AS dependency \
             FROM ops.people AS people \
             JOIN ops.ownerships AS ownerships ON ownerships.person_id = people.id \
             JOIN ops.services AS service ON ownerships.service_id = service.id \
             JOIN ops.service_dependencies AS dependencies ON dependencies.from_service_id = service.id \
             JOIN ops.services AS dependency ON dependencies.to_service_id = dependency.id \
             WHERE dependencies.criticality = 'runtime' \
             ORDER BY people.full_name, service.service_name, dependency.service_name",
        )
        .await
        .expect("equivalent SQL should execute"),
    );

    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "owner": "Ada Lovelace",
            "service": "billing-api",
            "dependency": "deployments",
        })]
    );
}

#[tokio::test]
async fn cypher_anonymous_labeled_nodes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (:Service {tier: 'prod'})-[:DEPENDS_ON {criticality: 'runtime'}]->(dependency:Service) \
         RETURN dependency.name AS dependency \
         ORDER BY dependency",
    )
    .await
    .expect("anonymous labeled node query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"dependency": "deployments"})]
    );
}

#[tokio::test]
async fn cypher_static_label_expression_patterns_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person&!(Team|Service))-[owns:OWNS&!(DEPENDS_ON|ALERTS)]->(service:Service&!Team) \
         RETURN person.name AS owner, service.name AS service, owns.source AS source \
         ORDER BY owner, service",
    )
    .await
    .expect("static label expression patterns should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"ops\".\"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments", "source": "pagerduty"}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "source": "catalog"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service \
         MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         RETURN service.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("transparent WITH Cypher query should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_nonterminal_with_scalar_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service, service.name AS source_name \
         WHERE source_name STARTS WITH 'billing' \
         MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         RETURN source_name AS source, target.name AS target \
         ORDER BY source_name, target",
    )
    .await
    .expect("non-terminal WITH scalar alias query should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_nonterminal_with_star_scalar_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (owner:Person)-[:OWNS]->(service:Service) \
         WITH *, owner.name AS owner_name, service.name AS service_name, length(path) AS hops \
         WHERE owner_name = 'Ada Lovelace' AND hops = 1 \
         MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         RETURN owner_name AS owner, service_name AS service, hops, target.name AS target \
         ORDER BY owner, service, target",
    )
    .await
    .expect("non-terminal WITH * scalar alias query should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "hops": 1, "target": "deployments"}),
            json!({"owner": "Ada Lovelace", "service": "billing-api", "hops": 1, "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_where_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service \
         WHERE service.tier = 'prod' \
         MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         RETURN service.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("transparent WITH WHERE Cypher query should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service AS s \
         WHERE s.tier = 'prod' \
         MATCH (s)-[:DEPENDS_ON]->(target:Service) \
         RETURN s.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("transparent WITH alias Cypher query should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_relationship_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WITH person AS p, owns AS rel, service AS s \
         RETURN p.name AS owner, type(rel) AS relationship_type, s.name AS service \
         ORDER BY owner, service",
    )
    .await
    .expect("transparent WITH relationship alias Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"ownership_id\" IS NULL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "relationship_type": "OWNS", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "relationship_type": "OWNS", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "relationship_type": "OWNS", "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_dropped_variables_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person)-[:OWNS]->(service:Service) \
         WITH service \
         MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
         RETURN service.name AS service, dependency.name AS dependency \
         ORDER BY service, dependency",
    )
    .await
    .expect("transparent WITH with dropped variables should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency": "deployments"}),
            json!({"service": "billing-api", "dependency": "experiments"}),
            json!({"service": "deployments", "dependency": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_star_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH * \
         MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         RETURN service.name AS source, target.name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("transparent WITH * Cypher query should execute");

    assert!(
        !execution.translated_sql().contains("WITH"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_star_carries_path_metadata() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON]->(target:Service) \
         WITH * \
         RETURN source.name AS source, target.name AS target, length(path) AS hops, size(path) AS path_size \
         ORDER BY source, target",
    )
    .await
    .expect("WITH * should carry path length metadata");

    assert!(
        execution.translated_sql().contains("1 AS \"hops\"")
            && execution.translated_sql().contains("1 AS \"path_size\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments", "hops": 1, "path_size": 1}),
            json!({"source": "billing-api", "target": "experiments", "hops": 1, "path_size": 1}),
            json!({"source": "deployments", "target": "experiments", "hops": 1, "path_size": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_transparent_with_star_where_filters_path_metadata() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH path = (source:Service)-[:DEPENDS_ON*1..2]->(target:Service) \
         WITH * WHERE length(path) = 2 AND size(path) = 2 \
         RETURN source.name AS source, target.name AS target, length(path) AS hops \
         ORDER BY source, target",
    )
    .await
    .expect("WITH * WHERE should filter on path length metadata");

    assert!(
        execution.translated_sql().contains("2 AS \"hops\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"source": "billing-api", "target": "experiments", "hops": 2})]
    );
}

#[tokio::test]
async fn cypher_multiple_match_clauses_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (owner:Person) \
         WHERE owner.team = 'platform' \
         MATCH (owner)-[:OWNS]->(service:Service) \
         MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
         RETURN owner.name AS owner, service.name AS service, dependency.name AS dependency \
         ORDER BY owner, service, dependency",
    )
    .await
    .expect("multiple MATCH Cypher query should execute");

    assert!(
        execution.translated_sql().contains("JOIN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "dependency": "deployments"}),
            json!({"owner": "Ada Lovelace", "service": "billing-api", "dependency": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_reverse_multihop_paths_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (dependency:Service)<-[:DEPENDS_ON]-(service:Service)<-[:OWNS]-(owner:Person) \
         WHERE dependency.name = 'deployments' \
         RETURN owner.name AS owner, service.name AS service, dependency.name AS dependency",
    )
    .await
    .expect("reverse multi-hop Cypher query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "owner": "Ada Lovelace",
            "service": "billing-api",
            "dependency": "deployments",
        })]
    );
}

#[tokio::test]
async fn cypher_connected_comma_patterns_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON]->(middle:Service), \
               (middle)-[:DEPENDS_ON]->(target:Service), \
               (source)-[:DEPENDS_ON]->(target) \
         RETURN source.name AS source, middle.name AS middle, target.name AS target \
         ORDER BY source, middle, target",
    )
    .await
    .expect("connected comma-separated Cypher patterns should execute");
    let graph_rows = execution_to_rows(execution.execution());

    let sql_rows = execution_to_rows(
        &CoralQuery::execute_sql(
            &[source],
            test_runtime(),
            "SELECT source.service_name AS source, middle.service_name AS middle, target.service_name AS target \
             FROM ops.services AS source \
             JOIN ops.service_dependencies AS source_middle ON source_middle.from_service_id = source.id \
             JOIN ops.services AS middle ON source_middle.to_service_id = middle.id \
             JOIN ops.service_dependencies AS middle_target ON middle_target.from_service_id = middle.id \
             JOIN ops.services AS target ON middle_target.to_service_id = target.id \
             JOIN ops.service_dependencies AS source_target ON source_target.from_service_id = source.id AND source_target.to_service_id = target.id \
             ORDER BY source.service_name, middle.service_name, target.service_name",
        )
        .await
        .expect("equivalent SQL should execute"),
    );

    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![json!({
            "source": "billing-api",
            "middle": "deployments",
            "target": "experiments",
        })]
    );
}

#[tokio::test]
async fn cypher_undirected_relationships_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service)-[:DEPENDS_ON]-(neighbor:Service) \
         WHERE service.name = 'deployments' \
         RETURN neighbor.name AS neighbor \
         ORDER BY neighbor",
    )
    .await
    .expect("undirected relationship Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" OR "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"neighbor": "billing-api"}),
            json!({"neighbor": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_match_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, person.name AS owner \
         ORDER BY service",
    )
    .await
    .expect("OPTIONAL MATCH Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner": "Ada Lovelace"}),
            json!({"service": "deployments", "owner": "Grace Hopper"}),
            json!({"service": "experiments", "owner": "Katherine Johnson"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_match_after_optional_match_executes_when_independent() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let after_optional = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
         MATCH (owner:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, dependency.name AS dependency, owner.name AS owner \
         ORDER BY service, dependency, owner",
    )
    .await
    .expect("independent MATCH after OPTIONAL MATCH should execute");

    let reordered = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         MATCH (owner:Person)-[:OWNS]->(service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
         RETURN service.name AS service, dependency.name AS dependency, owner.name AS owner \
         ORDER BY service, dependency, owner",
    )
    .await
    .expect("reordered equivalent query should execute");

    assert_eq!(
        execution_to_rows(after_optional.execution()),
        execution_to_rows(reordered.execution())
    );
    assert_eq!(
        execution_to_rows(after_optional.execution()),
        vec![
            json!({"service": "billing-api", "dependency": "deployments", "owner": "Ada Lovelace"}),
            json!({"service": "billing-api", "dependency": "experiments", "owner": "Ada Lovelace"}),
            json!({"service": "deployments", "dependency": "experiments", "owner": "Grace Hopper"}),
            json!({"service": "experiments", "owner": "Katherine Johnson"}),
        ]
    );
}

#[tokio::test]
async fn cypher_match_after_optional_match_can_depend_on_optional_binding() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         MATCH (target)-[:DEPENDS_ON]->(next:Service) \
         RETURN service.name AS service, target.name AS target, next.name AS next \
         ORDER BY service, target, next",
    )
    .await
    .expect("dependent MATCH after OPTIONAL MATCH should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "target": "deployments",
            "next": "experiments",
        })]
    );
}

#[tokio::test]
async fn cypher_match_after_optional_match_filters_on_optional_binding() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(target:Service) \
         MATCH (owner:Person)-[:OWNS]->(service) \
         WHERE target.tier = 'dev' \
         RETURN service.name AS service, owner.name AS owner, target.name AS target \
         ORDER BY service, owner, target",
    )
    .await
    .expect("post-optional WHERE filters over optional bindings should execute");

    assert!(
        execution.translated_sql().contains(" WHERE "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner": "Ada Lovelace", "target": "experiments"}),
            json!({"service": "deployments", "owner": "Grace Hopper", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_match_where_preserves_rows_with_null_optional_bindings() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
         WHERE person.team = 'platform' \
         RETURN service.name AS service, id(owns) AS ownership_id, type(owns) AS relationship_type, person.name AS owner \
         ORDER BY service",
    )
    .await
    .expect("OPTIONAL MATCH WHERE Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains(" WHERE "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "ownership_id": 100, "relationship_type": "OWNS", "owner": "Ada Lovelace"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_undirected_optional_match_where_preserves_rows_with_null_optional_bindings() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency_edge:DEPENDS_ON]-(dependency:Service) \
         WHERE dependency.tier = 'dev' \
         RETURN service.name AS service, dependency.name AS dependency, type(dependency_edge) AS relationship_type \
         ORDER BY service, dependency",
    )
    .await
    .expect("undirected OPTIONAL MATCH WHERE Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains(" WHERE "),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(" OR "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency": "experiments", "relationship_type": "DEPENDS_ON"}),
            json!({"service": "deployments", "dependency": "experiments", "relationship_type": "DEPENDS_ON"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_match_inline_property_maps_execute_as_join_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person {team: 'infra'})-[owns:OWNS {source: 'pagerduty'}]->(service) \
         RETURN service.name AS service, id(owns) AS ownership_id, person.name AS owner \
         ORDER BY service",
    )
    .await
    .expect("OPTIONAL MATCH inline property maps should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"source\" = 'pagerduty'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments", "ownership_id": 200, "owner": "Grace Hopper"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_multihop_optional_match_preserves_whole_pattern_null_semantics() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target:Service) \
         RETURN service.name AS service, middle.name AS middle, target.name AS target \
         ORDER BY service, middle, target",
    )
    .await
    .expect("multi-hop OPTIONAL MATCH query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "middle": "deployments", "target": "experiments"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_multihop_optional_match_between_bound_endpoints_preserves_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service), (target:Service) \
         OPTIONAL MATCH (source)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target) \
         RETURN count(*) AS pairs, count(middle.name) AS paths",
    )
    .await
    .expect("bound-endpoint multi-hop OPTIONAL MATCH query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"r1\".\"to_service_id\" = \"n1\".\"id\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"pairs": 16, "paths": 1})]
    );
}

#[tokio::test]
async fn cypher_optional_fixed_relationship_ranges_execute_as_repeated_hops() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    for pattern in [
        "(source)-[:DEPENDS_ON*2]->(target:Service)",
        "(source)-[:DEPENDS_ON*2..2]->(target:Service)",
        "(source)-[:DEPENDS_ON]->{2}(target:Service)",
    ] {
        let execution = CoralQuery::execute_cypher(
            std::slice::from_ref(&source),
            test_runtime(),
            &graph,
            &format!(
                "MATCH (source:Service) \
                 OPTIONAL MATCH {pattern} \
                 RETURN source.name AS source, target.name AS target \
                 ORDER BY source"
            ),
        )
        .await
        .expect("exact positive OPTIONAL MATCH relationship range should execute");

        assert!(
            execution
                .translated_sql()
                .matches("LEFT JOIN \"ops\".\"service_dependencies\"")
                .count()
                >= 2
                || execution.translated_sql().contains(" LEFT JOIN ("),
            "{}",
            execution.translated_sql()
        );
        assert_eq!(
            execution_to_rows(execution.execution()),
            vec![
                json!({"source": "billing-api", "target": "experiments"}),
                json!({"source": "deployments"}),
                json!({"source": "experiments"}),
                json!({"source": "legacy-sync"}),
            ]
        );
    }
}

#[tokio::test]
async fn cypher_optional_zero_hop_relationship_range_executes_as_identity() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON*0]->(target:Service) \
         RETURN service.name AS service, target.name AS target \
         ORDER BY service",
    )
    .await
    .expect("same-label optional zero-hop relationship range should execute");

    assert!(
        !execution.translated_sql().contains(" LEFT JOIN "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "target": "billing-api"}),
            json!({"service": "deployments", "target": "deployments"}),
            json!({"service": "experiments", "target": "experiments"}),
            json!({"service": "legacy-sync", "target": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_zero_hop_with_bound_endpoints_preserves_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (source:Service), (target:Service) \
         OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(target) \
         RETURN count(*) AS pairs",
    )
    .await
    .expect("bound-endpoint optional zero-hop relationship range should execute");

    assert!(
        !execution.translated_sql().contains(" LEFT JOIN "),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution
            .translated_sql()
            .contains("\"source\" = \"target\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"pairs": 16})]
    );

    let cross_label = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (source:Service), (person:Person) \
         OPTIONAL MATCH (source)-[:DEPENDS_ON*0]->(person) \
         RETURN count(*) AS pairs",
    )
    .await
    .expect("bound cross-label optional zero-hop relationship range should execute");

    assert!(
        !cross_label.translated_sql().contains(" LEFT JOIN "),
        "{}",
        cross_label.translated_sql()
    );
    assert!(
        !cross_label.translated_sql().contains("FALSE"),
        "{}",
        cross_label.translated_sql()
    );
    assert_eq!(
        execution_to_rows(cross_label.execution()),
        vec![json!({"pairs": 12})]
    );
}

#[tokio::test]
async fn cypher_multihop_optional_match_where_applies_to_whole_scope() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target:Service) \
         WHERE target.tier = 'dev' \
         RETURN service.name AS service, middle.name AS middle, target.name AS target \
         ORDER BY service, middle, target",
    )
    .await
    .expect("multi-hop OPTIONAL MATCH WHERE query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains(" WHERE "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "middle": "deployments", "target": "experiments"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_multihop_optional_match_between_bound_endpoints_keeps_where_local() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service), (target:Service) \
         OPTIONAL MATCH (source)-[:DEPENDS_ON]->(middle:Service)-[:DEPENDS_ON]->(target) \
         WHERE target.tier = 'prod' \
         RETURN count(*) AS pairs, count(middle.name) AS prod_paths",
    )
    .await
    .expect("bound-endpoint multi-hop OPTIONAL MATCH WHERE query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains(" WHERE "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"pairs": 16, "prod_paths": 0})]
    );
}

#[tokio::test]
async fn cypher_chained_optional_matches_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
         OPTIONAL MATCH (owner:Person)-[:OWNS]->(dependency) \
         RETURN service.name AS service, dependency.name AS dependency, owner.name AS dependency_owner \
         ORDER BY service, dependency, dependency_owner",
    )
    .await
    .expect("chained OPTIONAL MATCH query should execute");

    assert_eq!(execution.translated_sql().matches(" LEFT JOIN ").count(), 4);
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency": "deployments", "dependency_owner": "Grace Hopper"}),
            json!({"service": "billing-api", "dependency": "experiments", "dependency_owner": "Katherine Johnson"}),
            json!({"service": "deployments", "dependency": "experiments", "dependency_owner": "Katherine Johnson"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_chained_optional_match_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[:DEPENDS_ON]->(dependency:Service) \
         OPTIONAL MATCH (owner:Person {team: 'analytics'})-[:OWNS]->(dependency) \
         RETURN service.name AS service, dependency.name AS dependency, owner.name AS dependency_owner \
         ORDER BY service, dependency, dependency_owner",
    )
    .await
    .expect("chained OPTIONAL MATCH predicate query should execute");

    assert!(
        execution.translated_sql().contains(" LEFT JOIN ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency": "deployments"}),
            json!({"service": "billing-api", "dependency": "experiments", "dependency_owner": "Katherine Johnson"}),
            json!({"service": "deployments", "dependency": "experiments", "dependency_owner": "Katherine Johnson"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_match_false_where_preserves_left_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
         WHERE false \
         RETURN service.name AS service, id(owns) AS ownership_id, person.name AS owner \
         ORDER BY service",
    )
    .await
    .expect("OPTIONAL MATCH false WHERE should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_case_graph_null_checks_execute_against_optional_matches() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
         RETURN service.name AS service, \
                CASE \
                  WHEN person IS NULL THEN 'unowned' \
                  WHEN id(owns) IS NOT NULL THEN person.name \
                  ELSE 'unknown' \
                END AS ownership_state, \
                CASE WHEN id(person) IS NULL THEN 'missing' ELSE 'present' END AS owner_presence, \
                CASE \
                  WHEN person.team = service.team THEN 'team_match' \
                  ELSE 'team_missing' \
                END AS owner_team_state \
         ORDER BY service",
    )
    .await
    .expect("CASE graph null checks should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CASE WHEN \"n1\".\"id\" IS NULL THEN 'unowned'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("WHEN \"r0\".\"ownership_id\" IS NOT NULL THEN \"n1\".\"full_name\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("WHEN \"n1\".\"team\" = \"n0\".\"owning_team\" THEN 'team_match'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "ownership_state": "Ada Lovelace", "owner_presence": "present", "owner_team_state": "team_match"}),
            json!({"service": "deployments", "ownership_state": "Grace Hopper", "owner_presence": "present", "owner_team_state": "team_match"}),
            json!({"service": "experiments", "ownership_state": "Katherine Johnson", "owner_presence": "present", "owner_team_state": "team_match"}),
            json!({"service": "legacy-sync", "ownership_state": "unowned", "owner_presence": "missing", "owner_team_state": "team_missing"}),
        ]
    );
}

#[tokio::test]
async fn cypher_case_graph_metadata_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN service.name AS service, \
                CASE \
                  WHEN type(owns) = 'OWNS' \
                    AND service:Service \
                    AND 'Service' IN labels(service) \
                    AND labels(service) = ['Service'] \
                    AND 'source' IN keys(owns) THEN 'ownership' \
                  ELSE 'other' \
                END AS category, \
                CASE \
                  WHEN service:Person THEN 'person' \
                  WHEN service:Service THEN 'service' \
                  ELSE 'other' \
                END AS label_bucket, \
                CASE \
                  WHEN type(owns) IN ['OWNS'] \
                   AND keys(owns) = ['since', 'source'] THEN 'typed' \
                  ELSE 'other' \
                END AS type_bucket \
         ORDER BY service",
    )
    .await
    .expect("CASE graph metadata predicates should execute");

    assert!(
        execution.translated_sql().contains("THEN 'ownership'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "category": "ownership", "label_bucket": "service", "type_bucket": "typed"}),
            json!({"service": "deployments", "category": "ownership", "label_bucket": "service", "type_bucket": "typed"}),
            json!({"service": "experiments", "category": "ownership", "label_bucket": "service", "type_bucket": "typed"}),
        ]
    );
}

#[tokio::test]
async fn cypher_disconnected_comma_patterns_execute_as_cross_joins() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON]->(target:Service), (orphan:Person) \
         RETURN source.name AS source, target.name AS target, orphan.name AS person \
         ORDER BY source, target, person \
         LIMIT 4",
    )
    .await
    .expect("disconnected comma-separated patterns should execute");

    assert!(
        execution.translated_sql().contains("CROSS JOIN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments", "person": "Ada Lovelace"}),
            json!({"source": "billing-api", "target": "deployments", "person": "Grace Hopper"}),
            json!({"source": "billing-api", "target": "deployments", "person": "Katherine Johnson"}),
            json!({"source": "billing-api", "target": "experiments", "person": "Ada Lovelace"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_match_can_anchor_to_disconnected_mandatory_component() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service), (person:Person) \
         OPTIONAL MATCH (person)-[:OWNS]->(owned:Service) \
         WHERE owned.tier = 'missing' \
         RETURN service.name AS service, person.name AS person, owned.name AS owned \
         ORDER BY service, person \
         LIMIT 4",
    )
    .await
    .expect("optional match should anchor to the disconnected person component");

    assert!(
        execution.translated_sql().contains("CROSS JOIN")
            && execution.translated_sql().contains("LEFT JOIN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "person": "Ada Lovelace"}),
            json!({"service": "billing-api", "person": "Grace Hopper"}),
            json!({"service": "billing-api", "person": "Katherine Johnson"}),
            json!({"service": "deployments", "person": "Ada Lovelace"}),
        ]
    );
}

#[tokio::test]
async fn cypher_is_null_predicates_execute_with_sql_null_semantics() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NULL \
         RETURN service.name AS service",
    )
    .await
    .expect("Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" IS NULL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_exists_property_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE exists(service.tier) \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher exists property predicate query should execute");

    assert!(
        execution.translated_sql().contains(" IS NOT NULL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_or_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'dev' OR service.tier IS NULL \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher OR predicate query should execute");

    assert!(
        execution.translated_sql().contains(" OR "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_xor_predicates_execute_with_sql_null_semantics() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'prod' XOR service.name CONTAINS 'billing' \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher XOR predicate query should execute");

    assert!(
        execution.translated_sql().contains(" AND NOT ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "deployments"})]
    );
}

#[tokio::test]
async fn cypher_not_predicates_execute_with_sql_null_semantics() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE NOT (service.tier = 'prod') \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher NOT predicate query should execute");

    assert!(
        execution.translated_sql().contains("NOT ("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "experiments"})]
    );
}

#[tokio::test]
async fn cypher_bare_boolean_property_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let active = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.active \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("bare boolean property predicate query should execute");

    assert!(
        active.translated_sql().contains("\"n0\".\"active\" = true"),
        "{}",
        active.translated_sql()
    );
    assert_eq!(
        execution_to_rows(active.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );

    let inactive = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE NOT service.active \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("negated bare boolean property predicate query should execute");

    assert!(
        inactive.translated_sql().contains("NOT ("),
        "{}",
        inactive.translated_sql()
    );
    assert_eq!(
        execution_to_rows(inactive.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_constant_boolean_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let no_rows = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE false \
         RETURN service.name AS service",
    )
    .await
    .expect("constant false predicate query should execute");

    assert!(
        no_rows.translated_sql().contains("WHERE FALSE"),
        "{}",
        no_rows.translated_sql()
    );
    assert_eq!(execution_to_rows(no_rows.execution()), Vec::<Value>::new());

    let active = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.active OR false \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("constant boolean expression query should execute");

    assert!(
        active.translated_sql().contains(" OR FALSE"),
        "{}",
        active.translated_sql()
    );
    assert_eq!(
        execution_to_rows(active.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_literal_only_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 1 = 1 \
           AND 5 >= 3 \
           AND toLower('PROD') = 'prod' \
           AND coalesce(null, 'prod') IN ['prod', 'dev'] \
           AND nullIf('prod', 'prod') IS NULL \
         RETURN service.name AS service \
         ORDER BY service \
         LIMIT 2",
    )
    .await
    .expect("literal-only predicate query should execute");

    assert!(
        execution.translated_sql().contains("TRUE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );

    let no_rows = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE nullIf('prod', 'prod') IS NOT NULL \
         RETURN service.name AS service",
    )
    .await
    .expect("false literal-only predicate query should execute");

    assert_eq!(execution_to_rows(no_rows.execution()), Vec::<Value>::new());
}

#[tokio::test]
async fn cypher_in_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IN ['prod', null, 'dev'] \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher IN predicate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains(" IN ('prod', NULL, 'dev')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_literal_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         RETURN service.name AS service, 'virtual' AS kind, 1 AS version, true AS enabled, null AS missing",
    )
    .await
    .expect("literal projection query should execute");

    assert!(
        execution.translated_sql().contains(
            "'virtual' AS \"kind\", 1 AS \"version\", true AS \"enabled\", NULL AS \"missing\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "kind": "virtual",
            "version": 1,
            "enabled": true
        })]
    );
}

#[tokio::test]
async fn cypher_coalesce_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, coalesce(service.tier, 'unassigned') AS service_tier \
         ORDER BY service",
    )
    .await
    .expect("coalesce projection query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"n0\".\"tier\", 'unassigned') AS \"service_tier\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "service_tier": "prod"}),
            json!({"service": "deployments", "service_tier": "prod"}),
            json!({"service": "experiments", "service_tier": "dev"}),
            json!({"service": "legacy-sync", "service_tier": "unassigned"}),
        ]
    );
}

#[tokio::test]
async fn cypher_order_by_coalesce_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY coalesce(service.tier, 'zzzz'), service",
    )
    .await
    .expect("coalesce order expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY COALESCE(\"n0\".\"tier\", 'zzzz') ASC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments"}),
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_coalesce_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE coalesce(service.tier, 'unassigned') = 'unassigned' \
         RETURN service.name AS service",
    )
    .await
    .expect("coalesce predicate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"n0\".\"tier\", 'unassigned') = 'unassigned'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_coalesce_in_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE coalesce(service.tier, 'unassigned') IN ['prod', 'dev'] \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("coalesce IN predicate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"n0\".\"tier\", 'unassigned') IN ('prod', 'dev')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_null_if_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE nullIf(service.tier, 'dev') IS NULL \
         RETURN service.name AS service, nullIf(service.tier, 'prod') AS normalized_tier \
         ORDER BY service",
    )
    .await
    .expect("nullIf scalar query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("NULLIF(\"n0\".\"tier\", 'dev') IS NULL"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("NULLIF(\"n0\".\"tier\", 'prod') AS \"normalized_tier\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "experiments", "normalized_tier": "dev"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_to_string_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE toString(service.risk) STARTS WITH '0.9' \
         RETURN service.name AS service, toString(service.risk) AS risk_text \
         ORDER BY toString(service.risk), service",
    )
    .await
    .expect("toString scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST(\"n0\".\"risk_score\" AS VARCHAR) LIKE '0.9%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST(\"n0\".\"risk_score\" AS VARCHAR) AS \"risk_text\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "risk_text": "0.9"}),
            json!({"service": "legacy-sync", "risk_text": "0.95"}),
        ]
    );
}

#[tokio::test]
async fn cypher_scalar_cast_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE toInteger(service.id) = 10 \
         RETURN toInteger(service.id) AS id_int, \
                toFloat(service.risk) AS risk_float, \
                toBoolean(service.active) AS active_bool \
         ORDER BY toInteger(service.id)",
    )
    .await
    .expect("scalar cast expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST(\"n0\".\"id\" AS BIGINT) = 10"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST(\"n0\".\"risk_score\" AS DOUBLE) AS \"risk_float\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST(\"n0\".\"active\" AS BOOLEAN) AS \"active_bool\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "id_int": 10,
            "risk_float": 0.9,
            "active_bool": true,
        })]
    );
}

#[tokio::test]
async fn cypher_invalid_scalar_casts_yield_null_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         RETURN toInteger('abc') AS invalid_int, \
                toInteger('42') AS valid_int, \
                toFloat('x') AS invalid_float, \
                toBoolean('nope') AS invalid_bool, \
                toInteger(service.name) AS invalid_property, \
                coalesce(toInteger(service.name), -1) AS property_fallback",
    )
    .await
    .expect("invalid scalar casts should yield null instead of failing");

    for expected in [
        "TRY_CAST('abc' AS BIGINT) AS \"invalid_int\"",
        "TRY_CAST('42' AS BIGINT) AS \"valid_int\"",
        "TRY_CAST('x' AS DOUBLE) AS \"invalid_float\"",
        "TRY_CAST('nope' AS BOOLEAN) AS \"invalid_bool\"",
        "TRY_CAST(\"n0\".\"service_name\" AS BIGINT) AS \"invalid_property\"",
    ] {
        assert!(
            execution.translated_sql().contains(expected),
            "{}",
            execution.translated_sql()
        );
    }

    let projected_columns = execution
        .execution()
        .schema()
        .iter()
        .map(|column| column.name.as_str())
        .collect::<Vec<_>>();
    for expected in [
        "invalid_int",
        "valid_int",
        "invalid_float",
        "invalid_bool",
        "invalid_property",
        "property_fallback",
    ] {
        assert!(
            projected_columns.contains(&expected),
            "missing projected column {expected}: {projected_columns:?}"
        );
    }
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "valid_int": 42,
            "property_fallback": -1,
        })]
    );
}

#[tokio::test]
async fn cypher_nullable_scalar_cast_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         RETURN toStringOrNull(service.id) AS id_text, \
                toIntegerOrNull(service.id) AS id_int, \
                toIntegerOrNull('not-an-int') AS invalid_int, \
                toFloatOrNull('not-a-float') AS invalid_float, \
                toBooleanOrNull('not-a-bool') AS invalid_bool",
    )
    .await
    .expect("nullable scalar cast expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST(\"n0\".\"id\" AS VARCHAR) AS \"id_text\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST('not-an-int' AS BIGINT) AS \"invalid_int\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST('not-a-float' AS DOUBLE) AS \"invalid_float\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST('not-a-bool' AS BOOLEAN) AS \"invalid_bool\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "id_text": "10",
            "id_int": 10,
        })]
    );
}

#[tokio::test]
async fn cypher_arithmetic_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.id + 5 >= 25 \
         RETURN service.name AS service, \
                (service.id * 2) AS double_id, \
                toInteger(service.id / 10) AS id_bucket, \
                service.id % 20 AS id_mod, \
                service.risk ^ 2 AS risk_squared, \
                pow(service.risk, 2) AS risk_pow, \
                power(service.risk, 2) AS risk_power \
         ORDER BY service.id - 5",
    )
    .await
    .expect("arithmetic scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("(\"n0\".\"id\" + 5) >= 25"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("(\"n0\".\"id\" * 2) AS \"double_id\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRY_CAST((\"n0\".\"id\" / 10) AS BIGINT) AS \"id_bucket\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("power(\"n0\".\"risk_score\", 2) AS \"risk_squared\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("power(\"n0\".\"risk_score\", 2) AS \"risk_pow\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("power(\"n0\".\"risk_score\", 2) AS \"risk_power\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "deployments",
                "double_id": 40,
                "id_bucket": 2,
                "id_mod": 0,
                "risk_squared": 0.25,
                "risk_pow": 0.25,
                "risk_power": 0.25,
            }),
            json!({
                "service": "experiments",
                "double_id": 60,
                "id_bucket": 3,
                "id_mod": 10,
                "risk_squared": 0.0625,
                "risk_pow": 0.0625,
                "risk_power": 0.0625,
            }),
            json!({
                "service": "legacy-sync",
                "double_id": 80,
                "id_bucket": 4,
                "id_mod": 0,
                "risk_squared": 0.9025,
                "risk_pow": 0.9025,
                "risk_power": 0.9025,
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_searched_case_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                CASE \
                  WHEN service.risk >= 0.75 THEN 'high' \
                  WHEN service.active THEN 'active' \
                  ELSE 'low' \
                END AS risk_band \
         ORDER BY CASE \
                    WHEN service.risk >= 0.75 THEN 0 \
                    WHEN service.active THEN 1 \
                    ELSE 2 \
                  END, service",
    )
    .await
    .expect("searched CASE scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CASE WHEN \"n0\".\"risk_score\" >= 0.75 THEN 'high'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "ORDER BY CASE WHEN \"n0\".\"risk_score\" >= 0.75 THEN 0 WHEN \"n0\".\"active\" = true THEN 1 ELSE 2 END ASC"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "risk_band": "high"}),
            json!({"service": "legacy-sync", "risk_band": "high"}),
            json!({"service": "deployments", "risk_band": "active"}),
            json!({"service": "experiments", "risk_band": "low"}),
        ]
    );
}

#[tokio::test]
async fn cypher_xor_case_predicates_execute_with_sql_null_semantics() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                CASE \
                  WHEN service.tier = 'prod' XOR service.name CONTAINS 'billing' THEN 'xor' \
                  ELSE 'other' \
                END AS marker \
         ORDER BY service",
    )
    .await
    .expect("searched CASE XOR predicate query should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN (("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "marker": "other"}),
            json!({"service": "deployments", "marker": "xor"}),
            json!({"service": "experiments", "marker": "other"}),
            json!({"service": "legacy-sync", "marker": "other"}),
        ]
    );
}

#[tokio::test]
async fn cypher_generic_case_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                CASE service.tier \
                  WHEN 'prod' THEN 'production' \
                  WHEN 'dev' THEN 'development' \
                  ELSE 'unknown' \
                END AS tier_group \
         ORDER BY service",
    )
    .await
    .expect("generic CASE scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CASE WHEN \"n0\".\"tier\" = 'prod' THEN 'production'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier_group": "production"}),
            json!({"service": "deployments", "tier_group": "production"}),
            json!({"service": "experiments", "tier_group": "development"}),
            json!({"service": "legacy-sync", "tier_group": "unknown"}),
        ]
    );
}

#[tokio::test]
async fn cypher_string_case_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE toLower(service.name) CONTAINS 'api' \
         RETURN service.name AS service, toUpper(service.tier) AS tier_upper \
         ORDER BY toLower(service.name)",
    )
    .await
    .expect("string case scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("LOWER(\"n0\".\"service_name\") LIKE '%api%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("UPPER(\"n0\".\"tier\") AS \"tier_upper\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api", "tier_upper": "PROD"})]
    );
}

#[tokio::test]
async fn cypher_trim_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE trim(service.name) = 'billing-api' \
         RETURN trim('  billing-api  ') AS trimmed_service, \
                lTrim('  left') AS left_trimmed, \
                rTrim('right  ') AS right_trimmed \
         ORDER BY trim('  billing-api  ')",
    )
    .await
    .expect("trim scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("TRIM(\"n0\".\"service_name\") = 'billing-api'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("LTRIM('  left') AS \"left_trimmed\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "trimmed_service": "billing-api",
            "left_trimmed": "left",
            "right_trimmed": "right",
        })]
    );
}

#[tokio::test]
async fn cypher_gql_string_function_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE lower(service.name) CONTAINS 'api' \
         RETURN service.name AS service, \
                upper(service.tier) AS tier_upper, \
                btrim('  gql alias  ') AS trimmed_literal \
         ORDER BY btrim(service.name)",
    )
    .await
    .expect("GQL string alias query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("LOWER(\"n0\".\"service_name\") LIKE '%api%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("UPPER(\"n0\".\"tier\") AS \"tier_upper\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("TRIM('  gql alias  ') AS \"trimmed_literal\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "tier_upper": "PROD",
            "trimmed_literal": "gql alias",
        })]
    );
}

#[tokio::test]
async fn cypher_replace_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE replace(service.name, '-', '') = 'billingapi' \
         RETURN replace(service.name, '-', ' ') AS display_name, \
                replace('prod service', 'service', 'tier') AS literal_replace \
         ORDER BY replace(service.name, '-', '')",
    )
    .await
    .expect("replace scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("REPLACE(\"n0\".\"service_name\", '-', '') = 'billingapi'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("REPLACE('prod service', 'service', 'tier') AS \"literal_replace\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "display_name": "billing api",
            "literal_replace": "prod tier",
        })]
    );
}

#[tokio::test]
async fn cypher_character_length_and_substring_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE substring(service.name, 0, 7) = 'billing' \
         RETURN service.name AS service, \
                substring(service.name, 0, 7) AS prefix, \
                size(service.name) AS name_length, \
                character_length(service.tier) AS tier_length \
         ORDER BY char_length(service.name)",
    )
    .await
    .expect("string length and substring query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SUBSTRING(\"n0\".\"service_name\" FROM (0 + 1) FOR 7) = 'billing'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("character_length(\"n0\".\"service_name\") AS \"name_length\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "prefix": "billing",
            "name_length": 11,
            "tier_length": 4,
        })]
    );
}

#[tokio::test]
async fn cypher_literal_list_size_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "selected_tiers".to_string(),
            GraphCypherParameterValue::List(vec![
                GraphLiteral::String("prod".to_string()),
                GraphLiteral::String("dev".to_string()),
            ]),
        ),
        (
            "empty_tiers".to_string(),
            GraphCypherParameterValue::List(Vec::new()),
        ),
        (
            "empty_name".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String(String::new())),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         WHERE size(['prod', 'dev']) = 2 \
           AND size($selected_tiers) = 2 \
           AND isEmpty([]) \
           AND isEmpty($empty_tiers) \
           AND isEmpty($empty_name) \
           AND NOT isEmpty(['prod']) \
           AND NOT isEmpty($selected_tiers) \
         RETURN size(['prod', 'dev', 'critical']) AS literal_size, \
                size($selected_tiers) AS parameter_size, \
                size(service.name) AS name_length",
        &parameters,
    )
    .await
    .expect("literal list size query should execute");

    assert!(
        execution.translated_sql().contains("3 AS \"literal_size\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("2 AS \"parameter_size\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("character_length(\"n0\".\"service_name\") AS \"name_length\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "literal_size": 3,
            "parameter_size": 2,
            "name_length": 11,
        })]
    );
}

#[tokio::test]
async fn cypher_literal_list_indexes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "selected_tiers".to_string(),
            GraphCypherParameterValue::List(vec![
                GraphLiteral::String("prod".to_string()),
                GraphLiteral::String("dev".to_string()),
            ]),
        ),
        (
            "selected_index".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(1)),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = ['prod', 'critical'][0] \
         RETURN service.name AS service, \
                ['prod', 'dev'][0] AS first_tier, \
                ['prod', 'dev'][-1] AS last_tier, \
                ['prod', 'dev'][5] AS missing_tier, \
                $selected_tiers[$selected_index] AS parameter_tier \
         ORDER BY service",
        &parameters,
    )
    .await
    .expect("literal list index query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("'prod' AS \"first_tier\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("NULL AS \"missing_tier\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "billing-api",
                "first_tier": "prod",
                "last_tier": "dev",
                "parameter_tier": "dev",
            }),
            json!({
                "service": "deployments",
                "first_tier": "prod",
                "last_tier": "dev",
                "parameter_tier": "dev",
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_literal_list_slices_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "selected_tiers".to_string(),
            GraphCypherParameterValue::List(vec![
                GraphLiteral::String("prod".to_string()),
                GraphLiteral::String("critical".to_string()),
                GraphLiteral::String("dev".to_string()),
            ]),
        ),
        (
            "slice_start".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(1)),
        ),
        (
            "slice_end".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(3)),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IN ['dev', 'prod', 'critical'][..2] \
         RETURN service.name AS service, \
                ['prod', 'critical', 'dev'][0..2] AS first_tiers, \
                ['prod', 'critical', 'dev'][..-1] AS without_last, \
                $selected_tiers[$slice_start..$slice_end] AS parameter_slice, \
                ['prod', 'critical', 'dev'][1..][0] AS nested_first, \
                size(['prod', 'critical', 'dev'][1..]) AS slice_size, \
                isEmpty(['prod'][1..1]) AS empty_slice \
         ORDER BY service",
        &parameters,
    )
    .await
    .expect("literal list slice query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('prod', 'critical') AS \"first_tiers\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("'critical' AS \"nested_first\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("2 AS \"slice_size\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "billing-api",
                "first_tiers": ["prod", "critical"],
                "without_last": ["prod", "critical"],
                "parameter_slice": ["critical", "dev"],
                "nested_first": "critical",
                "slice_size": 2,
                "empty_slice": true,
            }),
            json!({
                "service": "deployments",
                "first_tiers": ["prod", "critical"],
                "without_last": ["prod", "critical"],
                "parameter_slice": ["critical", "dev"],
                "nested_first": "critical",
                "slice_size": 2,
                "empty_slice": true,
            }),
            json!({
                "service": "experiments",
                "first_tiers": ["prod", "critical"],
                "without_last": ["prod", "critical"],
                "parameter_slice": ["critical", "dev"],
                "nested_first": "critical",
                "slice_size": 2,
                "empty_slice": true,
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_is_empty_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE isEmpty(replace(service.name, service.name, '')) \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("isEmpty query should execute");

    assert!(
        execution.translated_sql().contains(
            "character_length(REPLACE(\"n0\".\"service_name\", \"n0\".\"service_name\", '')) = 0"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_literal_list_indexes_reject_dynamic_elements() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN [service.name][0] AS first_name",
    )
    .await
    .expect_err("literal list indexes should reject dynamic list elements");

    assert!(
        error
            .to_string()
            .contains("only string, numeric, boolean, and null literals are supported"),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_literal_list_slices_reject_dynamic_elements() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN [service.name, 'fallback'][0..1] AS names",
    )
    .await
    .expect_err("literal list slices should reject dynamic list elements");

    assert!(
        error
            .to_string()
            .contains("only string, numeric, boolean, and null literals are supported"),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_literal_list_size_rejects_dynamic_elements() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN size([service.name]) AS names",
    )
    .await
    .expect_err("literal list size should reject dynamic list elements");

    assert!(
        error
            .to_string()
            .contains("only string, numeric, boolean, and null literals are supported"),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_left_right_and_reverse_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE left(service.name, 7) = 'billing' \
         RETURN service.name AS service, \
                right(service.name, 3) AS suffix, \
                reverse(service.tier) AS reversed_tier \
         ORDER BY reverse(service.name)",
    )
    .await
    .expect("left, right, and reverse query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("left(\"n0\".\"service_name\", 7) = 'billing'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("right(\"n0\".\"service_name\", 3) AS \"suffix\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "suffix": "api",
            "reversed_tier": "dorp",
        })]
    );
}

#[tokio::test]
async fn cypher_indices_and_padding_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name = 'billing-api' \
         RETURN indices(service.name, 'i') AS i_positions, \
                lpad(service.name, 13, '*') AS padded_left, \
                rpad(service.tier, 8, '-') AS padded_right",
    )
    .await
    .expect("indices, lpad, and rpad query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("coral_string_indices(\"n0\".\"service_name\", 'i') AS \"i_positions\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("lpad(\"n0\".\"service_name\", 13, '*') AS \"padded_left\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("rpad(\"n0\".\"tier\", 8, '-') AS \"padded_right\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "i_positions": [1, 4, 10],
            "padded_left": "**billing-api",
            "padded_right": "prod----",
        })]
    );
}

#[tokio::test]
async fn cypher_string_predicate_function_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                contains(service.name, 'api') AS has_api, \
                startsWith(service.name, 'deploy') AS starts_deploy, \
                endsWith(service.name, 'sync') AS ends_sync \
         ORDER BY service",
    )
    .await
    .expect("string predicate function projection query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("contains(\"n0\".\"service_name\", 'api') AS \"has_api\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("starts_with(\"n0\".\"service_name\", 'deploy') AS \"starts_deploy\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ends_with(\"n0\".\"service_name\", 'sync') AS \"ends_sync\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "billing-api",
                "has_api": true,
                "starts_deploy": false,
                "ends_sync": false,
            }),
            json!({
                "service": "deployments",
                "has_api": false,
                "starts_deploy": true,
                "ends_sync": false,
            }),
            json!({
                "service": "experiments",
                "has_api": false,
                "starts_deploy": false,
                "ends_sync": false,
            }),
            json!({
                "service": "legacy-sync",
                "has_api": false,
                "starts_deploy": false,
                "ends_sync": true,
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_string_predicate_functions_execute_as_boolean_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE contains(service.name, 'api') \
            OR startsWith(service.name, 'deploy') \
            OR endsWith(service.name, 'sync') \
         RETURN service.name AS service, \
                CASE \
                  WHEN startsWith(service.name, 'bill') THEN 'billing' \
                  WHEN endsWith(service.name, 'sync') THEN 'sync' \
                  ELSE 'other' \
                END AS bucket \
         ORDER BY service",
    )
    .await
    .expect("string predicate function predicate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("contains(\"n0\".\"service_name\", 'api') = true"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("starts_with(\"n0\".\"service_name\", 'deploy') = true"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ends_with(\"n0\".\"service_name\", 'sync') = true"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "bucket": "billing"}),
            json!({"service": "deployments", "bucket": "other"}),
            json!({"service": "legacy-sync", "bucket": "sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_numeric_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         WHERE abs(service.risk - 1.0) < 0.11 \
         RETURN service.name AS service, \
                ceil(service.risk) AS risk_ceiling, \
                floor(service.risk) AS risk_floor, \
                round(service.risk, 1) AS risk_rounded, \
                round(service.risk) AS risk_nearest \
         ORDER BY round(service.risk, 1)",
    )
    .await
    .expect("numeric scalar function query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("abs((\"n0\".\"risk_score\" - 1.0)) < 0.11"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("round(\"n0\".\"risk_score\", 1) AS \"risk_rounded\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "risk_ceiling": 1.0,
            "risk_floor": 0.0,
            "risk_rounded": 0.9,
            "risk_nearest": 1.0,
        })]
    );
}

#[tokio::test]
async fn cypher_more_numeric_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'experiments'}) \
         RETURN service.name AS service, \
                sqrt(service.risk) AS risk_root, \
                sign(service.risk - 0.5) AS risk_sign, \
                ceiling(service.risk) AS risk_ceiling_alias, \
                exp(0.0) AS exp_zero, \
                ln(1.0) AS ln_one, \
                log10(100.0) AS log10_hundred",
    )
    .await
    .expect("additional numeric scalar function query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("sqrt(\"n0\".\"risk_score\") AS \"risk_root\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("signum((\"n0\".\"risk_score\" - 0.5)) AS \"risk_sign\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ceil(\"n0\".\"risk_score\") AS \"risk_ceiling_alias\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("ln(1.0) AS \"ln_one\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "experiments",
            "risk_root": 0.5,
            "risk_sign": -1.0,
            "risk_ceiling_alias": 1.0,
            "exp_zero": 1.0,
            "ln_one": 0.0,
            "log10_hundred": 2.0,
        })]
    );
}

#[tokio::test]
async fn cypher_is_nan_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'experiments'}) \
         WHERE isNaN(service.risk) = false \
         RETURN service.name AS service, \
                isNaN(service.risk) AS risk_is_nan \
         ORDER BY risk_is_nan, service",
    )
    .await
    .expect("isNaN scalar function query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("isnan(\"n0\".\"risk_score\") = false"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("isnan(\"n0\".\"risk_score\") AS \"risk_is_nan\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "experiments",
            "risk_is_nan": false,
        })]
    );
}

#[tokio::test]
async fn cypher_is_nan_non_numeric_operand_rejects_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'experiments'}) \
         RETURN isNaN(service.name) AS invalid",
    )
    .await
    .expect_err("isNaN over a string should fail before SQL execution");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("isNaN"), "{error:?}");
}

#[tokio::test]
async fn cypher_trigonometric_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'experiments'}) \
         WHERE sin(service.risk) >= 0 AND cot(1.0) > 0 \
         RETURN service.name AS service, \
                sin(0.0) AS zero_sin, \
                cos(0.0) AS one_cos, \
                tan(0.0) AS zero_tan, \
                asin(0.0) AS zero_asin, \
                acos(1.0) AS zero_acos, \
                atan(0.0) AS zero_atan, \
                atan2(0.0, 1.0) AS zero_atan2, \
                degrees(0.0) AS zero_degrees, \
                radians(0.0) AS zero_radians",
    )
    .await
    .expect("trigonometric scalar function query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("sin(\"n0\".\"risk_score\") >= 0"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("cot(1.0) > 0"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("atan2(0.0, 1.0)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "experiments",
            "zero_sin": 0.0,
            "one_cos": 1.0,
            "zero_tan": 0.0,
            "zero_asin": 0.0,
            "zero_acos": 0.0,
            "zero_atan": 0.0,
            "zero_atan2": 0.0,
            "zero_degrees": 0.0,
            "zero_radians": 0.0,
        })]
    );
}

#[tokio::test]
async fn cypher_math_constant_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'experiments'}) \
         WHERE service.risk < pi() \
         RETURN service.name AS service, pi() AS pi_value, e() AS e_value",
    )
    .await
    .expect("math constant scalar function query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("3.141592653589793 AS \"pi_value\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("2.718281828459045 AS \"e_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "experiments",
            "pi_value": std::f64::consts::PI,
            "e_value": std::f64::consts::E,
        })]
    );
}

#[tokio::test]
async fn cypher_haversin_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'experiments'}) \
         WHERE haversin(service.risk) < 0.02 \
         RETURN service.name AS service, haversin(0.0) AS zero_haversin",
    )
    .await
    .expect("haversin scalar function query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("((1 - cos(\"n0\".\"risk_score\")) / 2) < 0.02"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("((1 - cos(0.0)) / 2) AS \"zero_haversin\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "experiments",
            "zero_haversin": 0.0,
        })]
    );
}

#[tokio::test]
async fn cypher_unary_negation_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         WHERE -service.risk < -0.8 \
         RETURN service.name AS service, \
                -service.risk AS inverse_risk, \
                -(service.risk * 10) AS inverse_points \
         ORDER BY -service.risk",
    )
    .await
    .expect("unary negation scalar query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("-(\"n0\".\"risk_score\") < -0.8"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "inverse_risk": -0.9,
            "inverse_points": -9.0,
        })]
    );
}

#[tokio::test]
async fn cypher_scalar_null_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE coalesce(service.tier, null) IS NULL \
         RETURN service.name AS service",
    )
    .await
    .expect("scalar null predicate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(\"n0\".\"tier\", NULL) IS NULL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_literal_list_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_tiers".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("prod".to_string()),
            GraphLiteral::String("dev".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'billing-api'}) \
         RETURN service.name AS service, ['prod', 'critical'] AS tags, $selected_tiers AS selected_tiers",
        &parameters,
    )
    .await
    .expect("literal list projection query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('prod', 'critical') AS \"tags\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "tags": ["prod", "critical"],
            "selected_tiers": ["prod", "dev"],
        })]
    );
}

#[tokio::test]
async fn cypher_parameters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([
        (
            "tier".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::String("prod".to_string())),
        ),
        (
            "services".to_string(),
            GraphCypherParameterValue::List(vec![
                GraphLiteral::String("billing-api".to_string()),
                GraphLiteral::String("deployments".to_string()),
            ]),
        ),
        (
            "limit".to_string(),
            GraphCypherParameterValue::Literal(GraphLiteral::Integer(1)),
        ),
    ]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {tier: $tier}) \
         WHERE service.name IN $services \
         RETURN service.name AS service \
         ORDER BY service \
         LIMIT $limit",
        &parameters,
    )
    .await
    .expect("parameterized Cypher query should execute");

    assert!(
        execution.translated_sql().contains("'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        !execution.translated_sql().contains('$'),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_float_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.risk >= 0.75 AND service.risk IN [0.9, 0.95] \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher float predicate query should execute");

    assert!(
        execution.translated_sql().contains("0.75"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_boolean_scalar_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                service.risk >= 0.8 AS high_risk, \
                service.tier IS NULL AS missing_tier, \
                service.name =~ '^billing.*' AS billing_service \
         ORDER BY high_risk DESC, service",
    )
    .await
    .expect("Cypher boolean scalar projection query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"risk_score\" >= 0.8 AS \"high_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("regexp_like("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "high_risk": true, "missing_tier": false, "billing_service": true}),
            json!({"service": "legacy-sync", "high_risk": true, "missing_tier": true, "billing_service": false}),
            json!({"service": "deployments", "high_risk": false, "missing_tier": false, "billing_service": false}),
            json!({"service": "experiments", "high_risk": false, "missing_tier": false, "billing_service": false}),
        ]
    );
}

#[tokio::test]
async fn cypher_string_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name STARTS WITH 'bill' \
            OR service.name ENDS WITH 'sync' \
            OR service.name CONTAINS 'ploy' \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher string predicate query should execute");

    assert!(
        execution.translated_sql().contains(" LIKE "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_dynamic_string_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name STARTS WITH left(service.name, 4) \
            AND service.name ENDS WITH right(service.name, 3) \
            AND service.name CONTAINS substring(service.name, 1, 3) \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher dynamic string predicate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("starts_with(\"n0\".\"service_name\", left(\"n0\".\"service_name\", 4))"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ends_with(\"n0\".\"service_name\", right(\"n0\".\"service_name\", 3))"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_regex_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name =~ '^(billing|deploy).*' \
            OR service.name =~ '.*sync$' \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher regex predicate query should execute");

    assert!(
        execution.translated_sql().contains("regexp_like("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_chained_comparisons_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 10 <= service.id < 30 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher chained comparison query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_skip_limit_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "limit".to_string(),
        GraphCypherParameterValue::Literal(GraphLiteral::Integer(2)),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service \
         ORDER BY service \
         SKIP (1 + 0) LIMIT coalesce($limit, 10)",
        &parameters,
    )
    .await
    .expect("Cypher query with SKIP should execute");

    assert!(
        execution.translated_sql().ends_with(" LIMIT 2 OFFSET 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "deployments"}),
            json!({"service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_distinct_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         RETURN DISTINCT service.tier AS tier \
         ORDER BY tier",
    )
    .await
    .expect("Cypher query with DISTINCT should execute");

    assert!(
        execution.translated_sql().starts_with("SELECT DISTINCT "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tier": "dev"}), json!({"tier": "prod"})]
    );
}

#[tokio::test]
async fn explain_cypher_preserves_translated_sql_and_datafusion_plan() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let graph_plan = CoralQuery::explain_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         RETURN person.name AS owner, service.name AS service \
         LIMIT 5",
    )
    .await
    .expect("Cypher query should explain");

    assert!(
        graph_plan
            .translated_sql()
            .contains("SELECT \"n0\".\"full_name\" AS \"owner\""),
        "{}",
        graph_plan.translated_sql()
    );
    assert!(
        graph_plan.plan().optimized_logical_plan().contains("ops"),
        "{}",
        graph_plan.plan().optimized_logical_plan()
    );
}

#[tokio::test]
async fn execute_cypher_rejects_writes_before_runtime_execution() {
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[],
        test_runtime(),
        &graph,
        "CREATE (service:Service) RETURN service.name",
    )
    .await
    .expect_err("write query should be rejected");

    assert!(
        error.to_string().contains("UNSUPPORTED_CYPHER"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn execute_graph_plan_validates_declaration_against_runtime_catalog() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(
        r"
version: 1
name: missing_table
nodes:
  - label: Service
    table: { schema: ops, name: missing_services }
    key: id
    properties:
      name: service_name
relationships: []
",
    )
    .expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![NodePattern {
            variable: "service".to_string(),
            label: "Service".to_string(),
        }],
        relationships: Vec::new(),
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![GraphProjection::Property {
            property: GraphPropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let error = CoralQuery::execute_graph_plan(&[source], test_runtime(), &graph, &plan)
        .await
        .expect_err("missing mapped table should fail before SQL planning");

    assert!(
        error.to_string().contains("MAPPED_TABLE_NOT_FOUND"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn explain_cypher_validates_declaration_columns_against_runtime_catalog() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(
        r"
version: 1
name: missing_column
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: missing_service_name
relationships: []
",
    )
    .expect("graph should parse");

    let error = CoralQuery::explain_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) RETURN service.name AS service",
    )
    .await
    .expect_err("missing mapped column should fail before SQL planning");

    assert!(
        error.to_string().contains("MAPPED_COLUMN_NOT_FOUND"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn virtual_graph_declaration_validates_against_synthetic_catalog() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let catalog = CoralQuery::list_catalog(&[source], test_runtime(), Some("ops"))
        .await
        .expect("catalog should load");
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    graph
        .validate_against_catalog(&catalog)
        .expect("synthetic catalog should satisfy graph declaration");
}

#[tokio::test]
async fn virtual_graph_count_projection_executes_against_synthetic_file_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let plan = GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: GraphDirection::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![GraphProjection::CountAll {
            alias: "ownership_count".to_string(),
        }],
        predicates: Vec::new(),
        predicate: None,
        post_projection_predicate: None,
        order_by: Vec::new(),
        skip: None,
        limit: None,
    };

    let rows = execution_to_rows(
        CoralQuery::execute_graph_plan(&[source], test_runtime(), &graph, &plan)
            .await
            .expect("translated count SQL should execute")
            .execution(),
    );

    assert_eq!(rows, vec![json!({"ownership_count": 3})]);
}

#[tokio::test]
async fn cypher_grouped_count_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.tier AS tier, count(*) AS services \
         ORDER BY services DESC, tier",
    )
    .await
    .expect("grouped count Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" GROUP BY "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod", "services": 2}),
            json!({"tier": "dev", "services": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_order_by_aggregate_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.tier AS tier, count(*) AS services \
         ORDER BY count(*) DESC, service.tier",
    )
    .await
    .expect("aggregate expression ordering should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"services\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod", "services": 2}),
            json!({"tier": "dev", "services": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_hidden_aggregate_order_by_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.tier AS tier \
         ORDER BY count(*) DESC, avg(service.risk), tier",
    )
    .await
    .expect("hidden aggregate order expressions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY COUNT(*) DESC, AVG(\"n0\".\"risk_score\") ASC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tier": "prod"}), json!({"tier": "dev"})]
    );
}

#[tokio::test]
async fn cypher_terminal_with_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH service.tier AS tier, count(service) AS services, avg(service.risk) AS average_risk \
         RETURN tier, services, average_risk \
         ORDER BY services DESC, tier",
    )
    .await
    .expect("terminal WITH projection Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" GROUP BY "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod", "services": 2, "average_risk": 0.7}),
            json!({"tier": "dev", "services": 1, "average_risk": 0.25}),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_final_return_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH service.tier AS tier, count(service) AS services \
         RETURN tier AS service_tier, services AS total_services \
         ORDER BY total_services DESC, service_tier",
    )
    .await
    .expect("terminal WITH final aliases should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"n0\".\"id\") AS \"total_services\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service_tier": "prod", "total_services": 2}),
            json!({"service_tier": "dev", "total_services": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_reordered_return_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH service.tier AS tier, count(service) AS services \
         RETURN services AS service_count, tier AS service_tier \
         ORDER BY service_count DESC, service_tier",
    )
    .await
    .expect("terminal WITH reordered aliases should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"n0\".\"id\") AS \"service_count\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service_count": 2, "service_tier": "prod"}),
            json!({"service_count": 1, "service_tier": "dev"}),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_return_star_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH service.tier AS tier, count(service) AS services \
         RETURN * \
         ORDER BY services DESC, tier",
    )
    .await
    .expect("terminal WITH RETURN * should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"n0\".\"id\") AS \"services\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod", "services": 2}),
            json!({"tier": "dev", "services": 1}),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_star_explicit_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH *, service.name AS name, service.tier AS tier \
         RETURN tier AS service_tier, name AS service_name \
         ORDER BY service_name",
    )
    .await
    .expect("terminal WITH * explicit projection aliases should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"service_name\" AS \"service_name\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service_tier": "prod", "service_name": "billing-api"}),
            json!({"service_tier": "prod", "service_name": "deployments"}),
            json!({"service_tier": "dev", "service_name": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_star_return_star_expands_graph_variables_and_aliases() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.name = 'billing-api' \
         WITH *, service.tier AS tier_copy \
         RETURN * \
         ORDER BY tier_copy",
    )
    .await
    .expect("terminal WITH * RETURN * should expand graph variables and scalar aliases");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"id\" AS \"service.__id\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" AS \"tier_copy\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service.__id": 10,
            "service.__labels": ["Service"],
            "service.active": true,
            "service.id": 10,
            "service.name": "billing-api",
            "service.risk": 0.9,
            "service.team": "platform",
            "service.tier": "prod",
            "tier_copy": "prod"
        })]
    );
}

#[tokio::test]
async fn cypher_terminal_with_scalar_where_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         WITH person.name AS owner, service.name AS service \
         WHERE owner STARTS WITH 'Ada' \
         RETURN owner, service",
    )
    .await
    .expect("terminal WITH scalar WHERE should execute");

    assert!(
        execution.translated_sql().contains(" LIKE 'Ada%'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owner": "Ada Lovelace", "service": "billing-api"})]
    );
}

#[tokio::test]
async fn cypher_terminal_with_bare_boolean_alias_where_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service.name AS service, contains(service.name, 'api') AS has_api \
         WHERE has_api \
         RETURN service, has_api",
    )
    .await
    .expect("terminal WITH bare boolean alias WHERE should execute");

    assert!(
        execution
            .translated_sql()
            .contains("contains(\"n0\".\"service_name\", 'api') = true"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "billing-api", "has_api": true})]
    );
}

#[tokio::test]
async fn cypher_terminal_with_bare_non_boolean_alias_where_rejects_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service.name AS service \
         WHERE service \
         RETURN service",
    )
    .await
    .expect_err("terminal WITH bare string alias WHERE should reject before SQL execution");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("boolean"), "{error:?}");
}

#[tokio::test]
async fn cypher_terminal_with_aggregate_where_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH service.tier AS tier, count(service) AS services \
         WHERE services > 1 \
         RETURN tier, services",
    )
    .await
    .expect("terminal WITH aggregate WHERE should execute");

    assert!(
        execution.translated_sql().contains(" HAVING COUNT("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tier": "prod", "services": 2})]
    );
}

#[tokio::test]
async fn cypher_terminal_with_modifiers_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         WITH service.tier AS tier, count(service) AS services \
         ORDER BY services DESC, tier \
         LIMIT 1 \
         RETURN tier, services",
    )
    .await
    .expect("terminal WITH modifiers should execute");

    assert!(
        execution.translated_sql().contains(" LIMIT 1"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tier": "prod", "services": 2})]
    );
}

#[tokio::test]
async fn cypher_terminal_with_graph_variable_modifiers_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH service AS s \
         ORDER BY s.risk DESC \
         SKIP 1 \
         LIMIT 2 \
         RETURN s.name AS service, s.risk AS risk",
    )
    .await
    .expect("terminal WITH graph variable modifiers should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"n0\".\"risk_score\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "risk": 0.9}),
            json!({"service": "deployments", "risk": 0.5}),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_distinct_graph_variable_return_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (:Service)-[:DEPENDS_ON]->(target:Service) \
         WITH DISTINCT target AS t \
         ORDER BY t.name \
         RETURN t",
    )
    .await
    .expect("terminal WITH DISTINCT graph variable return should execute");

    assert!(
        execution.translated_sql().starts_with("SELECT DISTINCT "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "t.__id": 20,
                "t.__labels": ["Service"],
                "t.active": true,
                "t.id": 20,
                "t.name": "deployments",
                "t.risk": 0.5,
                "t.team": "infra",
                "t.tier": "prod"
            }),
            json!({
                "t.__id": 30,
                "t.__labels": ["Service"],
                "t.active": false,
                "t.id": 30,
                "t.name": "experiments",
                "t.risk": 0.25,
                "t.team": "analytics",
                "t.tier": "dev"
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_terminal_with_star_modifiers_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WITH * \
         ORDER BY service.risk DESC \
         LIMIT 1 \
         RETURN service.name AS service",
    )
    .await
    .expect("terminal WITH * modifiers should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_count_property_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN count(service.tier) AS tiered_services, count(DISTINCT service.tier) AS tiers",
    )
    .await
    .expect("count property Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(DISTINCT \"n0\".\"tier\") AS \"tiers\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"tiered_services": 3, "tiers": 2})]
    );
}

#[tokio::test]
async fn cypher_collect_property_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         RETURN person.team AS team, collect(service.name) AS services \
         ORDER BY team",
    )
    .await
    .expect("collect property Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(\"n1\".\"service_name\") FILTER (WHERE (\"n1\".\"service_name\") IS NOT NULL), make_array()) AS \"services\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics", "services": ["experiments"]}),
            json!({"team": "infra", "services": ["deployments"]}),
            json!({"team": "platform", "services": ["billing-api"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_collect_property_projection_drops_null_values() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) RETURN collect(service.tier) AS tiers",
    )
    .await
    .expect("collect property Cypher query should execute");

    let mut rows = execution_to_rows(execution.execution());
    sort_string_array_field(
        rows.get_mut(0)
            .expect("collect query should return one row"),
        "tiers",
    );
    assert_eq!(rows, vec![json!({"tiers": ["dev", "prod", "prod"]})]);
}

#[tokio::test]
async fn cypher_aggregate_expression_targets_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN collect(coalesce(service.tier, 'unknown')) AS tiers, \
                count(coalesce(service.tier, 'unknown')) AS tier_count, \
                sum(service.risk + 1) AS adjusted_risk, \
                collect(({tier: service.tier}).tier) AS selected_tiers, \
                sum(({risk: service.risk + 1}).risk) AS selected_adjusted_risk, \
                count(({kind: 'service'}).kind) AS literal_kind_count",
    )
    .await
    .expect("aggregate expression target Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ARRAY_AGG(COALESCE(\"n0\".\"tier\", 'unknown'))"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("SUM((\"n0\".\"risk_score\" + 1)) AS \"adjusted_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("ARRAY_AGG(\"n0\".\"tier\") FILTER (WHERE (\"n0\".\"tier\") IS NOT NULL)"),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("aggregate expression target query should return one row");
    sort_string_array_field(row, "tiers");
    sort_string_array_field(row, "selected_tiers");
    assert_eq!(row["tiers"], json!(["dev", "prod", "prod", "unknown"]));
    assert_eq!(row["tier_count"], json!(4));
    assert_close(row["adjusted_risk"].as_f64().unwrap(), 6.6);
    assert_eq!(row["selected_tiers"], json!(["dev", "prod", "prod"]));
    assert_close(row["selected_adjusted_risk"].as_f64().unwrap(), 6.6);
    assert_eq!(row["literal_kind_count"], json!(4));
}

#[tokio::test]
async fn cypher_predicate_aggregate_targets_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN collect(DISTINCT service.risk > 0.8) AS high_risk_flags, \
                count(service.tier IS NULL) AS tier_null_checks",
    )
    .await
    .expect("predicate aggregate target Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ARRAY_AGG(DISTINCT \"n0\".\"risk_score\" > 0.8)"),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("predicate aggregate target query should return one row");
    sort_bool_array_field(row, "high_risk_flags");
    assert_eq!(row["high_risk_flags"], json!([false, true]));
    assert_eq!(row["tier_null_checks"], json!(4));
}

#[tokio::test]
async fn cypher_collect_graph_variable_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN collect(service) AS service_ids, collect(DISTINCT service) AS distinct_service_ids",
    )
    .await
    .expect("collect graph variable Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(\"n0\".\"id\") FILTER (WHERE (\"n0\".\"id\") IS NOT NULL), make_array()) AS \"service_ids\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(DISTINCT \"n0\".\"id\") FILTER (WHERE (\"n0\".\"id\") IS NOT NULL), make_array()) AS \"distinct_service_ids\""),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("collect query should return one row");
    sort_i64_array_field(row, "service_ids");
    sort_i64_array_field(row, "distinct_service_ids");
    assert_eq!(
        rows,
        vec![json!({
            "service_ids": [10, 20, 30, 40],
            "distinct_service_ids": [10, 20, 30, 40]
        })]
    );
}

#[tokio::test]
async fn cypher_collect_keyed_relationship_variable_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (:Person)-[owns:OWNS]->(:Service) \
         RETURN collect(owns) AS ownership_ids, collect(DISTINCT owns) AS distinct_ownership_ids",
    )
    .await
    .expect("collect keyed relationship Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(\"r0\".\"ownership_id\") FILTER (WHERE (\"r0\".\"ownership_id\") IS NOT NULL), make_array()) AS \"ownership_ids\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COALESCE(ARRAY_AGG(DISTINCT \"r0\".\"ownership_id\") FILTER (WHERE (\"r0\".\"ownership_id\") IS NOT NULL), make_array()) AS \"distinct_ownership_ids\""),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("collect query should return one row");
    sort_i64_array_field(row, "ownership_ids");
    sort_i64_array_field(row, "distinct_ownership_ids");
    assert_eq!(
        rows,
        vec![json!({
            "ownership_ids": [100, 200, 300],
            "distinct_ownership_ids": [100, 200, 300]
        })]
    );
}

#[tokio::test]
async fn cypher_collect_optional_endpoint_variable_drops_unmatched_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(:Service) \
         RETURN service.name AS service, collect(endNode(dependency)) AS dependency_ids \
         ORDER BY service",
    )
    .await
    .expect("collect optional endpoint Cypher query should execute");

    assert!(
        execution.translated_sql().contains(
            "COALESCE(ARRAY_AGG(CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n1\".\"id\" END) FILTER"
        ),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_i64_array_field(row, "dependency_ids");
    }
    assert_eq!(
        rows,
        vec![
            json!({"service": "billing-api", "dependency_ids": [20, 30]}),
            json!({"service": "deployments", "dependency_ids": [30]}),
            json!({"service": "experiments", "dependency_ids": []}),
            json!({"service": "legacy-sync", "dependency_ids": []}),
        ]
    );
}

#[tokio::test]
async fn cypher_numeric_aggregate_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.tier AS tier, \
                sum(service.risk) AS total_risk, \
                avg(service.risk) AS average_risk, \
                min(service.risk) AS lowest_risk, \
                min(DISTINCT service.risk) AS distinct_lowest_risk, \
                max(service.risk) AS highest_risk, \
                max(DISTINCT service.risk) AS distinct_highest_risk \
         ORDER BY average_risk DESC, tier",
    )
    .await
    .expect("numeric aggregate Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SUM(\"n0\".\"risk_score\") AS \"total_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("MIN(DISTINCT \"n0\".\"risk_score\") AS \"distinct_lowest_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("MAX(DISTINCT \"n0\".\"risk_score\") AS \"distinct_highest_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "tier": "prod",
                "total_risk": 1.4,
                "average_risk": 0.7,
                "lowest_risk": 0.5,
                "distinct_lowest_risk": 0.5,
                "highest_risk": 0.9,
                "distinct_highest_risk": 0.9
            }),
            json!({
                "tier": "dev",
                "total_risk": 0.25,
                "average_risk": 0.25,
                "lowest_risk": 0.25,
                "distinct_lowest_risk": 0.25,
                "highest_risk": 0.25,
                "distinct_highest_risk": 0.25
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_statistical_aggregate_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'prod' \
         RETURN stDev(service.risk) AS sample_risk, \
                stDevP(service.risk) AS population_risk, \
                sum(DISTINCT service.risk) AS distinct_total_risk, \
                avg(DISTINCT service.risk) AS distinct_average_risk",
    )
    .await
    .expect("statistical aggregate Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("STDDEV_SAMP(\"n0\".\"risk_score\") AS \"sample_risk\""),
        "{}",
        execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT STDDEV_SAMP(risk_score) AS \"sample_risk\", \
                STDDEV_POP(risk_score) AS \"population_risk\", \
                SUM(DISTINCT risk_score) AS \"distinct_total_risk\", \
                AVG(DISTINCT risk_score) AS \"distinct_average_risk\" \
         FROM ops.services \
         WHERE tier = 'prod'",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows.len(), 1);
    assert_eq!(sql_rows.len(), 1);
    let graph_row = graph_rows
        .first()
        .expect("graph aggregate query should return one row");
    let sql_row = sql_rows
        .first()
        .expect("SQL aggregate query should return one row");
    assert_close(
        graph_row["sample_risk"].as_f64().unwrap(),
        sql_row["sample_risk"].as_f64().unwrap(),
    );
    assert_close(
        graph_row["population_risk"].as_f64().unwrap(),
        sql_row["population_risk"].as_f64().unwrap(),
    );

    assert_close(
        graph_row["sample_risk"].as_f64().unwrap(),
        0.282_842_712_474_619,
    );
    assert_close(graph_row["population_risk"].as_f64().unwrap(), 0.2);
    assert_close(graph_row["distinct_total_risk"].as_f64().unwrap(), 1.4);
    assert_close(graph_row["distinct_average_risk"].as_f64().unwrap(), 0.7);
}

#[tokio::test]
async fn cypher_percentile_cont_aggregate_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'prod' \
         RETURN percentileCont(service.risk, 0.75) AS p75_risk",
    )
    .await
    .expect("percentileCont aggregate Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("PERCENTILE_CONT(\"n0\".\"risk_score\", 0.75) AS \"p75_risk\""),
        "{}",
        execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT PERCENTILE_CONT(risk_score, 0.75) AS \"p75_risk\" \
         FROM ops.services \
         WHERE tier = 'prod'",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows.len(), 1);
    assert_eq!(sql_rows.len(), 1);

    let graph_row = graph_rows
        .first()
        .expect("graph percentile query should return one row");
    let sql_row = sql_rows
        .first()
        .expect("SQL percentile query should return one row");
    assert_close(
        graph_row["p75_risk"].as_f64().unwrap(),
        sql_row["p75_risk"].as_f64().unwrap(),
    );
    assert_close(graph_row["p75_risk"].as_f64().unwrap(), 0.8);
}

#[tokio::test]
async fn datafusion_percentile_disc_probe_executes_with_windowed_position_sql() {
    let execution = CoralQuery::execute_sql(
        &[],
        test_runtime(),
        "WITH values_table AS ( \
             SELECT column1 AS x FROM (VALUES (10), (20), (30), (40)) \
         ) \
         SELECT \
             (SELECT sub.x FROM ( \
                 SELECT x, \
                        CAST(row_number() OVER (ORDER BY x) AS BIGINT) AS rn, \
                        COUNT(*) OVER () AS n \
                 FROM values_table \
                 WHERE x IS NOT NULL \
             ) AS sub \
             WHERE sub.rn = CASE \
                 WHEN CAST(ceil(0.75 * sub.n) AS BIGINT) < 1 THEN 1 \
                 ELSE CAST(ceil(0.75 * sub.n) AS BIGINT) \
             END LIMIT 1) AS p75, \
             (SELECT sub.x FROM ( \
                 SELECT x, \
                        CAST(row_number() OVER (ORDER BY x) AS BIGINT) AS rn, \
                        COUNT(*) OVER () AS n \
                 FROM values_table \
                 WHERE x IS NOT NULL \
             ) AS sub \
             WHERE sub.rn = CASE \
                 WHEN CAST(ceil(0.5 * sub.n) AS BIGINT) < 1 THEN 1 \
                 ELSE CAST(ceil(0.5 * sub.n) AS BIGINT) \
             END LIMIT 1) AS p50, \
             (SELECT sub.x FROM ( \
                 SELECT x, \
                        CAST(row_number() OVER (ORDER BY x) AS BIGINT) AS rn, \
                        COUNT(*) OVER () AS n \
                 FROM values_table \
                 WHERE x IS NOT NULL \
             ) AS sub \
             WHERE sub.rn = CASE \
                 WHEN CAST(ceil(0.0 * sub.n) AS BIGINT) < 1 THEN 1 \
                 ELSE CAST(ceil(0.0 * sub.n) AS BIGINT) \
             END LIMIT 1) AS p0, \
             (SELECT sub.x FROM ( \
                 SELECT x, \
                        CAST(row_number() OVER (ORDER BY x) AS BIGINT) AS rn, \
                        COUNT(*) OVER () AS n \
                 FROM values_table \
                 WHERE x IS NOT NULL \
             ) AS sub \
             WHERE sub.rn = CASE \
                 WHEN CAST(ceil(1.0 * sub.n) AS BIGINT) < 1 THEN 1 \
                 ELSE CAST(ceil(1.0 * sub.n) AS BIGINT) \
             END LIMIT 1) AS p100",
    )
    .await
    .expect("windowed percentile-disc SQL probe should execute");

    assert_eq!(
        execution_to_rows(&execution),
        vec![json!({"p75": 30, "p50": 20, "p0": 10, "p100": 40})]
    );
}

#[tokio::test]
async fn cypher_percentile_disc_aggregate_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN percentileDisc(service.risk, 0.75) AS p75_risk",
    )
    .await
    .expect("percentileDisc aggregate Cypher query should execute");

    assert!(
        execution.translated_sql().contains(
            "row_number() OVER (ORDER BY \"__coral_percentile_disc_0_n0\".\"risk_score\")"
        ),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("MAX(\"__coral_percentile_disc_0\".\"__coral_value\") AS \"p75_risk\""),
        "{}",
        execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT (SELECT sub.risk_score FROM ( \
             SELECT risk_score, \
                    CAST(row_number() OVER (ORDER BY risk_score) AS BIGINT) AS rn, \
                    COUNT(*) OVER () AS n \
             FROM ops.services \
             WHERE risk_score IS NOT NULL \
         ) AS sub \
         WHERE sub.rn = CASE \
             WHEN CAST(ceil(0.75 * sub.n) AS BIGINT) < 1 THEN 1 \
             ELSE CAST(ceil(0.75 * sub.n) AS BIGINT) \
         END LIMIT 1) AS p75_risk",
    )
    .await
    .expect("equivalent percentile-disc SQL should execute");

    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(graph_rows, vec![json!({"p75_risk": 0.9})]);
}

#[tokio::test]
async fn cypher_grouped_percentile_disc_aggregate_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.active AS active, \
                percentileDisc(service.risk, 0.5) AS median_disc_risk \
         ORDER BY active",
    )
    .await
    .expect("grouped percentileDisc Cypher query should execute");

    assert!(
        execution.translated_sql().contains(
            "((\"__coral_percentile_disc_0\".\"__coral_group_0\" = \"n0\".\"active\") OR (\"__coral_percentile_disc_0\".\"__coral_group_0\" IS NULL AND \"n0\".\"active\" IS NULL))"
        ),
        "{}",
        execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT s0.active AS active, \
                MAX(pdisc.value) AS median_disc_risk \
         FROM ops.services AS s0 \
         LEFT JOIN ( \
             SELECT rows.active_group AS active_group, \
                    MAX(CASE WHEN rows.rn = CASE \
                        WHEN CAST(ceil(0.5 * rows.n) AS BIGINT) < 1 THEN 1 \
                        ELSE CAST(ceil(0.5 * rows.n) AS BIGINT) \
                    END THEN rows.value ELSE NULL END) AS value \
             FROM ( \
                 SELECT s1.active AS active_group, \
                        s1.risk_score AS value, \
                        CAST(row_number() OVER (PARTITION BY s1.active ORDER BY s1.risk_score) AS BIGINT) AS rn, \
                        COUNT(*) OVER (PARTITION BY s1.active) AS n \
                 FROM ops.services AS s1 \
                 WHERE s1.risk_score IS NOT NULL \
             ) AS rows \
             GROUP BY rows.active_group \
         ) AS pdisc \
         ON ((pdisc.active_group = s0.active) OR (pdisc.active_group IS NULL AND s0.active IS NULL)) \
         GROUP BY s0.active \
         ORDER BY active",
    )
    .await
    .expect("equivalent grouped percentile-disc SQL should execute");

    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows, sql_rows);
    assert_eq!(
        graph_rows,
        vec![
            json!({"active": false, "median_disc_risk": 0.25}),
            json!({"active": true, "median_disc_risk": 0.5}),
        ]
    );
}

#[tokio::test]
async fn cypher_gql_aggregate_function_aliases_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN collect_list(service.tier) AS tiers, \
                stdev_samp(service.risk) AS sample_risk, \
                stdev_pop(service.risk) AS population_risk",
    )
    .await
    .expect("GQL aggregate aliases should execute");

    assert!(
        execution
            .translated_sql()
            .contains("STDDEV_SAMP(\"n0\".\"risk_score\") AS \"sample_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "COALESCE(ARRAY_AGG(\"n0\".\"tier\") FILTER (WHERE (\"n0\".\"tier\") IS NOT NULL), make_array()) AS \"tiers\""
        ),
        "{}",
        execution.translated_sql()
    );

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("aggregate alias query should return one row");
    sort_string_array_field(row, "tiers");
    assert_eq!(row.get("tiers"), Some(&json!(["dev", "prod", "prod"])));
    assert_close(
        row.get("sample_risk")
            .and_then(Value::as_f64)
            .expect("sample_risk should be a float"),
        0.327_871_926_215_100_03,
    );
    assert_close(
        row.get("population_risk")
            .and_then(Value::as_f64)
            .expect("population_risk should be a float"),
        0.267_706_306_736_816_83,
    );
}

#[tokio::test]
async fn cypher_distinct_standard_deviation_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN stDev(DISTINCT service.risk) AS sample_risk, \
                stDevP(DISTINCT service.risk) AS population_risk",
    )
    .await
    .expect("distinct standard-deviation aggregate should execute");

    assert!(
        execution
            .translated_sql()
            .contains("SQRT(VAR_SAMP(DISTINCT \"n0\".\"risk_score\")) AS \"sample_risk\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("SQRT(VAR_POP(DISTINCT \"n0\".\"risk_score\")) AS \"population_risk\""),
        "{}",
        execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT SQRT(VAR_SAMP(DISTINCT risk_score)) AS \"sample_risk\", \
                SQRT(VAR_POP(DISTINCT risk_score)) AS \"population_risk\" \
         FROM ops.services",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows.len(), 1);
    assert_eq!(sql_rows.len(), 1);

    let row = graph_rows
        .first()
        .expect("aggregate query should return one row");
    let sql_row = sql_rows
        .first()
        .expect("equivalent aggregate query should return one row");
    assert_close(
        row["sample_risk"].as_f64().unwrap(),
        sql_row["sample_risk"].as_f64().unwrap(),
    );
    assert_close(
        row["population_risk"].as_f64().unwrap(),
        sql_row["population_risk"].as_f64().unwrap(),
    );
    assert_close(row["sample_risk"].as_f64().unwrap(), 0.334_165_627_596_057);
    assert_close(
        row["population_risk"].as_f64().unwrap(),
        0.289_395_922_569_755_6,
    );
}

#[tokio::test]
async fn cypher_median_aggregate_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier = 'prod' \
         RETURN median(service.risk) AS median_risk, \
                median(DISTINCT service.risk) AS distinct_median_risk",
    )
    .await
    .expect("median aggregate Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("MEDIAN(CAST(\"n0\".\"risk_score\" AS DOUBLE)) AS \"median_risk\""),
        "{}",
        execution.translated_sql()
    );

    let sql_execution = CoralQuery::execute_sql(
        std::slice::from_ref(&source),
        test_runtime(),
        "SELECT MEDIAN(risk_score) AS \"median_risk\", \
                MEDIAN(DISTINCT risk_score) AS \"distinct_median_risk\" \
         FROM ops.services \
         WHERE tier = 'prod'",
    )
    .await
    .expect("equivalent SQL should execute");

    let graph_rows = execution_to_rows(execution.execution());
    let sql_rows = execution_to_rows(&sql_execution);
    assert_eq!(graph_rows, sql_rows);

    let row = graph_rows
        .first()
        .expect("aggregate query should return one row");
    assert_close(row["median_risk"].as_f64().unwrap(), 0.7);
    assert_close(row["distinct_median_risk"].as_f64().unwrap(), 0.7);
}

#[tokio::test]
async fn cypher_count_node_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON]->(target:Service) \
         RETURN count(target) AS dependency_mentions, count(DISTINCT target) AS unique_targets",
    )
    .await
    .expect("count node Cypher query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(DISTINCT \"n1\".\"id\") AS \"unique_targets\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"dependency_mentions": 3, "unique_targets": 2})]
    );
}

#[tokio::test]
async fn cypher_count_keyed_relationship_variables_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN count(owns) AS ownerships",
    )
    .await
    .expect("counting a keyed relationship variable should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"r0\".\"ownership_id\") AS \"ownerships\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"ownerships": 3})]
    );
}

#[tokio::test]
async fn cypher_id_and_type_projections_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN id(person) AS person_id, id(owns) AS ownership_id, type(owns) AS relationship_type \
         ORDER BY ownership_id \
         LIMIT 2",
    )
    .await
    .expect("id() and type() projections should execute");

    assert!(
        execution
            .translated_sql()
            .contains(
                "CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END AS \"relationship_type\""
            ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"person_id": 1, "ownership_id": 100, "relationship_type": "OWNS"}),
            json!({"person_id": 2, "ownership_id": 200, "relationship_type": "OWNS"}),
        ]
    );
}

#[tokio::test]
async fn cypher_relationship_type_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN service.name AS service, \
                coalesce(type(owns), 'missing') AS ownership_type, \
                CASE WHEN type(owns) = 'OWNS' THEN type(owns) ELSE 'other' END AS type_bucket \
         ORDER BY coalesce(type(owns), 'missing'), service",
    )
    .await
    .expect("relationship type scalar expression query should execute");

    assert!(
        execution.translated_sql().contains(
            "COALESCE(CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END, 'missing') AS \"ownership_type\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "CASE WHEN TRUE THEN CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END ELSE 'other' END AS \"type_bucket\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "ownership_type": "OWNS", "type_bucket": "OWNS"}),
            json!({"service": "deployments", "ownership_type": "OWNS", "type_bucket": "OWNS"}),
            json!({"service": "experiments", "ownership_type": "OWNS", "type_bucket": "OWNS"}),
        ]
    );
}

#[tokio::test]
async fn cypher_relationship_type_scalar_expressions_preserve_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
         RETURN service.name AS service, coalesce(type(owns), 'unowned') AS ownership_type \
         ORDER BY service",
    )
    .await
    .expect("optional relationship type scalar expression query should execute");

    assert!(
        execution.translated_sql().contains(
            "COALESCE(CASE WHEN \"r0\".\"ownership_id\" IS NULL THEN NULL ELSE 'OWNS' END, 'unowned') AS \"ownership_type\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "ownership_type": "OWNS"}),
            json!({"service": "deployments", "ownership_type": "OWNS"}),
            json!({"service": "experiments", "ownership_type": "OWNS"}),
            json!({"service": "legacy-sync", "ownership_type": "unowned"}),
        ]
    );
}

#[tokio::test]
async fn cypher_relationship_endpoint_properties_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         WHERE startNode(dependency).tier = 'prod' \
           AND endNode(dependency).name = 'experiments' \
         RETURN startNode(dependency).name AS source, \
                endNode(dependency).name AS target, \
                lower(endNode(dependency).tier) AS target_tier \
         ORDER BY startNode(dependency).name",
    )
    .await
    .expect("relationship endpoint property query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"n0\".\"tier\" = 'prod'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"n1\".\"service_name\" = 'experiments'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "experiments", "target_tier": "dev"}),
            json!({"source": "deployments", "target": "experiments", "target_tier": "dev"}),
        ]
    );
}

#[tokio::test]
async fn cypher_reversed_relationship_endpoint_properties_keep_mapping_orientation() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (target:Service)<-[dependency:DEPENDS_ON]-(source:Service) \
         RETURN startNode(dependency).name AS source, endNode(dependency).name AS target \
         ORDER BY source, target",
    )
    .await
    .expect("reversed relationship endpoint property query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"source": "billing-api", "target": "deployments"}),
            json!({"source": "billing-api", "target": "experiments"}),
            json!({"source": "deployments", "target": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_undirected_cross_label_endpoint_properties_use_mapping_orientation() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service)-[owns:OWNS]-(person:Person) \
         RETURN startNode(owns).name AS owner, endNode(owns).name AS service \
         ORDER BY owner, service",
    )
    .await
    .expect("undirected cross-label endpoint property query should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_endpoint_properties_use_mapping_orientation() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
         RETURN left.name AS matched_left, \
                right.name AS matched_right, \
                startNode(dependency).name AS source, \
                endNode(dependency).name AS target \
         ORDER BY source, target, matched_left",
    )
    .await
    .expect("same-label undirected endpoint property query should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"from_service_id\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"to_service_id\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "matched_left": "billing-api",
                "matched_right": "deployments",
                "source": "billing-api",
                "target": "deployments"
            }),
            json!({
                "matched_left": "deployments",
                "matched_right": "billing-api",
                "source": "billing-api",
                "target": "deployments"
            }),
            json!({
                "matched_left": "billing-api",
                "matched_right": "experiments",
                "source": "billing-api",
                "target": "experiments"
            }),
            json!({
                "matched_left": "experiments",
                "matched_right": "billing-api",
                "source": "billing-api",
                "target": "experiments"
            }),
            json!({
                "matched_left": "deployments",
                "matched_right": "experiments",
                "source": "deployments",
                "target": "experiments"
            }),
            json!({
                "matched_left": "experiments",
                "matched_right": "deployments",
                "source": "deployments",
                "target": "experiments"
            }),
        ]
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_endpoint_graph_values_use_mapping_orientation() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
         WHERE startNode(dependency).name = 'billing-api' \
           AND endNode(dependency).name = 'deployments' \
         RETURN startNode(dependency) AS source, endNode(dependency) AS target \
         ORDER BY left.name \
         LIMIT 1",
    )
    .await
    .expect("same-label undirected endpoint graph value return should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "source.__id": 10,
            "source.__labels": ["Service"],
            "source.active": true,
            "source.id": 10,
            "source.name": "billing-api",
            "source.risk": 0.9,
            "source.team": "platform",
            "source.tier": "prod",
            "target.__id": 20,
            "target.__labels": ["Service"],
            "target.active": true,
            "target.id": 20,
            "target.name": "deployments",
            "target.risk": 0.5,
            "target.team": "infra",
            "target.tier": "prod"
        })]
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_endpoint_properties_work_in_predicates_and_aggregates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
         WHERE startNode(dependency).name = 'billing-api' \
         RETURN count(*) AS matched_rows, \
                count(DISTINCT endNode(dependency).name) AS distinct_targets, \
                sum(endNode(dependency).risk) AS duplicated_target_risk",
    )
    .await
    .expect("same-label undirected endpoint property predicate and aggregate query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(DISTINCT CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("SUM(CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "matched_rows": 4,
            "distinct_targets": 2,
            "duplicated_target_risk": 1.5,
        })]
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_endpoint_properties_work_in_exists_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[dependency:DEPENDS_ON]-(target:Service) \
           WHERE startNode(dependency).name = service.name \
             AND endNode(dependency).tier = 'dev' \
             AND 'risk' IN keys(endNode(dependency)) \
         } \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("same-label undirected endpoint properties inside EXISTS should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"__coral_exists_r0\".\"from_service_id\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_endpoint_properties_work_in_count_subqueries() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE COUNT { \
           MATCH (service)-[dependency:DEPENDS_ON]-(target:Service) \
           WHERE startNode(dependency).name = service.name \
             AND endNode(dependency).tier = 'dev' \
         } > 0 \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("same-label undirected endpoint properties inside COUNT predicate should execute");

    assert!(
        execution.translated_sql().contains("EXISTS (SELECT 1 FROM"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api"}),
            json!({"service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_count_projections_use_precomputed_endpoint_groups() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { \
                  MATCH (service)-[dependency:DEPENDS_ON]-(target:Service) \
                  WHERE target.tier = 'dev' \
                } AS dev_outbound_dependencies \
         ORDER BY dev_outbound_dependencies DESC, service",
    )
    .await
    .expect("same-label undirected endpoint properties inside COUNT projection should execute");

    assert!(
        execution
            .translated_sql()
            .contains("LEFT JOIN (SELECT CASE"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("COUNT(*) AS \"__coral_value\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dev_outbound_dependencies": 1}),
            json!({"service": "deployments", "dev_outbound_dependencies": 1}),
            json!({"service": "experiments", "dev_outbound_dependencies": 0}),
            json!({"service": "legacy-sync", "dev_outbound_dependencies": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_correlated_count_projection_rejects_non_precomputable_endpoint_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                COUNT { \
                  MATCH (service)-[dependency:DEPENDS_ON]-(target:Service) \
                  WHERE startNode(dependency).name = service.name \
                    AND endNode(dependency).tier = 'dev' \
                } AS dev_outbound_dependencies",
    )
    .await
    .expect_err("non-precomputable correlated COUNT projection should fail before execution");

    assert!(
        error.to_string().contains(
            "correlated relationship COUNT subqueries in projections must be precomputable"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_same_label_undirected_endpoint_identity_and_metadata_use_mapping_orientation() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
         WHERE 'Service' IN labels(startNode(dependency)) \
           AND 'risk' IN keys(endNode(dependency)) \
         RETURN left.name AS matched_left, \
                right.name AS matched_right, \
                id(startNode(dependency)) AS source_id, \
                elementId(endNode(dependency)) AS target_element_id, \
                labels(startNode(dependency)) AS source_labels, \
                keys(endNode(dependency)) AS target_keys \
         ORDER BY source_id, target_element_id, matched_left",
    )
    .await
    .expect("same-label undirected endpoint identity and metadata query should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_string_array_field(row, "target_keys");
    }
    assert_eq!(
        rows,
        vec![
            json!({
                "matched_left": "billing-api",
                "matched_right": "deployments",
                "source_id": 10,
                "target_element_id": "20",
                "source_labels": ["Service"],
                "target_keys": ["active", "id", "name", "risk", "team", "tier"]
            }),
            json!({
                "matched_left": "deployments",
                "matched_right": "billing-api",
                "source_id": 10,
                "target_element_id": "20",
                "source_labels": ["Service"],
                "target_keys": ["active", "id", "name", "risk", "team", "tier"]
            }),
            json!({
                "matched_left": "billing-api",
                "matched_right": "experiments",
                "source_id": 10,
                "target_element_id": "30",
                "source_labels": ["Service"],
                "target_keys": ["active", "id", "name", "risk", "team", "tier"]
            }),
            json!({
                "matched_left": "experiments",
                "matched_right": "billing-api",
                "source_id": 10,
                "target_element_id": "30",
                "source_labels": ["Service"],
                "target_keys": ["active", "id", "name", "risk", "team", "tier"]
            }),
            json!({
                "matched_left": "deployments",
                "matched_right": "experiments",
                "source_id": 20,
                "target_element_id": "30",
                "source_labels": ["Service"],
                "target_keys": ["active", "id", "name", "risk", "team", "tier"]
            }),
            json!({
                "matched_left": "experiments",
                "matched_right": "deployments",
                "source_id": 20,
                "target_element_id": "30",
                "source_labels": ["Service"],
                "target_keys": ["active", "id", "name", "risk", "team", "tier"]
            }),
        ]
    );

    let aggregate_execution = CoralQuery::execute_cypher(
        &[build_source(ops_manifest(temp.path()))],
        test_runtime(),
        &graph,
        "MATCH (left:Service)-[dependency:DEPENDS_ON]-(right:Service) \
         RETURN count(DISTINCT startNode(dependency)) AS distinct_sources",
    )
    .await
    .expect("same-label undirected endpoint identity aggregate query should execute");

    assert_eq!(
        execution_to_rows(aggregate_execution.execution()),
        vec![json!({"distinct_sources": 2})]
    );
}

#[tokio::test]
async fn cypher_relationship_endpoint_identity_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         WHERE id(startNode(dependency)) = 10 \
           AND elementId(endNode(dependency)) = '30' \
           AND 'Service' IN labels(startNode(dependency)) \
         RETURN id(startNode(dependency)) AS source_id, \
                elementId(endNode(dependency)) AS target_element_id, \
                labels(startNode(dependency)) AS source_labels",
    )
    .await
    .expect("relationship endpoint identity function query should execute");

    assert!(
        execution.translated_sql().contains("\"n0\".\"id\" = 10"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n1\".\"id\" AS VARCHAR) = '30'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "source_id": 10,
            "target_element_id": "30",
            "source_labels": ["Service"]
        })]
    );

    let count_execution = CoralQuery::execute_cypher(
        &[build_source(ops_manifest(temp.path()))],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         RETURN count(startNode(dependency)) AS dependencies",
    )
    .await
    .expect("relationship endpoint identity aggregate should execute");

    assert_eq!(
        execution_to_rows(count_execution.execution()),
        vec![json!({"dependencies": 3})]
    );
}

#[tokio::test]
async fn cypher_identity_scalar_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                id(service) + 1 AS next_service_id, \
                toString(id(service)) AS service_id_text, \
                CASE WHEN service.active THEN id(service) ELSE 0 END AS active_service_id \
         ORDER BY id(service)",
    )
    .await
    .expect("identity scalar expression query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("(\"n0\".\"id\" + 1) AS \"next_service_id\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n0\".\"id\" AS VARCHAR) AS \"service_id_text\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "next_service_id": 11, "service_id_text": "10", "active_service_id": 10}),
            json!({"service": "deployments", "next_service_id": 21, "service_id_text": "20", "active_service_id": 20}),
            json!({"service": "experiments", "next_service_id": 31, "service_id_text": "30", "active_service_id": 0}),
            json!({"service": "legacy-sync", "next_service_id": 41, "service_id_text": "40", "active_service_id": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_catalog_typed_scalar_type_errors_reject_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN coalesce(id(service), 'unknown') AS service_id",
    )
    .await
    .expect_err("catalog-typed scalar type mismatch should fail before SQL execution");

    assert!(
        error.to_string().contains("INVALID_SCALAR_TYPE"),
        "{error:?}"
    );
    assert!(error.to_string().contains("coalesce"), "{error:?}");
}

#[tokio::test]
async fn cypher_catalog_typed_aggregate_target_errors_reject_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) RETURN sum(service.name) AS bad_sum",
    )
    .await
    .expect_err("catalog-typed aggregate target mismatch should fail before SQL execution");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(error.to_string().contains("numeric"), "{error:?}");
}

#[tokio::test]
async fn cypher_catalog_typed_aggregate_expression_errors_reject_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) RETURN sum(toString(service.risk)) AS bad_sum",
    )
    .await
    .expect_err("catalog-typed aggregate expression mismatch should fail before SQL execution");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(error.to_string().contains("numeric"), "{error:?}");
}

#[tokio::test]
async fn cypher_catalog_typed_predicate_aggregate_errors_reject_before_sql_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) RETURN sum(service.risk > 0.8) AS bad_sum",
    )
    .await
    .expect_err("numeric aggregate over predicate should fail before SQL execution");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
    );
    assert!(error.to_string().contains("numeric"), "{error:?}");
}

#[tokio::test]
async fn cypher_element_id_scalar_expressions_preserve_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[owns:OWNS]->(service) \
         RETURN service.name AS service, coalesce(elementId(owns), 'missing') AS ownership_element_id \
         ORDER BY service",
    )
    .await
    .expect("optional elementId scalar expression query should execute");

    assert!(
        execution.translated_sql().contains(
            "COALESCE(CAST(\"r0\".\"ownership_id\" AS VARCHAR), 'missing') AS \"ownership_element_id\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "ownership_element_id": "100"}),
            json!({"service": "deployments", "ownership_element_id": "200"}),
            json!({"service": "experiments", "ownership_element_id": "300"}),
            json!({"service": "legacy-sync", "ownership_element_id": "missing"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_relationship_endpoint_values_preserve_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         RETURN service.name AS service, \
                coalesce(startNode(dependency).name, 'missing') AS source, \
                endNode(dependency).name AS dependency, \
                coalesce(elementId(endNode(dependency)), 'missing') AS dependency_id, \
                CASE WHEN startNode(dependency) IS NULL THEN 'missing' ELSE 'present' END AS source_presence \
         ORDER BY service, coalesce(endNode(dependency).name, 'zzzz')",
    )
    .await
    .expect("optional endpoint values should execute");

    assert!(
        execution.translated_sql().contains(
            "CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n0\".\"service_name\" END"
        ),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n1\".\"service_name\" END"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "source": "billing-api", "dependency": "deployments", "dependency_id": "20", "source_presence": "present"}),
            json!({"service": "billing-api", "source": "billing-api", "dependency": "experiments", "dependency_id": "30", "source_presence": "present"}),
            json!({"service": "deployments", "source": "deployments", "dependency": "experiments", "dependency_id": "30", "source_presence": "present"}),
            json!({"service": "experiments", "source": "missing", "dependency_id": "missing", "source_presence": "missing"}),
            json!({"service": "legacy-sync", "source": "missing", "dependency_id": "missing", "source_presence": "missing"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_relationship_endpoint_aggregates_count_only_matched_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         RETURN service.name AS service, \
                count(endNode(dependency)) AS dependencies, \
                count(DISTINCT startNode(dependency)) AS distinct_sources \
         ORDER BY service",
    )
    .await
    .expect("optional endpoint aggregate values should execute");

    assert!(
        execution.translated_sql().contains(
            "COUNT(CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n1\".\"id\" END) AS \"dependencies\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "COUNT(DISTINCT CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n0\".\"id\" END) AS \"distinct_sources\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependencies": 2, "distinct_sources": 1}),
            json!({"service": "deployments", "dependencies": 1, "distinct_sources": 1}),
            json!({"service": "experiments", "dependencies": 0, "distinct_sources": 0}),
            json!({"service": "legacy-sync", "dependencies": 0, "distinct_sources": 0}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_relationship_endpoint_property_aggregates_ignore_unmatched_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         RETURN count(endNode(dependency).name) AS named_dependencies, \
                sum(endNode(dependency).risk) AS dependency_risk",
    )
    .await
    .expect("optional endpoint property aggregate values should execute");

    assert!(
        execution.translated_sql().contains(
            "COUNT(CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n1\".\"service_name\" END) AS \"named_dependencies\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains(
            "SUM(CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n1\".\"risk_score\" END) AS \"dependency_risk\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"named_dependencies": 3, "dependency_risk": 1.0})]
    );
}

#[tokio::test]
async fn cypher_optional_relationship_endpoint_collect_drops_unmatched_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         RETURN service.name AS service, collect(endNode(dependency).name) AS dependencies \
         ORDER BY service",
    )
    .await
    .expect("optional endpoint collect should execute");

    assert!(
        execution.translated_sql().contains(
            "COALESCE(ARRAY_AGG(CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE \"n1\".\"service_name\" END) FILTER"
        ),
        "{}",
        execution.translated_sql()
    );
    let mut rows = execution_to_rows(execution.execution());
    for row in &mut rows {
        sort_string_array_field(row, "dependencies");
    }
    assert_eq!(
        rows,
        vec![
            json!({"service": "billing-api", "dependencies": ["deployments", "experiments"]}),
            json!({"service": "deployments", "dependencies": ["experiments"]}),
            json!({"service": "experiments", "dependencies": []}),
            json!({"service": "legacy-sync", "dependencies": []}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_relationship_endpoint_metadata_preserves_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         RETURN service.name AS service, \
                labels(endNode(dependency)) AS dependency_labels, \
                keys(startNode(dependency)) AS source_keys \
         ORDER BY service, coalesce(endNode(dependency).name, 'zzzz')",
    )
    .await
    .expect("optional endpoint metadata should execute");

    assert!(
        execution.translated_sql().contains(
            "CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE CASE WHEN \"n1\".\"id\" IS NULL THEN NULL ELSE make_array('Service') END END"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_labels": ["Service"], "source_keys": ["active", "id", "name", "risk", "team", "tier"]}),
            json!({"service": "billing-api", "dependency_labels": ["Service"], "source_keys": ["active", "id", "name", "risk", "team", "tier"]}),
            json!({"service": "deployments", "dependency_labels": ["Service"], "source_keys": ["active", "id", "name", "risk", "team", "tier"]}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_is_empty_endpoint_metadata_preserves_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         RETURN service.name AS service, \
                isEmpty(labels(endNode(dependency))) AS dependency_labels_empty, \
                isEmpty(keys(endNode(dependency))) AS dependency_keys_empty \
         ORDER BY service, coalesce(endNode(dependency).name, 'zzzz')",
    )
    .await
    .expect("isEmpty endpoint metadata should execute");

    assert!(
        execution.translated_sql().contains(
            "CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE false END = true"
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency_labels_empty": false, "dependency_keys_empty": false}),
            json!({"service": "billing-api", "dependency_labels_empty": false, "dependency_keys_empty": false}),
            json!({"service": "deployments", "dependency_labels_empty": false, "dependency_keys_empty": false}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_relationship_endpoint_metadata_membership_scopes_unmatched_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (service)-[dependency:DEPENDS_ON]->(dependency_service:Service) \
         WHERE 'Service' IN labels(endNode(dependency)) \
           AND 'tier' IN keys(startNode(dependency)) \
         RETURN service.name AS service, dependency_service.name AS dependency \
         ORDER BY service, dependency",
    )
    .await
    .expect("optional endpoint metadata membership should execute");

    assert!(
        execution.translated_sql().contains(
            "CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE true END = true"
        ),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CASE WHEN \"r0\".\"from_service_id\" IS NULL THEN NULL ELSE TRUE END"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "dependency": "deployments"}),
            json!({"service": "billing-api", "dependency": "experiments"}),
            json!({"service": "deployments", "dependency": "experiments"}),
            json!({"service": "experiments"}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_labels_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.name AS service, labels(service) AS labels \
         ORDER BY service",
    )
    .await
    .expect("labels() projection should execute");

    assert!(
        execution.translated_sql().contains("make_array('Service')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "labels": ["Service"]}),
            json!({"service": "deployments", "labels": ["Service"]}),
            json!({"service": "experiments", "labels": ["Service"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_labels_projection_preserves_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'legacy-sync'}) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, labels(person) AS owner_labels",
    )
    .await
    .expect("labels() projection over an optional node should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_keys_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service {name: 'billing-api'}) \
         RETURN keys(person) AS person_keys, keys(owns) AS ownership_keys, keys(service) AS service_keys",
    )
    .await
    .expect("keys() projection should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('since', 'source')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "person_keys": ["name", "team"],
            "ownership_keys": ["since", "source"],
            "service_keys": ["active", "id", "name", "risk", "team", "tier"],
        })]
    );
}

#[tokio::test]
async fn cypher_order_by_keys_function_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service {name: 'billing-api'}) \
         RETURN service.name AS service, keys(service) AS service_keys \
         ORDER BY keys(service), keys(owns)",
    )
    .await
    .expect("keys() order expression should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY CASE WHEN \"n1\".\"id\" IS NULL THEN NULL ELSE make_array('active', 'id', 'name', 'risk', 'team', 'tier') END ASC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "service": "billing-api",
            "service_keys": ["active", "id", "name", "risk", "team", "tier"],
        })]
    );
}

#[tokio::test]
async fn cypher_property_key_membership_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE 'name' IN keys(person) AND 'since' IN keys(owns) AND 'tier' IN keys(service) \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY owner, service",
    )
    .await
    .expect("keys() membership predicate query should execute");

    assert!(
        execution.translated_sql().contains("ELSE TRUE END"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );

    let no_rows = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 'missing' IN keys(service) \
         RETURN service.name AS service",
    )
    .await
    .expect("missing keys() membership predicate query should execute");

    assert!(
        no_rows.translated_sql().contains("ELSE FALSE END"),
        "{}",
        no_rows.translated_sql()
    );
    assert_eq!(execution_to_rows(no_rows.execution()), Vec::<Value>::new());
}

#[tokio::test]
async fn cypher_keys_projection_preserves_optional_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service {name: 'legacy-sync'}) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, keys(person) AS owner_keys",
    )
    .await
    .expect("keys() projection over an optional node should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_static_list_coalesce_preserves_optional_fallbacks() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                coalesce(keys(person), []) AS owner_keys, \
                coalesce(null, labels(service)) AS service_labels \
         ORDER BY service",
    )
    .await
    .expect("static list coalesce should execute");

    assert!(
        execution.translated_sql().contains("COALESCE(CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("array_resize(make_array(CAST(NULL AS VARCHAR)), 0)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_keys": ["name", "team"], "service_labels": ["Service"]}),
            json!({"service": "deployments", "owner_keys": ["name", "team"], "service_labels": ["Service"]}),
            json!({"service": "experiments", "owner_keys": ["name", "team"], "service_labels": ["Service"]}),
            json!({"service": "legacy-sync", "owner_keys": [], "service_labels": ["Service"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_coalesce_size_and_is_empty_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                size(coalesce(keys(person), [])) AS owner_key_count, \
                isEmpty(coalesce(keys(person), [])) AS owner_keys_empty, \
                size(coalesce([], [])) AS empty_count, \
                isEmpty(coalesce([], [])) AS empty_is_empty \
         ORDER BY service",
    )
    .await
    .expect("static list coalesce size/isEmpty should execute");

    assert!(
        execution.translated_sql().contains("COALESCE(CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_key_count": 2, "owner_keys_empty": false, "empty_count": 0, "empty_is_empty": true}),
            json!({"service": "deployments", "owner_key_count": 2, "owner_keys_empty": false, "empty_count": 0, "empty_is_empty": true}),
            json!({"service": "experiments", "owner_key_count": 2, "owner_keys_empty": false, "empty_count": 0, "empty_is_empty": true}),
            json!({"service": "legacy-sync", "owner_key_count": 0, "owner_keys_empty": true, "empty_count": 0, "empty_is_empty": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                CASE WHEN person IS NULL THEN [] ELSE keys(person) END AS owner_keys, \
                CASE WHEN person IS NOT NULL THEN labels(person) ELSE ['missing'] END AS owner_labels, \
                CASE WHEN person IS NULL THEN [] ELSE coalesce(keys(person), []) END AS coalesced_keys \
         ORDER BY service",
    )
    .await
    .expect("static list CASE should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("array_resize(make_array(CAST(NULL AS VARCHAR)), 0)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_keys": ["name", "team"], "owner_labels": ["Person"], "coalesced_keys": ["name", "team"]}),
            json!({"service": "deployments", "owner_keys": ["name", "team"], "owner_labels": ["Person"], "coalesced_keys": ["name", "team"]}),
            json!({"service": "experiments", "owner_keys": ["name", "team"], "owner_labels": ["Person"], "coalesced_keys": ["name", "team"]}),
            json!({"service": "legacy-sync", "owner_keys": [], "owner_labels": ["missing"], "coalesced_keys": []}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_size_and_is_empty_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                size(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS owner_key_count, \
                isEmpty(CASE WHEN person IS NULL THEN [] ELSE keys(person) END) AS owner_keys_empty, \
                size(CASE WHEN service.name IS NOT NULL THEN [] ELSE null END) AS empty_count, \
                isEmpty(CASE WHEN service.name IS NOT NULL THEN [] ELSE null END) AS empty_is_empty \
         ORDER BY service",
    )
    .await
    .expect("static list CASE size/isEmpty should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_key_count": 2, "owner_keys_empty": false, "empty_count": 0, "empty_is_empty": true}),
            json!({"service": "deployments", "owner_key_count": 2, "owner_keys_empty": false, "empty_count": 0, "empty_is_empty": true}),
            json!({"service": "experiments", "owner_key_count": 2, "owner_keys_empty": false, "empty_count": 0, "empty_is_empty": true}),
            json!({"service": "legacy-sync", "owner_key_count": 0, "owner_keys_empty": true, "empty_count": 0, "empty_is_empty": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_endpoint_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                coalesce(head(CASE WHEN person IS NULL THEN [] ELSE keys(person) END), 'missing') AS first_owner_key, \
                coalesce(last(CASE WHEN person IS NULL THEN [] ELSE keys(person) END), 'missing') AS last_owner_key, \
                coalesce(head(coalesce(keys(person), [])), 'missing') AS coalesced_first_key, \
                coalesce(last(CASE WHEN service.name IS NOT NULL THEN [] ELSE null END), 'missing') AS empty_last \
         ORDER BY service",
    )
    .await
    .expect("static list CASE endpoint functions should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "first_owner_key": "name", "last_owner_key": "team", "coalesced_first_key": "name", "empty_last": "missing"}),
            json!({"service": "deployments", "first_owner_key": "name", "last_owner_key": "team", "coalesced_first_key": "name", "empty_last": "missing"}),
            json!({"service": "experiments", "first_owner_key": "name", "last_owner_key": "team", "coalesced_first_key": "name", "empty_last": "missing"}),
            json!({"service": "legacy-sync", "first_owner_key": "missing", "last_owner_key": "missing", "coalesced_first_key": "missing", "empty_last": "missing"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_in_rhs_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                'team' IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END AS owner_key_visible, \
                service.team IN CASE WHEN person IS NULL THEN ['platform'] ELSE keys(person) END AS case_team_membership, \
                service.team IN coalesce(keys(person), ['platform']) AS coalesced_team_membership \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce IN right-hand sides should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_key_visible": true, "case_team_membership": false, "coalesced_team_membership": false}),
            json!({"service": "deployments", "owner_key_visible": true, "case_team_membership": false, "coalesced_team_membership": false}),
            json!({"service": "experiments", "owner_key_visible": true, "case_team_membership": false, "coalesced_team_membership": false}),
            json!({"service": "legacy-sync", "owner_key_visible": false, "case_team_membership": true, "coalesced_team_membership": true}),
        ]
    );

    let filtered = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.team IN CASE WHEN service.tier IS NULL THEN ['platform'] ELSE [] END \
           AND service.team IN coalesce(null, ['platform']) \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("static list coalesce IN right-hand side should filter rows");

    assert!(
        filtered.translated_sql().contains("CASE WHEN"),
        "{}",
        filtered.translated_sql()
    );
    assert_eq!(
        execution_to_rows(filtered.execution()),
        vec![json!({"service": "legacy-sync"})]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_slice_in_rhs_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         WHERE 'name' IN coalesce(keys(person), ['fallback', 'owner'])[0..1] \
            OR service.name IN coalesce(keys(person), ['legacy-sync', 'fallback'])[0..1] \
         RETURN service.name AS service, \
                'team' IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] AS case_slice_has_team, \
                'fallback' IN coalesce(keys(person), ['fallback', 'owner'])[0..1] AS coalesced_slice_has_fallback, \
                service.name IN coalesce(keys(person), ['legacy-sync', 'fallback'])[0..1] AS coalesced_slice_has_service \
         ORDER BY service",
    )
    .await
    .expect("sliced static list CASE/coalesce IN right-hand sides should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_slice_has_team": true, "coalesced_slice_has_fallback": false, "coalesced_slice_has_service": false}),
            json!({"service": "deployments", "case_slice_has_team": true, "coalesced_slice_has_fallback": false, "coalesced_slice_has_service": false}),
            json!({"service": "experiments", "case_slice_has_team": true, "coalesced_slice_has_fallback": false, "coalesced_slice_has_service": false}),
            json!({"service": "legacy-sync", "case_slice_has_team": false, "coalesced_slice_has_fallback": true, "coalesced_slice_has_service": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_static_list_in_rhs_preserves_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                service.name IN keys(person) AS service_name_is_owner_key, \
                service.name IN (keys(person) + ['extra']) AS service_name_is_concat_owner_key \
         ORDER BY service",
    )
    .await
    .expect("optional static list IN RHS should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CASE WHEN \"n1\".\"id\" IS NULL THEN NULL ELSE"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "service_name_is_owner_key": false, "service_name_is_concat_owner_key": false}),
            json!({"service": "deployments", "service_name_is_owner_key": false, "service_name_is_concat_owner_key": false}),
            json!({"service": "experiments", "service_name_is_owner_key": false, "service_name_is_concat_owner_key": false}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_label_membership_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE 'Service' IN labels(service) AND NOT ('Team' IN labels(service)) \
         RETURN count(service) AS services",
    )
    .await
    .expect("labels() membership predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("WHERE (TRUE AND NOT (FALSE))"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"services": 4})]
    );
}

#[tokio::test]
async fn cypher_metadata_list_equality_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE labels(service) = ['Service'] \
           AND ['Team'] <> labels(service) \
           AND keys(service) = ['active', 'id', 'name', 'risk', 'team', 'tier'] \
           AND ['since', 'source'] = keys(owns) \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY owner, service",
    )
    .await
    .expect("metadata list equality predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("WHERE (((TRUE AND TRUE) AND TRUE) AND TRUE)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );

    let no_rows = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE labels(service) = ['Team'] \
            OR keys(service) = ['name'] \
         RETURN service.name AS service",
    )
    .await
    .expect("non-matching metadata list equality predicates should execute");

    assert!(
        no_rows.translated_sql().contains("WHERE (FALSE OR FALSE)"),
        "{}",
        no_rows.translated_sql()
    );
    assert_eq!(execution_to_rows(no_rows.execution()), Vec::<Value>::new());
}

#[tokio::test]
async fn cypher_metadata_list_indexes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE labels(service)[0] = 'Service' \
           AND keys(service)[-1] = 'tier' \
         RETURN person.name AS owner, \
                service.name AS service, \
                labels(service)[0] AS service_label, \
                keys(owns)[0] AS first_ownership_key, \
                keys(service)[99] AS missing_key \
         ORDER BY keys(service)[1], owner, service",
    )
    .await
    .expect("metadata list indexes should execute");

    assert!(
        execution
            .translated_sql()
            .contains("'Service' AS \"service_label\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("NULL AS \"missing_key\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_label": "Service", "first_ownership_key": "since"}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_label": "Service", "first_ownership_key": "since"}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_label": "Service", "first_ownership_key": "since"}),
        ]
    );
}

#[tokio::test]
async fn cypher_metadata_list_slices_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE labels(service)[0..1] = ['Service'] \
           AND keys(service)[1..3] = ['id', 'name'] \
           AND isEmpty(labels(service)[1..]) \
         RETURN person.name AS owner, \
                service.name AS service, \
                labels(service)[0..1] AS service_labels, \
                keys(service)[1..4] AS service_key_window, \
                keys(owns)[-1..][0] AS last_ownership_key, \
                size(keys(service)[-2..]) AS service_key_tail_count, \
                isEmpty(keys(service)[6..]) AS key_tail_empty \
         ORDER BY keys(service)[1..3], owner, service",
    )
    .await
    .expect("metadata list slices should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('Service') AS \"service_labels\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("make_array('id', 'name', 'risk') AS \"service_key_window\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_labels": ["Service"], "service_key_window": ["id", "name", "risk"], "last_ownership_key": "source", "service_key_tail_count": 2, "key_tail_empty": true}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_labels": ["Service"], "service_key_window": ["id", "name", "risk"], "last_ownership_key": "source", "service_key_tail_count": 2, "key_tail_empty": true}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_labels": ["Service"], "service_key_window": ["id", "name", "risk"], "last_ownership_key": "source", "service_key_tail_count": 2, "key_tail_empty": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_optional_metadata_list_slices_preserve_nulls() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                labels(person)[0..1] AS owner_labels, \
                keys(person)[..1] AS owner_first_key \
         ORDER BY service",
    )
    .await
    .expect("optional metadata list slices should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ELSE make_array('Person') END AS \"owner_labels\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_labels": ["Person"], "owner_first_key": ["name"]}),
            json!({"service": "deployments", "owner_labels": ["Person"], "owner_first_key": ["name"]}),
            json!({"service": "experiments", "owner_labels": ["Person"], "owner_first_key": ["name"]}),
            json!({"service": "legacy-sync"}),
        ]
    );
}

#[tokio::test]
async fn cypher_empty_metadata_list_slices_execute_as_typed_lists() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN service.name AS service, \
                labels(service)[1..] AS label_tail, \
                keys(service)[10..] AS key_tail \
         ORDER BY keys(service)[10..], service",
    )
    .await
    .expect("empty metadata list slices should execute");

    assert!(
        execution
            .translated_sql()
            .contains("array_resize(make_array(CAST(NULL AS VARCHAR)), 0) AS \"label_tail\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "label_tail": [], "key_tail": []}),
            json!({"service": "deployments", "label_tail": [], "key_tail": []}),
            json!({"service": "experiments", "label_tail": [], "key_tail": []}),
            json!({"service": "legacy-sync", "label_tail": [], "key_tail": []}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_endpoint_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("name".to_string()),
            GraphLiteral::String("tier".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE head(labels(service)) = 'Service' \
           AND last(keys(service)) = 'tier' \
         RETURN person.name AS owner, \
                service.name AS service, \
                head(keys(service)) AS first_service_key, \
                last(keys(owns)) AS last_ownership_key, \
                head(keys(service)[6..]) AS missing_key, \
                head($selected_keys) AS selected_first_key, \
                last($selected_keys) AS selected_last_key \
         ORDER BY last(keys(service)), owner, service",
        &parameters,
    )
    .await
    .expect("static list endpoint functions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("'active' AS \"first_service_key\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("'tier' AS \"selected_last_key\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "first_service_key": "active", "last_ownership_key": "source", "selected_first_key": "name", "selected_last_key": "tier"}),
            json!({"owner": "Grace Hopper", "service": "deployments", "first_service_key": "active", "last_ownership_key": "source", "selected_first_key": "name", "selected_last_key": "tier"}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "first_service_key": "active", "last_ownership_key": "source", "selected_first_key": "name", "selected_last_key": "tier"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_tail_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("name".to_string()),
            GraphLiteral::String("tier".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE head(tail(keys(service))) = 'id' \
           AND size(tail(labels(service))) = 0 \
           AND isEmpty(tail(labels(service))) \
         RETURN person.name AS owner, \
                service.name AS service, \
                tail(keys(service)) AS service_key_tail, \
                tail(labels(service)) AS label_tail, \
                tail($selected_keys) AS selected_tail, \
                tail(tail($selected_keys)) AS selected_tail_tail, \
                head(tail($selected_keys)) AS selected_tail_head, \
                last(tail(keys(service))) AS service_tail_last, \
                size(tail(keys(service))) AS service_tail_size \
         ORDER BY tail(keys(service)), owner, service",
        &parameters,
    )
    .await
    .expect("static tail() list functions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('id', 'name', 'risk', 'team', 'tier') AS \"service_key_tail\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("array_resize(make_array(CAST(NULL AS VARCHAR)), 0) AS \"label_tail\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_key_tail": ["id", "name", "risk", "team", "tier"], "label_tail": [], "selected_tail": ["tier"], "selected_tail_tail": [], "selected_tail_head": "tier", "service_tail_last": "tier", "service_tail_size": 5}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_key_tail": ["id", "name", "risk", "team", "tier"], "label_tail": [], "selected_tail": ["tier"], "selected_tail_tail": [], "selected_tail_head": "tier", "service_tail_last": "tier", "service_tail_size": 5}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_key_tail": ["id", "name", "risk", "team", "tier"], "label_tail": [], "selected_tail": ["tier"], "selected_tail_tail": [], "selected_tail_head": "tier", "service_tail_last": "tier", "service_tail_size": 5}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_reverse_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("name".to_string()),
            GraphLiteral::String("tier".to_string()),
            GraphLiteral::String("risk".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE head(reverse(keys(service))) = 'tier' \
           AND any(key IN reverse(keys(service)) WHERE key = 'name') \
         RETURN person.name AS owner, \
                service.name AS service, \
                reverse(keys(service)) AS service_keys_reversed, \
                reverse(labels(service) + keys(service)) AS metadata_reversed, \
                tail(reverse($selected_keys)) AS selected_reversed_tail, \
                size(reverse(labels(service) + keys(service))) AS metadata_size \
         ORDER BY reverse(keys(service)), owner, service",
        &parameters,
    )
    .await
    .expect("static reverse() list functions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('tier', 'team', 'risk', 'name', 'id', 'active') AS \"service_keys_reversed\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_keys_reversed": ["tier", "team", "risk", "name", "id", "active"], "metadata_reversed": ["tier", "team", "risk", "name", "id", "active", "Service"], "selected_reversed_tail": ["tier", "name"], "metadata_size": 7}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_keys_reversed": ["tier", "team", "risk", "name", "id", "active"], "metadata_reversed": ["tier", "team", "risk", "name", "id", "active", "Service"], "selected_reversed_tail": ["tier", "name"], "metadata_size": 7}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_keys_reversed": ["tier", "team", "risk", "name", "id", "active"], "metadata_reversed": ["tier", "team", "risk", "name", "id", "active", "Service"], "selected_reversed_tail": ["tier", "name"], "metadata_size": 7}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_indexes_and_slices_over_folded_lists_execute_against_synthetic_sources()
{
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE reverse(keys(service))[0] = 'tier' \
           AND size((labels(service) + keys(service))[1..]) = 6 \
         RETURN person.name AS owner, \
                service.name AS service, \
                reverse(keys(service))[1] AS second_reversed_key, \
                reverse(keys(service))[1..3] AS reversed_key_window, \
                (labels(service) + keys(service))[1..] AS metadata_tail, \
                tail(reverse(keys(service))[1..]) AS reversed_tail_tail \
         ORDER BY reverse(keys(service))[0], owner, service",
    )
    .await
    .expect("static list indexes and slices over folded lists should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "second_reversed_key": "team", "reversed_key_window": ["team", "risk"], "metadata_tail": ["active", "id", "name", "risk", "team", "tier"], "reversed_tail_tail": ["risk", "name", "id", "active"]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "second_reversed_key": "team", "reversed_key_window": ["team", "risk"], "metadata_tail": ["active", "id", "name", "risk", "team", "tier"], "reversed_tail_tail": ["risk", "name", "id", "active"]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "second_reversed_key": "team", "reversed_key_window": ["team", "risk"], "metadata_tail": ["active", "id", "name", "risk", "team", "tier"], "reversed_tail_tail": ["risk", "name", "id", "active"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_indexes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                coalesce((CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END)[0], 'missing') AS case_first_key, \
                coalesce(coalesce(keys(person), ['fallback'])[0], 'missing') AS coalesced_first_key, \
                coalesce((CASE WHEN service.tier = 'prod' THEN [] ELSE null END)[0], 'missing') AS empty_first_key \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce indexes should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_first_key": "name", "coalesced_first_key": "name", "empty_first_key": "missing"}),
            json!({"service": "deployments", "case_first_key": "name", "coalesced_first_key": "name", "empty_first_key": "missing"}),
            json!({"service": "experiments", "case_first_key": "name", "coalesced_first_key": "name", "empty_first_key": "missing"}),
            json!({"service": "legacy-sync", "case_first_key": "fallback", "coalesced_first_key": "fallback", "empty_first_key": "missing"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_slices_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1] AS case_key_window, \
                coalesce(keys(person), ['fallback', 'owner'])[0..1] AS coalesced_key_window, \
                (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] AS tier_window \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce slices should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_key_window": ["name"], "coalesced_key_window": ["name"], "tier_window": []}),
            json!({"service": "deployments", "case_key_window": ["name"], "coalesced_key_window": ["name"], "tier_window": []}),
            json!({"service": "experiments", "case_key_window": ["name"], "coalesced_key_window": ["name"], "tier_window": ["not-prod"]}),
            json!({"service": "legacy-sync", "case_key_window": ["fallback"], "coalesced_key_window": ["fallback"], "tier_window": ["not-prod"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_slice_reducers_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                size((CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1]) AS case_window_size, \
                isEmpty(coalesce(keys(person), ['fallback'])[2..]) AS coalesced_tail_empty, \
                size((CASE WHEN service.tier = 'prod' THEN [] ELSE null END)[0..1]) AS empty_window_size, \
                isEmpty((CASE WHEN service.tier = 'prod' THEN [] ELSE null END)[0..1]) AS empty_window_is_empty \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce slice reducers should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_window_size": 1, "coalesced_tail_empty": true, "empty_window_size": 0, "empty_window_is_empty": true}),
            json!({"service": "deployments", "case_window_size": 1, "coalesced_tail_empty": true, "empty_window_size": 0, "empty_window_is_empty": true}),
            json!({"service": "experiments", "case_window_size": 1, "coalesced_tail_empty": true}),
            json!({"service": "legacy-sync", "case_window_size": 1, "coalesced_tail_empty": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_slice_indexes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                coalesce(((CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1])[0], 'missing') AS case_slice_first, \
                coalesce((coalesce(keys(person), ['fallback', 'owner'])[0..1])[0], 'missing') AS coalesced_slice_first, \
                coalesce(head((CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1]), 'missing') AS tier_head, \
                coalesce(last(coalesce(keys(person), ['fallback', 'owner'])[0..1]), 'missing') AS coalesced_slice_last \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce slice indexes and endpoints should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_slice_first": "name", "coalesced_slice_first": "name", "tier_head": "missing", "coalesced_slice_last": "name"}),
            json!({"service": "deployments", "case_slice_first": "name", "coalesced_slice_first": "name", "tier_head": "missing", "coalesced_slice_last": "name"}),
            json!({"service": "experiments", "case_slice_first": "name", "coalesced_slice_first": "name", "tier_head": "not-prod", "coalesced_slice_last": "name"}),
            json!({"service": "legacy-sync", "case_slice_first": "fallback", "coalesced_slice_first": "fallback", "tier_head": "not-prod", "coalesced_slice_last": "fallback"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_slice_comparisons_execute_against_synthetic_sources()
{
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         WHERE (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1] = ['name'] \
            OR ['fallback'] = coalesce(keys(person), ['fallback', 'owner'])[0..1] \
         RETURN service.name AS service, \
                (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[0..1] = ['name'] AS case_slice_matches, \
                ['fallback'] = coalesce(keys(person), ['fallback', 'owner'])[0..1] AS coalesced_slice_fallback, \
                coalesce(keys(person), ['fallback', 'owner'])[0..1] > ['fallback'] AS coalesced_slice_after_fallback, \
                (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] <> [] AS tier_window_non_empty \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce slice comparisons should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_slice_matches": true, "coalesced_slice_fallback": false, "coalesced_slice_after_fallback": true, "tier_window_non_empty": false}),
            json!({"service": "deployments", "case_slice_matches": true, "coalesced_slice_fallback": false, "coalesced_slice_after_fallback": true, "tier_window_non_empty": false}),
            json!({"service": "experiments", "case_slice_matches": true, "coalesced_slice_fallback": false, "coalesced_slice_after_fallback": true, "tier_window_non_empty": true}),
            json!({"service": "legacy-sync", "case_slice_matches": false, "coalesced_slice_fallback": true, "coalesced_slice_after_fallback": false, "tier_window_non_empty": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_concatenation_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "prefixes".to_string(),
        GraphCypherParameterValue::List(vec![GraphLiteral::String("prefix".to_string())]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE head($prefixes + tail(keys(service))) = 'prefix' \
           AND size(labels(service) + tail(labels(service))) = 1 \
           AND any(key IN ['active'] + tail(keys(service)) WHERE key = 'tier') \
           AND service.tier IN (['prod'] + ['dev']) \
         RETURN person.name AS owner, \
                service.name AS service, \
                $prefixes + tail(keys(service)) AS keys_with_prefix, \
                labels(service) + [] AS labels_copy, \
                [null] + tail(keys(service)) AS nullable_keys, \
                tail($prefixes + tail(keys(service))) AS concat_tail, \
                size($prefixes + tail(keys(service))) AS concat_size \
         ORDER BY $prefixes + tail(keys(service)), owner, service",
        &parameters,
    )
    .await
    .expect("static list concatenation should execute");

    assert!(
        execution.translated_sql().contains(
            "make_array('prefix', 'id', 'name', 'risk', 'team', 'tier') AS \"keys_with_prefix\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "keys_with_prefix": ["prefix", "id", "name", "risk", "team", "tier"], "labels_copy": ["Service"], "nullable_keys": [null, "id", "name", "risk", "team", "tier"], "concat_tail": ["id", "name", "risk", "team", "tier"], "concat_size": 6}),
            json!({"owner": "Grace Hopper", "service": "deployments", "keys_with_prefix": ["prefix", "id", "name", "risk", "team", "tier"], "labels_copy": ["Service"], "nullable_keys": [null, "id", "name", "risk", "team", "tier"], "concat_tail": ["id", "name", "risk", "team", "tier"], "concat_size": 6}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "keys_with_prefix": ["prefix", "id", "name", "risk", "team", "tier"], "labels_copy": ["Service"], "nullable_keys": [null, "id", "name", "risk", "team", "tier"], "concat_tail": ["id", "name", "risk", "team", "tier"], "concat_size": 6}),
        ]
    );
}

#[tokio::test]
async fn cypher_dynamic_unwind_alias_sources_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "WITH [1, 2, 3] AS list UNWIND list AS x RETURN x",
    )
    .await
    .expect("WITH alias UNWIND row source should execute");

    assert!(
        execution.translated_sql().contains("UNNEST"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})]
    );

    let empty = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "WITH [] AS list UNWIND list AS x RETURN x",
    )
    .await
    .expect("empty WITH alias UNWIND row source should execute");

    assert_eq!(empty.execution().row_count(), 0);
    assert!(execution_to_rows(empty.execution()).is_empty());

    let concatenated = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "WITH [1, 2, 3] AS first, [4, 5, 6] AS second \
         UNWIND (first + second) AS x \
         RETURN x",
    )
    .await
    .expect("WITH alias list concatenation UNWIND row source should execute");

    assert!(
        concatenated.translated_sql().contains("array_concat"),
        "{}",
        concatenated.translated_sql()
    );
    assert_eq!(
        execution_to_rows(concatenated.execution()),
        vec![
            json!({"x": 1}),
            json!({"x": 2}),
            json!({"x": 3}),
            json!({"x": 4}),
            json!({"x": 5}),
            json!({"x": 6}),
        ]
    );
}

#[tokio::test]
async fn cypher_unwind_non_bare_returns_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let aggregate = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "UNWIND [1, 2, 3] AS x RETURN count(x) AS c, sum(x) AS s, collect(x) AS xs",
    )
    .await
    .expect("UNWIND aggregate return should execute");

    assert!(
        aggregate.translated_sql().contains("WITH \"stage0\" AS"),
        "{}",
        aggregate.translated_sql()
    );
    assert_eq!(
        execution_to_rows(aggregate.execution()),
        vec![json!({"c": 3, "s": 6, "xs": [1, 2, 3]})]
    );

    let ordered = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "UNWIND [3, 1, 2] AS x RETURN x ORDER BY x",
    )
    .await
    .expect("UNWIND ordered return should execute");

    assert_eq!(
        execution_to_rows(ordered.execution()),
        vec![json!({"x": 1}), json!({"x": 2}), json!({"x": 3})]
    );

    let distinct = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "UNWIND [1, 1, 2, 2] AS x RETURN DISTINCT x ORDER BY x",
    )
    .await
    .expect("UNWIND distinct return should execute");

    assert_eq!(
        execution_to_rows(distinct.execution()),
        vec![json!({"x": 1}), json!({"x": 2})]
    );

    let expressions = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "UNWIND [1, 2, 3] AS x RETURN x * 2 AS d, toString(x) AS text ORDER BY d",
    )
    .await
    .expect("UNWIND expression return should execute");

    assert_eq!(
        execution_to_rows(expressions.execution()),
        vec![
            json!({"d": 2, "text": "1"}),
            json!({"d": 4, "text": "2"}),
            json!({"d": 6, "text": "3"}),
        ]
    );

    let alias_source = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "WITH [1, 2, 3] AS list UNWIND list AS x RETURN x, x + 10 AS y ORDER BY x",
    )
    .await
    .expect("WITH alias UNWIND multi-column return should execute");

    assert_eq!(
        execution_to_rows(alias_source.execution()),
        vec![
            json!({"x": 1, "y": 11}),
            json!({"x": 2, "y": 12}),
            json!({"x": 3, "y": 13}),
        ]
    );

    let terminal_with = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "UNWIND [1, 2, 3] AS x WITH x * 2 AS d RETURN d ORDER BY d",
    )
    .await
    .expect("UNWIND terminal WITH return should execute");

    assert_eq!(
        execution_to_rows(terminal_with.execution()),
        vec![json!({"d": 2}), json!({"d": 4}), json!({"d": 6})]
    );
}

#[tokio::test]
async fn cypher_dynamic_unwind_alias_source_matches_static_unwind_rows() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let static_execution = CoralQuery::execute_cypher(
        std::slice::from_ref(&source),
        test_runtime(),
        &graph,
        "UNWIND [1, 2, 3] AS ordinal \
         MATCH (service:Service) \
         WHERE service.id = ordinal * 10 \
         RETURN ordinal AS ordinal, service.name AS service \
         ORDER BY ordinal",
    )
    .await
    .expect("static UNWIND route should execute");
    let dynamic_execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "WITH [1, 2, 3] AS ordinals \
         UNWIND ordinals AS ordinal \
         MATCH (service:Service) \
         WHERE service.id = ordinal * 10 \
         RETURN ordinal AS ordinal, service.name AS service \
         ORDER BY ordinal",
    )
    .await
    .expect("dynamic WITH alias UNWIND route should execute");

    let static_rows = execution_to_rows(static_execution.execution());
    let dynamic_rows = execution_to_rows(dynamic_execution.execution());
    assert_eq!(dynamic_rows, static_rows);
    assert_eq!(
        dynamic_rows,
        vec![
            json!({"ordinal": 1, "service": "billing-api"}),
            json!({"ordinal": 2, "service": "deployments"}),
            json!({"ordinal": 3, "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_scalar_concatenation_unwinds_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "extra_tier".to_string(),
        GraphCypherParameterValue::Literal(GraphLiteral::String("dev".to_string())),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND ['prod'] + $extra_tier AS tier \
         MATCH (service:Service) \
         WHERE service.tier = tier \
         RETURN tier AS tier, service.name AS service \
         ORDER BY tier, service",
        &parameters,
    )
    .await
    .expect("static list scalar concatenation should execute");

    assert!(
        execution.translated_sql().contains("'dev'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "dev", "service": "experiments"}),
            json!({"tier": "prod", "service": "billing-api"}),
            json!({"tier": "prod", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_casts_unwind_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "UNWIND toBooleanList(['true', 'false', 'bad', 0, 2]) AS active_flag \
         MATCH (service:Service) \
         WHERE service.active = active_flag \
         RETURN active_flag AS active, count(*) AS services \
         ORDER BY active",
    )
    .await
    .expect("static list casts should execute through UNWIND");

    assert!(
        execution.translated_sql().contains(" UNION ALL "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"active": false, "services": 4}),
            json!({"active": true, "services": 4}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehensions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("tier".to_string()),
            GraphLiteral::String("missing".to_string()),
            GraphLiteral::String("name".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [k IN keys(service)] AS service_keys_copy, \
                [k IN $selected_keys] AS selected_keys_copy, \
                [l IN labels(service)] AS labels_copy \
         ORDER BY owner, service",
        &parameters,
    )
    .await
    .expect("static list comprehensions should execute");

    assert!(
        execution.translated_sql().contains(
            "make_array('active', 'id', 'name', 'risk', 'team', 'tier') AS \"service_keys_copy\""
        ),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_keys_copy": ["active", "id", "name", "risk", "team", "tier"], "selected_keys_copy": ["tier", "missing", "name"], "labels_copy": ["Service"]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_keys_copy": ["active", "id", "name", "risk", "team", "tier"], "selected_keys_copy": ["tier", "missing", "name"], "labels_copy": ["Service"]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_keys_copy": ["active", "id", "name", "risk", "team", "tier"], "selected_keys_copy": ["tier", "missing", "name"], "labels_copy": ["Service"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_filter_and_extract_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                filter(key IN keys(service) WHERE key STARTS WITH 't') AS service_t_keys, \
                extract(key IN filter(key IN keys(service) WHERE key <> 'id') | toUpper(key)) AS service_key_tokens \
         ORDER BY owner, service",
    )
    .await
    .expect("static filter()/extract() functions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('team', 'tier') AS \"service_t_keys\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_t_keys": ["team", "tier"], "service_key_tokens": ["ACTIVE", "NAME", "RISK", "TEAM", "TIER"]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_t_keys": ["team", "tier"], "service_key_tokens": ["ACTIVE", "NAME", "RISK", "TEAM", "TIER"]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_t_keys": ["team", "tier"], "service_key_tokens": ["ACTIVE", "NAME", "RISK", "TEAM", "TIER"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_filtered_static_list_comprehensions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("tier".to_string()),
            GraphLiteral::String("missing".to_string()),
            GraphLiteral::String("name".to_string()),
            GraphLiteral::Null,
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [k IN keys(service) WHERE k IN ['name', 'tier']] AS exposed_keys, \
                [k IN $selected_keys WHERE k IN keys(service)] AS selected_existing_keys, \
                [k IN ['name', null, 'tier'] WHERE k IS NOT NULL] AS non_null_literals, \
                [k IN keys(service) WHERE toUpper(k) STARTS WITH 'T'] AS upper_t_keys, \
                [x IN ['1', '2', 'bad'] WHERE toIntegerOrNull(x) >= 2] AS numeric_strings, \
                [x IN [1, 2, 3] WHERE x + 1 >= 3] AS arithmetic_values, \
                [x IN ['', 'a', null] WHERE isEmpty(x)] AS empty_strings, \
                [x IN ['', 'a', null] WHERE isEmpty(x) = false] AS non_empty_strings \
         ORDER BY owner, service",
        &parameters,
    )
    .await
    .expect("filtered static list comprehensions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('name', 'tier') AS \"exposed_keys\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "exposed_keys": ["name", "tier"], "selected_existing_keys": ["tier", "name"], "non_null_literals": ["name", "tier"], "upper_t_keys": ["team", "tier"], "numeric_strings": ["2"], "arithmetic_values": [2, 3], "empty_strings": [""], "non_empty_strings": ["a"]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "exposed_keys": ["name", "tier"], "selected_existing_keys": ["tier", "name"], "non_null_literals": ["name", "tier"], "upper_t_keys": ["team", "tier"], "numeric_strings": ["2"], "arithmetic_values": [2, 3], "empty_strings": [""], "non_empty_strings": ["a"]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "exposed_keys": ["name", "tier"], "selected_existing_keys": ["tier", "name"], "non_null_literals": ["name", "tier"], "upper_t_keys": ["team", "tier"], "numeric_strings": ["2"], "arithmetic_values": [2, 3], "empty_strings": [""], "non_empty_strings": ["a"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_mapped_static_list_comprehensions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [k IN keys(service) WHERE k IN ['name', 'tier'] | upper(k)] AS upper_keys, \
                [k IN [' service-name ', null] | trim(k)] AS trimmed_keys, \
                [k IN ['service-id'] | replace(k, '-', '_')] AS replaced_keys, \
                [k IN keys(service) WHERE k IN ['name', 'tier'] | left(k, 2)] AS key_prefixes, \
                [k IN ['service-name', null] | substring(k, 8, 4)] AS key_suffixes, \
                [k IN ['ops'] | right(k, 2)] AS right_suffixes, \
                [k IN ['abc'] | reverse(k)] AS reversed_literals, \
                [k IN ['name', null] | coalesce(k, 'missing')] AS coalesced_keys, \
                [k IN keys(service) WHERE k IN ['name', 'tier'] | nullIf(k, 'tier')] AS nullified_tier, \
                [k IN ['fallback'] | coalesce(null, k)] AS coalesced_second_arg, \
                [k IN ['id'] | nullIf('id', k)] AS nullified_second_arg \
         ORDER BY owner, service",
    )
    .await
    .expect("mapped static list comprehensions should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('NAME', 'TIER') AS \"upper_keys\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "upper_keys": ["NAME", "TIER"], "trimmed_keys": ["service-name", null], "replaced_keys": ["service_id"], "key_prefixes": ["na", "ti"], "key_suffixes": ["name", null], "right_suffixes": ["ps"], "reversed_literals": ["cba"], "coalesced_keys": ["name", "missing"], "nullified_tier": ["name", null], "coalesced_second_arg": ["fallback"], "nullified_second_arg": [null]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "upper_keys": ["NAME", "TIER"], "trimmed_keys": ["service-name", null], "replaced_keys": ["service_id"], "key_prefixes": ["na", "ti"], "key_suffixes": ["name", null], "right_suffixes": ["ps"], "reversed_literals": ["cba"], "coalesced_keys": ["name", "missing"], "nullified_tier": ["name", null], "coalesced_second_arg": ["fallback"], "nullified_second_arg": [null]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "upper_keys": ["NAME", "TIER"], "trimmed_keys": ["service-name", null], "replaced_keys": ["service_id"], "key_prefixes": ["na", "ti"], "key_suffixes": ["name", null], "right_suffixes": ["ps"], "reversed_literals": ["cba"], "coalesced_keys": ["name", "missing"], "nullified_tier": ["name", null], "coalesced_second_arg": ["fallback"], "nullified_second_arg": [null]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehensions_over_case_and_coalesce_sources_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                [k IN coalesce(keys(person), ['fallback', 'owner']) WHERE k <> 'owner' | toUpper(k)] AS owner_key_tokens, \
                [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE k STARTS WITH 't' | k] AS team_keys \
         ORDER BY service",
    )
    .await
    .expect("static list comprehensions over CASE/coalesce sources should execute");

    assert!(
        execution.translated_sql().contains("COALESCE("),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_key_tokens": ["NAME", "TEAM"], "team_keys": ["team"]}),
            json!({"service": "deployments", "owner_key_tokens": ["NAME", "TEAM"], "team_keys": ["team"]}),
            json!({"service": "experiments", "owner_key_tokens": ["NAME", "TEAM"], "team_keys": ["team"]}),
            json!({"service": "legacy-sync", "owner_key_tokens": ["FALLBACK"], "team_keys": []}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehensions_as_in_rhs_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                coalesce(service.tier, 'missing') IN [tier IN ['prod', 'dev'] WHERE tier <> 'dev' | tier] AS tier_is_prod, \
                'TEAM' IN [k IN coalesce(keys(person), ['fallback']) | toUpper(k)] AS owner_has_team_key, \
                'team' IN [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] AS case_has_team_key \
         ORDER BY service",
    )
    .await
    .expect("static list comprehensions should execute as IN RHS values");

    assert!(
        execution.translated_sql().contains(" IN ('prod')"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "tier_is_prod": true, "owner_has_team_key": true, "case_has_team_key": true}),
            json!({"service": "deployments", "tier_is_prod": true, "owner_has_team_key": true, "case_has_team_key": true}),
            json!({"service": "experiments", "tier_is_prod": false, "owner_has_team_key": true, "case_has_team_key": true}),
            json!({"service": "legacy-sync", "tier_is_prod": false, "owner_has_team_key": false, "case_has_team_key": false}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehension_comparisons_execute() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                [k IN coalesce(keys(person), ['fallback']) | k] = ['name', 'team'] AS owner_keys_match, \
                ['fallback'] = [k IN coalesce(keys(person), ['fallback']) | k] AS fallback_keys, \
                [k IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END | k] > ['fallback'] AS keys_after_fallback \
         ORDER BY service",
    )
    .await
    .expect("static list comprehension comparisons should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "owner_keys_match": true, "fallback_keys": false, "keys_after_fallback": true}),
            json!({"service": "deployments", "owner_keys_match": true, "fallback_keys": false, "keys_after_fallback": true}),
            json!({"service": "experiments", "owner_keys_match": true, "fallback_keys": false, "keys_after_fallback": true}),
            json!({"service": "legacy-sync", "owner_keys_match": false, "fallback_keys": true, "keys_after_fallback": false}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehension_length_maps_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [k IN keys(service) WHERE k IN ['name', 'tier'] | size(k)] AS key_lengths, \
                [k IN ['ops', null] | char_length(k)] AS literal_lengths, \
                [k IN ['deploy'] | character_length(k)] AS gql_literal_lengths \
         ORDER BY owner, service",
    )
    .await
    .expect("static list comprehension length maps should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array(4, 4) AS \"key_lengths\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "key_lengths": [4, 4], "literal_lengths": [3, null], "gql_literal_lengths": [6]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "key_lengths": [4, 4], "literal_lengths": [3, null], "gql_literal_lengths": [6]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "key_lengths": [4, 4], "literal_lengths": [3, null], "gql_literal_lengths": [6]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehension_string_filters_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [k IN keys(service) WHERE k STARTS WITH 't'] AS starts_with_t, \
                [k IN keys(service) WHERE k ENDS WITH 'e'] AS ends_with_e, \
                [k IN keys(service) WHERE k CONTAINS 'is'] AS contains_is, \
                [k IN keys(service) WHERE k =~ '^t.*'] AS regex_t, \
                [k IN keys(service) WHERE k > 'risk'] AS after_risk, \
                [k IN keys(service) WHERE k <= 'name'] AS through_name \
         ORDER BY owner, service",
    )
    .await
    .expect("static list comprehension string filters should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array('team', 'tier') AS \"starts_with_t\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "starts_with_t": ["team", "tier"], "ends_with_e": ["active", "name"], "contains_is": ["risk"], "regex_t": ["team", "tier"], "after_risk": ["team", "tier"], "through_name": ["active", "id", "name"]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "starts_with_t": ["team", "tier"], "ends_with_e": ["active", "name"], "contains_is": ["risk"], "regex_t": ["team", "tier"], "after_risk": ["team", "tier"], "through_name": ["active", "id", "name"]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "starts_with_t": ["team", "tier"], "ends_with_e": ["active", "name"], "contains_is": ["risk"], "regex_t": ["team", "tier"], "after_risk": ["team", "tier"], "through_name": ["active", "id", "name"]}),
        ]
    );
}

#[tokio::test]
async fn cypher_numeric_static_list_comprehension_maps_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "weights".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::Integer(2),
            GraphLiteral::Integer(4),
            GraphLiteral::Null,
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [x IN [1, 2, 3] | x + 1] AS incremented, \
                [x IN [1.5, 2.5] | x * 2] AS doubled, \
                [x IN $weights | x / 2] AS halved_weights, \
                [x IN [1, 3, 6] | abs(x - 3)] AS absolute_ints, \
                [x IN [1.5, null, 5.5] | abs(x - 3.0)] AS absolute_floats, \
                [x IN [4, 9] | sqrt(x)] AS roots, \
                [x IN [1.0, 3.0, 6.5, null] | sign(x - 3.0)] AS signs, \
                [x IN [0, 1, null] | round(exp(x), 0)] AS exponentials, \
                [x IN [1.0, 2.718281828459045, null] | round(log(x), 0)] AS natural_logs, \
                [x IN [1, 100, null] | log10(x)] AS base10_logs, \
                [x IN [2, 3, null] | pow(x, 3)] AS powers, \
                [x IN [2, 3] | power(x, 2)] AS squares, \
                [x IN [1.2, 2.8, null] | ceiling(x)] AS ceilings, \
                [x IN [1.2, 2.8, null] | floor(x)] AS floors, \
                [x IN [1.24, 1.25, 1.26] | round(x, 1)] AS rounded_tenths, \
                [x IN [1.4, 1.5, 1.6] | round(x)] AS rounded_wholes, \
                [k IN keys(service) | k STARTS WITH 't'] AS t_flags, \
                [x IN ['', 'a', null] | isEmpty(x)] AS empty_flags \
         ORDER BY owner, service",
        &parameters,
    )
    .await
    .expect("numeric static list comprehension maps should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array(2, 3, 4) AS \"incremented\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("make_array(2, 0, 3) AS \"absolute_ints\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "incremented": [2, 3, 4], "doubled": [3.0, 5.0], "halved_weights": [1.0, 2.0, null], "absolute_ints": [2, 0, 3], "absolute_floats": [1.5, null, 2.5], "roots": [2.0, 3.0], "signs": [-1, 0, 1, null], "exponentials": [1.0, 3.0, null], "natural_logs": [0.0, 1.0, null], "base10_logs": [0.0, 2.0, null], "powers": [8.0, 27.0, null], "squares": [4.0, 9.0], "ceilings": [2.0, 3.0, null], "floors": [1.0, 2.0, null], "rounded_tenths": [1.2, 1.3, 1.3], "rounded_wholes": [1.0, 2.0, 2.0], "t_flags": [false, false, false, false, true, true], "empty_flags": [true, false, null]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "incremented": [2, 3, 4], "doubled": [3.0, 5.0], "halved_weights": [1.0, 2.0, null], "absolute_ints": [2, 0, 3], "absolute_floats": [1.5, null, 2.5], "roots": [2.0, 3.0], "signs": [-1, 0, 1, null], "exponentials": [1.0, 3.0, null], "natural_logs": [0.0, 1.0, null], "base10_logs": [0.0, 2.0, null], "powers": [8.0, 27.0, null], "squares": [4.0, 9.0], "ceilings": [2.0, 3.0, null], "floors": [1.0, 2.0, null], "rounded_tenths": [1.2, 1.3, 1.3], "rounded_wholes": [1.0, 2.0, 2.0], "t_flags": [false, false, false, false, true, true], "empty_flags": [true, false, null]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "incremented": [2, 3, 4], "doubled": [3.0, 5.0], "halved_weights": [1.0, 2.0, null], "absolute_ints": [2, 0, 3], "absolute_floats": [1.5, null, 2.5], "roots": [2.0, 3.0], "signs": [-1, 0, 1, null], "exponentials": [1.0, 3.0, null], "natural_logs": [0.0, 1.0, null], "base10_logs": [0.0, 2.0, null], "powers": [8.0, 27.0, null], "squares": [4.0, 9.0], "ceilings": [2.0, 3.0, null], "floors": [1.0, 2.0, null], "rounded_tenths": [1.2, 1.3, 1.3], "rounded_wholes": [1.0, 2.0, 2.0], "t_flags": [false, false, false, false, true, true], "empty_flags": [true, false, null]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comprehension_cast_maps_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, \
                service.name AS service, \
                [x IN ['bad', '3', null] | toInteger(x)] AS ints, \
                [x IN ['bad', '2.5', null] | toFloat(x)] AS floats, \
                [x IN ['maybe', 'true', null] | toBoolean(x)] AS booleans, \
                [x IN ['bad', '3', null] | toIntegerOrNull(x)] AS nullable_ints, \
                [x IN ['maybe', 'true', null] | toBooleanOrNull(x)] AS nullable_booleans \
         ORDER BY owner, service",
    )
    .await
    .expect("static list comprehension cast maps should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array(NULL, 3, NULL) AS \"ints\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "ints": [null, 3, null], "floats": [null, 2.5, null], "booleans": [null, true, null], "nullable_ints": [null, 3, null], "nullable_booleans": [null, true, null]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "ints": [null, 3, null], "floats": [null, 2.5, null], "booleans": [null, true, null], "nullable_ints": [null, 3, null], "nullable_booleans": [null, true, null]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "ints": [null, 3, null], "floats": [null, 2.5, null], "booleans": [null, true, null], "nullable_ints": [null, 3, null], "nullable_booleans": [null, true, null]}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_reduce_expressions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[:OWNS]->(service:Service) \
         WHERE reduce(total = 0, x IN range(1, 3) | total + x) = 6 \
         RETURN person.name AS owner, \
                service.name AS service, \
                reduce(total = 0, x IN [1, 2, 3] | total + x) AS weight, \
                reduce(found = false, key IN keys(service) | found OR key = 'tier') AS has_tier_key \
         ORDER BY owner, service",
    )
    .await
    .expect("static reduce expressions should execute");

    assert!(
        execution.translated_sql().contains(" AS \"weight\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "weight": 6, "has_tier_key": true}),
            json!({"owner": "Grace Hopper", "service": "deployments", "weight": 6, "has_tier_key": true}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "weight": 6, "has_tier_key": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_comparison_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("name".to_string()),
            GraphLiteral::String("tier".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE tail(keys(service)) = ['id', 'name', 'risk', 'team', 'tier'] \
           AND [] = tail(labels(service)) \
           AND tail($selected_keys) = ['tier'] \
           AND tail(keys(service)) <> [] \
           AND tail(keys(service)) > ['id', 'name', 'risk', 'team'] \
           AND tail(keys(service)) < ['id', 'name', 'risk', 'team', 'tier', 'zzz'] \
           AND ['id', 'name', 'risk', 'team', 'tier', 'zzz'] > tail(keys(service)) \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY owner, service",
        &parameters,
    )
    .await
    .expect("static list comparison predicates should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_quantifier_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let parameters = BTreeMap::from([(
        "selected_keys".to_string(),
        GraphCypherParameterValue::List(vec![
            GraphLiteral::String("missing".to_string()),
            GraphLiteral::String("tier".to_string()),
        ]),
    )]);

    let execution = CoralQuery::execute_cypher_with_parameters(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE all(key IN keys(service) WHERE key <> 'deprecated') \
           AND any(key IN tail(keys(service)) WHERE key = 'tier') \
           AND any(key IN keys(service) WHERE key > 'team') \
           AND none(label IN labels(service) WHERE label = 'Team') \
           AND single(key IN ['name', 'tier', 'risk'] WHERE key STARTS WITH 'r') \
           AND any(key IN $selected_keys WHERE key IN keys(service)) \
         RETURN person.name AS owner, \
                service.name AS service, \
                all(key IN keys(service) WHERE key <> 'deprecated') AS keys_declared, \
                any(label IN labels(service) WHERE label = 'Service') AS has_service_label, \
                single(key IN keys(service) WHERE key < 'id') AS single_key_before_id \
         ORDER BY owner, service",
        &parameters,
    )
    .await
    .expect("static list collection predicates should execute");

    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "keys_declared": true, "has_service_label": true, "single_key_before_id": true}),
            json!({"owner": "Grace Hopper", "service": "deployments", "keys_declared": true, "has_service_label": true, "single_key_before_id": true}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "keys_declared": true, "has_service_label": true, "single_key_before_id": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_quantifiers_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                any(key IN CASE WHEN person IS NULL THEN ['fallback'] ELSE keys(person) END WHERE key = 'team') AS case_has_team_key, \
                any(key IN coalesce(keys(person), ['fallback']) WHERE key = 'fallback') AS coalesced_has_fallback, \
                all(key IN coalesce(keys(person), ['fallback']) WHERE key <> 'deprecated') AS coalesced_all_declared \
         ORDER BY service",
    )
    .await
    .expect("static list CASE/coalesce collection predicates should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_has_team_key": true, "coalesced_has_fallback": false, "coalesced_all_declared": true}),
            json!({"service": "deployments", "case_has_team_key": true, "coalesced_has_fallback": false, "coalesced_all_declared": true}),
            json!({"service": "experiments", "case_has_team_key": true, "coalesced_has_fallback": false, "coalesced_all_declared": true}),
            json!({"service": "legacy-sync", "case_has_team_key": false, "coalesced_has_fallback": true, "coalesced_all_declared": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_static_list_case_and_coalesce_slice_quantifiers_execute_against_synthetic_sources()
{
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         OPTIONAL MATCH (person:Person)-[:OWNS]->(service) \
         RETURN service.name AS service, \
                any(key IN (CASE WHEN person IS NULL THEN ['fallback', 'owner'] ELSE keys(person) END)[1..2] WHERE key = 'team') AS case_slice_has_team, \
                all(key IN coalesce(keys(person), ['fallback', 'owner'])[0..1] WHERE key <> 'deprecated') AS coalesced_slice_all_declared, \
                none(key IN (CASE WHEN service.tier = 'prod' THEN [] ELSE ['not-prod'] END)[0..1] WHERE key = 'prod') AS tier_slice_none_prod, \
                single(key IN coalesce(keys(person), ['fallback', 'owner'])[0..1] WHERE key STARTS WITH 'f') AS coalesced_slice_single_fallback \
         ORDER BY service",
    )
    .await
    .expect("sliced static list CASE/coalesce collection predicates should execute");

    assert!(
        execution.translated_sql().contains("CASE WHEN"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"service": "billing-api", "case_slice_has_team": true, "coalesced_slice_all_declared": true, "tier_slice_none_prod": true, "coalesced_slice_single_fallback": false}),
            json!({"service": "deployments", "case_slice_has_team": true, "coalesced_slice_all_declared": true, "tier_slice_none_prod": true, "coalesced_slice_single_fallback": false}),
            json!({"service": "experiments", "case_slice_has_team": true, "coalesced_slice_all_declared": true, "tier_slice_none_prod": true, "coalesced_slice_single_fallback": false}),
            json!({"service": "legacy-sync", "case_slice_has_team": false, "coalesced_slice_all_declared": true, "tier_slice_none_prod": true, "coalesced_slice_single_fallback": true}),
        ]
    );
}

#[tokio::test]
async fn cypher_metadata_list_sizes_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE size(labels(service)) = 1 \
           AND size(keys(owns)) = 2 \
         RETURN person.name AS owner, \
                service.name AS service, \
                size(labels(service)) AS service_label_count, \
                size(keys(service)) AS service_key_count \
         ORDER BY size(keys(service)), owner, service",
    )
    .await
    .expect("metadata list sizes should execute");

    assert!(
        execution
            .translated_sql()
            .contains("1 AS \"service_label_count\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("6 AS \"service_key_count\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "service_label_count": 1, "service_key_count": 6}),
            json!({"owner": "Grace Hopper", "service": "deployments", "service_label_count": 1, "service_key_count": 6}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "service_label_count": 1, "service_key_count": 6}),
        ]
    );
}

#[tokio::test]
async fn cypher_node_label_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service:Service AND NOT (service:Team) \
         RETURN count(service) AS services",
    )
    .await
    .expect("node label predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("WHERE (TRUE AND NOT (FALSE))"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"services": 4})]
    );
}

#[tokio::test]
async fn cypher_relationship_type_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE owns:OWNS AND NOT (owns:DEPENDS_ON) \
         RETURN count(owns) AS ownerships",
    )
    .await
    .expect("relationship type predicates should execute");

    assert!(
        execution
            .translated_sql()
            .contains("WHERE (TRUE AND NOT (FALSE))"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"ownerships": 3})]
    );
}

#[tokio::test]
async fn cypher_order_by_id_functions_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         RETURN person.name AS owner, service.name AS service \
         ORDER BY id(owns) DESC \
         LIMIT 2",
    )
    .await
    .expect("ORDER BY id() should execute");

    assert!(
        execution
            .translated_sql()
            .contains("ORDER BY \"r0\".\"ownership_id\" DESC"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Katherine Johnson", "service": "experiments"}),
            json!({"owner": "Grace Hopper", "service": "deployments"}),
        ]
    );
}

#[tokio::test]
async fn cypher_id_and_type_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE id(person) = 2 \
           AND id(owns) IN [100, 200] \
           AND type(owns) = 'OWNS' \
           AND type(owns) STARTS WITH 'OW' \
           AND type(owns) ENDS WITH 'NS' \
           AND type(owns) CONTAINS 'WN' \
           AND type(owns) =~ '^OW.*' \
         RETURN person.name AS owner, service.name AS service",
    )
    .await
    .expect("id() and type() predicates should execute");

    assert!(
        execution.translated_sql().contains("\"n0\".\"id\" = 2"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"ownership_id\" IN (100, 200)"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"owner": "Grace Hopper", "service": "deployments"})]
    );
}

#[tokio::test]
async fn cypher_element_id_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (person:Person)-[owns:OWNS]->(service:Service) \
         WHERE elementId(person) IN ['1', '2'] AND elementId(owns) STARTS WITH '1' \
         RETURN elementId(person) AS person_element_id, elementId(owns) AS ownership_element_id, service.name AS service \
         ORDER BY elementId(owns)",
    )
    .await
    .expect("elementId() query should execute");

    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n0\".\"id\" AS VARCHAR) IN ('1', '2')"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"r0\".\"ownership_id\" AS VARCHAR) LIKE '1%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({
            "person_element_id": "1",
            "ownership_element_id": "100",
            "service": "billing-api"
        })]
    );
}

#[tokio::test]
async fn cypher_count_keyless_relationship_variables_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         RETURN count(dependency) AS dependencies",
    )
    .await
    .expect("counting a keyless relationship variable should execute");

    assert!(
        execution
            .translated_sql()
            .contains("COUNT(\"r0\".\"from_service_id\") AS \"dependencies\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"dependencies": 3})]
    );
}

#[tokio::test]
async fn cypher_element_id_rejects_keyless_relationships_before_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         RETURN elementId(dependency) AS dependency_element_id",
    )
    .await
    .expect_err("elementId() on a keyless relationship should fail");

    assert!(
        error.to_string().contains("INVALID_ELEMENT_ID_PROJECTION"),
        "unexpected error: {error}"
    );
}

#[tokio::test]
async fn cypher_keyless_relationship_presence_predicates_execute_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         WHERE dependency IS NOT NULL \
         RETURN count(dependency) AS dependencies",
    )
    .await
    .expect("keyless relationship presence predicate should execute");

    assert!(
        execution
            .translated_sql()
            .contains("\"r0\".\"from_service_id\" IS NOT NULL"),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![json!({"dependencies": 3})]
    );
}

#[tokio::test]
async fn cypher_grouped_count_property_projection_executes_against_synthetic_sources() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let execution = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE service.tier IS NOT NULL \
         RETURN service.tier AS tier, count(service.name) AS named_services \
         ORDER BY named_services DESC, tier",
    )
    .await
    .expect("grouped count property Cypher query should execute");

    assert!(
        execution.translated_sql().contains(" GROUP BY "),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"tier": "prod", "named_services": 2}),
            json!({"tier": "dev", "named_services": 1}),
        ]
    );
}

#[tokio::test]
async fn explain_graph_plan_preserves_translated_sql_and_datafusion_plan() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");
    let plan = owner_service_plan();

    let graph_plan = CoralQuery::explain_graph_plan(&[source], test_runtime(), &graph, &plan)
        .await
        .expect("graph plan should explain");

    assert!(
        graph_plan
            .translated_sql()
            .contains("JOIN \"ops\".\"ownerships\""),
        "{}",
        graph_plan.translated_sql()
    );
    assert!(
        graph_plan.plan().optimized_logical_plan().contains("ops"),
        "{}",
        graph_plan.plan().optimized_logical_plan()
    );
}

fn owner_service_plan() -> GraphPlan {
    GraphPlan {
        nodes: vec![
            NodePattern {
                variable: "person".to_string(),
                label: "Person".to_string(),
            },
            NodePattern {
                variable: "service".to_string(),
                label: "Service".to_string(),
            },
        ],
        relationships: vec![RelationshipPattern {
            variable: None,
            relationship_type: "OWNS".to_string(),
            left: "person".to_string(),
            direction: GraphDirection::Outgoing,
            right: "service".to_string(),
        }],
        optional_relationships: Vec::new(),
        optional_matches: Vec::new(),
        distinct: false,
        projections: vec![
            GraphProjection::Property {
                property: GraphPropertyRef {
                    variable: "person".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("owner".to_string()),
            },
            GraphProjection::Property {
                property: GraphPropertyRef {
                    variable: "service".to_string(),
                    property: "name".to_string(),
                },
                alias: Some("service".to_string()),
            },
        ],
        predicates: vec![GraphPropertyPredicate {
            property: GraphPropertyRef {
                variable: "service".to_string(),
                property: "tier".to_string(),
            },
            operator: ComparisonOperator::Equal,
            rhs: GraphPredicateRhs::Literal(GraphLiteral::String("prod".to_string())),
        }],
        predicate: None,
        post_projection_predicate: None,
        order_by: vec![GraphOrderKey {
            expression: GraphOrderExpression::Property(GraphPropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            }),
            direction: GraphOrderDirection::Ascending,
            nulls: None,
        }],
        skip: None,
        limit: Some(25),
    }
}

fn write_staged_planning_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "staged_people.jsonl",
        &[
            json!({"id": 1, "full_name": "Alice", "age": 30}),
            json!({"id": 2, "full_name": "Bob", "age": 25}),
            json!({"id": 3, "full_name": "Carol", "age": 35}),
            json!({"id": 4, "full_name": "Dana", "age": 40}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_services.jsonl",
        &[
            json!({"id": 10, "service_name": "billing-api"}),
            json!({"id": 20, "service_name": "deployments"}),
            json!({"id": 30, "service_name": "experiments"}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_knows.jsonl",
        &[
            json!({"person_id": 1, "friend_id": 2}),
            json!({"person_id": 1, "friend_id": 3}),
            json!({"person_id": 2, "friend_id": 3}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_likes.jsonl",
        &[
            json!({"person_id": 1, "liked_person_id": 2}),
            json!({"person_id": 2, "liked_person_id": 3}),
            json!({"person_id": 3, "liked_person_id": 1}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_ownerships.jsonl",
        &[
            json!({"person_id": 1, "service_id": 10}),
            json!({"person_id": 2, "service_id": 20}),
            json!({"person_id": 3, "service_id": 30}),
        ],
    );
}

fn staged_planning_manifest(dir: &Path) -> Value {
    json!({
        "name": "staged",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "people",
                "description": "Synthetic staged people fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_people.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "full_name", "type": "Utf8" },
                    { "name": "age", "type": "Int64" }
                ]
            },
            {
                "name": "services",
                "description": "Synthetic staged services fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_services.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "service_name", "type": "Utf8" }
                ]
            },
            {
                "name": "knows",
                "description": "Synthetic staged KNOWS edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_knows.jsonl" },
                "columns": [
                    { "name": "person_id", "type": "Int64" },
                    { "name": "friend_id", "type": "Int64" }
                ]
            },
            {
                "name": "likes",
                "description": "Synthetic staged LIKES edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_likes.jsonl" },
                "columns": [
                    { "name": "person_id", "type": "Int64" },
                    { "name": "liked_person_id", "type": "Int64" }
                ]
            },
            {
                "name": "ownerships",
                "description": "Synthetic staged ownership edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "staged_ownerships.jsonl" },
                "columns": [
                    { "name": "person_id", "type": "Int64" },
                    { "name": "service_id", "type": "Int64" }
                ]
            }
        ]
    })
}

fn staged_planning_test_graph() -> GraphDeclaration {
    GraphDeclaration::from_yaml(STAGED_PLANNING_GRAPH).expect("staged graph should parse")
}

fn write_staged_planning_keyed_fixture(dir: &Path) {
    write_staged_planning_fixture(dir);
    write_jsonl_file(
        dir,
        "staged_knows.jsonl",
        &[
            json!({"id": 100, "person_id": 1, "friend_id": 2}),
            json!({"id": 101, "person_id": 1, "friend_id": 3}),
            json!({"id": 102, "person_id": 2, "friend_id": 3}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_likes.jsonl",
        &[
            json!({"id": 200, "person_id": 1, "liked_person_id": 2}),
            json!({"id": 201, "person_id": 3, "liked_person_id": 1}),
        ],
    );
    write_jsonl_file(
        dir,
        "staged_ownerships.jsonl",
        &[
            json!({"id": 300, "person_id": 1, "service_id": 10}),
            json!({"id": 301, "person_id": 2, "service_id": 20}),
            json!({"id": 302, "person_id": 3, "service_id": 30}),
        ],
    );
}

fn staged_planning_keyed_manifest(dir: &Path) -> Value {
    let mut manifest = staged_planning_manifest(dir);
    let tables = manifest
        .get_mut("tables")
        .and_then(Value::as_array_mut)
        .expect("staged manifest should contain tables");
    for table_name in ["knows", "likes", "ownerships"] {
        let table = tables
            .iter_mut()
            .find(|table| table.get("name").and_then(Value::as_str) == Some(table_name))
            .expect("keyed relationship table should exist");
        let columns = table
            .get_mut("columns")
            .and_then(Value::as_array_mut)
            .expect("keyed relationship table should contain columns");
        columns.insert(0, json!({"name": "id", "type": "Int64"}));
    }
    manifest
}

fn staged_planning_keyed_test_graph() -> GraphDeclaration {
    GraphDeclaration::from_yaml(STAGED_PLANNING_KEYED_GRAPH)
        .expect("keyed staged graph should parse")
}

fn write_ops_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "people.jsonl",
        &[
            json!({"id": 1, "full_name": "Ada Lovelace", "team": "platform"}),
            json!({"id": 2, "full_name": "Grace Hopper", "team": "infra"}),
            json!({"id": 3, "full_name": "Katherine Johnson", "team": "analytics"}),
        ],
    );
    write_jsonl_file(
        dir,
        "services.jsonl",
        &[
            json!({"id": 10, "service_name": "billing-api", "tier": "prod", "owning_team": "platform", "risk_score": 0.9, "active": true}),
            json!({"id": 20, "service_name": "deployments", "tier": "prod", "owning_team": "infra", "risk_score": 0.5, "active": true}),
            json!({"id": 30, "service_name": "experiments", "tier": "dev", "owning_team": "analytics", "risk_score": 0.25, "active": false}),
            json!({"id": 40, "service_name": "legacy-sync", "tier": null, "owning_team": "platform", "risk_score": 0.95, "active": false}),
        ],
    );
    write_jsonl_file(
        dir,
        "teams.jsonl",
        &[
            json!({"id": 1000, "team_name": "platform", "cost_center": "cc-platform"}),
            json!({"id": 2000, "team_name": "infra", "cost_center": "cc-infra"}),
            json!({"id": 3000, "team_name": "analytics", "cost_center": "cc-analytics"}),
        ],
    );
    write_jsonl_file(
        dir,
        "ownerships.jsonl",
        &[
            json!({"ownership_id": 100, "person_id": 1, "service_id": 10, "since": "2024-01-10"}),
            json!({"ownership_id": 200, "person_id": 2, "service_id": 20, "since": "2024-02-20", "source": "pagerduty"}),
            json!({"ownership_id": 300, "person_id": 3, "service_id": 30, "since": "2024-03-15", "source": "catalog"}),
        ],
    );
    write_jsonl_file(
        dir,
        "team_ownerships.jsonl",
        &[
            json!({"team_id": 1000, "service_id": 10, "source": "catalog"}),
            json!({"team_id": 2000, "service_id": 20, "source": "catalog"}),
            json!({"team_id": 3000, "service_id": 30, "source": "catalog"}),
            json!({"team_id": 1000, "service_id": 40, "source": "catalog"}),
        ],
    );
    write_jsonl_file(
        dir,
        "service_dependencies.jsonl",
        &[
            json!({"from_service_id": 10, "to_service_id": 20, "criticality": "runtime", "source": "catalog"}),
            json!({"from_service_id": 20, "to_service_id": 30, "criticality": "dev", "source": "deploy"}),
            json!({"from_service_id": 10, "to_service_id": 30, "criticality": "optional", "source": "catalog"}),
        ],
    );
}

fn ops_manifest(dir: &Path) -> Value {
    json!({
        "name": "ops",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "people",
                "description": "Synthetic people fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "people.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "full_name", "type": "Utf8" },
                    { "name": "team", "type": "Utf8" }
                ]
            },
            {
                "name": "services",
                "description": "Synthetic services fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "services.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "service_name", "type": "Utf8" },
                    { "name": "tier", "type": "Utf8" },
                    { "name": "owning_team", "type": "Utf8" },
                    { "name": "risk_score", "type": "Float64" },
                    { "name": "active", "type": "Boolean" }
                ]
            },
            {
                "name": "teams",
                "description": "Synthetic teams fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "teams.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "team_name", "type": "Utf8" },
                    { "name": "cost_center", "type": "Utf8" }
                ]
            },
            {
                "name": "ownerships",
                "description": "Synthetic ownership edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "ownerships.jsonl" },
                "columns": [
                    { "name": "ownership_id", "type": "Int64" },
                    { "name": "person_id", "type": "Int64" },
                    { "name": "service_id", "type": "Int64" },
                    { "name": "since", "type": "Utf8" },
                    { "name": "source", "type": "Utf8" }
                ]
            },
            {
                "name": "team_ownerships",
                "description": "Synthetic team ownership edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "team_ownerships.jsonl" },
                "columns": [
                    { "name": "team_id", "type": "Int64" },
                    { "name": "service_id", "type": "Int64" },
                    { "name": "source", "type": "Utf8" }
                ]
            },
            {
                "name": "service_dependencies",
                "description": "Synthetic service dependency edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "service_dependencies.jsonl" },
                "columns": [
                    { "name": "from_service_id", "type": "Int64" },
                    { "name": "to_service_id", "type": "Int64" },
                    { "name": "criticality", "type": "Utf8" },
                    { "name": "source", "type": "Utf8" }
                ]
            }
        ]
    })
}

fn write_route_fixture(dir: &Path) {
    write_jsonl_file(
        dir,
        "route_people.jsonl",
        &[
            json!({"id": 1, "full_name": "Ada Lovelace"}),
            json!({"id": 2, "full_name": "Grace Hopper"}),
        ],
    );
    write_jsonl_file(
        dir,
        "route_services.jsonl",
        &[
            json!({"id": 10, "service_name": "billing-api"}),
            json!({"id": 20, "service_name": "deployments"}),
        ],
    );
    write_jsonl_file(
        dir,
        "route_incidents.jsonl",
        &[
            json!({"id": 100, "title": "billing latency"}),
            json!({"id": 200, "title": "deploy failed"}),
        ],
    );
    write_jsonl_file(
        dir,
        "person_service_routes.jsonl",
        &[
            json!({"person_id": 1, "service_id": 10}),
            json!({"person_id": 2, "service_id": 20}),
        ],
    );
    write_jsonl_file(
        dir,
        "service_incident_routes.jsonl",
        &[
            json!({"service_id": 10, "incident_id": 100}),
            json!({"service_id": 20, "incident_id": 200}),
        ],
    );
}

fn route_manifest(dir: &Path) -> Value {
    json!({
        "name": "ops",
        "version": "0.1.0",
        "dsl_version": 3,
        "backend": "file",
        "tables": [
            {
                "name": "route_people",
                "description": "Synthetic route people fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "route_people.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "full_name", "type": "Utf8" }
                ]
            },
            {
                "name": "route_services",
                "description": "Synthetic route services fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "route_services.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "service_name", "type": "Utf8" }
                ]
            },
            {
                "name": "route_incidents",
                "description": "Synthetic route incidents fixture",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "route_incidents.jsonl" },
                "columns": [
                    { "name": "id", "type": "Int64" },
                    { "name": "title", "type": "Utf8" }
                ]
            },
            {
                "name": "person_service_routes",
                "description": "Synthetic person-to-service route edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "person_service_routes.jsonl" },
                "columns": [
                    { "name": "person_id", "type": "Int64" },
                    { "name": "service_id", "type": "Int64" }
                ]
            },
            {
                "name": "service_incident_routes",
                "description": "Synthetic service-to-incident route edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "service_incident_routes.jsonl" },
                "columns": [
                    { "name": "service_id", "type": "Int64" },
                    { "name": "incident_id", "type": "Int64" }
                ]
            }
        ]
    })
}

const OPS_GRAPH: &str = r"
version: 1
name: ops
description: Synthetic operations ownership graph
nodes:
  - label: Person
    table: { schema: ops, name: people }
    key: id
    properties:
      name: full_name
      team: team
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      id: id
      name: service_name
      tier: tier
      team: owning_team
      risk: risk_score
      active: active
  - label: Team
    table: { schema: ops, name: teams }
    key: id
    properties:
      name: team_name
      cost_center: cost_center
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
      source: source
  - type: OWNS
    table: { schema: ops, name: team_ownerships }
    from: { label: Team, key: team_id }
    to: { label: Service, key: service_id }
    properties:
      source: source
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      criticality: criticality
      source: source
";

const STAGED_PLANNING_GRAPH: &str = r"
version: 1
name: staged_planning
description: Synthetic staged planning graph
nodes:
  - label: Person
    table: { schema: staged, name: people }
    key: id
    properties:
      name: full_name
      age: age
  - label: Service
    table: { schema: staged, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: KNOWS
    table: { schema: staged, name: knows }
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
  - type: LIKES
    table: { schema: staged, name: likes }
    from: { label: Person, key: person_id }
    to: { label: Person, key: liked_person_id }
  - type: OWNS
    table: { schema: staged, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
";

const STAGED_PLANNING_KEYED_GRAPH: &str = r"
version: 1
name: staged_planning
description: Synthetic keyed staged planning graph
nodes:
  - label: Person
    table: { schema: staged, name: people }
    key: id
    properties:
      name: full_name
      age: age
  - label: Service
    table: { schema: staged, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: KNOWS
    table: { schema: staged, name: knows }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: friend_id }
  - type: LIKES
    table: { schema: staged, name: likes }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Person, key: liked_person_id }
  - type: OWNS
    table: { schema: staged, name: ownerships }
    key: id
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
";

const ROUTE_GRAPH: &str = r"
version: 1
name: routes
description: Synthetic cross-label route graph
nodes:
  - label: Person
    table: { schema: ops, name: route_people }
    key: id
    properties:
      name: full_name
  - label: Service
    table: { schema: ops, name: route_services }
    key: id
    properties:
      name: service_name
  - label: Incident
    table: { schema: ops, name: route_incidents }
    key: id
    properties:
      title: title
relationships:
  - type: ROUTES
    table: { schema: ops, name: person_service_routes }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
  - type: ROUTES
    table: { schema: ops, name: service_incident_routes }
    from: { label: Service, key: service_id }
    to: { label: Incident, key: incident_id }
";

const SERVICE_TYPE_ALTERNATIVE_GRAPH: &str = r"
version: 1
name: service-type-alternatives
description: Synthetic graph for same-endpoint relationship type alternatives
nodes:
  - label: Service
    table: { schema: ops, name: services }
    key: id
    properties:
      name: service_name
relationships:
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      criticality: criticality
      source: source
  - type: ALERTS
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      source: source
";
