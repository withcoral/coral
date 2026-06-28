use std::collections::BTreeMap;
use std::path::Path;

use coral_engine::{
    ComparisonOperator, CoralQuery, GraphCypherParameterValue, GraphDeclaration, GraphDirection,
    GraphGraphqlVariableValue, GraphLiteral, GraphOrderDirection, GraphOrderExpression,
    GraphOrderKey, GraphPlan, GraphPredicateRhs, GraphProjection, GraphPropertyPredicate,
    GraphPropertyRef, NodePattern, RelationshipPattern,
};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

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
            .contains("ARRAY_AGG(DISTINCT \"__coral_agg_1\") AS \"services\""),
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
async fn cypher_exists_subqueries_execute_as_boolean_scalar_projections() {
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
            .contains("(SELECT COUNT(*) AS \"__coral_exists_count_0\" FROM"),
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
async fn cypher_rejects_multiple_correlated_scalar_subqueries_in_one_projection() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN CASE \
                  WHEN EXISTS { MATCH (service)-[:DEPENDS_ON {criticality: 'runtime'}]->(:Service) } THEN 'runtime-dependency' \
                  WHEN EXISTS { MATCH (service)-[:DEPENDS_ON]->(:Service) } THEN 'non-runtime-dependency' \
                  ELSE 'isolated' \
                END AS dependency_state",
    )
    .await
    .expect_err("multiple correlated scalar subqueries should be rejected before SQL execution");

    assert!(
        error.to_string().contains("at most one correlated"),
        "{error}"
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
async fn cypher_exists_match_subqueries_reject_non_conjunctive_inner_where() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { \
           MATCH (service)-[:DEPENDS_ON]->(target:Service) \
           WHERE target.tier = 'dev' OR target.tier = 'prod' \
         } \
         RETURN service.name AS service",
    )
    .await
    .expect_err("non-conjunctive EXISTS MATCH WHERE should be rejected");

    assert!(
        error
            .to_string()
            .contains("WHERE clauses with property comparisons joined by AND"),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_exists_pattern_where_rejects_non_conjunctive_predicates() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         WHERE EXISTS { (service)-[:DEPENDS_ON]->(target:Service) WHERE target.tier = 'dev' OR target.tier = 'prod' } \
         RETURN service.name AS service",
    )
    .await
    .expect_err("non-conjunctive compact EXISTS pattern WHERE should be rejected");

    assert!(
        error
            .to_string()
            .contains("WHERE clauses with property comparisons joined by AND"),
        "{error}"
    );
}

#[tokio::test]
async fn cypher_count_subqueries_execute_as_correlated_scalar_counts() {
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
            .contains("SELECT COUNT(*) FROM \"ops\".\"service_dependencies\""),
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
         WHERE COUNT { MATCH (service:Service) WHERE service.tier = 'prod' } = 2 \
         RETURN team.name AS team, \
                COUNT { MATCH (service:Service) WHERE service.tier = 'dev' } AS dev_services \
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
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"team": "analytics", "dev_services": 1}),
            json!({"team": "infra", "dev_services": 1}),
            json!({"team": "platform", "dev_services": 1}),
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
        execution
            .translated_sql()
            .contains("\"__coral_count_n0\".\"owning_team\" = \"n0\".\"team_name\""),
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
            .contains("MEDIAN(\"n0\".\"risk_score\") AS \"medianRisk\""),
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
            .contains("CAST(\"n0\".\"risk_score\" AS VARCHAR) LIKE '0.9%' ESCAPE '\\'"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n0\".\"risk_score\" AS VARCHAR) AS \"risk_text\""),
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
            .contains("CAST(\"n0\".\"id\" AS BIGINT) = 10"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n0\".\"risk_score\" AS DOUBLE) AS \"risk_float\""),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution
            .translated_sql()
            .contains("CAST(\"n0\".\"active\" AS BOOLEAN) AS \"active_bool\""),
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
                service.risk ^ 2 AS risk_squared \
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
            .contains("CAST((\"n0\".\"id\" / 10) AS BIGINT) AS \"id_bucket\""),
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
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "service": "deployments",
                "double_id": 40,
                "id_bucket": 2,
                "id_mod": 0,
                "risk_squared": 0.25,
            }),
            json!({
                "service": "experiments",
                "double_id": 60,
                "id_bucket": 3,
                "id_mod": 10,
                "risk_squared": 0.0625,
            }),
            json!({
                "service": "legacy-sync",
                "double_id": 80,
                "id_bucket": 4,
                "id_mod": 0,
                "risk_squared": 0.9025,
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
            .contains("abs((\"n0\".\"risk_score\" - 1)) < 0.11"),
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
        execution.translated_sql().contains("ln(1) AS \"ln_one\""),
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
        execution.translated_sql().contains("cot(1) > 0"),
        "{}",
        execution.translated_sql()
    );
    assert!(
        execution.translated_sql().contains("atan2(0, 1)"),
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
            .contains("((1 - cos(0)) / 2) AS \"zero_haversin\""),
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
                sum(service.risk + 1) AS adjusted_risk",
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

    let mut rows = execution_to_rows(execution.execution());
    let row = rows
        .get_mut(0)
        .expect("aggregate expression target query should return one row");
    sort_string_array_field(row, "tiers");
    assert_eq!(row["tiers"], json!(["dev", "prod", "prod", "unknown"]));
    assert_eq!(row["tier_count"], json!(4));
    assert_close(row["adjusted_risk"].as_f64().unwrap(), 6.6);
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
    assert_eq!(graph_rows, sql_rows);

    let row = graph_rows
        .first()
        .expect("aggregate query should return one row");
    assert_close(row["sample_risk"].as_f64().unwrap(), 0.282_842_712_474_619);
    assert_close(row["population_risk"].as_f64().unwrap(), 0.2);
    assert_close(row["distinct_total_risk"].as_f64().unwrap(), 1.4);
    assert_close(row["distinct_average_risk"].as_f64().unwrap(), 0.7);
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
async fn cypher_distinct_standard_deviation_rejects_before_execution() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (service:Service) \
         RETURN stDevP(DISTINCT service.risk) AS population_risk",
    )
    .await
    .expect_err("distinct standard deviation should fail before execution");

    assert!(
        error.to_string().contains("UNSUPPORTED_CYPHER"),
        "{error:?}"
    );
    assert!(
        error.to_string().contains("stDevP(DISTINCT property)"),
        "{error:?}"
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
            .contains("MEDIAN(\"n0\".\"risk_score\") AS \"median_risk\""),
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
            json!({"owner": "Ada Lovelace", "service": "billing-api", "incremented": [2, 3, 4], "doubled": [3, 5], "halved_weights": [1, 2, null], "absolute_ints": [2, 0, 3], "absolute_floats": [1.5, null, 2.5], "roots": [2, 3], "signs": [-1, 0, 1, null], "ceilings": [2, 3, null], "floors": [1, 2, null], "rounded_tenths": [1.2, 1.3, 1.3], "rounded_wholes": [1, 2, 2], "t_flags": [false, false, false, false, true, true], "empty_flags": [true, false, null]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "incremented": [2, 3, 4], "doubled": [3, 5], "halved_weights": [1, 2, null], "absolute_ints": [2, 0, 3], "absolute_floats": [1.5, null, 2.5], "roots": [2, 3], "signs": [-1, 0, 1, null], "ceilings": [2, 3, null], "floors": [1, 2, null], "rounded_tenths": [1.2, 1.3, 1.3], "rounded_wholes": [1, 2, 2], "t_flags": [false, false, false, false, true, true], "empty_flags": [true, false, null]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "incremented": [2, 3, 4], "doubled": [3, 5], "halved_weights": [1, 2, null], "absolute_ints": [2, 0, 3], "absolute_floats": [1.5, null, 2.5], "roots": [2, 3], "signs": [-1, 0, 1, null], "ceilings": [2, 3, null], "floors": [1, 2, null], "rounded_tenths": [1.2, 1.3, 1.3], "rounded_wholes": [1, 2, 2], "t_flags": [false, false, false, false, true, true], "empty_flags": [true, false, null]}),
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
                [x IN ['1', '2', null] | toInteger(x)] AS ints, \
                [x IN ['1.5', '2.25', null] | toFloat(x)] AS floats, \
                [x IN ['true', 'FALSE', null] | toBoolean(x)] AS booleans, \
                [x IN ['bad', '3', null] | toIntegerOrNull(x)] AS nullable_ints, \
                [x IN ['maybe', 'true', null] | toBooleanOrNull(x)] AS nullable_booleans \
         ORDER BY owner, service",
    )
    .await
    .expect("static list comprehension cast maps should execute");

    assert!(
        execution
            .translated_sql()
            .contains("make_array(1, 2, NULL) AS \"ints\""),
        "{}",
        execution.translated_sql()
    );
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({"owner": "Ada Lovelace", "service": "billing-api", "ints": [1, 2, null], "floats": [1.5, 2.25, null], "booleans": [true, false, null], "nullable_ints": [null, 3, null], "nullable_booleans": [null, true, null]}),
            json!({"owner": "Grace Hopper", "service": "deployments", "ints": [1, 2, null], "floats": [1.5, 2.25, null], "booleans": [true, false, null], "nullable_ints": [null, 3, null], "nullable_booleans": [null, true, null]}),
            json!({"owner": "Katherine Johnson", "service": "experiments", "ints": [1, 2, null], "floats": [1.5, 2.25, null], "booleans": [true, false, null], "nullable_ints": [null, 3, null], "nullable_booleans": [null, true, null]}),
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
