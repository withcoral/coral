use std::path::Path;

use coral_engine::{
    ComparisonOperator, CoralQuery, GraphDeclaration, GraphDirection, GraphLiteral,
    GraphOrderDirection, GraphOrderKey, GraphPlan, GraphProjection, GraphPropertyPredicate,
    GraphPropertyRef, NodePattern, RelationshipPattern,
};
use serde_json::{Value, json};
use tempfile::TempDir;

use crate::harness::{build_source, dir_url, execution_to_rows, test_runtime, write_jsonl_file};

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
        projections: vec![GraphProjection::CountAll {
            alias: "ownership_count".to_string(),
        }],
        predicates: Vec::new(),
        order_by: Vec::new(),
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
            literal: GraphLiteral::String("prod".to_string()),
        }],
        order_by: vec![GraphOrderKey {
            property: GraphPropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            direction: GraphOrderDirection::Ascending,
        }],
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
            json!({"id": 10, "service_name": "billing-api", "tier": "prod"}),
            json!({"id": 20, "service_name": "deployments", "tier": "prod"}),
            json!({"id": 30, "service_name": "experiments", "tier": "dev"}),
        ],
    );
    write_jsonl_file(
        dir,
        "ownerships.jsonl",
        &[
            json!({"person_id": 1, "service_id": 10, "since": "2024-01-10"}),
            json!({"person_id": 2, "service_id": 20, "since": "2024-02-20"}),
            json!({"person_id": 3, "service_id": 30, "since": "2024-03-15"}),
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
                    { "name": "tier", "type": "Utf8" }
                ]
            },
            {
                "name": "ownerships",
                "description": "Synthetic ownership edges",
                "format": "jsonl",
                "source": { "location": dir_url(dir), "glob": "ownerships.jsonl" },
                "columns": [
                    { "name": "person_id", "type": "Int64" },
                    { "name": "service_id", "type": "Int64" },
                    { "name": "since", "type": "Utf8" }
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
      name: service_name
      tier: tier
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
";
