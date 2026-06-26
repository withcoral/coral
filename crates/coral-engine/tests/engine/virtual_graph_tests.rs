use std::path::Path;

use coral_engine::{
    ComparisonOperator, CoralQuery, GraphDeclaration, GraphDirection, GraphLiteral,
    GraphOrderDirection, GraphOrderKey, GraphPlan, GraphPredicateRhs, GraphProjection,
    GraphPropertyPredicate, GraphPropertyRef, NodePattern, RelationshipPattern,
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
async fn cypher_skip_limit_executes_against_synthetic_sources() {
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
         ORDER BY service \
         SKIP 1 LIMIT 2",
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
        distinct: false,
        projections: vec![GraphProjection::Property {
            property: GraphPropertyRef {
                variable: "service".to_string(),
                property: "name".to_string(),
            },
            alias: Some("service".to_string()),
        }],
        predicates: Vec::new(),
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
        distinct: false,
        projections: vec![GraphProjection::CountAll {
            alias: "ownership_count".to_string(),
        }],
        predicates: Vec::new(),
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
        order_by: vec![GraphOrderKey {
            property: GraphPropertyRef {
                variable: "person".to_string(),
                property: "name".to_string(),
            },
            direction: GraphOrderDirection::Ascending,
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
            json!({"id": 10, "service_name": "billing-api", "tier": "prod", "owning_team": "platform"}),
            json!({"id": 20, "service_name": "deployments", "tier": "prod", "owning_team": "infra"}),
            json!({"id": 30, "service_name": "experiments", "tier": "dev", "owning_team": "analytics"}),
            json!({"id": 40, "service_name": "legacy-sync", "tier": null, "owning_team": "platform"}),
        ],
    );
    write_jsonl_file(
        dir,
        "ownerships.jsonl",
        &[
            json!({"person_id": 1, "service_id": 10, "since": "2024-01-10"}),
            json!({"person_id": 2, "service_id": 20, "since": "2024-02-20", "source": "pagerduty"}),
            json!({"person_id": 3, "service_id": 30, "since": "2024-03-15", "source": "catalog"}),
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
                    { "name": "owning_team", "type": "Utf8" }
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
                    { "name": "since", "type": "Utf8" },
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
      team: owning_team
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    from: { label: Person, key: person_id }
    to: { label: Service, key: service_id }
    properties:
      since: since
      source: source
  - type: DEPENDS_ON
    table: { schema: ops, name: service_dependencies }
    from: { label: Service, key: from_service_id }
    to: { label: Service, key: to_service_id }
    properties:
      criticality: criticality
      source: source
";
