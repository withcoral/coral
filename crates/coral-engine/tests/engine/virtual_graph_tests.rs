use std::path::Path;

use coral_engine::{
    ComparisonOperator, CoralQuery, GraphDeclaration, GraphDirection, GraphLiteral,
    GraphOrderDirection, GraphOrderExpression, GraphOrderKey, GraphPlan, GraphPredicateRhs,
    GraphProjection, GraphPropertyPredicate, GraphPropertyRef, NodePattern, RelationshipPattern,
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
async fn cypher_disconnected_comma_patterns_are_rejected_before_sql_planning() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[:DEPENDS_ON]->(target:Service), (orphan:Person) \
         RETURN source.name AS source, target.name AS target",
    )
    .await
    .expect_err("disconnected comma-separated patterns should fail validation");

    assert!(
        error.to_string().contains("DISCONNECTED_PATTERN"),
        "unexpected error: {error}"
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
         WHERE service.tier IN ['prod', 'dev'] \
         RETURN service.name AS service \
         ORDER BY service",
    )
    .await
    .expect("Cypher IN predicate query should execute");

    assert!(
        execution.translated_sql().contains(" IN "),
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
                max(service.risk) AS highest_risk \
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
    assert_eq!(
        execution_to_rows(execution.execution()),
        vec![
            json!({
                "tier": "prod",
                "total_risk": 1.4,
                "average_risk": 0.7,
                "lowest_risk": 0.5,
                "highest_risk": 0.9
            }),
            json!({
                "tier": "dev",
                "total_risk": 0.25,
                "average_risk": 0.25,
                "lowest_risk": 0.25,
                "highest_risk": 0.25
            }),
        ]
    );
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
            .contains("'OWNS' AS \"relationship_type\""),
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
async fn cypher_count_keyless_relationship_variables_are_rejected() {
    let temp = TempDir::new().expect("temp dir");
    write_ops_fixture(temp.path());
    let source = build_source(ops_manifest(temp.path()));
    let graph = GraphDeclaration::from_yaml(OPS_GRAPH).expect("graph should parse");

    let error = CoralQuery::execute_cypher(
        &[source],
        test_runtime(),
        &graph,
        "MATCH (source:Service)-[dependency:DEPENDS_ON]->(target:Service) \
         RETURN count(dependency) AS dependencies",
    )
    .await
    .expect_err("counting a keyless relationship variable should fail");

    assert!(
        error.to_string().contains("INVALID_AGGREGATE_TARGET"),
        "{error:?}"
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
        "ownerships.jsonl",
        &[
            json!({"ownership_id": 100, "person_id": 1, "service_id": 10, "since": "2024-01-10"}),
            json!({"ownership_id": 200, "person_id": 2, "service_id": 20, "since": "2024-02-20", "source": "pagerduty"}),
            json!({"ownership_id": 300, "person_id": 3, "service_id": 30, "since": "2024-03-15", "source": "catalog"}),
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
      id: id
      name: service_name
      tier: tier
      team: owning_team
      risk: risk_score
      active: active
relationships:
  - type: OWNS
    table: { schema: ops, name: ownerships }
    key: ownership_id
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
